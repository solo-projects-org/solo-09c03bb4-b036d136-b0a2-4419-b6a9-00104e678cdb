"""Hand-off tests: failed capture / unconfirmed restoration / API restart.

These exercise the contract that a failed capture may be reported FAILED, but
until the instrument is *confirmed* back in its pre-acquisition RUN/STOP state
the row reports ``restored=false`` (restore pending), the instrument accepts no
new acquisition traffic, and recovery resumes automatically after reconnect or
an API restart -- without ever starting a previously paused instrument.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time

import aiosqlite
import pytest
import pytest_asyncio

from app import database as db
from app.config import InstrumentConfig, settings
from app.main import registry
from app.service import AcquisitionService
from instrument.scpi_sim import ScpiInstrument, build_default_state, serve_instrument


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _reset_registry(configs: tuple[InstrumentConfig, ...]) -> None:
    registry._configs = {c.name: c for c in configs}  # noqa: SLF001
    registry._locks = {name: asyncio.Lock() for name in registry._configs}  # noqa: SLF001
    registry._gates = {name: asyncio.Event() for name in registry._configs}  # noqa: SLF001
    for event in registry._gates.values():  # noqa: SLF001
        event.set()
    registry._queued = {name: 0 for name in registry._configs}  # noqa: SLF001
    registry._last_status = {}  # noqa: SLF001


class Lab:
    def __init__(self, tmp_path) -> None:
        self.db_path = str(tmp_path / "handoff.db")
        self.ports = {"inst1": _free_port(), "inst2": _free_port()}
        self.instruments = {
            name: ScpiInstrument(name, build_default_state(name)) for name in self.ports
        }
        self.servers: dict[str, asyncio.AbstractServer] = {}
        self.commands: dict[str, list[str]] = {name: [] for name in self.ports}
        self.connections: list[aiosqlite.Connection] = []
        self.services: list[AcquisitionService] = []
        self._started_instruments: set[str] = set()
        for name, inst in self.instruments.items():
            original = inst.submit

            def tracked(line, writer, _original=original, _name=name):
                self.commands[_name].append(line.strip().upper())
                _original(line, writer)

            inst.submit = tracked  # type: ignore[method-assign]

    def configs(self) -> tuple[InstrumentConfig, ...]:
        return tuple(
            InstrumentConfig(name, "127.0.0.1", port) for name, port in self.ports.items()
        )

    async def start_listener(self, name: str) -> None:
        """(Re)bind the TCP listener without starting a second queue worker."""
        from instrument.scpi_sim import handle_connection

        instrument = self.instruments[name]
        if name not in self._started_instruments:
            await instrument.start()
            self._started_instruments.add(name)

        async def handler(reader, writer):
            await handle_connection(reader, writer, instrument)

        self.servers[name] = await asyncio.start_server(
            handler, "127.0.0.1", self.ports[name]
        )

    def close_listener_now(self, name: str) -> None:
        server = self.servers.pop(name, None)
        if server is not None:
            server.close()

    async def stop_listener(self, name: str) -> None:
        server = self.servers.pop(name, None)
        if server is not None:
            server.close()
            await server.wait_closed()

    async def new_service(self) -> tuple[AcquisitionService, aiosqlite.Connection]:
        connection = await aiosqlite.connect(self.db_path)
        connection.row_factory = aiosqlite.Row
        await connection.executescript(db.SCHEMA)
        await connection.commit()
        service = AcquisitionService(connection, registry)
        self.connections.append(connection)
        self.services.append(service)
        return service, connection

    async def close(self) -> None:
        for service in self.services:
            await service.shutdown()
        for connection in self.connections:
            await connection.close()
        for server in list(self.servers.values()):
            server.close()
            await server.wait_closed()
        for inst in self.instruments.values():
            await inst.stop()


@pytest_asyncio.fixture
async def lab(two_simulators, tmp_path):
    """Fresh simulators/servers independent from the shared conftest pair."""
    test_lab = Lab(tmp_path)
    settings.db_path = test_lab.db_path
    settings.command_timeout = 2.0
    settings.restore_attempts = 1
    settings.restore_backoff = 0.05
    settings.recovery_retry_interval = 0.05
    settings.restore_settle = 0.0
    configs = test_lab.configs()
    settings.instruments = configs
    _reset_registry(configs)
    for name in test_lab.ports:
        await test_lab.start_listener(name)
    try:
        yield test_lab
    finally:
        await test_lab.close()


async def _wait_for(predicate, timeout: float = 10.0, interval: float = 0.02):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(interval)
    raise AssertionError("condition not met in time")


async def _wait_terminal(service: AcquisitionService, acquisition_id: int, timeout: float = 10.0):
    async def check():
        row = await service.get(acquisition_id)
        if row is not None and row["state"] in db.TERMINAL_STATES:
            return row
        return None

    return await _wait_for(check, timeout)


async def _run_state(host: str, port: int) -> str:
    from app.scpi import ScpiClient

    async with ScpiClient(InstrumentConfig("probe", host, port), timeout=1.0) as client:
        return await client.query_text(":RUN?")


@pytest.mark.asyncio
async def test_unconfirmed_restore_is_pending_and_blocks_only_that_instrument(lab):
    service, _ = await lab.new_service()
    inst1 = lab.instruments["inst1"]
    inst2 = lab.instruments["inst2"]

    # Close inst1's listener as soon as the pause command has executed.  The
    # already-open capture connection keeps working, so the capture succeeds,
    # but every restoration attempt now fails to connect.
    original_execute = type(inst1)._execute

    async def execute_then_go_offline(self, cmd):  # type: ignore[no-untyped-def]
        await original_execute(self, cmd)
        if (cmd.line or "").strip().upper() == ":STOP":
            lab.close_listener_now("inst1")

    type(inst1)._execute = execute_then_go_offline
    try:
        first_id = await service.create_and_spawn("inst1", 1)
        first = await _wait_terminal(service, first_id, timeout=15.0)
    finally:
        type(inst1)._execute = original_execute

    assert first["state"] == db.FAILED
    assert first["previous_state"] == "RUNNING"
    assert first["error_code"] == "RESTORE_FAILED"
    assert first["restored"] == 0          # restoration NOT confirmed
    assert first["raw_codes"] is None      # capture succeeded but is not served
    assert inst1.state.running is False    # device is still paused
    assert registry.gate_is_open("inst1") is False

    intent = await db.get_recovery(service._conn, "inst1")
    assert intent is not None and intent["pending"] == 1
    assert intent["target_state"] == "RUNNING"

    # A second acquisition on inst1 must queue without touching the device.
    commands_before = len(lab.commands["inst1"])
    second_id = await service.create_and_spawn("inst1", 1)
    await asyncio.sleep(0.2)
    second = await service.get(second_id)
    assert second["state"] == db.PENDING
    assert len(lab.commands["inst1"]) == commands_before  # no SCPI sent
    assert inst2.state.running is True

    # inst2 remains fully usable while inst1 is stuck pending.
    other_id = await service.create_and_spawn("inst2", 1)
    other = await _wait_terminal(service, other_id)
    assert other["state"] == db.SUCCESS
    assert other["restored"] == 1
    assert inst2.state.running is True

    # Instrument comes back: background recovery resumes automatically,
    # restores the recorded RUNNING target, flips the waiting rows and reopens
    # the gate; the queued acquisition then runs normally.
    await lab.start_listener("inst1")
    first_after = await _wait_for(
        lambda: _row_if(service, first_id, lambda row: row["restored"] == 1)
    )
    assert first_after["state"] == db.FAILED
    assert first_after["error_code"] == "RESTORE_FAILED"
    intent_after = await db.get_recovery(service._conn, "inst1")
    assert intent_after is not None and intent_after["pending"] == 0

    # The queued acquisition now runs; once it finishes the instrument must be
    # back in its original RUNNING state.
    second_done = await _wait_terminal(service, second_id, timeout=15.0)
    assert second_done["state"] == db.SUCCESS
    assert second_done["restored"] == 1
    assert second_done["previous_state"] == "RUNNING"
    assert inst1.state.running is True
    assert registry.gate_is_open("inst1") is True


async def _row_if(service, acquisition_id, predicate):
    row = await service.get(acquisition_id)
    if row is not None and predicate(row):
        return row
    return None


@pytest.mark.asyncio
async def test_previously_paused_target_is_preserved_across_offline_recovery(lab):
    service, _ = await lab.new_service()
    inst1 = lab.instruments["inst1"]
    inst1.state.running = False  # user had paused this instrument

    original_execute = type(inst1)._execute

    async def execute_then_go_offline(self, cmd):  # type: ignore[no-untyped-def]
        await original_execute(self, cmd)
        if (cmd.line or "").strip().upper() == ":STOP":
            lab.close_listener_now("inst1")

    type(inst1)._execute = execute_then_go_offline
    try:
        acquisition_id = await service.create_and_spawn("inst1", 1)
        row = await _wait_terminal(service, acquisition_id, timeout=15.0)
    finally:
        type(inst1)._execute = original_execute

    assert row["state"] == db.FAILED
    assert row["previous_state"] == "STOPPED"
    assert row["restored"] == 0
    assert inst1.state.running is False  # never started

    intent = await db.get_recovery(service._conn, "inst1")
    assert intent["target_state"] == "STOPPED"

    await lab.start_listener("inst1")
    restored = await _wait_for(
        lambda: _row_if(service, acquisition_id, lambda r: r["restored"] == 1)
    )
    assert restored["state"] == db.FAILED
    assert inst1.state.running is False   # still paused after confirmed restore
    assert registry.gate_is_open("inst1") is True


@pytest.mark.asyncio
async def test_startup_is_non_blocking_and_does_not_trust_leftover_pause(lab):
    # Simulate a crash while inst1 was paused for an acquisition whose original
    # state was RUNNING, and inst1 is now offline.  inst2 is healthy.
    await lab.stop_listener("inst1")
    lab.instruments["inst1"].state.running = False  # stale leftover pause
    connection = await aiosqlite.connect(lab.db_path)
    connection.row_factory = aiosqlite.Row
    await connection.executescript(db.SCHEMA)
    await connection.commit()
    interrupted = await db.create_acquisition(connection, "inst1", 1)
    await db.mark_in_progress(connection, interrupted, "RUNNING")
    await connection.close()

    service, _ = await lab.new_service()
    started = time.monotonic()
    await service.startup_recovery()
    elapsed = time.monotonic() - started
    assert elapsed < 1.0, f"startup blocked for {elapsed:.2f}s on an offline instrument"

    row = await service.get(interrupted)
    assert row["state"] == db.FAILED
    assert row["error_code"] == "API_RESTARTED"
    assert row["restored"] == 0              # NOT confirmed merely by restart
    assert row["previous_state"] == "RUNNING"
    assert registry.gate_is_open("inst1") is False

    intent = await db.get_recovery(service._conn, "inst1")
    assert intent["pending"] == 1
    assert intent["target_state"] == "RUNNING"  # leftover STOPPED is not used

    # API is usable and the healthy instrument works while inst1 is offline.
    other_id = await service.create_and_spawn("inst2", 1)
    other = await _wait_terminal(service, other_id)
    assert other["state"] == db.SUCCESS
    assert registry.gate_is_open("inst2") is True

    # Status/listing must answer without waiting for the offline instrument.
    listed = await asyncio.wait_for(service.list(None, 10), timeout=1.0)
    assert {row["id"] for row in listed} >= {interrupted, other_id}
    fetched = await asyncio.wait_for(service.get(interrupted), timeout=1.0)
    assert fetched["restored"] == 0

    # Reconnect: the original RUNNING target is resumed automatically.
    await lab.start_listener("inst1")
    confirmed = await _wait_for(
        lambda: _row_if(service, interrupted, lambda r: r["restored"] == 1)
    )
    assert confirmed["state"] == db.FAILED
    assert lab.instruments["inst1"].state.running is True
    assert registry.gate_is_open("inst1") is True


@pytest.mark.asyncio
async def test_api_restart_resumes_pending_recovery_intent(lab):
    service, _ = await lab.new_service()
    inst1 = lab.instruments["inst1"]
    await lab.stop_listener("inst1")

    # A previous process recorded a pending intent targeting RUNNING.
    await db.upsert_pending_recovery(
        service._conn,
        "inst1",
        "RUNNING",
        acquisition_id=42,
        error_code="RESTORE_FAILED",
        error_message="offline",
    )
    await service.shutdown()

    # Restart: a brand-new service picks up the durable intent and retries in
    # the background while the API is already usable.
    restarted, _ = await lab.new_service()
    started = time.monotonic()
    await restarted.startup_recovery()
    assert time.monotonic() - started < 1.0
    assert registry.gate_is_open("inst1") is False
    intent = await db.get_recovery(restarted._conn, "inst1")
    assert intent["pending"] == 1 and intent["target_state"] == "RUNNING"

    await lab.start_listener("inst1")
    await _wait_for(lambda: _intent_confirmed(restarted, "inst1"))
    assert inst1.state.running is True
    assert registry.gate_is_open("inst1") is True


async def _intent_confirmed(service, name: str) -> bool:
    intent = await db.get_recovery(service._conn, name)
    return intent is not None and intent["pending"] == 0


@pytest.mark.asyncio
async def test_recovery_dropout_keeps_target_and_retries_until_confirmed(lab):
    service, _ = await lab.new_service()
    inst1 = lab.instruments["inst1"]

    # Pending intent to restore RUNNING; the first recovery cycles drop the
    # connection (simulating the instrument exiting again mid-restore).
    await db.upsert_pending_recovery(
        service._conn, "inst1", "RUNNING", error_code="RESTORE_FAILED",
        error_message="initial failure",
    )
    inst1.state.running = False
    await registry.close_gate("inst1")

    original_execute = type(inst1)._execute
    armed = {"on": True}

    async def drop_while_recovering(self, cmd):  # type: ignore[no-untyped-def]
        if armed["on"] and (cmd.line or "").strip().upper() == "*OPC?":
            if cmd.writer is not None:
                cmd.writer.close()
            return
        return await original_execute(self, cmd)

    type(inst1)._execute = drop_while_recovering
    try:
        # Recovery is triggered through the normal startup path so the
        # background worker is created exactly as in production.
        await service.startup_recovery()
        await _wait_for(lambda: _attempts_at_least(service, "inst1", 2))
        intent = await db.get_recovery(service._conn, "inst1")
        assert intent is not None
        assert intent["pending"] == 1          # failure did not release it
        assert intent["target_state"] == "RUNNING"
        assert registry.gate_is_open("inst1") is False
        assert inst1.state.running is False
    finally:
        armed["on"] = False
        type(inst1)._execute = original_execute

    confirmed = await _wait_for(lambda: _intent_confirmed(service, "inst1"), timeout=10.0)
    assert confirmed
    assert inst1.state.running is True
    assert registry.gate_is_open("inst1") is True

    # No other rows existed, so a successful post-recovery acquisition works.
    acquisition_id = await service.create_and_spawn("inst1", 1)
    row = await _wait_terminal(service, acquisition_id)
    assert row["state"] == db.SUCCESS
    assert row["restored"] == 1


async def _attempts_at_least(service, name: str, count: int) -> bool:
    intent = await db.get_recovery(service._conn, name)
    return intent is not None and int(intent["attempts"]) >= count and intent["pending"] == 1


@pytest.mark.asyncio
async def test_late_command_after_disconnect_never_releases_early(lab):
    """A command the instrument already received executes late; no early pass.

    The capture's ``:STOP`` is executed after a delay longer than the command
    timeout, so the capture times out and the first restoration attempt's
    ``*OPC?`` is queued behind that still-running command and also times out.
    The row must then be FAILED with ``restored=false`` (pending), never a
    premature success; once the queue drains and the (idempotent) STOP is
    confirmed, the original STOPPED target is restored and the instrument was
    never started.
    """
    service, _ = await lab.new_service()
    inst1 = lab.instruments["inst1"]
    inst1.state.running = False  # previously paused: target must stay STOPPED
    settings.command_timeout = 0.3
    settings.restore_attempts = 1
    settings.restore_backoff = 0.05
    settings.restore_settle = 0.0

    original_execute = type(inst1)._execute
    delayed = {"done": False}

    async def delay_first_stop(self, cmd):  # type: ignore[no-untyped-def]
        if not delayed["done"] and (cmd.line or "").strip().upper() == ":STOP":
            delayed["done"] = True
            await asyncio.sleep(0.9)  # executes late, after client timed out
        return await original_execute(self, cmd)

    type(inst1)._execute = delay_first_stop
    try:
        acquisition_id = await service.create_and_spawn("inst1", 1)
        first = await _wait_terminal(service, acquisition_id, timeout=15.0)
        assert first["state"] == db.FAILED
        assert first["previous_state"] == "STOPPED"
        assert first["restored"] == 0          # not released early
        assert first["raw_codes"] is None

        intent = await db.get_recovery(service._conn, "inst1")
        assert intent is not None and intent["pending"] == 1
        assert intent["target_state"] == "STOPPED"
        assert registry.gate_is_open("inst1") is False

        confirmed = await _wait_for(
            lambda: _row_if(service, acquisition_id, lambda r: r["restored"] == 1),
            timeout=15.0,
        )
    finally:
        type(inst1)._execute = original_execute
        settings.command_timeout = 2.0
        settings.restore_attempts = 1

    assert confirmed["state"] == db.FAILED
    assert registry.gate_is_open("inst1") is True
    assert inst1.state.running is False
    assert await _run_state("127.0.0.1", lab.ports["inst1"]) == "0"


@pytest.mark.asyncio
async def test_failure_does_not_alter_previous_success(lab):
    service, connection = await lab.new_service()
    inst1 = lab.instruments["inst1"]

    success_id = await service.create_and_spawn("inst1", 1)
    success = await _wait_terminal(service, success_id)
    assert success["state"] == db.SUCCESS
    before = dict(success)

    # Failure before the instrument is touched (unknown channel) must not
    # expose a waveform and must leave the old SUCCESS row byte-identical.
    failure_id = await service.create_and_spawn("inst1", 9)
    failure = await _wait_terminal(service, failure_id)
    assert failure["state"] == db.FAILED
    assert failure["raw_codes"] is None
    assert failure["restored"] == 1  # never paused, so nothing to restore

    after = await service.get(success_id)
    for key in (
        "state", "point_count", "raw_codes", "times", "voltages",
        "x_origin", "x_increment", "y_origin", "y_increment", "restored",
        "previous_state", "error_code", "updated_at",
    ):
        assert after[key] == before[key], key
    assert inst1.state.running is True
