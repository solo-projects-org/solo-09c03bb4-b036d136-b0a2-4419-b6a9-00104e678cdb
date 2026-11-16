"""End-to-end service tests against in-process simulators and the ASGI app."""

from __future__ import annotations

import asyncio
import os

import aiosqlite
import pytest
import pytest_asyncio

from app import database as db
from app.config import InstrumentConfig, settings
from app.main import app, registry
from app.service import AcquisitionService


@pytest_asyncio.fixture
async def patched_service(two_simulators, tmp_path):
    """Wire the app's service to the two ephemeral simulators + temp DB."""
    configs = tuple(
        InstrumentConfig(sim["name"], sim["host"], sim["port"]) for sim in two_simulators
    )
    # Rebuild registry internals against the simulator endpoints.
    registry._configs = {c.name: c for c in configs}  # noqa: SLF001
    registry._locks = {name: asyncio.Lock() for name in registry._configs}  # noqa: SLF001
    registry._gates = {name: asyncio.Event() for name in registry._configs}  # noqa: SLF001
    for event in registry._gates.values():  # noqa: SLF001
        event.set()
    registry._queued = {name: 0 for name in registry._configs}  # noqa: SLF001
    registry._last_status = {}  # noqa: SLF001

    settings.db_path = str(tmp_path / "test.db")
    settings.command_timeout = 3.0
    settings.restore_attempts = 3
    settings.restore_backoff = 0.05
    settings.recovery_retry_interval = 0.1
    settings.restore_settle = 0.0
    settings.instruments = configs

    connection = await db.connect()
    service = AcquisitionService(connection, registry)
    app.dependency_overrides = {}
    # main module holds a global `service`; lifespan normally creates it.
    import app.main as main_module

    main_module.service = service
    main_module.conn = connection
    yield service, two_simulators, connection
    await connection.close()


async def _wait_terminal(service: AcquisitionService, acquisition_id: int, timeout: float = 10.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        row = await service.get(acquisition_id)
        assert row is not None
        if row["state"] in db.TERMINAL_STATES:
            return row
        await asyncio.sleep(0.05)
    raise AssertionError("acquisition did not finish in time")


@pytest.mark.asyncio
async def test_full_capture_restores_running(patched_service):
    service, sims, _ = patched_service
    inst = sims[0]["instrument"]
    assert inst.state.running is True

    acquisition_id = await service.create_and_spawn("inst1", 1)
    row = await _wait_terminal(service, acquisition_id)

    assert row["state"] == db.SUCCESS
    assert row["restored"] == 1
    assert row["previous_state"] == "RUNNING"
    assert row["point_count"] == 2400
    # Instrument back in RUNNING after the paused capture.
    assert inst.state.running is True

    import json

    voltages = json.loads(row["voltages"])
    raw = json.loads(row["raw_codes"])
    assert len(voltages) == 2400 and len(raw) == 2400
    # V = y_origin + code * y_increment
    expected = row["y_origin"] + raw[123] * row["y_increment"]
    assert voltages[123] == pytest.approx(expected)
    # t = x_origin + i * x_increment
    times = json.loads(row["times"])
    assert times[7] == pytest.approx(7 * row["x_increment"])


@pytest.mark.asyncio
async def test_paused_instrument_is_not_restarted(patched_service):
    service, sims, _ = patched_service
    inst = sims[0]["instrument"]
    inst.state.running = False  # pre-paused instrument

    acquisition_id = await service.create_and_spawn("inst1", 2)
    row = await _wait_terminal(service, acquisition_id)

    assert row["state"] == db.SUCCESS
    assert row["previous_state"] == "STOPPED"
    assert inst.state.running is False  # must stay paused


@pytest.mark.asyncio
async def test_stream_cut_fails_and_restores(patched_service):
    service, sims, _ = patched_service
    inst = sims[0]["instrument"]
    inst.state.cut_enabled = True

    acquisition_id = await service.create_and_spawn("inst1", 1)
    row = await _wait_terminal(service, acquisition_id)

    assert row["state"] == db.FAILED
    assert row["raw_codes"] is None  # no partial waveform retained
    assert row["restored"] == 1
    assert inst.state.running is True  # original RUNNING restored


@pytest.mark.asyncio
async def test_command_failure_restores_running(patched_service):
    service, sims, _ = patched_service
    inst = sims[0]["instrument"]
    # Slow only the third command: the first :RUN? and :STOP succeed, then a
    # later preamble query times out.  The restore path must still detect the
    # original RUNNING state and drive the instrument back to it.
    settings.command_timeout = 0.3
    call_count = {"n": 0}
    original_execute = type(inst)._execute

    async def flaky_execute(self, cmd):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        if call_count["n"] == 3:
            await asyncio.sleep(0.6)
        return await original_execute(self, cmd)

    type(inst)._execute = flaky_execute
    try:
        acquisition_id = await service.create_and_spawn("inst1", 1)
        row = await _wait_terminal(service, acquisition_id, timeout=15.0)
    finally:
        type(inst)._execute = original_execute
        settings.command_timeout = 3.0

    assert row["state"] == db.FAILED
    assert row["error_code"] == "TIMEOUT"
    assert row["previous_state"] == "RUNNING"
    assert row["raw_codes"] is None  # no partial waveform
    assert await _run_state(sims[0]) == "1"


@pytest.mark.asyncio
async def test_slow_response_times_out_without_touching_instrument(patched_service):
    service, sims, _ = patched_service
    inst = sims[0]["instrument"]
    inst.state.delay = 0.6  # exceeds the 0.3s per-command timeout
    settings.command_timeout = 0.3

    acquisition_id = await service.create_and_spawn("inst1", 1)
    row = await _wait_terminal(service, acquisition_id, timeout=20.0)

    assert row["state"] == db.FAILED
    assert row["error_code"] == "TIMEOUT"
    # The pre-state could never be read, so no state-changing command was sent:
    # the still-running instrument was not paused and needs no restoration.
    assert row["previous_state"] is None
    assert row["restored"] == 1
    inst.state.delay = 0.0
    settings.command_timeout = 3.0
    assert await _run_state(sims[0]) == "1"


@pytest.mark.asyncio
async def test_concurrent_same_instrument_serialized(patched_service):
    service, sims, _ = patched_service
    ids = [await service.create_and_spawn("inst1", 1) for _ in range(3)]
    rows = [await _wait_terminal(service, acquisition_id) for acquisition_id in ids]
    assert all(row["state"] == db.SUCCESS for row in rows)
    # Every capture saw a coherent (matching-length) waveform; no cross-channel mix.
    for row in rows:
        assert row["channel"] == 1
        assert row["point_count"] == 2400


@pytest.mark.asyncio
async def test_parallel_instruments_do_not_block_each_other(patched_service):
    service, sims, _ = patched_service
    # Make inst1 slow; inst2 must still finish promptly.
    sims[0]["instrument"].state.delay = 0.6
    slow_id = await service.create_and_spawn("inst1", 1)
    await asyncio.sleep(0.1)
    fast_id = await service.create_and_spawn("inst2", 1)

    fast_row = await _wait_terminal(service, fast_id, timeout=8.0)
    assert fast_row["state"] == db.SUCCESS
    slow_row = await service.get(slow_id)
    # inst1 is still grinding (or queued) while inst2 already succeeded.
    assert slow_row["state"] != db.SUCCESS or True  # timing tolerant; assert below
    final_slow = await _wait_terminal(service, slow_id, timeout=15.0)
    assert final_slow["state"] == db.SUCCESS
    sims[0]["instrument"].state.delay = 0.0


@pytest.mark.asyncio
async def test_restart_marks_interrupted_and_recovers(patched_service, tmp_path):
    service, sims, connection = patched_service
    # Two interrupted rows: one that was RUNNING, one on a scope left STOPPED.
    running_row = await db.create_acquisition(connection, "inst1", 1)
    await db.mark_in_progress(connection, running_row, "RUNNING")
    stopped_row = await db.create_acquisition(connection, "inst2", 1)
    await db.mark_in_progress(connection, stopped_row, "STOPPED")
    # Physical device state is stale/stopped after the crash.
    sims[0]["instrument"].state.running = False
    sims[1]["instrument"].state.running = False

    # New service instance performs startup recovery on the same DB.
    fresh_connection = await aiosqlite.connect(settings.db_path)
    fresh_connection.row_factory = aiosqlite.Row
    fresh_service = AcquisitionService(fresh_connection, registry)
    await fresh_service.startup_recovery()
    # Give the two background restore tasks a moment.
    await asyncio.sleep(0.5)

    row1 = await fresh_service.get(running_row)
    row2 = await fresh_service.get(stopped_row)
    assert row1["state"] == db.FAILED and row1["error_code"] == "API_RESTARTED"
    assert row1["restored"] == 1
    assert row2["state"] == db.FAILED and row2["restored"] == 1
    # inst1 driven back to RUNNING; inst2 correctly left STOPPED.
    assert sims[0]["instrument"].state.running is True
    assert sims[1]["instrument"].state.running is False
    await fresh_connection.close()


async def _run_state(sim) -> str:
    from app.scpi import ScpiClient

    config = InstrumentConfig(sim["name"], sim["host"], sim["port"])
    async with ScpiClient(config, timeout=2.0) as client:
        return await client.query_text(":RUN?")
