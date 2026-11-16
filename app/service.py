"""Acquisition orchestration: per-instrument locking, capture and restoration.

Guarantees implemented here:

* A per-instrument asyncio lock serializes all work touching one instrument,
  so concurrent requests on the same instrument can never mix channels or
  scaling.  Different instruments have independent locks and run in parallel.
* The instrument is paused before reading.  Its previous run/stop state is
  captured first and always restored at the end -- including timeout, stream
  truncation and every other failure path.
* An acquisition is only reported successful after the restore is confirmed.
  A previously paused instrument is never restarted (restore is a no-op STOP).
* Failed captures never expose partial waveform data.
* On API startup, rows that never reached a terminal state are marked failed
  and their instruments are restored before new work is admitted.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

import aiosqlite

from . import database as db
from .config import InstrumentConfig, settings
from .scpi import ScpiClient, ScpiError, ScpiTimeoutError

LOG = logging.getLogger("acq-service")

RUNNING = "RUNNING"
STOPPED = "STOPPED"


class UnknownInstrumentError(Exception):
    pass


class InstrumentRegistry:
    """Holds the per-instrument lock, recovery gate and runtime status."""

    def __init__(self, configs: tuple[InstrumentConfig, ...]) -> None:
        self._configs = {c.name: c for c in configs}
        self._locks = {name: asyncio.Lock() for name in self._configs}
        # When an instrument is recovering at startup, its gate stays closed.
        self._gates = {name: asyncio.Event() for name in self._configs}
        for event in self._gates.values():
            event.set()
        self._queued = {name: 0 for name in self._configs}
        self._last_status: dict[str, dict[str, object]] = {}

    @property
    def names(self) -> list[str]:
        return list(self._configs)

    def config(self, name: str) -> InstrumentConfig:
        try:
            return self._configs[name]
        except KeyError as exc:
            raise UnknownInstrumentError(name) from exc

    def lock(self, name: str) -> asyncio.Lock:
        return self._locks[name]

    def is_busy(self, name: str) -> bool:
        return self._locks[name].locked()

    def queued(self, name: str) -> int:
        return self._queued[name]

    @contextlib.asynccontextmanager
    async def reserve_blocking(self, name: str) -> AsyncIterator[None]:
        """Serialize all work on one instrument; waiters queue in order."""
        self._queued[name] += 1
        try:
            async with self._locks[name]:
                yield
        finally:
            self._queued[name] -= 1

    async def wait_gate_open(self, name: str) -> None:
        await self._gates[name].wait()

    async def close_gate(self, name: str) -> None:
        self._gates[name].clear()

    async def open_gate(self, name: str) -> None:
        self._gates[name].set()

    def gate_is_open(self, name: str) -> bool:
        return self._gates[name].is_set()

    def record_status(self, name: str, status: dict[str, object]) -> None:
        self._last_status[name] = status

    def last_status(self, name: str) -> dict[str, object] | None:
        return self._last_status.get(name)


class AcquisitionService:
    def __init__(self, conn: aiosqlite.Connection, registry: InstrumentRegistry) -> None:
        self._conn = conn
        self._registry = registry
        self._tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------------ public

    async def create_and_spawn(self, instrument: str, channel: int) -> int:
        self._registry.config(instrument)  # validates name / 404
        acquisition_id = await db.create_acquisition(self._conn, instrument, channel)
        task = asyncio.create_task(
            self._run_acquisition(acquisition_id, self._registry.config(instrument), channel),
            name=f"acq-{acquisition_id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return acquisition_id

    async def get(self, acquisition_id: int) -> aiosqlite.Row | None:
        return await db.get_acquisition(self._conn, acquisition_id)

    async def list(self, instrument: str | None, limit: int) -> list[aiosqlite.Row]:
        if instrument is not None:
            self._registry.config(instrument)  # 404 on unknown instrument
        return await db.list_acquisitions(self._conn, instrument, limit)

    async def startup_recovery(self) -> None:
        """Handle rows left non-terminal by a previous process, then reopen.

        Rows are first marked failed (so they are never reported as in-flight
        again), then one background restoration task per affected instrument
        drives the device back to its recorded prior state.  Each affected row
        is flipped to ``restored=1`` only after that restoration is confirmed.
        The instrument's gate stays closed until the task finishes, so new
        acquisitions cannot touch the device meanwhile.
        """
        ids = await db.pending_acquisition_ids(self._conn)
        if not ids:
            return
        LOG.warning("recovering %d interrupted acquisition(s): %s", len(ids), ids)

        by_instrument: dict[str, list[int]] = {}
        for acquisition_id in ids:
            row = await db.get_acquisition(self._conn, acquisition_id)
            assert row is not None
            message = (
                "API process restarted before acquisition completed; "
                "marked failed and instrument state restored"
            )
            await db.mark_restart_failed(self._conn, acquisition_id, message)
            by_instrument.setdefault(row["instrument"], []).append(acquisition_id)

        for instrument_name, acq_ids in by_instrument.items():
            if instrument_name not in self._registry.names:
                LOG.error(
                    "interrupted rows %s reference unknown instrument %s; "
                    "leaving restored=0",
                    acq_ids,
                    instrument_name,
                )
                continue
            config = self._registry.config(instrument_name)
            await self._registry.close_gate(instrument_name)
            # Run restoration synchronously with startup: the lifespan waits
            # for this method, so no new acquisition can touch the instrument
            # before its prior state has been restored (or retries exhausted).
            await self._startup_restore(config, acq_ids)

    async def _startup_restore(
        self,
        config: InstrumentConfig,
        acquisition_ids: list[int],
    ) -> None:
        try:
            async with self._registry.reserve_blocking(config.name):
                # Prefer the most recent recorded prior state; fall back to the
                # live device state, then to a safe STOPPED default.
                target_state = STOPPED
                for acquisition_id in reversed(acquisition_ids):
                    row = await db.get_acquisition(self._conn, acquisition_id)
                    if row is not None and row["previous_state"] in (RUNNING, STOPPED):
                        target_state = row["previous_state"]
                        break
                else:
                    try:
                        target_state = await self._query_run_state(config)
                    except ScpiError:
                        target_state = STOPPED
                await self._restore_with_retry(config, target_state)
                for acquisition_id in acquisition_ids:
                    await db.update_state(
                        self._conn, acquisition_id, db.FAILED, restored=1
                    )
                LOG.info(
                    "startup recovery complete for %s -> %s, rows %s",
                    config.name, target_state, acquisition_ids,
                )
        except Exception:
            LOG.exception(
                "startup recovery failed for %s; rows %s remain restored=0",
                config.name, acquisition_ids,
            )
        finally:
            # Gate reopens even on failure: the per-instrument lock still
            # serializes, and each new acquisition performs its own restoration.
            await self._registry.open_gate(config.name)

    # ------------------------------------------------------------- acquisition

    async def _run_acquisition(
        self,
        acquisition_id: int,
        config: InstrumentConfig,
        channel: int,
    ) -> None:
        registry = self._registry
        await registry.wait_gate_open(config.name)
        # The lock is the single serialization point: wait for any in-flight
        # acquisition / recovery on this instrument instead of failing, so the
        # next acquisition can never touch the instrument mid-restoration.
        async with registry.reserve_blocking(config.name):
            try:
                await self._acquire_locked(acquisition_id, config, channel)
            except Exception:
                LOG.exception("acquisition %s failed", acquisition_id)

    async def _acquire_locked(
        self,
        acquisition_id: int,
        config: InstrumentConfig,
        channel: int,
    ) -> None:
        previous_state: str | None = None
        client: ScpiClient | None = None
        failure: tuple[str, str] | None = None
        capture: dict[str, object] | None = None

        try:
            client = ScpiClient(config, settings.command_timeout)
            await client.connect()

            # 1) Remember the pre-acquisition run state (must not change it yet).
            previous_state = await self._query_run_state(config)
            await db.mark_in_progress(self._conn, acquisition_id, previous_state)
            LOG.info(
                "acquisition %s: %s ch%d, previous state %s",
                acquisition_id, config.name, channel, previous_state,
            )

            # 2) Pause measurement, no matter what the previous state was.
            await client.send(":STOP")

            # 3) Validate channel, then read the same waveform's preamble.
            presence = await client.query_text(f":CHAN{channel}:POS?")
            if presence != "1":
                raise ScpiError(f"channel {channel} not available")

            point_count = await client.query_int(":WAV:POIN?")
            x_origin = await client.query_float(":WAV:XOR?")
            x_increment = await client.query_float(":WAV:XINC?")
            y_origin = await client.query_float(":WAV:YOR?")
            y_increment = await client.query_float(":WAV:YINC?")

            # 4) Fixed-length IEEE 488.2 block, signed int16 little-endian.
            raw_codes = await client.query_ieee_block_int16_le(":WAV:DATA?")
            if len(raw_codes) != point_count:
                raise ScpiError(
                    f"point count mismatch: preamble={point_count}, block={len(raw_codes)}"
                )

            # 5) Convert to engineering units; retain raw codes and scaling.
            times = [x_origin + i * x_increment for i in range(point_count)]
            voltages = [y_origin + code * y_increment for code in raw_codes]
            capture = {
                "point_count": point_count,
                "x_origin": x_origin,
                "x_increment": x_increment,
                "y_origin": y_origin,
                "y_increment": y_increment,
                "raw_codes": raw_codes,
                "times": times,
                "voltages": voltages,
            }
            await db.mark_success(self._conn, acquisition_id, **capture)  # type: ignore[arg-type]

        except ScpiTimeoutError as exc:
            failure = ("TIMEOUT", str(exc))
        except ScpiError as exc:
            failure = (type(exc).__name__.upper(), str(exc))
        except Exception as exc:  # defensive: never leave a row non-terminal
            failure = ("UNEXPECTED_ERROR", repr(exc))

        # --- Restoration happens before any terminal state is reported. ---
        restore_error: str | None = None
        try:
            if previous_state is None:
                # We never learned the original state (connect/query failed).
                # Default to a safe STOP rather than starting a paused scope.
                target_state = STOPPED
                LOG.warning(
                    "acquisition %s: previous state unknown, restoring to STOP only",
                    acquisition_id,
                )
            else:
                target_state = previous_state
            await self._restore_with_retry(
                config, target_state, client=client,
            )
        except ScpiError as exc:
            restore_error = str(exc)
            LOG.error("acquisition %s: restore failed: %s", acquisition_id, exc)
        finally:
            if client is not None:
                await client.close()

        if failure is not None:
            code, message = failure
            if restore_error:
                message = f"{message}; restoration failed: {restore_error}"
            await db.mark_failure(self._conn, acquisition_id, code, message)
            await db.mark_terminal(self._conn, acquisition_id, db.FAILED)
            LOG.warning("acquisition %s FAILED: %s %s", acquisition_id, code, message)
            return

        if restore_error:
            await db.mark_failure(
                self._conn,
                acquisition_id,
                "RESTORE_FAILED",
                f"capture succeeded but instrument restoration failed: {restore_error}",
            )
            await db.mark_terminal(self._conn, acquisition_id, db.FAILED)
            return

        assert capture is not None
        await db.mark_terminal(self._conn, acquisition_id, db.SUCCESS)
        LOG.info("acquisition %s SUCCESS, restored to %s", acquisition_id, target_state)

    # ------------------------------------------------------------------ restore

    async def _restore_with_retry(
        self,
        config: InstrumentConfig,
        target_state: str,
        client: ScpiClient | None = None,
    ) -> None:
        """Drive the instrument back to *target_state*, confirming each step.

        A fresh connection is used when the supplied one is unusable.  Retries
        are bounded; on exhaustion the last :class:`ScpiError` is re-raised so
        the caller cannot report success.
        """
        attempts = settings.restore_attempts
        last_error: ScpiError | None = None

        for attempt in range(1, attempts + 1):
            owned_client = client is None
            active = client
            try:
                if active is None:
                    active = ScpiClient(config, settings.command_timeout)
                    await active.connect()
                if target_state == RUNNING:
                    await active.send(":RUN")
                else:
                    # STOP is idempotent: a scope that was already paused is
                    # never started, and re-STOP leaves a running one paused.
                    await active.send(":STOP")
                actual = await self._read_run_state(active)
                if actual != target_state:
                    raise ScpiError(
                        f"restore confirmation mismatch: wanted {target_state}, got {actual}"
                    )
                self._registry.record_status(config.name, {
                    "restored_state": actual,
                    "restored_at": db.utcnow(),
                })
                return
            except ScpiError as exc:
                last_error = exc
                LOG.warning(
                    "restore %s -> %s attempt %d/%d failed: %s",
                    config.name, target_state, attempt, attempts, exc,
                )
                if owned_client and active is not None:
                    await active.close()
                client = None  # force reconnect on the next attempt
                if attempt < attempts:
                    await asyncio.sleep(settings.restore_backoff * attempt)
        assert last_error is not None
        raise last_error

    async def _query_run_state(self, config: InstrumentConfig) -> str:
        async with ScpiClient(config, settings.command_timeout) as client:
            return await self._read_run_state(client)

    @staticmethod
    async def _read_run_state(client: ScpiClient) -> str:
        reply = await client.query_text(":RUN?")
        if reply == "1":
            return RUNNING
        if reply == "0":
            return STOPPED
        raise ScpiError(f"unexpected :RUN? reply {reply!r}")
