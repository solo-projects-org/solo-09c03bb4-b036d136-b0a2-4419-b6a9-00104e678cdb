"""Acquisition orchestration: per-instrument locking, capture and restoration.

Guarantees implemented here:

* A per-instrument asyncio lock serializes all work touching one instrument,
  so concurrent requests on the same instrument can never mix channels or
  scaling.  Different instruments have independent locks and run in parallel.
* The instrument's run/stop state is read on a fresh, drained connection
  *before* any state-changing command.  A previously paused instrument is
  therefore never started; if the state cannot be read, nothing is changed and
  the original state is preserved by construction.
* The instrument is paused before reading and always driven back to its
  recorded state.  Restoration never reuses a connection whose response stream
  may be out of sync, and it first drains commands the instrument already
  received (``*OPC?``) so a command that executes late after a disconnect
  cannot be mistaken for a later state.
* An acquisition is only reported successful after restoration is confirmed by
  a ``:RUN?`` read-back on a fresh connection.
* If restoration cannot be confirmed within the bounded retries, the
  acquisition is FAILED with ``restored=false`` (pending restoration), the
  original RUNNING/STOPPED target is persisted per instrument, and the
  instrument's gate is closed.  New acquisitions for that instrument wait
  without touching it.  A background worker keeps retrying (including after an
  API restart) until it confirms the target, then flips the waiting rows to
  ``restored=true`` and reopens the gate.
* Failed captures never expose partial waveform data; only a confirmed capture
  is persisted.  Historical successful rows are never rewritten.
* Startup recovery performs only fast database bookkeeping before the API is
  opened; network restoration runs in per-instrument background tasks, so an
  offline or slow instrument cannot delay the API, other instruments or status
  queries.
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
VALID_RUN_STATES = (RUNNING, STOPPED)


class UnknownInstrumentError(Exception):
    pass


class InstrumentRegistry:
    """Holds the per-instrument lock, recovery gate and runtime status."""

    def __init__(self, configs: tuple[InstrumentConfig, ...]) -> None:
        self._configs = {c.name: c for c in configs}
        self._locks = {name: asyncio.Lock() for name in self._configs}
        # Closed while an instrument still needs a confirmed restoration.
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
        # One long-lived background worker per instrument that ever needed
        # recovery.  It is woken by a new durable intent instead of polling a
        # possibly-closing task, so an intent written at the exact moment a
        # previous restoration completes is never lost.
        self._recovery_tasks: dict[str, asyncio.Task] = {}
        self._recovery_wake: dict[str, asyncio.Event] = {}
        # Serializes "is an intent pending?" checks with intent creation and
        # gate release, so a freshly created intent cannot race with a worker
        # that is just about to conclude it is idle and reopen the gate.
        self._recovery_state_lock = asyncio.Lock()
        self._closing = False

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

    async def shutdown(self) -> None:
        """Stop background work without losing any restoration target.

        Recovery tasks are cancelled first: their intents stay pending in
        SQLite, so the next start resumes the same target.  In-flight
        acquisitions get a short grace period to finish their own restoration;
        whatever is still running afterwards is cancelled, leaving its row
        non-terminal with ``previous_state`` recorded, which the next startup
        reconciles and restores.
        """
        self._closing = True
        recovery_tasks = list(self._recovery_tasks.values())
        for task in recovery_tasks:
            task.cancel()
        if recovery_tasks:
            await asyncio.gather(*recovery_tasks, return_exceptions=True)
        self._recovery_tasks.clear()

        acquisition_tasks = [task for task in self._tasks if not task.done()]
        # Give only tasks that have already touched the device (and are
        # therefore running their restoration) a bounded grace period.  Tasks
        # still waiting for the gate/lock are cancelled immediately: their rows
        # remain PENDING and the next startup reconciles them without touching
        # the instrument.  Anything cancelled mid-capture also stays
        # non-terminal with ``previous_state`` recorded for startup recovery.
        in_flight: list[asyncio.Task] = []
        for task in acquisition_tasks:
            acquisition_id = self._acquisition_id_from_task(task)
            if acquisition_id is None:
                continue
            row = await db.get_acquisition(self._conn, acquisition_id)
            if row is not None and row["state"] in (db.IN_PROGRESS, db.RESTORING):
                in_flight.append(task)
        waiting = [task for task in acquisition_tasks if task not in in_flight]
        for task in waiting:
            task.cancel()
        if in_flight:
            _, still_running = await asyncio.wait(
                in_flight, timeout=max(0.0, settings.shutdown_grace)
            )
            for task in still_running:
                task.cancel()
            waiting.extend(still_running)
        if waiting:
            await asyncio.gather(*waiting, return_exceptions=True)
        self._tasks.clear()

    @staticmethod
    def _acquisition_id_from_task(task: asyncio.Task) -> int | None:
        name = task.get_name()
        if not name.startswith("acq-"):
            return None
        try:
            return int(name[len("acq-"):])
        except ValueError:
            return None

    async def startup_recovery(self) -> None:
        """Reconcile rows left by a previous process, then open the API.

        Only database work happens here.  For every affected instrument the
        original target state is derived from the recorded ``previous_state``
        (never from the instrument's possibly stale, leftover STOPPED state),
        persisted as a durable intent, and the instrument's gate is closed.  A
        background worker performs the actual restoration, so an offline or
        slow instrument does not delay the API or any other instrument.
        """
        interrupted_ids = await db.pending_acquisition_ids(self._conn)
        unrestored = await db.unrestored_acquisitions(self._conn)

        interrupted: list[aiosqlite.Row] = []
        for acquisition_id in interrupted_ids:
            row = await db.get_acquisition(self._conn, acquisition_id)
            if row is not None:
                interrupted.append(row)

        # A row that reached a terminal state but never confirmed restoration
        # also belongs to the reconciliation set (e.g. a previous process died
        # during the recovery worker loop).
        seen = {int(row["id"]) for row in interrupted}
        terminal_unrestored = [
            row
            for row in unrestored
            if int(row["id"]) not in seen and row["state"] in db.TERMINAL_STATES
        ]

        # The original instrument target is the earliest recorded pre-state.
        # Earliest wins because acquisitions are serialized per instrument:
        # later rows could only start after the earlier one was restored.
        targets: dict[str, str] = {}
        for row in sorted(interrupted + terminal_unrestored, key=lambda item: item["id"]):
            previous = row["previous_state"]
            if previous in VALID_RUN_STATES:
                targets.setdefault(row["instrument"], previous)

        for row in interrupted:
            previous = row["previous_state"]
            if previous in VALID_RUN_STATES:
                message = (
                    "API process restarted before acquisition completed; "
                    f"restoration to {previous} is pending"
                )
                await db.mark_restart_failed(self._conn, int(row["id"]), message, restored=False)
            else:
                # The pre-state was never read, which also means no :STOP (or
                # any other state-changing command) was ever sent.
                message = (
                    "API process restarted before acquisition completed; "
                    "instrument was not modified"
                )
                await db.mark_restart_failed(self._conn, int(row["id"]), message, restored=True)

        for row in terminal_unrestored:
            if row["previous_state"] not in VALID_RUN_STATES:
                # Nothing state-changing was ever sent for this row.
                await db.set_restored(self._conn, int(row["id"]), True)

        existing_pending = {row["instrument"]: row for row in await db.pending_recoveries(self._conn)}
        affected = set(targets) | set(existing_pending)
        for name in sorted(affected - set(self._registry.names)):
            LOG.error(
                "restoration pending for unknown instrument %s; its rows stay "
                "restored=false because no endpoint can be recovered",
                name,
            )

        for name in self._registry.names:
            if name not in affected:
                await self._registry.open_gate(name)
                continue
            existing = existing_pending.get(name)
            if existing is not None:
                # Never replace a target that is already awaiting confirmation.
                target = existing["target_state"]
            else:
                target = targets[name]
            if target not in VALID_RUN_STATES:
                LOG.error("no valid restoration target for %s; refusing to start it", name)
                target = STOPPED

            await self._registry.close_gate(name)
            await db.upsert_pending_recovery(
                self._conn,
                name,
                target,
                error_code=existing["error_code"] if existing is not None else "RESTORE_PENDING",
                error_message=(
                    existing["error_message"]
                    if existing is not None
                    else "restoration pending after API restart"
                ),
            )
            self._registry.record_status(
                name,
                {
                    "pending_restore": True,
                    "target_state": target,
                    "attempts": int(existing["attempts"]) if existing is not None else 0,
                    "last_error": existing["error_message"] if existing is not None else None,
                    "updated_at": db.utcnow(),
                },
            )
            async with self._recovery_state_lock:
                self._ensure_recovery_worker(name)
            LOG.warning(
                "startup: %s requires restoration to %s; API remains available",
                name,
                target,
            )

        if interrupted:
            LOG.warning(
                "startup recovery scheduled for %d interrupted acquisition(s): %s",
                len(interrupted),
                [int(row["id"]) for row in interrupted],
            )

    # ------------------------------------------------------------- acquisition

    async def _run_acquisition(
        self,
        acquisition_id: int,
        config: InstrumentConfig,
        channel: int,
    ) -> None:
        registry = self._registry
        # Wait until the instrument has no unconfirmed restoration, then take
        # the per-instrument lock.  Re-check the gate after acquiring it: a
        # previous acquisition may have just failed its restoration and closed
        # the gate while this task was queued for the lock.
        while True:
            await registry.wait_gate_open(config.name)
            async with registry.reserve_blocking(config.name):
                if not registry.gate_is_open(config.name):
                    continue
                try:
                    await self._acquire_locked(acquisition_id, config, channel)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOG.exception("acquisition %s failed unexpectedly", acquisition_id)
                    # Last-resort safety net: if this row had already recorded
                    # a pre-state, its instrument may be paused.  Never let a
                    # later acquisition touch it until that is confirmed.
                    await self._fail_safe_pending(acquisition_id, config)
                return

    async def _fail_safe_pending(
        self, acquisition_id: int, config: InstrumentConfig
    ) -> None:
        try:
            row = await db.get_acquisition(self._conn, acquisition_id)
            if row is None or row["state"] in db.TERMINAL_STATES:
                return
            previous = row["previous_state"]
            if previous in VALID_RUN_STATES:
                await db.upsert_pending_recovery(
                    self._conn,
                    config.name,
                    previous,
                    acquisition_id=acquisition_id,
                    error_code="INTERNAL_ERROR",
                    error_message=(
                        "internal error after the instrument was paused; "
                        f"restoration to {previous} is pending"
                    ),
                )
                await db.mark_failure(
                    self._conn,
                    acquisition_id,
                    "INTERNAL_ERROR",
                    "internal error during acquisition; restoration pending",
                )
                await db.mark_terminal(
                    self._conn, acquisition_id, db.FAILED, restored=False
                )
                async with self._recovery_state_lock:
                    await self._registry.close_gate(config.name)
                    self._ensure_recovery_worker(config.name)
            else:
                # No pre-state means no state-changing command was sent.
                await db.mark_failure(
                    self._conn,
                    acquisition_id,
                    "INTERNAL_ERROR",
                    "internal error before the instrument was modified",
                )
                await db.mark_terminal(
                    self._conn, acquisition_id, db.FAILED, restored=True
                )
        except Exception:
            LOG.exception(
                "could not finalize acquisition %s after internal error; "
                "the row stays non-terminal and startup recovery will reconcile it",
                acquisition_id,
            )

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

            # 1) Remember the pre-acquisition run state.  No state-changing
            #    command has been sent yet, so if this fails the instrument is
            #    untouched.  Any earlier command the instrument already
            #    received executes before this query (serial instrument queue).
            previous_state = await self._read_run_state(client)
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

            # 5) Convert to engineering units in memory.  Nothing is persisted
            #    until restoration is confirmed, so a failure can never serve
            #    a partial waveform.
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
        except ScpiTimeoutError as exc:
            failure = ("TIMEOUT", str(exc))
        except ScpiError as exc:
            failure = (type(exc).__name__.upper(), str(exc))
        except Exception as exc:  # defensive: never leave a row non-terminal
            failure = ("UNEXPECTED_ERROR", repr(exc))
        finally:
            # Always drop the capture connection before restoring: restore uses
            # a fresh connection and must not inherit buffered/stale replies.
            if client is not None:
                await client.close()

        if previous_state is None:
            # We never read the pre-state, therefore we never sent :STOP.  The
            # instrument was not modified; the correct way to preserve its
            # original state is not to touch it at all.
            code, message = failure or ("UNEXPECTED_ERROR", "previous state unknown")
            message = f"{message}; instrument was not modified, no restoration needed"
            await db.mark_failure(self._conn, acquisition_id, code, message)
            await db.mark_terminal(self._conn, acquisition_id, db.FAILED, restored=True)
            LOG.warning("acquisition %s FAILED before touching %s: %s", acquisition_id, config.name, code)
            return

        # Capture is finished (successfully or not); the instrument must be
        # driven back to its recorded state before any terminal result is
        # reported.  No waveform has been written yet.
        error_code = failure[0] if failure is not None else None
        error_message = failure[1] if failure is not None else None
        await db.mark_restoring(
            self._conn,
            acquisition_id,
            error_code=error_code,
            error_message=error_message,
        )

        try:
            await self._restore_with_retry(config, previous_state)
        except ScpiError as exc:
            await self._mark_restore_pending(
                acquisition_id,
                config,
                previous_state,
                restore_error=exc,
                capture_failure=failure,
            )
            return
        except Exception as exc:  # defensive: never release an unconfirmed device
            LOG.exception("unexpected restoration error for %s", config.name)
            await self._mark_restore_pending(
                acquisition_id,
                config,
                previous_state,
                restore_error=ScpiError(f"unexpected error: {exc!r}"),
                capture_failure=failure,
            )
            return

        if failure is not None:
            code, message = failure
            await db.mark_failure(self._conn, acquisition_id, code, message)
            await db.mark_terminal(self._conn, acquisition_id, db.FAILED, restored=True)
            LOG.warning("acquisition %s FAILED: %s %s", acquisition_id, code, message)
            return

        assert capture is not None
        await db.mark_success(
            self._conn,
            acquisition_id,
            restored=True,
            **capture,  # type: ignore[arg-type]
        )
        LOG.info("acquisition %s SUCCESS, restored to %s", acquisition_id, previous_state)

    async def _mark_restore_pending(
        self,
        acquisition_id: int,
        config: InstrumentConfig,
        target_state: str,
        *,
        restore_error: ScpiError,
        capture_failure: tuple[str, str] | None,
    ) -> None:
        """Record an unconfirmed restoration and block the instrument.

        Order matters and runs while the per-instrument lock is held:
        persist the durable intent first, close the gate, then finalize the row
        as FAILED with ``restored=false``.  Even if the process exits between
        any two steps, the intent (or the row's ``previous_state``) preserves
        the target for the next startup.
        """
        if capture_failure is not None:
            code, capture_message = capture_failure
            message = (
                f"{capture_message}; restoration to {target_state} could not be "
                f"confirmed: {restore_error}"
            )
        else:
            code = "RESTORE_FAILED"
            message = (
                "capture succeeded but instrument restoration to "
                f"{target_state} could not be confirmed: {restore_error}"
            )

        await db.upsert_pending_recovery(
            self._conn,
            config.name,
            target_state,
            acquisition_id=acquisition_id,
            error_code=code,
            error_message=message,
        )
        try:
            # Finalize the row before waking the worker: the worker flips
            # terminal ``restored=0`` rows once it confirms the device, and it
            # must never confirm a row that is still being finalized (which
            # would leave it permanently pending).
            await db.mark_failure(self._conn, acquisition_id, code, message)
            await db.mark_terminal(self._conn, acquisition_id, db.FAILED, restored=False)
        finally:
            # The gate is closed and the worker started even if the row write
            # failed: the durable intent above is authoritative, and the
            # instrument must not be touched again until it is confirmed.
            async with self._recovery_state_lock:
                await self._registry.close_gate(config.name)
                self._ensure_recovery_worker(config.name)
        self._registry.record_status(
            config.name,
            {
                "pending_restore": True,
                "target_state": target_state,
                "attempts": settings.restore_attempts,
                "last_error": str(restore_error),
                "updated_at": db.utcnow(),
            },
        )
        LOG.error(
            "acquisition %s FAILED; %s restoration to %s is PENDING "
            "(gate closed, background recovery started)",
            acquisition_id, config.name, target_state,
        )

    # ------------------------------------------------------------------ restore

    async def _restore_with_retry(
        self,
        config: InstrumentConfig,
        target_state: str,
    ) -> str:
        """Drive the instrument back to *target_state* and confirm it.

        Every attempt uses a fresh connection.  It first waits (``*OPC?``) for
        commands already received by the instrument, then sends the state
        command, waits for completion and finally reads ``:RUN?`` back on the
        same in-sync connection.  A timed-out command may still execute later;
        the next attempt's drain makes its effect visible before re-issuing the
        (idempotent) state command.  On exhaustion the last error is raised so
        the caller cannot report success.
        """
        if target_state not in VALID_RUN_STATES:
            # Unknown targets must never start a paused instrument.
            target_state = STOPPED
        attempts = max(1, settings.restore_attempts)
        last_error: ScpiError | None = None

        for attempt in range(1, attempts + 1):
            try:
                actual = await self._restore_once(config, target_state)
                self._registry.record_status(
                    config.name,
                    {
                        "pending_restore": False,
                        "target_state": target_state,
                        "restored_state": actual,
                        "attempts": attempt,
                        "restored_at": db.utcnow(),
                    },
                )
                return actual
            except ScpiError as exc:
                last_error = exc
                LOG.warning(
                    "restore %s -> %s attempt %d/%d failed: %s",
                    config.name, target_state, attempt, attempts, exc,
                )
                if attempt < attempts:
                    await asyncio.sleep(settings.restore_backoff * attempt)

        assert last_error is not None
        raise last_error

    async def _restore_once(self, config: InstrumentConfig, target_state: str) -> str:
        async with ScpiClient(config, settings.command_timeout) as client:
            # The instrument queue is FIFO: every command it already received
            # (including one accepted before an earlier connection dropped and
            # still executing late) was enqueued before this *OPC?, so the
            # acknowledgement below also covers it.
            await client.query_text("*OPC?")
            if target_state == RUNNING:
                await client.send(":RUN")
            else:
                # STOP is idempotent: a scope that was already paused is never
                # started, and re-STOP leaves a running one paused.
                await client.send(":STOP")
            actual = await self._read_run_state(client)
        if actual != target_state:
            raise ScpiError(
                f"restore confirmation mismatch: wanted {target_state}, got {actual}"
            )

        # Re-check on a fresh connection after a short settle window.  This
        # catches a state command that was accepted but only became visible
        # between the first read-back and now.  A changed state means the
        # restoration is NOT confirmed.
        await asyncio.sleep(settings.restore_settle)
        async with ScpiClient(config, settings.command_timeout) as verify:
            await verify.query_text("*OPC?")
            confirmed = await self._read_run_state(verify)
        if confirmed != target_state:
            raise ScpiError(
                "restore confirmation changed after settle: "
                f"wanted {target_state}, now {confirmed}"
            )
        return confirmed

    @staticmethod
    async def _read_run_state(client: ScpiClient) -> str:
        reply = await client.query_text(":RUN?")
        if reply == "1":
            return RUNNING
        if reply == "0":
            return STOPPED
        raise ScpiError(f"unexpected :RUN? reply {reply!r}")

    # ----------------------------------------------------------------- recovery

    def _ensure_recovery_worker(self, instrument: str) -> None:
        """Start (or wake) the persistent per-instrument recovery worker."""
        if self._closing:
            return
        wake = self._recovery_wake.setdefault(instrument, asyncio.Event())
        wake.set()
        current = self._recovery_tasks.get(instrument)
        if current is not None and not current.done():
            return
        self._recovery_tasks[instrument] = asyncio.create_task(
            self._recovery_loop(instrument), name=f"recover-{instrument}"
        )

    async def _recovery_loop(self, instrument: str) -> None:
        """Retry pending restorations until confirmed; survive repeated exits.

        The intent stays pending across every failure (including the
        instrument exiting again mid-restore).  Only a confirmed read-back
        clears it, flips the affected rows to ``restored=true`` and reopens the
        gate.  The target is read from SQLite each cycle, so it is never lost
        if this task is cancelled and the next process resumes it.
        """
        config = self._registry.config(instrument)
        wake = self._recovery_wake.setdefault(instrument, asyncio.Event())
        interval = max(0.01, settings.recovery_retry_interval)

        while not self._closing:
            try:
                outcome = await self._recovery_cycle(config, instrument)
                if outcome == "confirmed":
                    continue
                if outcome == "failed":
                    # Keep the gate closed and back off before trying again;
                    # wake immediately if a new intent arrives meanwhile.
                    wake.clear()
                    try:
                        await asyncio.wait_for(wake.wait(), timeout=interval)
                    except TimeoutError:
                        pass
                    continue

                # No pending intent: release the gate.  The recovery state
                # lock serializes this check with intent creation, so a new
                # intent cannot be missed between the check and the open.
                # Lock ordering is recovery-state -> (none): the instrument
                # lock is never taken here, so there is no deadlock path.
                wake.clear()
                async with self._recovery_state_lock:
                    if await self._intent_is_pending(instrument):
                        continue
                    if not self._registry.gate_is_open(instrument):
                        await self._registry.open_gate(instrument)
                try:
                    await asyncio.wait_for(wake.wait(), timeout=interval)
                except TimeoutError:
                    pass
            except asyncio.CancelledError:
                # Intent stays durable; the next process resumes the same
                # target.  The gate remains closed in this process, which is
                # about to exit anyway.
                raise
            except Exception:
                # Never let an unexpected error kill the retry loop or lose
                # the durable intent.
                LOG.exception("unexpected recovery error for %s; will retry", instrument)
                await asyncio.sleep(interval)

    async def _intent_is_pending(self, instrument: str) -> bool:
        row = await db.get_recovery(self._conn, instrument)
        return row is not None and bool(int(row["pending"]))

    async def _recovery_cycle(self, config: InstrumentConfig, instrument: str) -> str:
        """Perform one restoration attempt if an intent is pending.

        Returns ``"idle"`` when there is nothing to restore, ``"confirmed"``
        after a verified restoration, and ``"failed"`` when the attempt failed.
        On failure the intent remains pending and the gate stays closed.
        """
        if not await self._intent_is_pending(instrument):
            return "idle"

        await self._registry.close_gate(instrument)
        async with self._registry.reserve_blocking(instrument):
            row = await db.get_recovery(self._conn, instrument)
            if row is None or not int(row["pending"]):
                # Confirmed elsewhere while waiting for the lock.
                return "idle"
            target = str(row["target_state"])
            if target not in VALID_RUN_STATES:
                LOG.error(
                    "recovery for %s has invalid target %r; using STOPPED",
                    instrument,
                    target,
                )
                target = STOPPED

            try:
                actual = await self._restore_with_retry(config, target)
            except ScpiError as exc:
                try:
                    await db.record_recovery_attempt(
                        self._conn,
                        instrument,
                        error_code="RESTORE_PENDING",
                        error_message=str(exc),
                    )
                    updated = await db.get_recovery(self._conn, instrument)
                    attempts = int(updated["attempts"]) if updated is not None else 0
                except Exception:
                    LOG.exception("could not record recovery attempt for %s", instrument)
                    attempts = 0
                self._registry.record_status(
                    instrument,
                    {
                        "pending_restore": True,
                        "target_state": target,
                        "attempts": attempts,
                        "last_error": str(exc),
                        "updated_at": db.utcnow(),
                    },
                )
                LOG.warning(
                    "recovery for %s -> %s still pending (attempts=%d): %s",
                    instrument, target, attempts, exc,
                )
                return "failed"
            except asyncio.CancelledError:
                raise

            # Confirmed.  Mark the rows first, then clear the intent.  A crash
            # in between leaves the intent pending (a harmless re-restore), so
            # the target can never be lost.  The gate opens only after both.
            await db.mark_instrument_rows_restored(self._conn, instrument)
            await db.clear_recovery(self._conn, instrument)
            self._registry.record_status(
                instrument,
                {
                    "pending_restore": False,
                    "target_state": target,
                    "restored_state": actual,
                    "restored_at": db.utcnow(),
                },
            )
        await self._registry.open_gate(instrument)
        LOG.info("recovery for %s confirmed %s; gate reopened", instrument, actual)
        return "confirmed"
