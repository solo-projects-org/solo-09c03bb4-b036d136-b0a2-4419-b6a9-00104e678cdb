"""SQLite persistence (async via aiosqlite).

One row per acquisition.  The row is created in ``PENDING`` before any SCPI
traffic starts, so an abrupt API restart can always find in-flight work, mark
it failed and drive the instrument back to its pre-acquisition state.

A second, per-instrument table stores the *restoration intent*: the run/stop
state the instrument must be driven back to, plus whether it has been confirmed
yet.  It is written before any restoration attempt and only cleared after a
confirmed ``:RUN?`` read-back, which makes the target durable across process
restarts and lets an offline instrument be recovered later without ever losing
the original RUNNING/STOPPED goal.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import aiosqlite

from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS acquisition (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument       TEXT NOT NULL,
    channel          INTEGER NOT NULL,
    state            TEXT NOT NULL,
    previous_state   TEXT,
    point_count      INTEGER,
    x_origin         REAL,
    x_increment      REAL,
    y_origin         REAL,
    y_increment      REAL,
    raw_codes        TEXT,
    times            TEXT,
    voltages         TEXT,
    error_code       TEXT,
    error_message    TEXT,
    restored         INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_acq_state ON acquisition(state);
CREATE INDEX IF NOT EXISTS idx_acq_instrument ON acquisition(instrument, id);

CREATE TABLE IF NOT EXISTS instrument_recovery (
    instrument       TEXT PRIMARY KEY,
    target_state     TEXT NOT NULL,
    pending          INTEGER NOT NULL DEFAULT 1,
    acquisition_id   INTEGER,
    error_code       TEXT,
    error_message    TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_pending ON instrument_recovery(pending);
"""

# Terminal / lifecycle states.
PENDING = "PENDING"        # row created, SCPI work not necessarily started
IN_PROGRESS = "IN_PROGRESS"
RESTORING = "RESTORING"    # capture finished/failed, restoring instrument state
SUCCESS = "SUCCESS"
FAILED = "FAILED"

TERMINAL_STATES = (SUCCESS, FAILED)
NON_TERMINAL_STATES = (PENDING, IN_PROGRESS, RESTORING)


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


async def connect() -> aiosqlite.Connection:
    db_path = settings.db_path
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.executescript(SCHEMA)
    await conn.commit()
    return conn


async def create_acquisition(conn: aiosqlite.Connection, instrument: str, channel: int) -> int:
    now = utcnow()
    cursor = await conn.execute(
        """
        INSERT INTO acquisition
            (instrument, channel, state, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (instrument, channel, PENDING, now, now),
    )
    await conn.commit()
    return int(cursor.lastrowid)


async def get_acquisition(conn: aiosqlite.Connection, acquisition_id: int) -> aiosqlite.Row | None:
    async with conn.execute("SELECT * FROM acquisition WHERE id = ?", (acquisition_id,)) as cursor:
        return await cursor.fetchone()


async def list_acquisitions(
    conn: aiosqlite.Connection,
    instrument: str | None = None,
    limit: int = 100,
) -> list[aiosqlite.Row]:
    if instrument:
        query = "SELECT * FROM acquisition WHERE instrument = ? ORDER BY id DESC LIMIT ?"
        params: tuple = (instrument, limit)
    else:
        query = "SELECT * FROM acquisition ORDER BY id DESC LIMIT ?"
        params = (limit,)
    async with conn.execute(query, params) as cursor:
        return list(await cursor.fetchall())


async def update_state(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    state: str,
    **fields: object,
) -> None:
    assignments = ["state = ?", "updated_at = ?"]
    params: list[object] = [state, utcnow()]
    for key, value in fields.items():
        assignments.append(f"{key} = ?")
        params.append(value)
    params.append(acquisition_id)
    await conn.execute(
        f"UPDATE acquisition SET {', '.join(assignments)} WHERE id = ?",
        params,
    )
    await conn.commit()


async def mark_in_progress(conn: aiosqlite.Connection, acquisition_id: int, previous_state: str) -> None:
    await update_state(conn, acquisition_id, IN_PROGRESS, previous_state=previous_state)


async def mark_restoring(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    *,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    """Capture finished (successfully or not); instrument restore is in flight.

    No waveform is persisted here.  Partial or complete samples are only
    written once the instrument has been confirmed back in its prior state, so
    a FAILED row can never serve a waveform.
    """
    await update_state(
        conn,
        acquisition_id,
        RESTORING,
        error_code=error_code,
        error_message=error_message,
    )


async def mark_success(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    *,
    point_count: int,
    x_origin: float,
    x_increment: float,
    y_origin: float,
    y_increment: float,
    raw_codes: list[int],
    times: list[float],
    voltages: list[float],
    restored: bool,
) -> None:
    """Write the waveform and finalize as SUCCESS.

    Only called after a confirmed restoration; ``restored`` stays false if the
    caller could not confirm it (defensive, should not normally happen).
    """
    await update_state(
        conn,
        acquisition_id,
        SUCCESS,
        point_count=point_count,
        x_origin=x_origin,
        x_increment=x_increment,
        y_origin=y_origin,
        y_increment=y_increment,
        raw_codes=json.dumps(raw_codes),
        times=json.dumps(times),
        voltages=json.dumps(voltages),
        restored=1 if restored else 0,
    )


async def mark_failure(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    error_code: str,
    error_message: str,
) -> None:
    await update_state(
        conn,
        acquisition_id,
        RESTORING,
        error_code=error_code,
        error_message=error_message,
    )


async def mark_terminal(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    state: str,
    *,
    restored: bool,
) -> None:
    """Finalize an acquisition.

    ``restored`` is explicit: a failed capture whose instrument could not be
    confirmed back to its original state must stay ``restored=0`` (visible as
    pending restoration) until a later recovery guarantees it.
    """
    assert state in TERMINAL_STATES
    await update_state(conn, acquisition_id, state, restored=1 if restored else 0)


async def mark_restart_failed(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    message: str,
    *,
    restored: bool = False,
) -> None:
    """Used at startup: the process died while this acquisition was active.

    ``restored`` is only true when the row provably never modified the
    instrument (the pre-state was never read).  Otherwise the row stays
    ``restored=0`` until the instrument-level recovery intent is confirmed.
    """
    await update_state(
        conn,
        acquisition_id,
        FAILED,
        error_code="API_RESTARTED",
        error_message=message,
        restored=1 if restored else 0,
    )


async def set_restored(
    conn: aiosqlite.Connection,
    acquisition_id: int,
    restored: bool,
) -> None:
    await conn.execute(
        "UPDATE acquisition SET restored = ?, updated_at = ? WHERE id = ?",
        (1 if restored else 0, utcnow(), acquisition_id),
    )
    await conn.commit()


async def pending_acquisition_ids(conn: aiosqlite.Connection) -> list[int]:
    async with conn.execute(
        f"""
        SELECT id FROM acquisition
        WHERE state IN (?, ?, ?)
        ORDER BY id
        """,
        NON_TERMINAL_STATES,
    ) as cursor:
        return [int(row[0]) for row in await cursor.fetchall()]


async def unrestored_acquisitions(conn: aiosqlite.Connection) -> list[aiosqlite.Row]:
    """Rows that still await a confirmed instrument restoration."""
    async with conn.execute(
        """
        SELECT * FROM acquisition
        WHERE restored = 0
        ORDER BY id
        """
    ) as cursor:
        return list(await cursor.fetchall())


# --------------------------------------------------------------------------
# Instrument restoration intents
# --------------------------------------------------------------------------


async def get_recovery(conn: aiosqlite.Connection, instrument: str) -> aiosqlite.Row | None:
    async with conn.execute(
        "SELECT * FROM instrument_recovery WHERE instrument = ?", (instrument,)
    ) as cursor:
        return await cursor.fetchone()


async def pending_recoveries(conn: aiosqlite.Connection) -> list[aiosqlite.Row]:
    async with conn.execute(
        "SELECT * FROM instrument_recovery WHERE pending = 1 ORDER BY instrument"
    ) as cursor:
        return list(await cursor.fetchall())


async def upsert_pending_recovery(
    conn: aiosqlite.Connection,
    instrument: str,
    target_state: str,
    *,
    acquisition_id: int | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    """Persist the restoration goal before any attempt is made.

    If an intent is already pending, its target is preserved.  A newer failure
    must never overwrite the original RUNNING/STOPPED goal (acquisitions are
    blocked while pending, so this is defensive).  ``attempts`` is retained
    across updates so operators can see how long an instrument has been
    waiting.
    """
    now = utcnow()
    await conn.execute(
        """
        INSERT INTO instrument_recovery
            (instrument, target_state, pending, acquisition_id,
             error_code, error_message, attempts, created_at, updated_at)
        VALUES (?, ?, 1, ?, ?, ?, 0, ?, ?)
        ON CONFLICT(instrument) DO UPDATE SET
            pending = 1,
            target_state = CASE
                WHEN instrument_recovery.pending = 1
                THEN instrument_recovery.target_state
                ELSE excluded.target_state
            END,
            acquisition_id = COALESCE(
                excluded.acquisition_id, instrument_recovery.acquisition_id
            ),
            error_code = excluded.error_code,
            error_message = excluded.error_message,
            attempts = CASE
                WHEN instrument_recovery.pending = 1
                THEN instrument_recovery.attempts
                ELSE 0
            END,
            created_at = CASE
                WHEN instrument_recovery.pending = 1
                THEN instrument_recovery.created_at
                ELSE excluded.created_at
            END,
            updated_at = excluded.updated_at
        """,
        (
            instrument,
            target_state,
            acquisition_id,
            error_code,
            error_message,
            now,
            now,
        ),
    )
    await conn.commit()


async def record_recovery_attempt(
    conn: aiosqlite.Connection,
    instrument: str,
    *,
    error_code: str,
    error_message: str,
) -> None:
    """Keep a pending intent pending and record the latest failed attempt."""
    await conn.execute(
        """
        UPDATE instrument_recovery
        SET pending = 1,
            attempts = attempts + 1,
            error_code = ?,
            error_message = ?,
            updated_at = ?
        WHERE instrument = ?
        """,
        (error_code, error_message, utcnow(), instrument),
    )
    await conn.commit()


async def clear_recovery(conn: aiosqlite.Connection, instrument: str) -> None:
    """Mark the intent confirmed.  Only call after a read-back verification."""
    await conn.execute(
        """
        UPDATE instrument_recovery
        SET pending = 0,
            error_code = NULL,
            error_message = NULL,
            updated_at = ?
        WHERE instrument = ?
        """,
        (utcnow(), instrument),
    )
    await conn.commit()


async def mark_instrument_rows_restored(
    conn: aiosqlite.Connection,
    instrument: str,
) -> None:
    """Flip acquisitions awaiting restoration once the instrument is confirmed.

    Terminal rows whose restoration was pending become ``restored=1``.  A row
    still in ``RESTORING`` can only belong to a process that died (or failed to
    finalize) mid-restoration; it is finalized as FAILED with its restoration
    now confirmed, and never exposes a waveform.  ``PENDING`` rows (queued
    acquisitions that never touched the device) and already-successful
    historical rows are untouched.
    """
    await conn.execute(
        """
        UPDATE acquisition
        SET state = ?,
            restored = 1,
            error_code = COALESCE(error_code, 'RESTORE_INTERRUPTED'),
            error_message = COALESCE(
                error_message,
                'restoration confirmed after the process stopped tracking this row'
            ),
            updated_at = ?
        WHERE instrument = ?
          AND restored = 0
          AND state IN (?, ?, ?)
        """,
        (FAILED, utcnow(), instrument, FAILED, SUCCESS, RESTORING),
    )
    await conn.commit()
