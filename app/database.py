"""SQLite persistence (async via aiosqlite).

One row per acquisition.  The row is created in ``PENDING`` before any SCPI
traffic starts, so an abrupt API restart can always find in-flight work, mark
it failed and drive the instrument back to its pre-acquisition state.
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
"""

# Terminal / lifecycle states.
PENDING = "PENDING"        # row created, SCPI work not necessarily started
IN_PROGRESS = "IN_PROGRESS"
RESTORING = "RESTORING"    # capture finished/failed, restoring instrument state
SUCCESS = "SUCCESS"
FAILED = "FAILED"

TERMINAL_STATES = (SUCCESS, FAILED)


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
) -> None:
    await update_state(
        conn,
        acquisition_id,
        RESTORING,
        point_count=point_count,
        x_origin=x_origin,
        x_increment=x_increment,
        y_origin=y_origin,
        y_increment=y_increment,
        raw_codes=json.dumps(raw_codes),
        times=json.dumps(times),
        voltages=json.dumps(voltages),
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


async def mark_terminal(conn: aiosqlite.Connection, acquisition_id: int, state: str) -> None:
    assert state in TERMINAL_STATES
    await update_state(conn, acquisition_id, state, restored=1)


async def mark_restart_failed(conn: aiosqlite.Connection, acquisition_id: int, message: str) -> None:
    """Used at startup: the process died while this acquisition was active."""
    await update_state(
        conn,
        acquisition_id,
        FAILED,
        error_code="API_RESTARTED",
        error_message=message,
        restored=0,
    )


async def pending_acquisition_ids(conn: aiosqlite.Connection) -> list[int]:
    async with conn.execute(
        f"""
        SELECT id FROM acquisition
        WHERE state IN (?, ?, ?)
        ORDER BY id
        """,
        (PENDING, IN_PROGRESS, RESTORING),
    ) as cursor:
        return [int(row[0]) for row in await cursor.fetchall()]
