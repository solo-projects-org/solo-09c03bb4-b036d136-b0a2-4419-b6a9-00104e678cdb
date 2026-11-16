"""FastAPI entry point and HTTP routing."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from . import database as db
from .config import settings
from .schemas import (
    AcquisitionCreated,
    AcquisitionCreate,
    AcquisitionStatus,
    Health,
    InstrumentStatus,
    WaveformResult,
)
from .scpi import ScpiClient, ScpiError
from .service import AcquisitionService, InstrumentRegistry, UnknownInstrumentError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("api")

registry = InstrumentRegistry(settings.instruments)
service: AcquisitionService | None = None
conn = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global service, conn
    conn = await db.connect()
    service = AcquisitionService(conn, registry)
    # Mark interrupted rows as failed and restore every affected instrument
    # before the API starts accepting traffic.
    await service.startup_recovery()
    LOG.info("startup recovery finished; API ready")
    yield
    if conn is not None:
        await conn.close()


app = FastAPI(
    title="Oscilloscope Waveform Acquisition API",
    version="1.0.0",
    description=(
        "Single-instance acquisition service for TCP SCPI oscilloscopes. "
        "Pauses the selected instrument/channel, reads one waveform as an "
        "IEEE 488.2 int16 little-endian block with its X/Y preamble, retains "
        "raw codes plus scaling, and always restores the previous run state."
    ),
    lifespan=lifespan,
)


def _require_service() -> AcquisitionService:
    assert service is not None
    return service


def _row_to_status(row) -> AcquisitionStatus:
    result = None
    if row["state"] == db.SUCCESS:
        result = WaveformResult(
            point_count=row["point_count"],
            x_origin=row["x_origin"],
            x_increment=row["x_increment"],
            y_origin=row["y_origin"],
            y_increment=row["y_increment"],
            raw_codes=json.loads(row["raw_codes"]),
            times=json.loads(row["times"]),
            voltages=json.loads(row["voltages"]),
        )
    return AcquisitionStatus(
        acquisition_id=row["id"],
        instrument=row["instrument"],
        channel=row["channel"],
        state=row["state"],
        previous_state=row["previous_state"],
        restored=bool(row["restored"]),
        error_code=row["error_code"],
        error_message=row["error_message"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        result=result,
    )


@app.get("/health", response_model=Health, tags=["system"])
async def health() -> Health:
    return Health(status="ok", instruments=registry.names)


@app.get("/instruments", response_model=list[InstrumentStatus], tags=["instruments"])
async def list_instruments() -> list[InstrumentStatus]:
    output: list[InstrumentStatus] = []
    for name in registry.names:
        config = registry.config(name)
        live_state: str | None = None
        # Read-only liveness probe; on failure we report null instead of 500.
        try:
            async with ScpiClient(config, settings.command_timeout) as client:
                reply = await client.query_text(":RUN?")
                live_state = "RUNNING" if reply == "1" else "STOPPED"
        except ScpiError as exc:
            LOG.info("liveness probe for %s failed: %s", name, exc)
        output.append(
            InstrumentStatus(
                name=name,
                host=config.host,
                port=config.port,
                busy=registry.is_busy(name),
                queued=registry.queued(name),
                gate_open=registry.gate_is_open(name),
                live_run_state=live_state,
                last_restore=registry.last_status(name),
            )
        )
    return output


@app.post(
    "/instruments/{instrument}/acquisitions",
    response_model=AcquisitionCreated,
    status_code=202,
    tags=["acquisitions"],
)
async def create_acquisition(instrument: str, payload: AcquisitionCreate) -> AcquisitionCreated:
    svc = _require_service()
    try:
        acquisition_id = await svc.create_and_spawn(instrument, payload.channel)
    except UnknownInstrumentError:
        raise HTTPException(status_code=404, detail=f"unknown instrument {instrument!r}")
    return AcquisitionCreated(
        acquisition_id=acquisition_id,
        instrument=instrument,
        channel=payload.channel,
        state=db.PENDING,
    )


@app.get(
    "/acquisitions/{acquisition_id}",
    response_model=AcquisitionStatus,
    tags=["acquisitions"],
)
async def get_acquisition(acquisition_id: int) -> AcquisitionStatus:
    svc = _require_service()
    row = await svc.get(acquisition_id)
    if row is None:
        raise HTTPException(status_code=404, detail="acquisition not found")
    return _row_to_status(row)


@app.get("/acquisitions", response_model=list[AcquisitionStatus], tags=["acquisitions"])
async def list_acquisitions(
    instrument: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> list[AcquisitionStatus]:
    svc = _require_service()
    try:
        rows = await svc.list(instrument, limit)
    except UnknownInstrumentError:
        raise HTTPException(status_code=404, detail=f"unknown instrument {instrument!r}")
    return [_row_to_status(row) for row in rows]


@app.get(
    "/acquisitions/{acquisition_id}/result",
    response_model=WaveformResult,
    tags=["acquisitions"],
)
async def get_result(acquisition_id: int) -> WaveformResult:
    svc = _require_service()
    row = await svc.get(acquisition_id)
    if row is None:
        raise HTTPException(status_code=404, detail="acquisition not found")
    if row["state"] != db.SUCCESS:
        raise HTTPException(
            status_code=409,
            detail={
                "state": row["state"],
                "error_code": row["error_code"],
                "error_message": row["error_message"],
                "note": "no waveform is available for non-successful acquisitions",
            },
        )
    return WaveformResult(
        point_count=row["point_count"],
        x_origin=row["x_origin"],
        x_increment=row["x_increment"],
        y_origin=row["y_origin"],
        y_increment=row["y_increment"],
        raw_codes=json.loads(row["raw_codes"]),
        times=json.loads(row["times"]),
        voltages=json.loads(row["voltages"]),
    )
