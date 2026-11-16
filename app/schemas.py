"""Pydantic response/request models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AcquisitionCreate(BaseModel):
    channel: int = Field(..., ge=1, le=99, description="Oscilloscope channel number")


class AcquisitionCreated(BaseModel):
    acquisition_id: int
    instrument: str
    channel: int
    state: str
    detail: str = "accepted; poll GET /acquisitions/{id} for status"


class WaveformResult(BaseModel):
    point_count: int
    x_origin: float
    x_increment: float
    y_origin: float
    y_increment: float
    raw_codes: list[int]
    times: list[float]
    voltages: list[float]


class AcquisitionStatus(BaseModel):
    acquisition_id: int
    instrument: str
    channel: int
    state: str
    previous_state: str | None
    restored: bool
    error_code: str | None
    error_message: str | None
    created_at: str
    updated_at: str
    result: WaveformResult | None = None


class InstrumentStatus(BaseModel):
    name: str
    host: str
    port: int
    busy: bool
    queued: int
    gate_open: bool
    live_run_state: str | None
    last_restore: dict[str, object] | None


class Health(BaseModel):
    status: str
    instruments: list[str]
