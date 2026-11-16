"""HTTP-level tests driving the real ASGI app via httpx in-process transport."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app import database as db
from app.config import InstrumentConfig, settings
from app.main import app, registry
from app.service import AcquisitionService


@pytest_asyncio.fixture
async def client(two_simulators, tmp_path):
    configs = tuple(
        InstrumentConfig(sim["name"], sim["host"], sim["port"]) for sim in two_simulators
    )
    registry._configs = {c.name: c for c in configs}  # noqa: SLF001
    registry._locks = {name: asyncio.Lock() for name in registry._configs}  # noqa: SLF001
    registry._gates = {name: asyncio.Event() for name in registry._configs}  # noqa: SLF001
    for event in registry._gates.values():  # noqa: SLF001
        event.set()
    registry._queued = {name: 0 for name in registry._configs}  # noqa: SLF001
    registry._last_status = {}  # noqa: SLF001

    settings.db_path = str(tmp_path / "http.db")
    settings.command_timeout = 3.0
    settings.restore_attempts = 3
    settings.restore_backoff = 0.05
    settings.recovery_retry_interval = 0.1
    settings.restore_settle = 0.0
    settings.instruments = configs

    connection = await db.connect()
    service = AcquisitionService(connection, registry)
    import app.main as main_module

    main_module.service = service
    main_module.conn = connection

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http:
        yield http, service, two_simulators
    await connection.close()


async def _wait_state(http: AsyncClient, acquisition_id: int, terminal: set[str]) -> dict:
    for _ in range(200):
        response = await http.get(f"/acquisitions/{acquisition_id}")
        assert response.status_code == 200
        body = response.json()
        if body["state"] in terminal:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError("acquisition never reached a terminal state")


@pytest.mark.asyncio
async def test_health_and_instruments(client):
    http, _, sims = client
    response = await http.get("/health")
    assert response.status_code == 200
    assert response.json()["instruments"] == ["inst1", "inst2"]

    response = await http.get("/instruments")
    assert response.status_code == 200
    statuses = {item["name"]: item for item in response.json()}
    assert statuses["inst1"]["live_run_state"] == "RUNNING"
    assert statuses["inst2"]["busy"] is False


@pytest.mark.asyncio
async def test_create_poll_result_flow(client):
    http, _, _ = client
    response = await http.post("/instruments/inst1/acquisitions", json={"channel": 2})
    assert response.status_code == 202
    acquisition_id = response.json()["acquisition_id"]

    body = await _wait_state(http, acquisition_id, {"SUCCESS", "FAILED"})
    assert body["state"] == "SUCCESS"
    assert body["restored"] is True
    assert body["previous_state"] == "RUNNING"
    assert body["result"]["point_count"] == 2400
    assert len(body["result"]["raw_codes"]) == 2400

    result = await http.get(f"/acquisitions/{acquisition_id}/result")
    assert result.status_code == 200
    payload = result.json()
    assert payload["point_count"] == 2400
    # Engineering conversion matches raw * scaling.
    expected_v = payload["y_origin"] + payload["raw_codes"][10] * payload["y_increment"]
    assert payload["voltages"][10] == pytest.approx(expected_v)
    assert payload["times"][10] == pytest.approx(10 * payload["x_increment"])


@pytest.mark.asyncio
async def test_unknown_instrument_and_bad_channel(client):
    http, _, _ = client
    response = await http.post("/instruments/nope/acquisitions", json={"channel": 1})
    assert response.status_code == 404
    response = await http.post("/instruments/inst1/acquisitions", json={"channel": 0})
    assert response.status_code == 422
    response = await http.get("/acquisitions/99999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_failed_acquisition_has_no_result(client):
    http, _, sims = client
    sims[0]["instrument"].state.cut_enabled = True
    response = await http.post("/instruments/inst1/acquisitions", json={"channel": 1})
    acquisition_id = response.json()["acquisition_id"]
    body = await _wait_state(http, acquisition_id, {"SUCCESS", "FAILED"})
    assert body["state"] == "FAILED"
    assert body["result"] is None

    result = await http.get(f"/acquisitions/{acquisition_id}/result")
    assert result.status_code == 409

    listing = await http.get("/acquisitions", params={"instrument": "inst1"})
    assert listing.status_code == 200
    assert any(item["acquisition_id"] == acquisition_id for item in listing.json())


@pytest.mark.asyncio
async def test_concurrent_requests_same_instrument(client):
    http, _, _ = client
    responses = await asyncio.gather(*[
        http.post("/instruments/inst2/acquisitions", json={"channel": 1}) for _ in range(4)
    ])
    ids = [r.json()["acquisition_id"] for r in responses]
    assert len(set(ids)) == 4
    bodies = [await _wait_state(http, acquisition_id, {"SUCCESS"}) for acquisition_id in ids]
    assert all(body["state"] == "SUCCESS" for body in bodies)


@pytest.mark.asyncio
async def test_pending_restore_blocks_only_that_instrument_over_http(client):
    http, _, sims = client
    inst1 = sims[0]["instrument"]

    # Refuse every run/stop state command from now on: the capture fails, and
    # restoration to the recorded RUNNING target can never be confirmed.
    original_submit = inst1.submit
    blocked = {"on": True}

    def submit(line, writer):
        if blocked["on"] and line.strip().upper() in (":RUN", ":STOP"):
            try:
                writer.close()
            except RuntimeError:
                pass
            return
        original_submit(line, writer)

    inst1.submit = submit  # type: ignore[method-assign]
    try:
        response = await http.post("/instruments/inst1/acquisitions", json={"channel": 1})
        assert response.status_code == 202
        failed_id = response.json()["acquisition_id"]
        body = await _wait_state(http, failed_id, {"FAILED"})
        assert body["restored"] is False
        assert body["restore_pending"] is True
        assert body["previous_state"] == "RUNNING"
        assert body["result"] is None
        assert body["error_code"] in {"SCPIPROTOCOLERROR", "SCPITIMEOUTERROR", "TIMEOUT"}

        # No waveform for the failed row, and no silent success.
        result = await http.get(f"/acquisitions/{failed_id}/result")
        assert result.status_code == 409

        # A later acquisition must be accepted but must not touch inst1.
        queued = await http.post("/instruments/inst1/acquisitions", json={"channel": 1})
        assert queued.status_code == 202
        queued_id = queued.json()["acquisition_id"]
        await asyncio.sleep(0.2)
        pending = (await http.get(f"/acquisitions/{queued_id}")).json()
        assert pending["state"] == "PENDING"

        # Status remains available and the other instrument is fully usable.
        statuses = {item["name"]: item for item in (await http.get("/instruments")).json()}
        assert statuses["inst1"]["gate_open"] is False
        assert statuses["inst1"]["last_restore"]["pending_restore"] is True
        assert statuses["inst2"]["gate_open"] is True

        other = await http.post("/instruments/inst2/acquisitions", json={"channel": 1})
        other_id = other.json()["acquisition_id"]
        other_body = await _wait_state(http, other_id, {"SUCCESS", "FAILED"})
        assert other_body["state"] == "SUCCESS"
    finally:
        blocked["on"] = False
        inst1.submit = original_submit  # type: ignore[method-assign]

    # Automatic hand-off: the original RUNNING target is restored, the failed
    # row flips to restored, the gate opens, and the queued acquisition runs.
    restored = await _wait_field(http, failed_id, "restored", True)
    assert restored["state"] == "FAILED"
    assert restored["restore_pending"] is False

    queued_body = await _wait_state(http, queued_id, {"SUCCESS", "FAILED"})
    assert queued_body["state"] == "SUCCESS"
    assert queued_body["restored"] is True
    assert queued_body["previous_state"] == "RUNNING"


async def _wait_field(http: AsyncClient, acquisition_id: int, field: str, value):
    for _ in range(300):
        body = (await http.get(f"/acquisitions/{acquisition_id}")).json()
        if body[field] == value:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"{field} never became {value!r}")


@pytest.mark.asyncio
async def test_status_endpoint_survives_offline_instrument(client):
    http, _, sims = client
    # Point inst1 at a closed port; status must not wait for it or fail.
    from app.scpi import ScpiClient as _Client  # noqa: F401  (import sanity)

    dead_port = 1  # connection refused immediately
    registry._configs["inst1"] = InstrumentConfig(  # noqa: SLF001
        "inst1", "127.0.0.1", dead_port
    )
    started = asyncio.get_running_loop().time()
    response = await http.get("/instruments")
    elapsed = asyncio.get_running_loop().time() - started
    assert response.status_code == 200
    assert elapsed < 2.0
    statuses = {item["name"]: item for item in response.json()}
    assert statuses["inst1"]["live_run_state"] is None
    assert statuses["inst2"]["live_run_state"] == "RUNNING"

    # Health and acquisition status queries are unaffected as well.
    assert (await http.get("/health")).status_code == 200
    assert (await http.get("/acquisitions")).status_code == 200
    # inst2 still captures normally while inst1 is unreachable.
    response = await http.post("/instruments/inst2/acquisitions", json={"channel": 1})
    body = await _wait_state(http, response.json()["acquisition_id"], {"SUCCESS", "FAILED"})
    assert body["state"] == "SUCCESS"
