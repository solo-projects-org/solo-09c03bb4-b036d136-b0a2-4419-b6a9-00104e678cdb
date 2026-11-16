"""SCPI simulator protocol tests, including fault injection."""

from __future__ import annotations

import asyncio

import pytest

from app.config import InstrumentConfig
from app.scpi import ScpiClient, ScpiDeviceError, ScpiProtocolError, ScpiTimeoutError


def _client(sim, timeout: float = 3.0) -> ScpiClient:
    return ScpiClient(InstrumentConfig(sim["name"], sim["host"], sim["port"]), timeout=timeout)


@pytest.mark.asyncio
async def test_idn_run_stop_and_opc(simulator):
    async with _client(simulator) as client:
        assert "LABSCOPE" in await client.query_text("*IDN?")
        assert await client.query_text(":RUN?") == "1"
        await client.send(":STOP")
        assert await client.query_text(":RUN?") == "0"
        await client.send(":RUN")
        assert await client.query_text(":RUN?") == "1"
        assert await client.query_text("*OPC?") == "1"
        assert await client.query_text(":BUSY?") == "0"


@pytest.mark.asyncio
async def test_preamble_and_ieee_block(simulator):
    async with _client(simulator) as client:
        await client.send(":STOP")
        points = await client.query_int(":WAV:POIN?")
        assert points == 2400
        xinc = await client.query_float(":WAV:XINC?")
        yinc = await client.query_float(":WAV:YINC?")
        codes = await client.query_ieee_block_int16_le(":WAV:DATA?")
        assert len(codes) == points
        assert all(-32768 <= c <= 32767 for c in codes)
        # Frozen waveform: repeated reads return identical codes.
        again = await client.query_ieee_block_int16_le(":WAV:DATA?")
        assert again == codes
        assert xinc > 0 and yinc > 0
        await client.send(":RUN")


@pytest.mark.asyncio
async def test_channel_isolation(simulator):
    async with _client(simulator) as client:
        await client.send(":STOP")
        xinc1 = await client.query_float(":WAV:XINC?")
        xinc2 = await client.query_float(":WAV:XINC? (@2)")
        assert xinc1 != xinc2
        codes1 = await client.query_ieee_block_int16_le(":WAV:DATA? (@1)")
        codes2 = await client.query_ieee_block_int16_le(":WAV:DATA? (@2)")
        assert codes1 != codes2
        with pytest.raises(ScpiDeviceError):
            await client.query_text(":CHAN9:POS?")
        await client.send(":RUN")


@pytest.mark.asyncio
async def test_stream_cut_truncates_block(simulator):
    async with _client(simulator) as client:
        await client.send(":STOP")
        await client.send(":SYST:CUT 1")
        with pytest.raises((ScpiProtocolError, ScpiTimeoutError)):
            await client.query_ieee_block_int16_le(":WAV:DATA?")
    # One-shot fault cleared; a fresh connection still serves the stopped scope.
    async with _client(simulator) as client:
        assert await client.query_text(":RUN?") == "0"
        codes = await client.query_ieee_block_int16_le(":WAV:DATA?")
        assert len(codes) == 2400
        await client.send(":RUN")


@pytest.mark.asyncio
async def test_slow_response_times_out(simulator):
    async with _client(simulator) as client:
        await client.send(":SYST:DELAY 0.5")
    slow = ScpiClient(
        InstrumentConfig(simulator["name"], simulator["host"], simulator["port"]),
        timeout=0.15,
    )
    with pytest.raises(ScpiTimeoutError):
        async with slow:
            await slow.query_text(":RUN?")
    async with _client(simulator) as client:
        await client.send(":SYST:DELAY 0")
        assert await client.query_text(":RUN?") == "1"


@pytest.mark.asyncio
async def test_commands_keep_running_after_disconnect(simulator):
    # Open a connection, enqueue :STOP then drop immediately; the serial worker
    # must still execute the STOP.
    reader, writer = await asyncio.open_connection(simulator["host"], simulator["port"])
    writer.write(b":STOP\n")
    await writer.drain()
    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionError, RuntimeError):
        pass
    await asyncio.sleep(0.3)
    async with _client(simulator) as client:
        assert await client.query_text(":RUN?") == "0"
        await client.send(":RUN")


@pytest.mark.asyncio
async def test_drop_injection_closes_connection(simulator):
    # The counter must survive a connection change, since fault injection is
    # instrument-wide state.
    async with _client(simulator) as client:
        await client._write_line(":SYST:DROP 1")  # raw write: no *OPC? handshake
    async with _client(simulator) as client:
        with pytest.raises(ScpiProtocolError):
            await client.query_text(":RUN?")
    # Counter consumed; a further connection works normally again.
    async with _client(simulator) as client:
        assert await client.query_text(":RUN?") == "1"


@pytest.mark.asyncio
async def test_serial_execution_never_interleaves(simulator):
    # Two connections hammering queries must each get complete, parseable replies.
    async def session() -> None:
        async with _client(simulator) as client:
            for _ in range(5):
                reply = await client.query_text(":WAV:POIN?")
                assert reply == "2400"
                codes = await client.query_ieee_block_int16_le(":WAV:DATA?")
                assert len(codes) == 2400

    await asyncio.gather(session(), session(), session())
