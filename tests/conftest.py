"""Pytest fixtures: in-process SCPI simulators on ephemeral ports."""

from __future__ import annotations

import asyncio
import socket

import pytest
import pytest_asyncio

from instrument.scpi_sim import ScpiInstrument, build_default_state, serve_instrument


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest_asyncio.fixture
async def simulator():
    name = "testinst"
    port = _free_port()
    instrument = ScpiInstrument(name, build_default_state(name))
    server = await serve_instrument("127.0.0.1", port, instrument)
    try:
        yield {"host": "127.0.0.1", "port": port, "instrument": instrument, "name": name}
    finally:
        await instrument.stop()
        server.close()
        await server.wait_closed()
        await asyncio.sleep(0)


@pytest_asyncio.fixture
async def two_simulators():
    ports = [_free_port(), _free_port()]
    instruments = []
    servers = []
    for name, port in zip(("inst1", "inst2"), ports, strict=True):
        inst = ScpiInstrument(name, build_default_state(name))
        server = await serve_instrument("127.0.0.1", port, inst)
        instruments.append(inst)
        servers.append(server)
    try:
        yield [
            {"host": "127.0.0.1", "port": port, "instrument": inst, "name": name}
            for name, port, inst in zip(("inst1", "inst2"), ports, instruments, strict=True)
        ]
    finally:
        for inst in instruments:
            await inst.stop()
        for server in servers:
            server.close()
            await server.wait_closed()
        await asyncio.sleep(0)
