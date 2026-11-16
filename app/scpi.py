"""Minimal async TCP SCPI client with IEEE 488.2 binary-block support.

Every operation is bounded by a per-command timeout so a slow or silent
instrument cannot wedge the acquisition worker.  A truncated binary block or a
connection reset while reading is surfaced as :class:`ScpiProtocolError`; the
caller treats every such error as a failed capture and restores the device.
"""

from __future__ import annotations

import asyncio
import struct
from collections.abc import Callable

from .config import InstrumentConfig


class ScpiError(Exception):
    """Base class for SCPI transport/protocol failures."""


class ScpiTimeoutError(ScpiError):
    pass


class ScpiProtocolError(ScpiError):
    pass


class ScpiDeviceError(ScpiError):
    """The instrument replied with an SCPI error (``-nnn,...`` text)."""


# Map asyncio/OS level failures onto our domain errors.
_TRANSIENT_ERRORS = (
    ConnectionError,
    BrokenPipeError,
    asyncio.IncompleteReadError,
    TimeoutError,
    OSError,
)


class ScpiClient:
    def __init__(
        self,
        config: InstrumentConfig,
        timeout: float,
        connect_timeout: float | None = None,
    ) -> None:
        self._config = config
        self._timeout = timeout
        # Connecting to a reachable host should not be punished by the short
        # per-command timeout used to catch slow SCPI responses.
        self._connect_timeout = connect_timeout or max(timeout, 3.0)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    @property
    def name(self) -> str:
        return self._config.name

    async def connect(self) -> None:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._config.host, self._config.port),
                timeout=self._connect_timeout,
            )
        except (TimeoutError, OSError) as exc:
            raise ScpiTimeoutError(
                f"connect to {self._config.host}:{self._config.port} failed: {exc}"
            ) from exc
        self._reader = reader
        self._writer = writer

    async def close(self) -> None:
        if self._writer is None:
            return
        writer, self._writer = self._writer, None
        self._reader = None
        # Fire-and-forget: wait_closed() can be cancelled when this call races
        # with the event loop tearing down a timed-out read, which would leak
        # a pending task.  Closing the transport is sufficient to free the socket.
        try:
            writer.close()
        except (OSError, RuntimeError):
            pass

    async def __aenter__(self) -> "ScpiClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def _write_line(self, command: str) -> None:
        if self._writer is None:
            raise ScpiProtocolError("client is not connected")
        try:
            self._writer.write(command.encode("ascii") + b"\n")
            await asyncio.wait_for(self._writer.drain(), timeout=self._timeout)
        except (TimeoutError, *_TRANSIENT_ERRORS) as exc:
            raise ScpiTimeoutError(f"write {command!r} failed: {exc}") from exc

    async def _read_line(self) -> bytes:
        if self._reader is None:
            raise ScpiProtocolError("client is not connected")
        try:
            data = await asyncio.wait_for(self._reader.readline(), timeout=self._timeout)
        except TimeoutError as exc:
            raise ScpiTimeoutError("read timed out") from exc
        except _TRANSIENT_ERRORS as exc:
            raise ScpiProtocolError(f"connection lost while reading: {exc}") from exc
        if not data:
            raise ScpiProtocolError("connection closed by instrument")
        return data

    async def query_text(self, command: str) -> str:
        await self._write_line(command)
        raw = await self._read_line()
        text = raw.decode("ascii", errors="replace").strip()
        if text.startswith("-") and "," in text:
            code, _, message = text.partition(",")
            raise ScpiDeviceError(f"instrument error {code}: {message}")
        return text

    async def query_float(self, command: str) -> float:
        text = await self.query_text(command)
        try:
            return float(text)
        except ValueError as exc:
            raise ScpiProtocolError(f"non-numeric reply to {command!r}: {text!r}") from exc

    async def query_int(self, command: str) -> int:
        text = await self.query_text(command)
        try:
            return int(text)
        except ValueError as exc:
            raise ScpiProtocolError(f"non-integer reply to {command!r}: {text!r}") from exc

    async def send(self, command: str) -> None:
        """Send a non-query command and confirm completion with ``*OPC?``.

        The simulator executes commands serially; ``*OPC?`` returning means the
        preceding command has finished, which gives us a bounded, explicit
        acknowledgement instead of fire-and-forget.
        """
        await self._write_line(command)
        ack = await self.query_text("*OPC?")
        if ack != "1":
            raise ScpiProtocolError(f"unexpected *OPC? reply: {ack!r}")

    async def query_ieee_block_int16_le(self, command: str) -> list[int]:
        """Read an IEEE 488.2 definite block containing int16 little-endian.

        Any short read (stream cut, timeout mid-block, connection reset) raises
        :class:`ScpiProtocolError`; partial samples are never returned.
        """
        await self._write_line(command)
        reader = self._reader
        if reader is None:
            raise ScpiProtocolError("client is not connected")

        read_exact = self._read_exact(reader)

        # Header: '#' followed by one length-of-length digit.
        hash_byte = await read_exact(1)
        if hash_byte != b"#":
            raise ScpiProtocolError(f"expected IEEE block, got {hash_byte!r}")
        length_digit = await read_exact(1)
        if not (length_digit.isdigit() and length_digit != b"0"):
            raise ScpiProtocolError(f"bad IEEE block length-of-length: {length_digit!r}")
        length_bytes = await read_exact(int(length_digit))
        if not length_bytes.isdigit():
            raise ScpiProtocolError(f"bad IEEE block length: {length_bytes!r}")
        payload_length = int(length_bytes)
        if payload_length <= 0 or payload_length % 2 != 0:
            raise ScpiProtocolError(f"invalid int16 payload length: {payload_length}")

        payload = await read_exact(payload_length)
        # Terminating newline after the block.
        tail = await read_exact(1)
        if tail != b"\n":
            raise ScpiProtocolError(f"expected block terminator, got {tail!r}")

        count = payload_length // 2
        return list(struct.unpack(f"<{count}h", payload))

    def _read_exact(self, reader: asyncio.StreamReader) -> Callable[[int], "asyncio.Future[bytes]"]:
        async def read_exact(size: int) -> bytes:
            try:
                data = await asyncio.wait_for(reader.readexactly(size), timeout=self._timeout)
            except asyncio.IncompleteReadError as exc:
                raise ScpiProtocolError(
                    f"truncated IEEE block: wanted {size} bytes, got {len(exc.partial)}"
                ) from exc
            except TimeoutError as exc:
                raise ScpiTimeoutError("timed out while reading IEEE block") from exc
            except _TRANSIENT_ERRORS as exc:
                raise ScpiProtocolError(f"connection lost inside IEEE block: {exc}") from exc
            return data

        return read_exact
