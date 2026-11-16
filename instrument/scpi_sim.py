"""SCPI simulated oscilloscope, accessible over raw TCP.

Design properties required by the acquisition service:

* Commands are executed **serially per instrument**: every accepted
  connection feeds a single, instrument-wide command queue.  Responses are
  therefore never interleaved between connections.
* Commands that have been fully received keep executing after the issuing
  connection drops: queue items carry their own write callback, and a dropped
  connection simply discards later replies instead of cancelling the command.
* The instrument process is independent of the API process, so an API restart
  never resets instrument state.
* Slow responses and mid-stream disconnects ("stream cuts") can be configured
  at runtime through SCPI commands for test/fault-injection purposes.

SCPI surface (line oriented, ``\\n`` terminated; binary blocks are IEEE 488.2)::

    *IDN?                      identification
    :RUN                       start continuous measurement
    :STOP                      freeze / pause measurement
    :RUN?                      "1" running, "0" stopped
    *OPC?                      "1" once all earlier commands completed
    :BUSY?                     "1" while a command is being processed
    :CHAN<n>:POS?              channel exists -> "1"
    :WAV:POIN?                 number of points in the current waveform
    :WAV:XOR?                  X origin (seconds)
    :WAV:XINC?                 X increment (seconds per point)
    :WAV:YOR?                  Y origin (volts)
    :WAV:YINC?                 Y increment (volts per code)
    :WAV:DATA?                 IEEE 488.2 definite block, int16 little-endian
    :SYST:DELAY <seconds>      artificial delay applied to every command
    :SYST:DELAY?
    :SYST:CUT <0|1>            when 1, :WAV:DATA? disconnects mid-block
    :SYST:CUT?
    *RST                       reset fault injection (state is kept)
"""

from __future__ import annotations

import asyncio
import logging
import os
import struct
import time
from dataclasses import dataclass, field

LOG = logging.getLogger("scpi-sim")

CRLF = b"\n"


class ScpiError(Exception):
    """Protocol/usage error; rendered as an SCPI ``-nnn`` error reply."""


@dataclass
class ChannelSpec:
    points: int
    x_origin: float
    x_increment: float
    y_origin: float
    y_increment: float
    frequency: float
    amplitude: int
    phase: float


@dataclass
class InstrumentState:
    name: str
    running: bool = True
    # Freeze key: increments on every :RUN; a frozen waveform is captured at the
    # moment of :STOP and keeps its sample index until the next :RUN.
    run_generation: int = 0
    stop_sample_index: int = 0
    delay: float = 0.0
    cut_enabled: bool = False
    # When > 0, the next N commands (across any connection) fail by closing the
    # requesting socket.  Used to inject deterministic mid-acquisition faults.
    drop_next: int = 0
    channels: dict[int, ChannelSpec] = field(default_factory=dict)
    started_at: float = field(default_factory=time.time)

    def channel(self, number: int) -> ChannelSpec:
        try:
            return self.channels[number]
        except KeyError as exc:
            raise ScpiError(f"-222,channel {number} not found") from exc


def build_default_state(name: str) -> InstrumentState:
    """Two independent channels with different scaling and signal shape."""
    state = InstrumentState(name=name)
    state.channels[1] = ChannelSpec(
        points=2400,
        x_origin=0.0,
        x_increment=2.5e-4,          # 4 kS/s -> 0.6 s window
        y_origin=-0.05,
        y_increment=0.0025,          # 2.5 mV per LSB
        frequency=50.0,
        amplitude=12000,
        phase=0.0,
    )
    state.channels[2] = ChannelSpec(
        points=2400,
        x_origin=0.0,
        x_increment=5.0e-4,          # 2 kS/s -> 1.2 s window
        y_origin=0.1,
        y_increment=0.001,           # 1 mV per LSB
        frequency=120.0,
        amplitude=8000,
        phase=0.7,
    )
    return state


def _waveform_codes(state: InstrumentState, channel: int) -> list[int]:
    """Deterministic int16 codes for the *current, same* waveform.

    While running the sample index follows wall time.  On :STOP the index is
    frozen so repeated preamble/data queries within one paused acquisition
    always describe the identical waveform (same point count and same codes).
    """
    spec = state.channel(channel)
    import math

    if state.running:
        base = int((time.time() - state.started_at) / spec.x_increment)
    else:
        base = state.stop_sample_index
    codes: list[int] = []
    for i in range(spec.points):
        idx = base + i
        angle = 2.0 * math.pi * spec.frequency * idx * spec.x_increment + spec.phase
        value = int(round(spec.amplitude * math.sin(angle)))
        # clamp to signed 16-bit range
        value = max(-32768, min(32767, value))
        codes.append(value)
    return codes


def _ieee_block(payload: bytes) -> bytes:
    """Wrap *payload* in an IEEE 488.2 definite-length arbitrary block."""
    length = len(payload)
    digits = str(length).encode("ascii")
    header = b"#" + str(len(digits)).encode("ascii") + digits
    return header + payload


class _StreamCut(Exception):
    """Internal signal: reply with *partial* then close the connection."""

    def __init__(self, partial: bytes) -> None:
        super().__init__("stream cut")
        self.partial = partial


@dataclass
class _Command:
    line: str | None
    writer: "asyncio.StreamWriter | None"
    enqueued_at: float | None


class ScpiInstrument:
    """Holds instrument state and the serial, instrument-wide command queue."""

    def __init__(self, name: str, state: InstrumentState | None = None) -> None:
        self.name = name
        self.state = state or build_default_state(name)
        self._queue: asyncio.Queue[_Command] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._busy = False

    async def start(self) -> None:
        self._worker_task = asyncio.create_task(self._worker(), name=f"{self.name}-worker")

    async def stop(self) -> None:
        await self._queue.put(_Command(None, None, None))  # poison pill
        if self._worker_task:
            await self._worker_task

    def submit(self, line: str, writer: "asyncio.StreamWriter | None") -> None:
        """Enqueue one fully received command line."""
        self._queue.put_nowait(_Command(line, writer, time.monotonic()))

    @property
    def busy(self) -> bool:
        return self._busy

    async def _worker(self) -> None:
        while True:
            cmd = await self._queue.get()
            if cmd.line is None:  # poison pill
                self._queue.task_done()
                return
            self._busy = True
            try:
                await self._execute(cmd)
            except Exception:  # pragma: no cover - defensive
                LOG.exception("%s: command failed: %r", self.name, cmd.line)
            finally:
                self._busy = False
                self._queue.task_done()

    async def _execute(self, cmd: "_Command") -> None:
        line = (cmd.line or "").strip()
        if not line:
            return
        LOG.info("%s -> %s", self.name, line)
        # Artificial slow-response delay applies to every command, including
        # queries; it runs before processing, inside the serial worker.
        if self.state.delay:
            await asyncio.sleep(self.state.delay)
        # Deterministic fault injection: drop the connection for this command.
        # The command is still dequeued/processed order-wise, and later
        # commands (possibly on a new connection) continue to execute serially.
        if self.state.drop_next > 0:
            if cmd.writer is not None:
                try:
                    cmd.writer.close()
                except RuntimeError:
                    pass
            self.state.drop_next -= 1
            return
        try:
            reply = self._handle(line)
        except _StreamCut as cut:
            # Fault injection: send the partial block then tear the transport
            # down.  The command itself has still been executed.
            if cmd.writer is not None:
                try:
                    cmd.writer.write(cut.partial)
                    await cmd.writer.drain()
                except ConnectionError:
                    pass
                try:
                    cmd.writer.close()
                except RuntimeError:
                    pass
            return
        except ScpiError as exc:
            reply = str(exc).encode("ascii") + CRLF
        if reply is not None and cmd.writer is not None and not cmd.writer.is_closing():
            try:
                cmd.writer.write(reply)
                await cmd.writer.drain()
            except (ConnectionError, BrokenPipeError, RuntimeError):
                # Client disconnected mid-reply: the command itself has still
                # been executed, which is the required semantics.
                LOG.warning("%s: client gone while replying to %r", self.name, line)

    def _handle(self, line: str) -> bytes | None:
        upper = line.upper()
        state = self.state

        if upper == "*IDN?":
            return f"ACME,LABSCOPE-{self.name},SN-{self.name},1.0".encode() + CRLF
        if upper == "*RST":
            state.delay = 0.0
            state.cut_enabled = False
            state.drop_next = 0
            return None
        if upper == ":RUN":
            if not state.running:
                state.running = True
                state.run_generation += 1
            return None
        if upper == ":STOP":
            if state.running:
                state.running = False
                ch1 = state.channels[1]
                state.stop_sample_index = int(
                    (time.time() - state.started_at) / ch1.x_increment
                )
            return None
        if upper == ":RUN?":
            return (b"1" if state.running else b"0") + CRLF
        if upper == "*OPC?":
            # Serial worker guarantees that reaching this point means every
            # earlier queued command has completed.
            return b"1" + CRLF
        if upper == ":BUSY?":
            # From inside the serial worker, this query itself is the command
            # currently executing.  Report whether *other* commands are queued
            # behind it (i.e. the instrument still has pending work).
            return (b"1" if self._queue.qsize() > 0 else b"0") + CRLF

        if upper.startswith(":SYST:DELAY"):
            return self._handle_delay(line, upper)
        if upper.startswith(":SYST:CUT"):
            return self._handle_cut(line, upper)
        if upper.startswith(":SYST:DROP"):
            return self._handle_drop(line, upper)

        if upper.startswith(":CHAN"):
            return self._handle_channel_query(upper)
        if upper.startswith(":WAV:"):
            return self._handle_wave_query(line, upper)
        raise ScpiError(f"-113,unknown command {line!r}")

    def _handle_delay(self, line: str, upper: str) -> bytes | None:
        if upper == ":SYST:DELAY?":
            return f"{self.state.delay:g}".encode() + CRLF
        payload = line.split(None, 1)
        if len(payload) != 2:
            raise ScpiError("-109,missing parameter")
        try:
            value = float(payload[1])
        except ValueError as exc:
            raise ScpiError("-111,bad delay value") from exc
        if value < 0:
            raise ScpiError("-222,delay must be non-negative")
        self.state.delay = value
        return None

    def _handle_cut(self, line: str, upper: str) -> bytes | None:
        if upper == ":SYST:CUT?":
            return (b"1" if self.state.cut_enabled else b"0") + CRLF
        payload = line.split(None, 1)
        if len(payload) != 2 or payload[1] not in ("0", "1"):
            raise ScpiError("-111,cut expects 0 or 1")
        self.state.cut_enabled = payload[1] == "1"
        return None

    def _handle_drop(self, line: str, upper: str) -> bytes | None:
        if upper == ":SYST:DROP?":
            return str(self.state.drop_next).encode() + CRLF
        payload = line.split(None, 1)
        if len(payload) != 2 or not payload[1].isdigit():
            raise ScpiError("-111,drop expects a non-negative integer")
        self.state.drop_next = int(payload[1])
        return None

    def _handle_channel_query(self, upper: str) -> bytes:
        # :CHAN<n>:POS?  (channel presence check)
        body = upper[len(":CHAN"):]
        if not body.endswith(":POS?"):
            raise ScpiError("-113,expected :CHAN<n>:POS?")
        number_text = body[: -len(":POS?")]
        if not number_text.isdigit():
            raise ScpiError("-111,bad channel number")
        number = int(number_text)
        self.state.channel(number)  # raises ScpiError when unknown
        return b"1" + CRLF

    def _handle_wave_query(self, line: str, upper: str) -> bytes:
        # Optional channel selector suffix: :WAV:...? (@n) or bare query for ch1
        channel = 1
        if "(" in line:
            selector = line[line.index("(") + 1: line.rfind(")")].strip().lstrip("@")
            if not selector.isdigit():
                raise ScpiError("-111,bad channel selector")
            channel = int(selector)
        base = upper.split("(", 1)[0].strip()
        spec = self.state.channel(channel)
        if base == ":WAV:POIN?":
            return str(spec.points).encode() + CRLF
        if base == ":WAV:XOR?":
            return f"{spec.x_origin:g}".encode() + CRLF
        if base == ":WAV:XINC?":
            return f"{spec.x_increment:g}".encode() + CRLF
        if base == ":WAV:YOR?":
            return f"{spec.y_origin:g}".encode() + CRLF
        if base == ":WAV:YINC?":
            return f"{spec.y_increment:g}".encode() + CRLF
        if base == ":WAV:DATA?":
            codes = _waveform_codes(self.state, channel)
            payload = struct.pack(f"<{len(codes)}h", *codes)
            block = _ieee_block(payload) + CRLF
            if self.state.cut_enabled:
                # Fault injection: send half the block, then the transport is
                # torn down.  The command itself still "executed".
                cut = block[: max(16, len(block) // 2)]
                self.state.cut_enabled = False  # one-shot fault
                raise _StreamCut(cut)
            return block
        raise ScpiError("-113,unknown waveform query")

async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    instrument: ScpiInstrument,
) -> None:
    """Read complete command lines and enqueue them; drops never cancel work."""
    peer = writer.get_extra_info("peername")
    LOG.info("%s: connection opened from %s", instrument.name, peer)
    try:
        while True:
            try:
                data = await reader.readline()
            except ConnectionError:
                break
            if not data:  # EOF / disconnect
                break
            line = data.decode("ascii", errors="replace").strip()
            if not line:
                continue
            instrument.submit(line, writer)
    finally:
        # The reader has reached EOF: close this socket now.  Commands already
        # queued keep executing serially; the worker simply swallows writes to
        # a closed/dead socket, which preserves "received commands still run
        # after disconnect" without any lingering housekeeping task.
        try:
            writer.close()
        except (ConnectionError, RuntimeError):
            pass
        LOG.info("%s: connection closed from %s", instrument.name, peer)


async def serve_instrument(host: str, port: int, instrument: ScpiInstrument) -> asyncio.base_events.Server:
    await instrument.start()

    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_connection(reader, writer, instrument)

    server = await asyncio.start_server(_handler, host, port)
    LOG.info("%s listening on %s:%s", instrument.name, host, port)
    return server


async def run_forever(name: str, host: str, port: int) -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    state = build_default_state(name)
    instrument = ScpiInstrument(name, state)
    server = await serve_instrument(host, port, instrument)
    try:
        async with server:
            await server.serve_forever()
    finally:
        await instrument.stop()
