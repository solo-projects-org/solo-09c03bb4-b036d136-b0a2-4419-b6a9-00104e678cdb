"""Entry point: python -m instrument.main <name> <host> <port>."""

from __future__ import annotations

import asyncio
import os
import sys

from .scpi_sim import run_forever


def main() -> None:
    name = os.environ.get("INSTRUMENT_NAME", sys.argv[1] if len(sys.argv) > 1 else "inst1")
    host = os.environ.get("INSTRUMENT_HOST", sys.argv[2] if len(sys.argv) > 2 else "0.0.0.0")
    port = int(os.environ.get("INSTRUMENT_PORT", sys.argv[3] if len(sys.argv) > 3 else "5025"))
    asyncio.run(run_forever(name, host, port))


if __name__ == "__main__":
    main()
