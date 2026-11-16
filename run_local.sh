#!/usr/bin/env bash
# Local (non-Docker) launcher: API + two simulators on 127.0.0.1.
# Requires the project dependencies in ./.pydeps (see README).
set -euo pipefail

cd "$(dirname "$0")"
export PYTHONPATH="$(pwd)/.pydeps:$(pwd)"
export DB_PATH="${DB_PATH:-$(pwd)/data/acquisitions.db}"
export INST1_HOST=127.0.0.1 INST1_PORT=5025
export INST2_HOST=127.0.0.1 INST2_PORT=5026
mkdir -p data

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

python3 -m instrument.main inst1 127.0.0.1 5025 &
pids+=("$!")
python3 -m instrument.main inst2 127.0.0.1 5026 &
pids+=("$!")
sleep 1
python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8000 &
pids+=("$!")

wait
