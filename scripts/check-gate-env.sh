#!/usr/bin/env bash
# Cheap native macOS regression check for the catalog environment's C compiler.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
if [[ $(uname -s) == Darwin ]]; then
  pixi run --locked --manifest-path env/mycelium-gate/pixi.toml \
    env CGO_ENABLED=1 go build -o /dev/null ./cmd/myce
fi
