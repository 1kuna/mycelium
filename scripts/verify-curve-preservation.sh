#!/usr/bin/env bash
# Canonical release guard. `capture [client flags]` fetches full jobs + curves.
# Keep stdout (snapshot/diff receipt), stderr and exit status.
set -euo pipefail
exec "${MYCE_BIN:-myce}" release verify-curves "$@"
