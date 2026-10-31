#!/usr/bin/env bash
# Canonical release guard. Keep stdout (diff receipt), stderr and exit status.
set -euo pipefail
exec "${MYCE_BIN:-myce}" release verify-curves "$@"
