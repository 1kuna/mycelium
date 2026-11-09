#!/usr/bin/env bash
# Entire platform preparation + offline gate stays under fixture custody.
set -euo pipefail
mode=${1:-quick}
go mod download
if [[ $mode == full ]]; then
  if [[ -d .gate-predecessors ]]; then
    cache=$(go env GOCACHE)/myce-upgrade-compat
    mkdir -p "$cache"
    for archive in .gate-predecessors/source-*.tar; do
      pending=$(mktemp "$cache/source.XXXXXX")
      cp "$archive" "$pending"
      mv "$pending" "$cache/$(basename "$archive")"
    done
  fi
  python3 scripts/upgrade-compat.py prepare
fi
exec bash scripts/gate.sh "$@"
