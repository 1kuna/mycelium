#!/usr/bin/env bash
# Runs inside the profile's isolated source folder, never in the caller checkout.
# gate-quick passes `quick`; its job input gate-selection.tsv replaces Git history.
set -euo pipefail
mode=${1:-full}
case "$mode" in full) gate=(full);; quick) gate=(quick --selection gate-selection.tsv);; *) echo "gate profile: unknown mode: $mode" >&2; exit 2;; esac
for tool in go git python3 uv; do
  if ! command -v "$tool" >/dev/null; then
    printf 'gate profile: missing required tool: %s\n' "$tool" >&2
    exit 1
  fi
done
python3 - "$(go env GOVERSION)" <<'PYTHON'
import re, sys
version = re.match(r"go(\d+)\.(\d+)(?:\.(\d+))?", sys.argv[1])
if version is None or tuple(int(part or 0) for part in version.groups()) < (1, 25, 6):
    raise SystemExit("Go 1.25.6 or newer is required")
PYTHON
# Prepare module dependencies before gate.sh deliberately switches offline.
go mod download
./scripts/gate.sh "${gate[@]}"
printf 'gate %s: exit=0\n' "$mode" > gate-result.txt
