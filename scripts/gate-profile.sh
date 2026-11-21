#!/usr/bin/env bash
# Runs inside the profile's isolated source folder, never in the caller checkout.
# gate-quick passes `quick`; its job input gate-selection.tsv replaces Git history.
set -euo pipefail
export GATE_TARGET=native
export GATE_FLEET_PROFILE=1  # gate.sh skips local-host admission regressions
mode=${1:-full}
case "$mode" in full) gate=(full);; quick) gate=(quick --selection gate-selection.tsv);; *) echo "gate profile: unknown mode: $mode" >&2; exit 2;; esac
export GATE_FAILURE_REPORT="$PWD/gate-failures.txt"
: > "$GATE_FAILURE_REPORT"
phase=setup
failed() {
  status=$?
  if [[ $status != 0 && ! -s $GATE_FAILURE_REPORT ]]; then
    if [[ $status -gt 128 ]] && name=$(kill -l "$status" 2>/dev/null); then
      printf 'gate %s %s: signal SIG%s (exit %s)\n' "$mode" "$phase" "$name" "$status" >> "$GATE_FAILURE_REPORT"
    else
      printf 'gate %s %s: exit %s\n' "$mode" "$phase" "$status" >> "$GATE_FAILURE_REPORT"
    fi
  fi
}
trap failed EXIT
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
if [[ $mode == full ]]; then
  # Source transport has no Git history. Fetch the pinned public predecessor
  # and seed its modules before gate.sh disables network dependency resolution.
  python3 scripts/upgrade-compat.py prepare
fi
phase=execution
./scripts/gate.sh "${gate[@]}"
printf 'gate %s: exit=0\n' "$mode" > gate-result.txt
