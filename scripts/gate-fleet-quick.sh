#!/usr/bin/env bash
# Fleet quick gate (MY-37 fallback when the development host is busy).
# Usage: scripts/gate-fleet-quick.sh [BASE]   (BASE defaults to origin/main)
# Submits one Linux leg (gate-quick) and one Darwin arm64 leg (gate-quick-darwin)
# with the same related-test selection; exits 0 only when both legs succeed.
# Commit first: each leg's source is HEAD.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
base=$(git merge-base HEAD "${1:-origin/main}")
mkdir -p .reports
python3 scripts/gate-related.py "$base" > .reports/gate-selection.tsv
jobs=()
for profile in gate-quick gate-quick-darwin; do
  receipt=$(myce run --profile "$profile" --input gate-selection.tsv=.reports/gate-selection.tsv)
  job=$(python3 -c 'import json, sys; print(json.load(sys.stdin)["job_id"])' <<<"$receipt")
  echo "$profile: job $job"
  jobs+=("$job")
done
# myce wait waits for every job and exits 0 only when all succeeded.
if myce wait "${jobs[@]}"; then
  echo "fleet quick: linux+darwin passed"
else
  status=$?
  echo "fleet quick: failed (exit $status); inspect with myce logs JOB_ID" >&2
  exit "$status"
fi
