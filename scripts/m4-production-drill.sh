#!/bin/bash
# Run from the assigned worktree. All assets and git worktrees stay in .reports.
# No fleet environment, SSH, sudo, daemon or hardware calls.
set -u
STOP_ONLY=0
case "${1:-}" in
  "") ;;
  --stop-only) STOP_ONLY=1 ;;
  *) printf 'usage: bash scripts/m4-production-drill.sh [--stop-only]\n'; exit 2 ;;
esac
ROOT=$(pwd)
mkdir -p "$ROOT/.reports/m4-lineage" || exit 1
RUN=$(mktemp -d "$ROOT/.reports/m4-lineage/run.XXXXXX") || exit 1
printf '%s\n' "$RUN" > "$ROOT/.reports/m4-lineage/latest"
git worktree add --detach "$RUN/production" 57ed4e6 || exit 1
git worktree add --detach "$RUN/compat" 08816cf || exit 1
(cd "$RUN/production" && go build -o "$RUN/myce-57ed4e6" ./cmd/myce) || exit 1
(cd "$RUN/compat" && go build -o "$RUN/myce-08816cf" ./cmd/myce) || exit 1
cp "$ROOT/internal/myce/testdata/m4-production-bridge_test.go.txt" "$RUN/production/internal/myce/m4_drill_bridge_test.go" || exit 1
(cd "$RUN/production" && go test -c -o "$RUN/production-bridge" ./internal/myce) || exit 1
go build -o "$RUN/myce-M" ./cmd/myce || exit 1
MYCE_M4_STOP_ONLY="$STOP_ONLY" MYCE_M4_PRODUCTION_BINARY="$RUN/myce-57ed4e6" MYCE_M4_COMPAT_BINARY="$RUN/myce-08816cf" MYCE_M4_BRIDGE_BINARY="$RUN/production-bridge" MYCE_M4_CURRENT_BINARY="$RUN/myce-M" \
 go test ./internal/myce -run '^TestM4ProductionLineageDrill$' -count=1 -v -timeout 5m > "$RUN/drill.log" 2>&1
RC=$?
cat "$RUN/drill.log"
printf 'DRILL_EXIT=%s\nEVIDENCE=%s\n' "$RC" "$RUN"
exit "$RC"
