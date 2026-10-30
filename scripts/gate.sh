#!/usr/bin/env bash
# Per change: ./scripts/gate.sh [quick [BASE]]; per batch/deploy: ... full.
set -euo pipefail
umask 022
mode=${1:-quick}
base=${2:-origin/integ/108}
budgeted=${MYCE_GATE_BUDGET:-}
case "$mode" in quick|full) ;; *) echo 'usage: scripts/gate.sh [quick [BASE]|full]' >&2; exit 2;; esac
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
if [[ $mode == quick && ${MYCE_GATE_BUDGET:-} != 1 ]]; then
  exec python3 "$repo/scripts/gate-budget.py" "$repo/scripts/gate.sh" "$@"
fi
export GOTOOLCHAIN=local GOTELEMETRY=off
# Share Go's build and module caches across runs and worktrees (Go locks them
# safely). Per-run private copies cost 1-3 GB each and filled the disk (2026-10-23).
shared_build=$(go env GOCACHE)
shared_modules=$(go env GOMODCACHE)
mkdir -p "$repo/.gate"
supervised=
if [[ $mode == quick && $budgeted == 1 && -n ${GATE_SCRATCH:-} ]]; then
  scratch=$GATE_SCRATCH
  supervised=1
else
  scratch=$(mktemp -d "$repo/.gate/run.XXXXXX")
fi
unset GATE_SCRATCH
started=$SECONDS
cleanup() {
  status=$?
  trap - EXIT
  # Scratch is rebuildable test state (fake HOME, temp files): delete it outright.
  # The quick supervisor stops detached fixture children before removing scratch.
  if [[ $supervised != 1 ]]; then
    chmod -R u+w "$scratch" 2>/dev/null || true
    rm -rf "$scratch" || status=1
    if [[ -d $scratch ]]; then echo "gate scratch retained: $scratch" >&2; fi
  fi
  if [[ $budgeted != 1 ]]; then
    echo "gate $mode: exit=$status wall=$((SECONDS-started))s"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
if [[ $mode == quick ]]; then
  git rev-parse --verify "$base" >/dev/null
  python3 scripts/gate-related.py "$base" > "$scratch/selection"
fi
mkdir -p "$scratch"/{home,tmp,gopath,bin,python-bin}
# HOME/XDG isolation hides uv-managed interpreters. Resolve before isolation,
# exposing only Python 3.11 so its installation directory cannot shadow Go/tools.
python_path="$(uv python find --offline --no-python-downloads 3.11)"
ln -s "$python_path" "$scratch/python-bin/python3.11"
export PATH="$scratch/python-bin:$PATH"
export HOME="$scratch/home" USERPROFILE="$scratch/home" TMPDIR="$scratch/tmp"
export GOCACHE="$shared_build" GOMODCACHE="$shared_modules" GOPATH="$scratch/gopath"
export XDG_CONFIG_HOME="$HOME/.config" XDG_CACHE_HOME="$HOME/.cache" XDG_DATA_HOME="$HOME/.local/share"
export GOTOOLCHAIN=local GOTELEMETRY=off GOPROXY=off GOSUMDB=off GOENV=off GOWORK=off
export UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never UV_CACHE_DIR="$scratch/uv" PIXI_HOME="$scratch/pixi"
for gate_setting in ${!MYCE_@}; do unset "$gate_setting"; done
# Go's race runtime otherwise sleeps one second on EVERY helper-process exit.
# Tests join workers explicitly; keep instrumentation, omit that artificial wait.
export GORACE=atexit_sleep_ms=0
# Two packages bound compiler/RAM contention on the shared laptop. TestMain
# fixtures have private HOME/temp roots and use fake inventory/daemons.
export GOFLAGS=-p=2
step() {
  local name=$1
  shift
  local begin=$SECONDS
  "$@"
  echo "$name: $((SECONDS-begin))s"
}
wait_for_race_slot() {
  # Existing workers also run the older pair/full commands. Count go processes,
  # not their Python/shell supervisors; targeted -run commands aren't full runs.
  while [[ $({ pgrep -fl 'go test' || true; } | awk '/^[0-9]+ (.*\/)?go test / && /-race/ && !/-run/ { n++ } END { print n+0 }') -ge 2 ]]; do
    echo 'waiting for a full-race slot (two other runs active)' >&2
    sleep 15
  done
}
step build go build -o "$scratch/bin/myce" ./cmd/myce
step vet go vet ./...
# Same matrix as ReleaseTargets; darwin/arm64 is the native CGO build.
while read -r os arch cgo; do
  if [[ $cgo == 1 && $os != $(go env GOHOSTOS) ]]; then
    echo "build-$os-$arch: skipped (native CGO toolchain required; verify on $os)"
    continue
  fi
  step "build-$os-$arch" env GOOS="$os" GOARCH="$arch" CGO_ENABLED="$cgo" go build -o "$scratch/bin/myce-$os-$arch" ./cmd/myce
done < <("$scratch/bin/myce" release build-targets)
if [[ $mode == full ]]; then
  # Includes the real CLI dev/checkpoint/model/onboarding e2e. A separate dev
  # pair would repeat the same tests and builds.
  wait_for_race_slot
  # The s050 two-core Linux core suite took 896.644s of a 900s budget.
  # Observe JSON log growth instead of killing healthy package progress;
  # the release owner stops a run stalled for 20m.
  step race-and-dev go test -race -vet=off -json -p 2 -parallel 4 -count=1 -timeout 0 ./...
else
  # Leave ten seconds before supervisor TERM for Go's timeout stack dump.
  # Recompute for each invocation so packages share the remaining work budget.
  quick_timeout() {
    python3 - <<'PYTIME'
import os
import time
remaining = float(os.environ['GATE_TEST_DEADLINE']) - time.monotonic()
if remaining <= 0:
    raise SystemExit('quick: diagnostic test budget exhausted; run ./scripts/gate.sh full')
print(f'{remaining:.3f}s')
PYTIME
  }
  while IFS=$'\t' read -r dir names; do
    package=$(go list "./$dir")
    echo "quick $package: $names"
    timeout=$(quick_timeout)
    step race-related go test -race -vet=off -json -p 2 -parallel 4 -count=1 -timeout "$timeout" "$package" -run "$names"
  done < "$scratch/selection"
  if [[ ! -s $scratch/selection ]]; then
    timeout=$(quick_timeout)
    step race-dev-smoke go test -race -vet=off -json -p 2 -parallel 4 -count=1 -timeout "$timeout" ./cmd/myce -run '^TestDevStandaloneSmoke$'
  fi
fi
