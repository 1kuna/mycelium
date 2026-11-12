#!/usr/bin/env bash
# Per change: ./scripts/gate.sh [quick [BASE]]; per batch/deploy: ... full.
# quick --selection FILE takes gate-related.py output computed elsewhere, for
# a source archive without Git history (the gate-quick fleet profile).
set -euo pipefail
umask 022
mode=${1:-quick}
base=${2:-}
selection=
budgeted=${MYCE_GATE_BUDGET:-}
case "$mode" in quick|full) ;; *) echo 'usage: scripts/gate.sh [quick [BASE|--selection FILE]|full]' >&2; exit 2;; esac
if [[ $mode == quick && $base == --selection ]]; then
  selection=${3:?'usage: scripts/gate.sh quick --selection FILE'}
  [[ -f $selection ]] || { echo "quick: selection file not found: $selection" >&2; exit 2; }
  selection=$(cd "$(dirname "$selection")" && pwd)/$(basename "$selection")
  base=
  set -- quick --selection "$selection"
fi
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
# Darwin development includes a directly invoked Linux VM; fleet profiles
# explicitly retain their native-only leg. No queue credential is needed.
if [[ $(uname -s) == Darwin && ${GATE_TARGET:-both} == both && -f scripts/gate-local.py ]]; then
  exec python3 scripts/gate-local.py "$@"
fi
if [[ ${MYCE_GATE_BUDGET:-} != 1 ]]; then
  exec python3 "$repo/scripts/gate-budget.py" "$repo/scripts/gate.sh" "$@"
fi
export GOTOOLCHAIN=local GOTELEMETRY=off
# Share Go's build and module caches across runs and worktrees (Go locks them
# safely). Per-run private copies cost 1-3 GB each and filled the disk (2026-10-23).
shared_build=$(go env GOCACHE)
shared_modules=$(go env GOMODCACHE)
mkdir -p "$repo/.gate"
export GATE_TIMING_CACHE="$shared_build/mycelium-gate-timings"
export GATE_TIMING_REPORT="$repo/gate-timings.jsonl"
export GOVERSION=$(go env GOVERSION)
cores=$(python3 scripts/gate-progress.py --cpus)
supervised=
if [[ $budgeted == 1 && -n ${GATE_SCRATCH:-} ]]; then
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
if [[ $mode == quick && -n $selection ]]; then
  cp "$selection" "$scratch/selection"
elif [[ $mode == quick ]]; then
  # Compare this branch's changes, excluding later unrelated main merges.
  if [[ -z $base ]]; then base=$(git merge-base HEAD origin/main); fi
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
for gate_setting in ${!MYCE_@}; do
  # Process custody must survive nested test workers; it contains no authority.
  [[ $gate_setting == MYCE_PROCESS_SESSION ]] || unset "$gate_setting"
done
# Go's race runtime otherwise sleeps one second on EVERY helper-process exit.
# Tests join workers explicitly; keep instrumentation, omit that artificial wait.
export GORACE=atexit_sleep_ms=0
# Compile parallelism follows affinity/quota and any GOMAXPROCS contract.
# TestMain fixtures retain private HOME/temp roots and fake inventory/daemons.
export GOFLAGS=-p=$cores
step() {
  local name=$1
  shift
  local begin=$SECONDS
  local result=0
  python3 scripts/gate-progress.py "$name" "$@" || result=$?
  echo "$name: $((SECONDS-begin))s"
  return "$result"
}
step gate-progress-regressions python3 scripts/test-gate-progress.py
if [[ -f scripts/test-gate-local.py && ${GATE_FLEET_PROFILE:-} == 1 ]]; then
  # Fleet legs never run gate-local.py or its host FIFO admission; those tests
  # probe the executing host (ps, sysctl) and stay covered by every local gate.
  echo 'gate-local-regressions: skipped (fleet profile; covered by local gates)'
elif [[ -f scripts/test-gate-local.py ]]; then
  step gate-local-regressions python3 scripts/test-gate-local.py
fi
if [[ -f scripts/test-gate-reconcile.py ]]; then
  step gate-reconcile-regressions python3 scripts/test-gate-reconcile.py
fi
step gate-scheduler-regressions python3 scripts/test-gate-scheduler.py
if [[ -f scripts/test-gate-shards.py ]]; then
  step gate-shards-regressions python3 scripts/test-gate-shards.py
fi
if [[ -f scripts/test-gate-revert-check.py ]]; then
  step gate-revert-check-regressions python3 scripts/test-gate-revert-check.py
fi
if [[ -f scripts/test-gate-default-cache.py ]]; then
  step gate-default-cache-regressions python3 scripts/test-gate-default-cache.py
fi
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
# CLI shards share one race-instrumented synthetic child image. Outside the
# gate, devTestBinary still builds its own image for ordinary go test users.
step build-test-cli go build -race -tags=myce_test -o "$scratch/bin/myce-test" ./cmd/myce
export GATE_TEST_CLI_BINARY="$scratch/bin/myce-test"
if [[ $mode == full ]]; then
  step upgrade-compat-supervisor python3 scripts/test-upgrade-compat.py
  # Real cross-release queue/worker and installer proof. Previous source must
  # already be local/cached; this gate remains offline.
  # Includes the real CLI dev/checkpoint/model/onboarding e2e. A separate dev
  # pair would repeat the same tests and builds.
  go list ./... | while read -r package; do
    printf '%s\t.*\n' "$package"
  done > "$scratch/selection"
  printf '@upgrade-compat\t\n' >> "$scratch/selection"
  # The Python client suite, including the consumer skill's runnable examples (#1224).
  printf '@python-client\t\n' >> "$scratch/selection"
fi
# One watchdog per package; release any completed slot, not the oldest PID.
if [[ ! -s $scratch/selection ]]; then
  printf 'cmd/myce\t^TestDevStandaloneSmoke$\n' > "$scratch/selection"
fi
python3 scripts/gate-scheduler.py "$mode" "$cores" "$scratch/selection"
