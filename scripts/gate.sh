#!/usr/bin/env bash
# Per change: ./scripts/gate.sh [quick [BASE]]; per batch/deploy: ... full.
set -euo pipefail
umask 022
mode=${1:-quick}
base=${2:-origin/integ/108}
case "$mode" in quick|full) ;; *) echo 'usage: scripts/gate.sh [quick [BASE]|full]' >&2; exit 2;; esac
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
if [[ $mode == quick && ${MYCE_GATE_BUDGET:-} != 1 ]]; then
  exec python3 "$repo/scripts/gate-budget.py" "$repo/scripts/gate.sh" "$@"
fi
export GOTOOLCHAIN=local GOTELEMETRY=off
case "$(uname -s)" in
  Darwin) test -x /usr/bin/trash ;;
  *) command -v trash-put >/dev/null ;;
esac
# Read caches only to seed private copies; builds/tests never write user caches.
seed_build=$(go env GOCACHE)
seed_modules=$(go env GOMODCACHE)
gate_user_home=$HOME
mkdir -p "$repo/.gate"
scratch=$(mktemp -d "$repo/.gate/run.XXXXXX")
started=$SECONDS
cleanup() {
  status=$?
  trap - EXIT
  # Recoverable cleanup after tests have joined their children.
  case "$(uname -s)" in
    Darwin) env HOME="$gate_user_home" /usr/bin/trash "$scratch" || status=1 ;;
    *) env HOME="$gate_user_home" trash-put "$scratch" || status=1 ;;
  esac
  if [[ -d $scratch ]]; then echo "gate scratch retained: $scratch" >&2; fi
  echo "gate $mode: exit=$status wall=$((SECONDS-started))s"
  exit "$status"
}
trap cleanup EXIT
if [[ $mode == quick ]]; then
  git rev-parse --verify "$base" >/dev/null
  python3 scripts/gate-related.py "$base" > "$scratch/selection"
fi
mkdir -p "$scratch"/{home,tmp,gocache,modcache,gopath,bin}
copy_cache() {
  if [[ -d $1 ]]; then
    case "$(uname -s)" in
      Darwin) cp -cR "$1/." "$2/" ;;
      Linux) cp -a --reflink=auto "$1/." "$2/" ;;
      *) cp -R "$1/." "$2/" ;;
    esac
  fi
}
copy_cache "$seed_build" "$scratch/gocache"
copy_cache "$seed_modules" "$scratch/modcache"
export HOME="$scratch/home" USERPROFILE="$scratch/home" TMPDIR="$scratch/tmp"
export GOCACHE="$scratch/gocache" GOMODCACHE="$scratch/modcache" GOPATH="$scratch/gopath"
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
  step "build-$os-$arch" env GOOS="$os" GOARCH="$arch" CGO_ENABLED="$cgo" go build -o "$scratch/bin/myce-$os-$arch" ./cmd/myce
done < <("$scratch/bin/myce" release build-targets)
if [[ $mode == full ]]; then
  # Includes the real CLI dev/checkpoint/model/onboarding e2e. A separate dev
  # pair would repeat the same tests and builds.
  wait_for_race_slot
  step race-and-dev go test -race -vet=off -json -p 2 -parallel 4 -count=1 -timeout 15m ./...
else
  while IFS=$'\t' read -r dir names; do
    package=$(go list "./$dir")
    echo "quick $package: $names"
    step race-related go test -race -vet=off -json -p 2 -parallel 4 -count=1 -timeout 3m "$package" -run "$names"
  done < "$scratch/selection"
  if [[ ! -s $scratch/selection ]]; then
    step race-dev-smoke go test -race -vet=off -json -p 2 -parallel 4 -count=1 -timeout 3m ./cmd/myce -run '^TestDevStandaloneSmoke$'
  fi
fi
