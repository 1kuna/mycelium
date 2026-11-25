#!/usr/bin/env bash
# Native quick/full checks. Fleet quick supplies an already computed selection.
set -euo pipefail
usage() {
  echo 'usage: scripts/gate.sh [quick [BASE|--selection FILE]|full]' >&2
  exit 2
}
mode=${1:-quick}
selection=
case "$mode" in
  quick)
    if [[ ${2:-} == --selection ]]; then
      [[ $# == 3 && -s $3 ]] || usage
      selection=$(cd "$(dirname "$3")" && pwd)/$(basename "$3")
    else
      [[ $# -le 2 ]] || usage
    fi
    ;;
  full) [[ $# == 1 ]] || usage ;;
  *) usage ;;
esac
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/myce-gate.XXXXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
export GOTOOLCHAIN=local GOTELEMETRY=off GOENV=off GOWORK=off
shared_build=$(go env GOCACHE)
shared_modules=$(go env GOMODCACHE)
python_client=python3.11
if ! command -v "$python_client" >/dev/null; then
  python_client=$(uv python find --offline --no-python-downloads 3.11 2>/dev/null) || python_client=python3.11
fi
if [[ $mode == quick && -z $selection ]]; then
  selection=$scratch/selection.tsv
  python3 scripts/gate-related.py "${2:-origin/main}" > "$selection"
fi

# Keep credentials and installed config away from fixtures. Go's normal caches
# are safe to share across checkouts. Let tests own their own process cleanup.
for setting in ${!MYCE_@}; do
  [[ $setting == MYCE_PROCESS_SESSION ]] || unset "$setting"
done
mkdir -p "$scratch"/{home,tmp,gopath}
export HOME="$scratch/home" USERPROFILE="$scratch/home" TMPDIR="$scratch/tmp"
export XDG_CONFIG_HOME="$HOME/.config" XDG_CACHE_HOME="$HOME/.cache" XDG_DATA_HOME="$HOME/.local/share"
export GOCACHE="$shared_build" GOMODCACHE="$shared_modules" GOPATH="$scratch/gopath"
export UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never UV_CACHE_DIR="$scratch/uv" PIXI_HOME="$scratch/pixi"
# Fixtures explicitly join helper processes, so omit the race runtime's
# one-second exit delay for every child.
export GORACE=atexit_sleep_ms=0
# A selected shared-queue proof must exercise a real disposable database.
export MYCE_GATE_REQUIRE_POSTGRES=1

status=0
step() {
  local label=$1 code=0
  shift
  printf '\n== %s ==\n' "$label"
  "$@" || code=$?
  if [[ $code != 0 ]]; then
    status=1
    printf '%s: exit %s\n' "$label" "$code" >&2
    if [[ -n ${GATE_FAILURE_REPORT:-} ]]; then
      printf '%s: exit %s\n' "$label" "$code" >> "$GATE_FAILURE_REPORT"
    fi
  fi
}
go_tests() {
  go test -race -count=1 -timeout=1h -p=1 -json "$@" | python3 scripts/gate-test-output.py
}
python_tests() {
  PYTHONPATH="$repo/clients/python/src:$repo/clients/python/tests" "$python_client" -m unittest "$@"
}

step docs-links python3 scripts/check-doc-links.py
step docs-links-regressions python3 scripts/test-check-doc-links.py
step selector-regressions python3 scripts/test-gate-related.py
step build go build -o /dev/null ./cmd/myce
step vet go vet ./...
if [[ $mode == full ]]; then
  step go-tests go_tests -vet=off ./...
  step python-client python_tests discover -s clients/python/tests
  step upgrade-compat-regressions python3 scripts/test-upgrade-compat.py
  step upgrade-compat-prepare python3 scripts/upgrade-compat.py prepare
  step upgrade-compat python3 scripts/upgrade-compat.py run
else
  while IFS=$'\t' read -r package pattern || [[ -n $package ]]; do
    [[ -n $package ]] || continue
    case "$package" in
      @python-client)
        step python-client python_tests "$pattern"
        ;;
      *)
        step "quick $package $pattern" go_tests -vet=off -run "$pattern" "./$package"
        ;;
    esac
  done < "$selection"
fi
# Parallel worktrees grew the shared build cache by ~55 GB in three hours
# (2026-11-25). Keep it bounded; trimming never changes the gate result.
python3 scripts/trim-go-cache.py "$shared_build" || true
printf 'gate %s: exit=%s\n' "$mode" "$status"
exit "$status"
