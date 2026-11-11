#!/usr/bin/env bash
# Pre-merge check (#1224): build and vet the would-be merge of HEAD onto the
# current main, after the revert check. Quick proves the branch; this proves the
# tree main will actually have, which caught neither #1359 nor the holds revert.
# Usage: scripts/gate-merge-check.sh [BASE] [--allow-reverts]   (BASE: origin/main)
# Commit first: only HEAD is merged. Set MERGE_CHECK_NO_FETCH=1 to skip the fetch.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
base=origin/main
allow=
for arg in "$@"; do
  case "$arg" in
    --allow-reverts) allow=--allow-reverts ;;
    -*) echo "usage: scripts/gate-merge-check.sh [BASE] [--allow-reverts]" >&2; exit 2 ;;
    *) base=$arg ;;
  esac
done
started=$SECONDS
if [[ ${MERGE_CHECK_NO_FETCH:-} != 1 && $base == origin/* ]]; then
  git fetch -q origin "${base#origin/}"
fi
if [[ -n $(git status --porcelain --untracked-files=no) ]]; then
  echo 'merge-check: warning: uncommitted changes are not part of the checked merge' >&2
fi
python3 scripts/gate-revert-check.py "$base" ${allow:+"$allow"}
# Writes objects only; the working tree, index and refs are untouched.
if ! tree=$(git merge-tree --write-tree --name-only "$base" HEAD); then
  printf 'merge-check: HEAD does not merge cleanly onto %s:\n%s\n' "$base" "$tree" >&2
  exit 1
fi
tree=${tree%%$'\n'*}
commit=$(git commit-tree "$tree" -p "$(git rev-parse "$base")" -p HEAD -m 'merge-check')
mkdir -p .gate
# One fixed path per checkout keeps Go's directory-keyed build cache warm across
# checks (-trimpath would too, but vet then reads stale cached cgo paths).
work=$repo/.gate/merge-check
if [[ -e $work ]]; then
  echo "merge-check: removing leftover $work" >&2
  git worktree remove --force "$work" 2>/dev/null || rm -rf "$work"
  git worktree prune
fi
cleanup() {
  status=$?
  cd "$repo"
  git worktree remove --force "$work" >/dev/null 2>&1 || rm -rf "$work"
  git worktree prune
  echo "merge-check: exit=$status wall=$((SECONDS-started))s merge=${commit:0:12} base=$(git rev-parse --short=12 "$base")"
  exit "$status"
}
trap cleanup EXIT
git worktree add -q --detach "$work" "$commit"
cd "$work"
export GOTOOLCHAIN=local GOTELEMETRY=off GOFLAGS=${GOFLAGS:--p=4}
go build ./...
go vet ./...
# Linux-only files are invisible to a Darwin build; type-check them too.
if [[ $(go env GOOS) != linux ]]; then
  GOOS=linux CGO_ENABLED=0 go vet ./...
fi
