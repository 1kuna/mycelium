#!/usr/bin/env bash
# Run only as a Linux fleet test-tier payload with network access (loopback).
# Optional MYCE_TEST_QUEUE_CPU_QUOTA compares an explicit quota to the default.
set -euo pipefail
test -z "${MYCE_TOKEN:-}"
test "$(uname -s)" = Linux
test -n "${MYCE_JOB_ID:-}"
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
mkdir "$scratch/tmp"
export TMPDIR="$scratch/tmp"
export GOMAXPROCS=2 GOFLAGS=-p=2
go build -o "$scratch/myce" ./cmd/myce
export MYCE_NATIVE_QUEUE_INSTALL_TEST=1 MYCE_TEST_BINARY="$scratch/myce"
count=3
timeout=8m
if [ "${MYCE_TEST_PHYSICAL_DIAG:-}" = 1 ] || [ "${MYCE_TEST_PHYSICAL_CUTOVER:-}" = 1 ]; then
  # One matrix already contains independent scan/reassembly/decode probes for
  # two physical layouts and two cache conditions, plus multi-GB preparation.
  count=1
  timeout=20m
fi
go test -json ./internal/myce -run '^TestQueueInstalledCutover$' -count="$count" -timeout="$timeout" > installed-cutover.jsonl
printf 'installed cutover: exit=0\n' > installed-cutover-result.txt
