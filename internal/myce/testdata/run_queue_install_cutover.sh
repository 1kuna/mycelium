#!/usr/bin/env bash
# Run only as a Linux fleet test-tier payload with network access (loopback).
# Optional MYCE_TEST_QUEUE_CPU_QUOTA compares an explicit quota to the default.
set -euo pipefail
test -z "${MYCE_TOKEN:-}"
test "$(uname -s)" = Linux
test -n "${MYCE_JOB_ID:-}"
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
export GOMAXPROCS=2 GOFLAGS=-p=2
go build -o "$scratch/myce" ./cmd/myce
export MYCE_NATIVE_QUEUE_INSTALL_TEST=1 MYCE_TEST_BINARY="$scratch/myce"
go test -json ./internal/myce -run '^TestQueueInstalledCutover$' -count=3 -timeout=8m > installed-cutover.jsonl
printf 'installed cutover: exit=0\n' > installed-cutover-result.txt
