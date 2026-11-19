#!/usr/bin/env bash
# Fleet Linux test-tier payload. Supply installed-baseline.tar from git archive
# of the recorded before revision. Both binaries use the SAME current test
# harness, synthetic shape and process limits on the SAME execution worker.
set -euo pipefail
test -z "${MYCE_TOKEN:-}"
test "$(uname -s)" = Linux
test -n "${MYCE_JOB_ID:-}"
output=$PWD
scratch=$(mktemp -d)
trap 'rm -rf -- "$scratch"' EXIT
export GOMAXPROCS=2 GOFLAGS=-p=2
mkdir "$scratch/before"
tar -xf installed-baseline.tar -C "$scratch/before"
(cd "$scratch/before" && go build -o "$scratch/myce-before" ./cmd/myce)
go build -o "$scratch/myce-after" ./cmd/myce
go test -c -o "$scratch/cutover-tests" ./internal/myce
export MYCE_NATIVE_QUEUE_INSTALL_TEST=1
trials=${MYCE_TEST_TRIALS:-3}
case "$trials" in 1|3) ;; *) exit 2 ;; esac
for padding in ${MYCE_TEST_PADDING_SWEEP:-${MYCE_TEST_RECORD_PADDING_KIB:-0}}; do
  export MYCE_TEST_RECORD_PADDING_KIB=$padding
  for side in before after; do
    export MYCE_TEST_BINARY="$scratch/myce-$side"
    if [ "$side" = after ]; then export MYCE_TEST_EXPECT_READY=1; else export MYCE_TEST_EXPECT_READY=0; fi
    # Package working directory is required by testdata fixture lookup.
    name="installed-$side.jsonl"
    if [ -n "${MYCE_TEST_PADDING_SWEEP:-}" ]; then name="installed-$padding-$side.jsonl"; fi
    (cd internal/myce && go tool test2json -t -p mycelium/internal/myce \
        "$scratch/cutover-tests" -test.v=test2json \
        -test.run='^TestQueueInstalledCutover$' -test.count="$trials" -test.timeout=20m) \
        > "$output/$name"
  done
done
printf 'installed pair: exit=0\n' > installed-pair-result.txt
