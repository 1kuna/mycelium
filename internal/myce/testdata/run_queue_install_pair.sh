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
for side in before after; do
    export MYCE_TEST_BINARY="$scratch/myce-$side"
    if [ "$side" = after ]; then export MYCE_TEST_EXPECT_READY=1; else export MYCE_TEST_EXPECT_READY=0; fi
    # Package working directory is required by testdata fixture lookup.
    (cd internal/myce && go tool test2json -t -p mycelium/internal/myce \
        "$scratch/cutover-tests" -test.v=test2json \
        -test.run='^TestQueueInstalledCutover$' -test.count=3 -test.timeout=8m) \
        > "$output/installed-$side.jsonl"
done
printf 'installed pair: exit=0\n' > installed-pair-result.txt
