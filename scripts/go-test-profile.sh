#!/usr/bin/env bash
# Profile arguments append literally to go test; preserve its status and output.
set -euo pipefail
for tool in go git python3 uv; do command -v "$tool" >/dev/null; done
go mod download
set +e
go test -json -p 2 -parallel 4 -timeout 15m "$@" 2>&1 | tee go-test-log.jsonl
status=${PIPESTATUS[0]}
set -e
printf 'go test: exit=%s\n' "$status" > go-test-result.txt
exit "$status"
