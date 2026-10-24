#!/usr/bin/env bash
# Runs inside the profile's isolated source folder, never in the caller checkout.
set -euo pipefail
for tool in go git python3 uv; do command -v "$tool" >/dev/null; done
[[ $(go env GOVERSION) == go1.25.6 ]]
# Prepare module dependencies before gate.sh deliberately switches offline.
go mod download
./scripts/gate.sh full
printf 'gate full: exit=0\n' > gate-result.txt
