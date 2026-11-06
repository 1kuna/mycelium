#!/bin/sh
# Run from the repository root: ./myce dev -- sh examples/quickstart.sh
set -eu

# Plain run submits a CPU command, follows its state and streams its real logs.
./myce run --mem 128M -- sh -c 'test -z "${MYCE_TOKEN:-}" && printf "hello from CPU\n"'

# Fake GPU inventory exercises placement; the shell command still runs locally.
receipt=$(./myce run --profile quickstart --key "quickstart-$$-$(date +%s)")
job=$(printf '%s\n' "$receipt" | sed -n 's/.*"job_id":"\([a-f0-9]*\)".*/\1/p')
test -n "$job"
printf 'Submitted fake-GPU job %s\n' "$job"
./myce wait "$job"
./myce result "$job" --out quickstart-result
printf 'hello from Mycelium\n' | cmp - quickstart-result/result.txt
printf 'Verified quickstart-result/result.txt\n'
