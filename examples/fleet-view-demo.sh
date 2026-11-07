#!/bin/sh
# Opt-in demo workload for `myce view` against `myce dev` (for screenshots and
# recordings). It submits a varied stream of small jobs that report progress,
# takes one fake machine out part-way through, and adds a production job.
#
#   ./myce dev --workers 5                       # terminal 1
#   ./myce view --queue http://127.0.0.1:7331 --token dev-token --anonymize   # terminal 2
#   examples/fleet-view-demo.sh                  # terminal 3
#
# It only talks to a queue on this computer: it refuses any other address,
# so it cannot load a real fleet by accident. Jobs are `sleep` loops.
set -eu

QUEUE=${MYCE_QUEUE:-http://127.0.0.1:7331}
TOKEN=${MYCE_TOKEN:-dev-token}
MYCE=${MYCE:-./myce}
SECONDS_TOTAL=${DEMO_SECONDS:-45}
DROP=${DEMO_DROP_MACHINE:-fake-5} # set empty to keep every machine

case "$QUEUE" in
  http://127.0.0.1:*|http://localhost:*|http://\[::1\]:*) ;;
  *) echo "fleet-view-demo: refusing non-local queue $QUEUE; this is for myce dev only" >&2; exit 2 ;;
esac
export MYCE_QUEUE="$QUEUE" MYCE_TOKEN="$TOKEN"

dir=$(mktemp -d "${TMPDIR:-/tmp}/myce-view-demo.XXXXXX")
trap 'rm -rf "$dir"' EXIT INT TERM

# submit NAME PROJECT STEPS GPU_GB [CLASS] [RESUMABLE]
submit() {
  f="$dir/$1.yaml"
  cat > "$f" <<EOF
name: $1
project: $2
class: ${5:-experiment}
resumable: ${6:-false}
run:
  command:
    - sh
    - -c
    - |
      i=0
      while [ \$i -lt $3 ]; do
        i=\$((i + 1))
        printf '{"completed":%d,"total":%d,"unit":"steps"}\n' \$i $3 > "\$MYCE_PROGRESS_FILE.tmp"
        mv "\$MYCE_PROGRESS_FILE.tmp" "\$MYCE_PROGRESS_FILE"
        sleep 1
      done
needs:
  platform: [fake]
  memory_gb: $4
  cpu_cores: 1
EOF
  "$MYCE" submit "$f" >/dev/null 2>&1 || echo "fleet-view-demo: submit $1 failed" >&2
}

projects="atlas birch cobalt dune"
pick() { # pick N WORDS... -> the Nth word, wrapping
  n=$1; shift; set -- "$@"; i=$(( n % $# + 1 )); eval "echo \${$i}"
}
kinds="embed render sim eval transcode index"
sizes="10 18 6 26 14 22 8 32"
steps="6 9 5 12 7 10 4 14"

n=0
start=$(date +%s)
dropped=0
prod=0
while :; do
  now=$(date +%s)
  t=$((now - start))
  [ "$t" -ge "$SECONDS_TOTAL" ] && break
  # A wave of two or three jobs keeps a short queue in front of the machines.
  for _ in 1 2 3; do
    n=$((n + 1))
    [ $((n % 4)) -eq 0 ] && [ "$t" -gt 6 ] && continue
    name="$(pick $n $kinds)-$(printf %02d $n)"
    submit "$name" "$(pick $((n / 2)) $projects)" "$(pick $n $steps)" "$(pick $((n * 3)) $sizes)" experiment "$( [ $((n % 3)) -eq 0 ] && echo true || echo false )" &
  done
  wait
  if [ -n "$DROP" ] && [ "$dropped" -eq 0 ] && [ "$t" -ge 12 ]; then
    "$MYCE" leave --name "$DROP" --now >/dev/null 2>&1 || echo "fleet-view-demo: could not take $DROP out" >&2
    dropped=1
  fi
  if [ "$prod" -eq 0 ] && [ "$t" -ge 17 ]; then
    submit nightly-index atlas 8 40 production true
    prod=1
  fi
  sleep 2
done
echo "fleet-view-demo: submitted $n jobs over ${SECONDS_TOTAL}s"
