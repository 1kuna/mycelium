#!/usr/bin/env bash
# Browser smoke test for `myce view`: build myce, start `myce dev` with a few
# jobs, start `myce view --anonymize`, load the page in headless Chrome or
# Chromium, and require live frames with no script errors or CSP violations.
# The page records frames, errors and readiness on <html data-*>; ?frames=3
# closes its event stream after three frames so the browser can print the DOM.
#
#   scripts/view-smoke.sh            # MYCE_VIEW_BROWSER=/path/to/chrome to choose
#
# Exits 0 on pass, 1 on failure, 77 when no browser is installed. Every process
# it starts is stopped on exit; ports are chosen by the OS.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
browser=${MYCE_VIEW_BROWSER:-}
if [[ -z $browser ]]; then
  for b in "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" "/Applications/Chromium.app/Contents/MacOS/Chromium" \
    "$(command -v google-chrome || true)" "$(command -v google-chrome-stable || true)" "$(command -v chromium || true)" "$(command -v chromium-browser || true)"; do
    if [[ -n $b && -x $b ]]; then browser=$b; break; fi
  done
fi
[[ -n $browser ]] || { echo "view-smoke: skipped, no Chrome or Chromium (set MYCE_VIEW_BROWSER)"; exit 77; }

# Short path: Chrome keeps Unix sockets in its profile and TMPDIR.
work=$(mktemp -d /tmp/mvsmoke.XXXXXX)
pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

(cd "$repo" && go build -o "$work/myce" ./cmd/myce)
cd "$work"
./myce dev --workers 2 --listen 127.0.0.1:0 --db "$work/state/myce.db" --checkpoints "$work/ck" >"$work/dev.log" 2>&1 &
pids+=($!)
queue=
for _ in $(seq 1 100); do
  queue=$(sed -n 's/^Mycelium dev: \(http[^ ]*\).*/\1/p' "$work/dev.log" | head -1)
  [[ -n $queue ]] && break
  sleep 0.1
done
[[ -n $queue ]] || { echo "view-smoke: myce dev did not start"; cat "$work/dev.log"; exit 1; }
export MYCE_QUEUE=$queue MYCE_TOKEN=dev-token
for n in 1 2 3; do
  printf 'name: smoke-%s\nproject: smoke\nrun:\n  command: ["sleep", "3"]\nneeds:\n  cpu_cores: 1\n' "$n" >"$work/job$n.yaml"
  ./myce submit "$work/job$n.yaml" >/dev/null 2>&1
done
./myce view --listen 127.0.0.1:0 --anonymize >"$work/view.log" 2>&1 &
pids+=($!)
page=
for _ in $(seq 1 100); do
  page=$(sed -n 's/^Mycelium view: \(http[^ ]*\).*/\1/p' "$work/view.log" | head -1)
  [[ -n $page ]] && break
  sleep 0.1
done
[[ -n $page ]] || { echo "view-smoke: myce view did not start"; cat "$work/view.log"; exit 1; }

dom=$(TMPDIR=$work "$browser" --headless=new --disable-gpu --no-first-run --no-default-browser-check \
  --user-data-dir="$work/profile" --window-size=1280,720 --virtual-time-budget=4000 --dump-dom "$page/?frames=3" 2>/dev/null)
attr() { sed -n "s/.*data-$1=\"\([^\"]*\)\".*/\1/p" <<<"$dom" | head -1; }
frames=$(attr frames) errors=$(attr errors) ready=$(attr ready)
if [[ $errors != 0 || $ready != 1 || ${frames:-0} -lt 3 ]]; then
  echo "view-smoke: FAIL frames=${frames:-none} errors=${errors:-none} ready=${ready:-none} last=$(attr last-error)"
  exit 1
fi
echo "view-smoke: pass (frames=$frames errors=0)"
