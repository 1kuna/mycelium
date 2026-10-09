# First production test (founder-operated, not executed)

Run in order; stop at any failed check. This is an isolated v2 test, not an
Atlas/Verso migration. No command stops, edits, or replaces Verso or the old B70
`mycelium-real-fleet.service`. Do not reboot B70: that would disrupt both.
Use the v2 paths and units from [operations](operations.md), which differ from
v1's `.mycelium` paths. Do not reuse the older discovery report's `myce-v2`
paths/units with these templates. Linux NAS is `/mnt/nas`, Mac NAS is
`/Volumes/NAS`; both must map NAS `192.0.2.64` share `dev`.

**Prerequisites:** scheduled Spark GPU admission approved by its existing owner,
no competing job admitted to this test queue, existing mounts, user linger
already enabled, Go 1.25.6+ on Mac, Python 3.10+, and a GPU-enabled `llama-server`
already installed on Spark. vLLM alone is insufficient for this implementation.
A reviewed real catalog must exist at `ops/private/myce-models.yaml`, have one
CUDA chat model named `production-smoke`, a real Spark GGUF path, a measured
weights/KV/concurrency budget, and explicit settings from its official model
card/generation config. Record served identity, template/reasoning, context,
output limit, sampler, and tool settings before using it. Do not substitute the
fake engine, borrow an Atlas process/model, install software, or change guards.
These are unresolved fleet prerequisites, not claims of readiness.

## 1. Prepare and install the CPU-only queue on B70

First complete [private test preparation](operations.md#private-founder-test-preparation)
on Mac. It builds both architectures, checks absent v2 destinations and existing
NAS mounts/linger, installs private config and renders both units. Stop on any
failed prerequisite. Then Mac:

```sh
ssh b70 'set -eu; systemd-analyze --user verify ~/.config/systemd/user/myce-queue.service; systemctl --user daemon-reload; systemctl --user enable --now myce-queue.service; systemctl --user is-active myce-queue.service; ss -ltnH "sport = :7331"; journalctl --user -u myce-queue.service -n 20 --no-pager'
```

Success: active queue on `192.0.2.153:7331`, no sandbox warnings, no worker on
B70. On B70, verify actual GPU hiding (read-only):

```sh
queue_pid=$(systemctl --user show myce-queue.service -p MainPID --value)
sudo nsenter -t "$queue_pid" -m -- sh -c 'test ! -e /dev/dri && test ! -e /dev/nvidia0'
```

Success: exit zero. Stop v2 queue if hiding is unsupported; never remove the
sandbox. No B70 worker is installed or enabled.

## 2. Install the Spark CUDA worker

Mac: first verify admission and inventory. This is a hard gate:

```sh
ssh spark 'set -eu; command -v llama-server; nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits; cat /proc/meminfo'
```

Success: installed GPU build, exactly one CUDA device, and either a finite
nonnegative numeric MiB reading or a supported unified-memory marker (`N/A`,
`[N/A]`, `Not Supported`, `[Not Supported]`). Spark's GB10 markers select
`MemAvailable` (kB) from `/proc/meminfo`; verify it is present and valid and that
headroom **after the configured cap and reserve** fits the reviewed catalog.
Use `--reserve-memory-gb 16` as an initial Spark reserve and increase it for
Atlas growth/pressure; do not advertise total 128 GB RAM. The
[Spark example](../deploy/myce-spark-worker.service) uses auto detection; use
`--memory-mode unified` to override detection. Missing sources are a hard gate:
status must show zero usable memory and a diagnostic, and no work may start.
An explicitly configured `--memory-mode fixed --memory-gb VALUE` is an operator
budget exception, requiring independently enforced headroom; a cap alone never
bypasses errors. Memory accounting does not resolve Atlas ownership/admission.

After admission, install the privately rendered worker unit (Mac):

```sh
scp /tmp/myce-production-build/myce-worker.service spark:/home/user/.config/systemd/user/myce-worker.service
ssh spark 'set -eu; test -z "$(ss -ltnH "sport = :7332")"; test -w /mnt/nas/mycelium/checkpoints; systemd-analyze --user verify ~/.config/systemd/user/myce-worker.service; systemctl --user daemon-reload; systemctl --user enable --now myce-worker.service; systemctl --user is-active myce-worker.service'
```

Success: active, no inventory/model-config errors. On B70, open a dedicated
shell for subsequent CLI commands:

```sh
set -eu
set -a; . ~/.config/myce/queue.env; set +a
MYCE="$HOME/.local/bin/myce"
"$MYCE" status
```

Success: exactly one worker `spark-v2-test`, CUDA, positive usable memory, no NAS
warning or memory-source diagnostic. Observe available memory again under existing
Atlas usage; it must reflect allocations outside Mycelium and the reserve.
Queue limits do not apply to worker children; do not copy the discovery
report's 256 MiB smoke-only unit into a model-serving install.

## 3. Confirm NAS writes in both directions

Mac, after checking mount identity in step 1:

```sh
ssh b70 'set -eu; umask 077; test ! -e /mnt/nas/mycelium/checkpoints/synthetic-probe; mkdir /mnt/nas/mycelium/checkpoints/synthetic-probe; printf b70 > /mnt/nas/mycelium/checkpoints/synthetic-probe/b70.tmp; mv /mnt/nas/mycelium/checkpoints/synthetic-probe/b70.tmp /mnt/nas/mycelium/checkpoints/synthetic-probe/b70'
ssh spark 'set -eu; root=/mnt/nas/mycelium/checkpoints/synthetic-probe; test "$(cat "$root/b70")" = b70; printf spark > "$root/spark.tmp"; mv "$root/spark.tmp" "$root/spark"'
ssh b70 'test "$(cat /mnt/nas/mycelium/checkpoints/synthetic-probe/spark)" = spark'
```

Success: both reads match after rename. Leave this small evidence folder intact.
A missing share blocks the test; a writable local folder is not acceptance.

## 4. Run the smoke job

B70 CLI shell (management diagnostic exception to GPU-only workloads):

```sh
cat > ~/.local/share/myce/smoke.yaml <<'YAML'
name: spark-v2-smoke
project: v2-founder-test
priority: production
run:
  command: [nvidia-smi, '--query-gpu=name,driver_version,uuid', '--format=csv,noheader']
needs: {memory_gb: 0.01, platform: [cuda]}
allowed_machines: [spark-v2-test]
resumable: false
YAML
smoke_id=$("$MYCE" submit ~/.local/share/myce/smoke.yaml)
"$MYCE" logs "$smoke_id" --follow
"$MYCE" status
```

Success: `succeeded`, attempt 1, Spark placement, GB10/driver output. Log-follow
ending alone is not success.

## 5. Make a real model call on Spark

B70 CLI shell, only with the reviewed catalog and live admission:

```sh
python3 - <<'PY'
import json, os, urllib.request
request = urllib.request.Request(os.environ['MYCE_QUEUE'] + '/v1/chat/completions',
    data=json.dumps({'model':'production-smoke','messages':[{'role':'user','content':'Reply with hello.'}]}).encode(),
    headers={'Authorization':'Bearer '+os.environ['MYCE_TOKEN'], 'Content-Type':'application/json',
             'X-Myce-Project':'v2-founder-test','X-Myce-Priority':'production'})
with urllib.request.urlopen(request, timeout=600) as response:
    answer=json.load(response)
assert answer['model'] == 'production-smoke' and answer['choices'][0]['message']['content']
print(answer['choices'][0]['message']['content'])
PY
"$MYCE" status
```

Success: complete real answer; status shows `production-smoke` ready on Spark,
zero in-flight calls, then absent after catalog idle timeout. Inspect Spark's
worker `engine-*.log` for GPU offload and the reviewed contract; HTTP 200 alone
does not prove GPU execution. If serving identity/settings differ, stop; no
CPU fallback or fake-answer acceptance.

## 6. Loss and checkpoint recovery

Mac copy the standalone diagnostic (no project runtime needed):

```sh
scp examples/resumable.py spark:/home/user/.local/share/myce/resumable.py
```

B70 CLI shell, wait until the model is absent in status before submitting:

```sh
cat > ~/.local/share/myce/recovery.yaml <<'YAML'
name: spark-v2-recovery
project: v2-founder-test
priority: production
run:
  command: [python3, /home/user/.local/share/myce/resumable.py]
needs: {memory_gb: 0.01, platform: [cuda]}
allowed_machines: [spark-v2-test]
resumable: true
YAML
recovery_id=$("$MYCE" submit ~/.local/share/myce/recovery.yaml)
"$MYCE" logs "$recovery_id" --follow
```

After `checkpoint=3` appears, Mac second terminal:

```sh
ssh spark 'systemctl --user kill --kill-whom=main --signal=SIGKILL myce-worker.service'
```

Success: restart-on-failure cleans old children, job resumes with attempt 2
and `resume` at least 3, then succeeds with `DONE saved=60`; no old attempt logs
arrive afterward. The worker spool and NAS must remain intact.

Optional cable test: submit a new recovery job with the same command; once it
has checkpointed, physically unplug **Spark only** for 10 seconds and reconnect.
Keep local console access. Success: with the sole worker quiet, status warns
about network loss and retains assignment; logs/results replay once on return.
The 5/20/60-second failover rules require another healthy worker; do not invent
a B70 worker to prove them. Fake-clock/full-system tests cover that case locally.

## 7. Reboot test

B70 CLI shell: submit a new recovery job, wait for a checkpoint, then restart
only v2 queue (not the host):

```sh
reboot_id=$("$MYCE" submit ~/.local/share/myce/recovery.yaml)
"$MYCE" logs "$reboot_id" --follow
# From a second B70 shell, after a checkpoint appears:
systemctl --user restart myce-queue.service
```

Success: live Spark command keeps its attempt, job ID survives, spooled output
replays, and job succeeds. Queue offers a 3-minute recovery window.

For an actual **Spark host** reboot, obtain a maintenance window from its
existing owner first; do not reboot while Atlas or other jobs depend on it.
Submit another recovery job and wait for a checkpoint, then on Spark console:

```sh
sudo reboot
```

After boot (Mac):

```sh
ssh spark 'systemctl --user is-active myce-worker.service; journalctl --user -u myce-worker.service -n 30 --no-pager'
ssh b70 'set -a; . ~/.config/myce/queue.env; set +a; ~/.local/bin/myce status'
```

Success: worker starts before login via existing linger, resumes from the last
complete save with a new attempt and succeeds. No B70 host reboot is permitted
by this plan because it would touch Verso/v1; queue restart is the bounded test.

## 8. Full undo, preserving recovery

Only after test jobs finish (or explicitly cancel their IDs and wait). Mac:

```sh
ssh spark 'set -eu; if systemctl --user is-active --quiet myce-worker.service; then set -a; . ~/.config/myce/worker.env; set +a; ~/.local/bin/myce leave --name spark-v2-test --now; systemctl --user disable --now myce-worker.service; fi'
ssh b70 'systemctl --user disable --now myce-queue.service'
for host in spark b70; do
  ssh "$host" 'set -eu; umask 077; recovery="$HOME/myce-founder-recovery"; test ! -e "$recovery"; mkdir -m 700 "$recovery"; mv ~/.local/bin/myce "$recovery/binary"; mv ~/.config/myce "$recovery/config"; mv ~/.local/share/myce "$recovery/state"; for unit in myce-queue.service myce-worker.service; do if test -e "$HOME/.config/systemd/user/$unit"; then systemctl --user disable --now "$unit"; mv "$HOME/.config/systemd/user/$unit" "$recovery/$unit"; fi; done; systemctl --user daemon-reload; test -x "$recovery/binary"; test -d "$recovery/config"; test -d "$recovery/state"'
done
ssh b70 'test -f ~/myce-founder-recovery/myce-queue.service; test -z "$(ss -ltnH "sport = :7331")"'
ssh spark 'test -z "$(ss -ltnH "sport = :7332")"'
```

Success: only v2 is disabled, its ports are closed, private binary/config/state/
units are recoverable. Leave NAS checkpoints/probes and recovery bundles intact.
No deletion, token printing, old-service changes, mount changes, or guard changes.
Restore by reversing these moves only for a separately approved test.

## NAS model setup and cache acceptance (run only with fleet authorization)

1. Create/mount a NAS master-model folder. Choose the exact approved GGUF and
   obtain trusted SHA-256/byte count. Explicitly run `myce model fetch FILE
   --model-root NAS_MODELS --url HF_HTTPS_FILE_URL --size-bytes N --sha256 HASH`.
   Confirm a second fetch refuses to overwrite the master. Record the catalog
   and inference contract without tokens.
2. Add relative `path`, `size_bytes`, and `sha256` to the shared catalog. Start
   each worker with its mount's `--model-root`, local `--model-cache`, and
   bounded `--model-cache-bytes`. Confirm the cache is on local disk, not NAS.
3. Before any latency-sensitive call, measure actual cold copy speed on each
   eligible machine: `myce model sync MODEL --to MACHINE`, then `myce status`.
   Record model size, achieved bytes/s, elapsed duration, cache allowance and
   free disk. Use one copy at a time (`--model-copy-parallel 1`); repeat on the
   other machine separately to avoid measuring NAS contention. B70/Spark
   historical reads of about 74/42 MB/s are context, not current acceptance.
   Select a cache allowance below actual free disk, with operating headroom
   (B70 historically has only about 100 GB free). Pre-copy all production models
   to every eligible target; wait for verified ready cache reports. Pin models
   and confirm standby caches on every landing machine, even before windows.
   Submit a model call. Verify local temporary copy, verified atomic publication,
   and llama-server's local file path. Confirm no internet fetch during loading.
   Record copy-plus-load duration separately from inference.
4. Check cached-model/free-byte reports. With unloaded disposable cache entries,
   reduce the allowance to provoke LRU eviction. Verify recovery in NAS Trash;
   confirm a loaded/pinned model survives. Test actual local filesystem pressure
   and visible refusal when neither eviction nor available space can fit it.
5. Leave a disposable `.partial` in a stopped worker's cache and restart it.
   Confirm byte-offset resume, full final checksum and that partial bytes never
   reach the engine. Interrupt a sync with cancellation, NAS/network loss and
   worker restart; confirm preserved bytes and visible paused/error status.
   Submit real work during a copy and verify the copy pauses, then resumes when
   idle. Fill a bounded disposable cache and prove pinned/loaded/more-used
   copies survive. Compare two workers with measured speeds and warm/cold
   caches; confirm placement favors the lower estimated load cost.
   Test bad size/checksum and NAS unavailability, expecting visible failures.
6. Pin production by machine, then by platform, and inspect assigned ready state.
   Submit memory-hungry competing work: pinned memory must remain reserved.
   Exercise weekday/time-zone start/end boundaries and unpin. Confirm SQLite
   persistence after a queue restart and warmup after a worker restart.
7. Run resumable and non-resumable lower-priority experiments. Submit production
   while busy and measure submission-to-process-start, targeting <15s excluding
   model load. The resumable case has 10s default grace; the non-resumable case
   stops immediately and requeues without a loss retry. Check stale-result
   rejection. Repeat with deadline work: 2m grace and no non-resumable victim.

Laptop regression: `go test ./... -race` includes fake-clock cache/pin/preemption
checks and `TestDevProductionStartsUnder15Seconds`, using real API/commands with
fake GPU inventory. These checks authorize no real-machine action.
