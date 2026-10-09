# First real-machine test: ordered founder checklist

Run these steps in order under explicit fleet authorization; stop at the first
failed gate. This document is the single ordered checklist. The other guides
explain contracts and general operations, not additional acceptance sequences.
No fleet command here was executed during the local pre-production review.

The old v1 service was already removed from B70 on **2026-10-09**. Do not
reinstall it. Preserve unrelated legacy data, Verso, Atlas, guards and controllers.
B70 is CPU queue only at `192.0.2.153:7331`; never start a B70 worker
in this first Spark acceptance test. The separately authorized weekend
measurement below is a local maintenance experiment, not queue cutover. Spark
is the only worker in this first test. Queue restart is allowed within this test;
B70 host reboot requires a separate Verso maintenance window. A Spark host reboot
requires its owner's maintenance window and local console access.

## 1. Qualify prerequisites and build locally

- Obtain Spark GPU admission from its existing owner, and maintenance authority
  for the planned fault/reboot tests. No competing jobs may use this test queue.
- Use existing mounts of `//storage.example/share`: Spark `/mnt/nas`,
  B70 `/mnt/nas` or `/mnt/nas`. Confirm with `findmnt -T PATH`;
  a writable local folder is not a NAS mount. Do not install or remount anything
  to pass a gate. Choose B70's actual mount once and use it consistently below.
- Confirm existing user linger, Python 3.10+, and an installed GPU-enabled
  `llama-server` on Spark. For the GPU batch below, Spark also needs existing
  CUDA-enabled PyTorch. If absent, stop and arrange that prerequisite separately.
- Review `ops/private/myce-models.yaml`: a CUDA chat model named
  `production-smoke` and a separate disposable `copy-smoke` for step 8,
  both present in the initial shared catalog, with immutable NAS-relative GGUF
  `path`, trusted `size_bytes`
  and `sha256`, total weights/KV/concurrency `memory_gb`, and explicit args.
  Read its official model card/generation configuration; record actual identity,
  reasoning/template, context, output budget, sampler and tool settings. No fake
  engine, borrowed Atlas process, or CPU fallback is acceptable.

On Mac, from this checkout:

```sh
set -eu
umask 077
mkdir -p /tmp/myce-production-build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /tmp/myce-production-build/myce-amd64 ./cmd/myce
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o /tmp/myce-production-build/myce-arm64 ./cmd/myce
test -s ops/private/myce-models.yaml
shasum -a 256 /tmp/myce-production-build/myce-amd64 /tmp/myce-production-build/myce-arm64
```

Record the commit and corresponding hashes. On each host, check that v2 binary,
config/state and service destinations are absent, ports 7331 (B70) and 7332
(Spark) are free, and `loginctl show-user user -p Linger --value` is `yes`.
Existing/ambiguous destinations stop this fresh-install checklist.

Linux systemd **user** services require lingering to start before login and to
survive logout. `myce doctor` checks `loginctl show-user UID --property=Linger
--value` and fails unless it says `yes`. If disabled, stop this gate; an
administrator must explicitly run `loginctl enable-linger user`, then repeat
that check and doctor. Doctor never enables it or elevates. `enable --now`
alone does not prove boot-time availability. macOS launch agents start at login.

## 2. Prepare private files and prove NAS identity

On both hosts, after step 1:

```sh
set -eu
umask 077
mkdir -p ~/.local/bin ~/.config/systemd/user
mkdir -m 700 ~/.config/myce ~/.local/share/myce
```

From Mac, transfer the amd64 binary to B70 and arm64 binary to Spark as
`/home/user/.local/bin/myce`. Transfer the identical approved catalog as
`/home/user/.config/myce/models.yaml` on both. Set binary mode 0700 and catalog
0600. Compare each host's `sha256sum` with its local build hash.

Privately create one random fleet token and store it mode 0600 in each host's
config; never print it, use shell tracing, or put it in argv, history or the repo.
B70 `queue.env` and Spark `worker.env` both contain `MYCE_TOKEN` and
`MYCE_QUEUE=http://192.0.2.153:7331`. Use a trusted editor/secret transport.

In dedicated host shells, define `NAS` as the verified mount from step 1:

```sh
# Spark:
NAS=/mnt/nas
# B70: choose its actual verified mount, not a directory that merely exists:
# NAS=/mnt/nas
# NAS=/mnt/nas
MYCE="$HOME/.local/bin/myce"
ROOT="$NAS/mycelium/checkpoints"
MODELS="$NAS/mycelium/models"
```

Create `ROOT` and `MODELS` only beneath the verified mounted share. Provision the
approved immutable master into `MODELS` before starting the worker. If an explicit
download is needed, use `myce model fetch FILE --model-root "$MODELS" --url
APPROVED_HF_HTTPS_URL --size-bytes EXACT_BYTES --sha256 APPROVED_HASH`; loading
never downloads. Verify byte count and SHA-256; a second fetch must refuse an
existing destination. Never change an existing master to test a bad checksum.

Create a new shared probe folder under `ROOT`, write `b70.tmp`, fsync and rename
it to `b70` on B70, then read it on Spark. Do the reverse with `spark.tmp`/`spark`.
Both reads must match. Leave probe evidence intact. Each root must refer to the
same share directory despite differing mount paths.

## 3. Start the CPU-only B70 queue with GPUs hidden

Render `deploy/myce-queue.service` privately: replace `REPLACE_QUEUE_IP` with
`192.0.2.153` and `REPLACE_CHECKPOINT_ROOT` with B70's `ROOT`. Add
`--model-config %h/.config/myce/models.yaml` to `ExecStart`. Install only this
queue unit as `~/.config/systemd/user/myce-queue.service`. Keep
`PrivateDevices=yes`, `GOMAXPROCS=1`, and the initial CPU/RAM limits.

On B70:

```sh
systemd-analyze --user verify ~/.config/systemd/user/myce-queue.service
systemctl --user daemon-reload
systemctl --user enable --now myce-queue.service
systemctl --user is-active myce-queue.service
ss -ltnH 'sport = :7331'
queue_pid=$(systemctl --user show myce-queue.service -p MainPID --value)
sudo nsenter -t "$queue_pid" -m -- sh -c 'test ! -e /dev/dri && test ! -e /dev/nvidia0'
set -a; . ~/.config/myce/queue.env; set +a
"$MYCE" status
```

Acceptance: active queue at port 7331, no sandbox warnings, GPUs hidden, no B70
worker. If hiding is unsupported, stop the v2 queue; do not remove the sandbox.
No host restart or Verso change is involved.

## 4. Start the Spark CUDA worker with unified memory and 16 GiB reserve

On Spark, read `nvidia-smi` and `/proc/meminfo`: exactly one CUDA device,
valid positive `MemAvailable`, and headroom under current Atlas usage must fit
reviewed model memory after reserve. Confirm `torch.cuda.is_available()`.
Choose a local-disk cache allowance below current free disk, leaving OS/spool
headroom. Record `df` and the selected byte allowance; 100 GiB is a default,
not a safe assumption about free space.

Render `deploy/myce-spark-worker.service` privately using Spark's `ROOT`. Set
`--memory-mode unified --reserve-memory-gb 16`; keep `--memory-gb 0` unless an
approved cap is needed (cap applies before reserve). Add these to `ExecStart`,
replacing `CACHE_BYTES` with the measured allowance:

```text
--model-config %h/.config/myce/models.yaml --model-root /mnt/nas/mycelium/models --model-cache %h/.local/share/myce/model-cache --model-cache-bytes CACHE_BYTES --model-copy-parallel 1 --model-listen 192.0.2.16:7332 --model-advertise http://192.0.2.16:7332
```

Install as `~/.config/systemd/user/myce-worker.service`. On Spark:

```sh
systemd-analyze --user verify ~/.config/systemd/user/myce-worker.service
systemctl --user daemon-reload
systemctl --user enable --now myce-worker.service
systemctl --user is-active myce-worker.service
```

B70: `"$MYCE" status` must show exactly `spark-v2-test`, CUDA, positive usable
memory, matching catalog, no NAS/inventory diagnostic. Live memory must reflect
external usage and the reserve. A missing/invalid OS probe must visibly block
work; fixed-mode substitution cannot cure this gate. Do not apply queue-only
memory limits to the worker or its engines.

## 5. Measure copy speed and pre-copy one model before a call

B70:

```sh
"$MYCE" model sync production-smoke --to spark-v2-test
"$MYCE" status
```

Wait for `cached_models` to include `production-smoke` and its copy report to be
`ready`. Record exact bytes, wall duration, achieved bytes/s, filesystem free
space and remaining cache allowance. This real measured cold copy replaces the
40 MB/s initial estimate; do not substitute historical B70/Spark speeds. Confirm
local `.partial` becomes a full local cache file only after size/SHA verification.
Recompute the full local checksum on Spark against the approved master.

Cancellation pauses a target request and retains its prefix:
`myce model sync production-smoke --to spark-v2-test --cancel`; resend without
`--cancel` to resume. A completed request cancellation does not evict the file.
If testing interruption requires another copy, use a separately cataloged
immutable disposable model within the same approved disk budget. Never modify
production masters or unload a pin to force this test.

## 6. Pin and make a real model call

B70:

```sh
"$MYCE" model pin production-smoke --project v2-founder-test --machine spark-v2-test
"$MYCE" status
```

Wait for the assigned loaded slot to be `ready`, then:

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

Acceptance: real complete answer, zero in-flight calls, ready pin stays loaded
past idle timeout. Inspect Spark's spool `engine-*.log` and process args for GPU
offload, correct local cache path and the reviewed inference contract. HTTP 200
alone is insufficient. Stop on identity/settings mismatch or CPU fallback.
Keep the pin through steps 7–8; reserve its memory in the batch budget.

## 7. Run one real GPU batch; confirm it completes once

B70, with the existing Spark PyTorch prerequisite and remaining headroom:

```sh
cat > ~/.local/share/myce/batch.yaml <<'YAML'
name: spark-v2-gpu-batch
project: v2-founder-test
priority: production
run:
  command: [python3, '-c', 'import torch; assert torch.cuda.is_available(); x=torch.ones((1024,1024),device="cuda"); y=x@x; torch.cuda.synchronize(); assert y[0,0].item()==1024; print("GPU_BATCH_DONE",flush=True)']
needs: {memory_gb: 2, platform: [cuda]}
allowed_machines: [spark-v2-test]
resumable: false
YAML
batch_id=$("$MYCE" submit ~/.local/share/myce/batch.yaml)
"$MYCE" logs "$batch_id" --follow
"$MYCE" status
```

Acceptance: Spark placement, succeeded, attempt/start count 1, exactly one
`GPU_BATCH_DONE`, pin still ready. Review 2 GiB against actual runtime overhead
before submission; amend it if the installed runtime needs more. Logs ending
alone is not success.

## 8. Test copy cancellation, network loss and priority arrival

Using the disposable `copy-smoke` catalog model that fits beside the pin, run
`myce model sync copy-smoke --to spark-v2-test` and wait for nonzero partial bytes.
Keep both models in the initial queue/worker catalog; do not edit it mid-test. During the
copy submit the batch from step 7. Acceptance: production starts promptly
(target <15s excluding model load), copy pauses, partial bytes remain, pin stays
loaded, batch finishes once, and verified copy resumes when idle. No partial file
may appear in engine args. Record queue start latency and copy progress.

Repeat with a queue service restart mid-copy. The request and pin must survive;
Spark keeps its loaded engine and the copy completes after reconciliation.
Repeat with Spark-only cable loss mid-copy, with local console access. Queue
connection loss pauses copying; shared-network NAS calls may wait on mount
timeouts. Reconnect: preserved bytes resume, full verification precedes publication,
no new batch starts while disconnected, and no job ID disappears. With one
worker quiet, status retains assignment and warns about network loss; failover
requires another healthy worker and is covered by laptop tests, not a new B70 worker.

For cache pressure or bad-hash testing use isolated disposable entries and a
bounded test allowance. Verify refusal is visible, evictions are recoverable in
NAS Trash, and pinned/loaded/more-used copies survive. Never change live model
bytes, fill the host disk, empty Trash or silently lower an active pin's allowance.

## 9. Test job loss and reboot recovery

Copy `examples/resumable.py` to Spark's `~/.local/share/myce/resumable.py`.
This is a checkpoint/process diagnostic exception to GPU-only work; it is not a
GPU throughput test. Submit this spec from B70:

```yaml
name: spark-v2-recovery
project: v2-founder-test
priority: production
run:
  command: [python3, /home/user/.local/share/myce/resumable.py]
needs: {memory_gb: 0.01, platform: [cuda]}
allowed_machines: [spark-v2-test]
resumable: true
```

After `checkpoint=3`, kill only the worker main process:
`systemctl --user kill --kill-whom=main --signal=SIGKILL myce-worker.service`.
Its restart-on-failure must clean owned old children before a new attempt,
resume from a complete save, and succeed with one `DONE saved=60`. Pin warmup
is expected after an actual worker restart, and must use verified local files.

Submit a new recovery job. After a checkpoint, restart only B70's v2 queue:
`systemctl --user restart myce-queue.service`. Acceptance: same live Spark
attempt, job ID retained, logs replay once, job succeeds; queue has a 3-minute
recovery grace. Active model HTTP calls are not durable across queue restart;
record connection errors and decide explicitly whether to resubmit them.

For a new recovery job, within the approved Spark maintenance window and from
its local console, `sudo reboot`. Acceptance: linger starts the worker before
login, new attempt resumes last complete save and succeeds, pin reloads from
verified cache, no orphan survives. B70 host reboot remains outside this plan
because of Verso. Partitioned commands may overlap until reconnection; verify
one retained winner and stale-result rejection, not exactly-once external side effects.

## 10. Undo only v2, retaining all recovery evidence

Cancel unfinished test job IDs and wait for stop acknowledgement, then B70:

```sh
"$MYCE" model unpin production-smoke --project v2-founder-test
"$MYCE" model sync production-smoke --to spark-v2-test --cancel
"$MYCE" model sync copy-smoke --to spark-v2-test --cancel
"$MYCE" leave --name spark-v2-test --now
```

On Spark, disable/stop `myce-worker.service`; on B70 disable/stop
`myce-queue.service`. Confirm all owned child processes have exited and ports
7332/7331 are closed. Never stop Atlas or Verso.

On each host, create a private new `~/myce-founder-recovery` bundle, move only
v2's binary, `~/.config/myce`, `~/.local/share/myce`, and the installed v2 unit
into it, then `systemctl --user daemon-reload`. Verify the binary is executable,
config/state/SQLite/spool/cache and unit files are readable and their original
paths recorded. Do not overwrite an existing recovery bundle. Leave NAS masters,
Trash, probes and checkpoints intact. No deletion or automatic Trash emptying.
Restoration reverses these moves only for a separately authorized test.

Record checks, hashes, job/attempt IDs, counts, copy speed, actual inference
contract, faults and undo results without tokens or private document text.
Laptop checks (`go test ./... -race`, including both `TestPreDeploy*` scenarios,
and the full Python suite with zero skips) support software acceptance only;
this checklist's unexecuted fleet gates still determine hardware readiness.

## B70 GPU0 weekend joint residency measurement

This is a separate, opt-in maintenance test. It does not authorize a B70 worker
cutover or change the first-machine checklist. Arrange a Verso maintenance
window with its owner. Stop new production submissions through its existing
owner-controlled procedure; let actual jobs finish, stop the existing audio /
naming / experiment GPU0 owner through its documented procedure, and verify no
other process owns GPU0. Never improvise controller commands or borrow GPU1.
Preserve the exact original service/controller settings for restoration.

1. Prepare a private, reviewed catalog named `verso-models.yaml` with these
   distinct entries: `verso-asr`, `verso-diarization`, `verso-alignment`,
   `verso-short-turn`, and `verso-naming`. Use exact deployed weights and server
   wrappers; the audio wrappers are application integration work, not supplied
   by Mycelium. No fake servers or changed runtime/math may pass this test.
   Include all qualified assist/companion weights in the short-turn entry or
   separate listed models. If accuracy mode is in scope, add its three recognizers
   and fusion to a separate attempt. Missing catalog/wrapper/model identity is a
   failed prerequisite. Use approved NAS manifests, explicit XPU recipes from
   `environments.yaml`, `device: '0'`, loopback readiness and measured/reviewed
   conservative declared memory budgets. Preserve naming context/template/sampler.
2. Check current official model cards, generation config and qualified runtime
   settings for each. Record model revision/file hashes, Python/torch/XPU and
   llama.cpp build, reasoning/template, context/output limit, sampler and parser
   settings. Existing environments and runtime tools must already be available;
   arrange missing prerequisites separately. Do not run installs to bypass a gate.
3. Confirm `xpu-smi discovery` maps device **0** to the intended physical GPU0.
   Check `xpu-smi stats -d 0 --json`: `device_id` must be 0 and `device_level`
   must include a finite current `XPUM_STATS_MEMORY_USED` value (MiB). Mycelium
   refuses unavailable/unsupported counters; configured memory is not a reading.
   Verify each framework respects `ONEAPI_DEVICE_SELECTOR=level_zero:0` and
   `ZE_AFFINITY_MASK=0`. Record native inventory and total card capacity. Choose
   a device usage budget below that capacity with explicit safety headroom;
   `memory_gb` is GiB despite its name. Native host accounting is
   `MemTotal - MemAvailable`, which includes other workloads/reclaimable cache.
4. Run locally on B70, with any `b70-gpu0` Mycelium join stopped. If such a worker
   was previously joined, use its graceful leave/reconciliation first; use the
   exact `--worker`, `--queue` and `--state-dir` it used. An active join, unfinished
   spool or live saved engine refuses measurement even with `--force`. Do not
   change spool paths to evade this protection. Never stop the CPU queue.
5. Prepare `verso-warmup.json`, a JSON object keyed by every listed model. Each
   value is `{ "method": "POST", "path": "/QUALIFIED_ENDPOINT", "body":
   {"QUALIFIED_INPUT": "NAS_REFERENCE_OR_INPUT"}, "headers": {"Content-Type":
   ["application/json"]} }`. Replace these placeholders with representative
   qualified audio lengths, batches, naming context and concurrency inputs.
   Each warmup executes once while earlier models remain loaded. Test concurrent
   peak workloads separately: sequential warmup is not concurrent acceptance.

In the approved local B70 shell, set the following to reviewed **existing** paths
and the explicit budget from step 3 (do not copy these placeholder strings):

```sh
# MYCE_TOKEN comes from private credentials, never tracing or command arguments.
# These task-specific variables do not replace HOME or other system variables.
myce_measure_catalog=/VERIFIED/NAS/mycelium/verso-models.yaml
myce_measure_envs=/VERIFIED/NAS/mycelium/environments.yaml
myce_measure_models=/VERIFIED/NAS/mycelium/models
myce_measure_spool=/EXACT/LOCAL/b70-gpu0-spool
myce_measure_budget=REVIEWED_DEVICE_USAGE_BUDGET_GIB
myce_measure_report=/NEW/LOCAL/REPORT/DIRECTORY/verso-joint-attempt-1
myce measure --queue "$MYCE_QUEUE" --worker b70-gpu0 \
  --state-dir "$myce_measure_spool" --platform xpu --device 0 \
  --memory-gb "$myce_measure_budget" \
  --model-config "$myce_measure_catalog" --environment-config "$myce_measure_envs" \
  --model-root "$myce_measure_models" \
  --models verso-asr,verso-diarization,verso-alignment,verso-short-turn,verso-naming \
  --warmup /REVIEWED/LOCAL/verso-warmup.json \
  --report-prefix "$myce_measure_report" --output plain
```

Default cache locations match join's `model-cache` and `environment-cache` under
its spool. Supply its explicit cache paths when different. Cache eviction is
blocked during measurement; pre-copy enough space first. Model loads are bounded
by five minutes and any shorter `ready_timeout`; use a qualified timeout in the
catalog. The command records one simultaneous resident set and unloads all
attempted engines in reverse order even if load/warmup/probe fails. It keeps the
exclusive spool lock through cleanup. Optional stop commands cannot avoid KILL
escalation. Verify native process/device release afterwards before restoring the
original Verso owner through its documented procedure.

Without `--force`, queue unavailability, outstanding production batches,
production pins for that worker/platform, or active calls/production slots there
refuse the test. `--force` bypasses only that queue safety check; use it solely
when the approved maintenance procedure has stopped all competing production.
It neither stops production for you nor bypasses the spool lock/recovery checks.

The command writes `PREFIX.json` and `PREFIX.txt`, refuses overwriting them, and
prints the selected stdout format. Reports contain native absolute device/host
peaks, baseline, incremental per-model deltas, combined delta, engine readiness
load seconds, catalog hash, sample interval, physical device binding and fit result. Preparation time is
excluded from load seconds. Peaks are sampled at 100 ms; shorter spikes can be
missed. Incremental attribution is order-dependent and system-level, not a
per-process allocator measurement. Run distinct report prefixes with audio-first
and naming-first order, then representative concurrent workload tests with the
existing native guard. A cold-readiness-only run cannot qualify audio activations.

Acceptance requires every exact model loaded, representative warmups successful,
`fits_device_budget: true`, sufficient host headroom, clean native release and
successful restoration of original production ownership. A false/partial report,
unsupported memory counter or failed cleanup fails the gate. Preserve all attempt
reports. Do not pin production or promise immediate starts based on disk bytes
or a single sample. If mandatory audio+naming cannot fit GPU0, stop migration and
make a capacity/product decision; no hidden GPU1 move, smaller context or fallback.
