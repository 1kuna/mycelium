Start with [the agent starting guide](START-HERE.md) when integrating another project.

# Integrating Atlas and Verso

Projects submit bounded GPU work and let go of the GPU afterward. Keep Atlas's
SharePoint watcher and CPU preparation outside Mycelium. Verso controls when
its B70 GPUs are lent; no background worker should compete with its existing
production workload. These are integration recipes, not evidence that either
project has migrated or that fleet hardware has been tested.

## Copy-paste laptop walkthrough

From this checkout, start the real queue/runner with simulated GPU inventory:

```sh
go build -o /tmp/myce-step3 ./cmd/myce
/tmp/myce-step3 dev --workers 1 --memory-gb 8 --model-idle 5s --token dev-token
```

Keep it open. In another terminal **in this checkout**, create a fresh Python
3.10+ environment first. On Macs, `python3` may still be Apple's unsupported
3.9; use an installed `python3.13` (or another 3.10+ interpreter). No system
Python packages are changed:

```sh
python3.13 -m venv /tmp/myce-adopter-venv
. /tmp/myce-adopter-venv/bin/activate
python -m pip install './clients/python[yaml,openai]'
```

Then set the local connection:

```sh
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
MYCE=/tmp/myce-step3
```

### Atlas batch

This executable demonstration uses a shell instead of Atlas's GPU indexing
command. It tests submission/logging; it does not produce an index. For real
use, replace the argv and workdir with the project's existing GPU batch entry
point, declare its measured memory/platform/environment, and install its runtime
on every eligible worker. Do not invent a new Atlas command interface here.

```sh
cat > /tmp/atlas-batch.yaml <<'YAML'
name: atlas-index-batch-7
project: atlas
priority: experiment
run:
  command: [sh, -c, 'echo "Atlas batch 7: job=$MYCE_JOB_ID attempt=$MYCE_ATTEMPT"; echo "Replace this demonstration with the real GPU index command"']
needs:
  memory_gb: 8
  platform: any
expected_minutes: 90
resumable: false
YAML
atlas_id=$("$MYCE" submit /tmp/atlas-batch.yaml)
"$MYCE" wait "$atlas_id"
"$MYCE" logs "$atlas_id"
"$MYCE" status
```

A deployment job can use this shape after configuring the real command:

```yaml
name: atlas-index-batch-7
project: atlas
priority: experiment
run:
  command: [/srv/atlas/.venv/bin/python, /srv/atlas/REPLACE_GPU_BATCH_ENTRYPOINT.py, --batch, "7"]
  workdir: /srv/atlas
  env: {PYTHONUNBUFFERED: "1"}
needs:
  memory_gb: 40
  platform: [cuda]
environment: atlas-cuda
allowed_machines: [spark, pc4090]
expected_minutes: 90
resumable: false
```

The `REPLACE` path is deliberately not claimed to exist. `needs.model` provides
an affinity hint, not an inference API. A batch that runs its own GPU code
reserves the memory itself. Avoid nesting queue calls from a running batch that
already holds the memory needed by those calls. Make model calls from the CPU
orchestrator instead when using queue-managed inference.

### Atlas chat and OCR

Atlas's main GPU workload on the DGX Spark is a roughly 35B chat model plus an
OCR server, both running vLLM 0.27.1. Its embeddings use FastEmbed on CPU and
stay outside Mycelium. Keep that existing embedding contract unchanged.

Use two `engine: server` entries with Atlas's exact reviewed executable and
argv, rather than the `vllm` preset. The preset adds its own flags and aliases;
generic server entries preserve supplied argv. See the
[placeholder catalog](../examples/atlas-judge-vllm.example.yaml). Replace every
placeholder from approved local configuration, including immutable file
manifests and measured joint memory. Preserve model identity, math,
context/output limits, generation configuration, template, reasoning and tool
parsers. No fleet settings are inferred by this example.

Call from the CPU orchestrator with a finite deadline and no SDK retries:

```python
import os
from openai import OpenAI
client = OpenAI(
    base_url=os.environ["MYCE_QUEUE"].rstrip("/") + "/v1",
    api_key=os.environ["MYCE_TOKEN"],
    default_headers={"X-Myce-Project": "atlas", "X-Myce-Priority": "production"},
    timeout=600, max_retries=0,
)
result = client.chat.completions.create(
    model="REPLACE_APPROVED_CHAT_CATALOG_NAME",
    messages=[{"role": "user", "content": "Synthetic test input"}],
)
# Consume the complete result locally; never print document text or error bodies.
```

OCR callers can use OpenAI routes when their exact server supports them, or
`/models/REPLACE_APPROVED_OCR_CATALOG_NAME/PATH` for its native API. Generic
retry defaults to off; opt in only when replay cannot duplicate side effects.
Apply [private workload settings](guide-servers.md#private-workloads) before
sending company documents. Workers never inherit the fleet token.

### Verso production model calls

For a Verso inference stage already exposed through a supported OpenAI endpoint,
move the call into the CPU orchestrator and route it at production priority:

```python
import os
from mycelium import Client
with Client().openai(project="verso", priority="production", timeout=600) as sdk:
    result = sdk.chat.completions.create(
        model=os.environ.get("VERSO_INFERENCE_MODEL", "dev-model"),
        messages=[{"role": "user", "content": "Summarize this transcript: The witness confirmed the date."}],
    )
    print(result.choices[0].message.content)
```

This complete call works against dev. On the fleet, set `VERSO_INFERENCE_MODEL`
to the existing approved model and preserve its preprocessing and generation
contract. Unsupported speech/diarization interfaces stay plain-command batch
jobs; this chat example does not replace that pipeline. Do not keep a local
model server alive in Verso: the queue loads on demand, protects active calls,
and unloads idle models. A failed stream or queue restart must be handled as a
failed request; accept a complete answer before publishing its derived output.

### Verso production and resumable work

Create this complete checkpointing demonstration. The `sleep` is a simulated
GPU unit, explicitly labeled; replace one unit with real bounded Verso GPU work
and make its outputs idempotent before production use.

```sh
# Wait for earlier models to idle-unload before occupying all memory.
python3 - <<'PYWAIT'
import json, os, time, urllib.request
for _ in range(300):
    req = urllib.request.Request(os.environ["MYCE_QUEUE"] + "/api/status",
        headers={"Authorization": "Bearer " + os.environ["MYCE_TOKEN"]})
    status = json.load(urllib.request.urlopen(req, timeout=5))
    if not status["loaded_models"]: break
    time.sleep(0.1)
else: raise SystemExit("model did not unload; inspect myce status")
PYWAIT
cat > /tmp/verso-resumable.py <<'PY'
import os, pathlib, signal, time
root = pathlib.Path(os.environ["MYCE_CHECKPOINT_DIR"])
p = root / "progress"
n = int(p.read_text()) if p.exists() else 0
print(f"resumed unit {n}; attempt {os.environ['MYCE_ATTEMPT']}", flush=True)
stopping = False
def stop(*_):
    global stopping
    stopping = True
signal.signal(signal.SIGTERM, stop)
def save():
    tmp = root / "progress.tmp"
    with tmp.open("w") as f:
        f.write(str(n)); f.flush(); os.fsync(f.fileno())
    tmp.replace(p)
    fd = os.open(root, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
while n < 100 and not stopping:
    time.sleep(0.2)  # demonstration only: replace with one real GPU unit
    n += 1
    try:
        save()
        print(f"checkpoint unit {n}", flush=True)
    except OSError as e: print(f"save will retry: {e}", flush=True)
while True:
    try: save(); break
    except OSError as e:
        print(f"final save will retry: {e}", flush=True); time.sleep(1)
print(f"saved unit {n}; attempt {os.environ['MYCE_ATTEMPT']}", flush=True)
PY
cat > /tmp/verso-experiment.yaml <<'YAML'
name: verso-resumable-demo
project: verso
priority: experiment
run:
  command: [python3, /tmp/verso-resumable.py]
  env: {PYTHONUNBUFFERED: "1"}
needs:
  memory_gb: 8
  platform: any
expected_minutes: 1
resumable: true
YAML
experiment_id=$("$MYCE" submit /tmp/verso-experiment.yaml)
# Wait for evidence of a complete checkpoint, rather than guessing startup time.
export experiment_id
python - <<'PYREADY'
import os, time
from mycelium import Client
client = Client()
for _ in range(300):
    if any("checkpoint unit" in c["text"] for c in client.logs(os.environ["experiment_id"])):
        break
    time.sleep(0.1)
else:
    raise SystemExit("experiment did not checkpoint; inspect myce status")
PYREADY
cat > /tmp/verso-production.yaml <<'YAML'
name: verso-production-demo
project: verso
priority: production
run:
  command: [sh, -c, 'echo "Verso production started"; sleep 1; echo "Verso production finished"']
needs:
  memory_gb: 8
  platform: any
resumable: false
YAML
production_id=$("$MYCE" submit /tmp/verso-production.yaml)
"$MYCE" wait "$production_id" "$experiment_id" # one detached wait for both
"$MYCE" logs "$production_id"
"$MYCE" logs "$experiment_id"
"$MYCE" status
```

Production should pause the lower-priority resumable experiment, finish, and
allow the experiment to restart from its copied checkpoint. Check the logs for
multiple attempts and continued progress. On the fleet, use absolute paths
installed on every eligible worker, or use an allow-list. `/tmp` is only for
this local walkthrough.

For real Verso production, replace the shell argv with the approved GPU
transcription/diarization command and measured reservation. Use `resumable: false`
until that command supports atomic checkpoints, SIGTERM save/exit, and safe
output replay; setting a flag alone cannot make it resumable. A resumable
production job uses the same checkpoint contract as the demonstration, and
retains `priority: production` so loss detection uses the production timeout.

## Project-shaped deployment jobs

Use these complete YAML shapes after substituting the project's approved entry
point and measuring its memory. The paths, models, budgets and machine names
below are configuration examples, not discovered production command interfaces.
All eligible workers need the same installed runtime/workdir and shared inputs.

```yaml
name: verso-transcription-production
project: verso
priority: production
run:
  command: [/srv/verso/.venv/bin/python, /srv/verso/REPLACE_GPU_TRANSCRIPTION_ENTRYPOINT.py, --input, /mnt/nas/verso/jobs/input.wav, --output, /mnt/nas/verso/jobs/output.json]
  workdir: /srv/verso
  env: {PYTHONUNBUFFERED: "1"}
needs:
  memory_gb: 12
  platform: [xpu]
  model: verso-transcription
environment: verso-xpu
allowed_machines: [b70-lent]
expected_minutes: 20
resumable: false
```

This worker must be started only when Verso lends the GPUs. For CUDA/Metal
placement, first verify that the project command and exact model math support
that platform; do not simply change the platform field to `any`.

```yaml
name: verso-resumable-evaluation
project: verso
priority: experiment
run:
  command: [/srv/verso/.venv/bin/python, /srv/verso/REPLACE_CHECKPOINT_AWARE_GPU_ENTRYPOINT.py, --input, /mnt/nas/verso/eval/manifest.json, --output, /mnt/nas/verso/eval/results]
  workdir: /srv/verso
  env: {PYTHONUNBUFFERED: "1"}
needs:
  memory_gb: 12
  platform: [xpu]
  model: verso-transcription
environment: verso-xpu
allowed_machines: [b70-lent]
expected_minutes: 90
resumable: true
```

The second entry point must read `MYCE_CHECKPOINT_DIR`, handle SIGTERM, save
atomically, and resume exactly as demonstrated. Shared output writes must be
idempotent or attempt-isolated; the checkpoint folder alone does not isolate
`--output`. To make the checkpoint-aware job production, change priority to
`production`; the same contract applies. For a deadline job set `priority:
deadline` and add `deadline: 2026-10-09T18:00:00Z`. To stop waiting after a chosen
time add `give_up_at: 2026-10-10T18:00:00Z`; neither field terminates running work.

For long jobs, run the wait below detached as described in
[the agent workflow](START-HERE.md#waiting-for-long-jobs-agents).

After writing the selected job to `job.yaml`, the deployment submission flow is:

```sh
id=$("$HOME/.local/bin/myce" submit job.yaml)
"$HOME/.local/bin/myce" wait "$id"
"$HOME/.local/bin/myce" logs "$id"
"$HOME/.local/bin/myce" status
# If cancellation is desired:
# "$HOME/.local/bin/myce" cancel "$id"
```

## Production handoff

Mount the same shared NAS root on queue and workers; local mount paths may differ. Start
queue with the configured 5s/20s/60s priority loss timeouts, and workers with a
1s heartbeat and explicit checkpoint root. CUDA uses live dedicated memory or
unified Linux `MemAvailable`; Metal uses live macOS free + inactive memory.
XPU/fake use configured budgets. All subtract the configured reserve. Live CUDA
and Metal can cap free memory with a positive `--memory-gb` before subtracting
reserve; the cap never replaces the live probe. Follow
[operations](operations.md) for local service installation and lending/reclaiming
GPUs. This document performs no fleet launch or production cutover.

A submission is not completion. Retain the ID and run one detached `myce wait
ID` (or `myce wait ID && <next step>`), then end the agent turn. Read logs/status
once it exits; do not keep an agent checking status. See [waiting for long jobs](START-HERE.md#waiting-for-long-jobs-agents)
for exit codes and saving the outcome. `--timeout DURATION` never cancels a job.
`logs --follow` itself does not assert success. Command failure
fails immediately; infrastructure loss may restart it. Queue restart interrupts
active HTTP model requests, so callers must decide whether to resubmit. Do not
blindly retry external writes. See [checkpoint guide](guide-resumable-jobs.md)
for atomic save/reconciliation details and [reference](job-reference.md) for all
fields, defaults, and endpoint contracts.

## Python project client

The distribution is `mycelium-client`; the import package is `mycelium`.
Requires Python 3.10+. Install from this checkout into the project's own environment (no published
PyPI release is assumed):

```sh
python -m pip install './clients/python[yaml,openai]'
```

A CPU orchestrator can submit a batch, read logs, inspect completion, or cancel:

```python
from pathlib import Path
from mycelium import Client
client = Client()  # MYCE_QUEUE and MYCE_TOKEN
job = client.submit(Path("job.yaml"))
for chunk in client.logs(job["id"], follow=True):
    print(chunk["text"], end="", flush=True)
finished = client.wait(job["id"], timeout=600)
if finished["state"] != "succeeded":
    raise RuntimeError(f"job {job['id']} ended {finished['state']}: {finished.get('reason', '')}")
print(client.get(job["id"]))
print(client.list())
# client.cancel(job["id"])  # an explicit cancellation decision
```

For Atlas's chat path, the helper sets OpenAI routing headers, a 600-second
timeout and no SDK retries; override the timeout to match loading plus inference:

```python
from mycelium import Client
with Client().openai(project="atlas", priority="production", timeout=600) as sdk:
    result = sdk.chat.completions.create(model="dev-model",
        messages=[{"role": "user", "content": "Synthetic test input"}])
    # Consume locally; do not log private inputs, outputs or error bodies.
```

Use the approved real catalog name outside dev. `Client.openai` also accepts
`priority="deadline", deadline="2026-10-09T18:00:00Z"`. Client network/auth errors
propagate; `wait` returns every terminal state including failure and raises
`TimeoutError` at its own wait deadline. That deadline does not cancel the job.
The batch worker does not inherit the fleet token, so keep queue submission and
model-call orchestration in the CPU caller.

For a checkpoint-aware Verso command, these helpers replace the manual save and
signal machinery. This complete miniature command still uses a simulated work
unit; replace it with the approved bounded GPU unit before production use:

```python
import time
from mycelium import Checkpoint, StopSignal
checkpoint = Checkpoint()  # MYCE_CHECKPOINT_DIR, current attempt with prior saves
n = checkpoint.load(default={"next_unit": 0})["next_unit"]
with StopSignal() as stop:
    while n < 100 and not stop.requested:
        time.sleep(0.2)  # demonstration only
        n += 1
        try:
            checkpoint.save({"next_unit": n})
        except OSError as exc:
            print(f"save will retry: {exc}", flush=True)
    while True:
        try:
            checkpoint.save({"next_unit": n})
            break
        except OSError as exc:
            print(f"final save will retry: {exc}", flush=True)
            time.sleep(1)
```

`Checkpoint.save` writes atomic JSON through file fsync/rename/directory fsync
(the Linux CIFS directory no-op is skipped; see the resumable-jobs guide);
`load` chooses the newest complete JSON and ignores `.tmp`. I/O errors and
corrupt complete JSON fail visibly; temporary NAS errors can be retried as above.
`StopSignal` sets `requested` on SIGTERM/SIGINT and restores handlers on exit;
use it on the main thread and save outside the signal handler. Neither helper
makes arbitrary outputs retry-safe. See [Python client](../clients/python/README.md).

## NAS model masters and instant production

Provision a writable NAS model folder once and use immutable GGUF masters.
Download an approved Hugging Face file explicitly with `myce model fetch`,
supplying its exact size and trusted SHA-256; normal model calls never download.
Queue and workers share the same catalog; each worker supplies its own mount
with `--model-root /local/mount/models` and local disk with
`--model-cache /local/disk/myce-models --model-cache-bytes 107374182400`.
See [model guide](guide-model-calls.md) for the full commands. Size and checksum
are checked before local publication; copies and verification count as load time.
Check cache reports and load refusal messages in `myce status`.

For interactive production, use `myce model pin MODEL --project atlas --machine
spark` or `--platform cuda`. Wait for a ready assigned model before directing
latency-sensitive traffic to it. A pin reserves memory, so other work may wait.
Optional weekday/time windows are active reservations; cache pre-copy occurs
before the window, but engine warmup at its start can still take time. Unpin when this guarantee is no longer needed.

**Experiments should be resumable and checkpoint frequently.** Production now
stops lower-priority non-resumable work immediately and restarts it later without
spending a loss retry. Resumable work has 10 seconds by default to save and exit;
deadline preemption retains 2 minutes. Workers receive authenticated immediate
stop notifications. Read `start_latency_seconds` to measure production queue
clearance, and measure model load separately when a pin is not ready.

### Pre-copy API and latency planning

Before production traffic, run `myce model sync MODEL --to MACHINE|PLATFORM|all`.
The authenticated HTTP equivalent is `POST /api/models/sync` with
`{"model":"MODEL","to":"TARGET"}`. Add `"cancel":true` to pause/suppress that
request; resend with false to resume. This copies NAS masters only, never fetches
from the internet or installs software. Queue restarts retain requests; worker
restarts retain and verify partial prefixes. Always wait for `cached_models` and
`copy_reports[].state == "ready"` before assuming a cache is warm. A cancelled
request may already have completed; it does not evict a verified cache entry.

`GET /api/status` exposes `sync_requests`, `copy_reports` (bytes, total, state,
error, achieved bytes_per_second), `copy_bytes_per_second`, and
`model_copy_seconds` on each machine. A zero measured speed means no measurement
yet; cold estimates use 40 MB/s. Cached/loaded models have zero NAS transfer cost;
engine load and inference time remain separate. The queue prefers an idle fitting
worker with a warm cache or a lower estimated transfer cost, preserving priority
and explicit platform preferences. Pin standby caches are protected even when
their time window is inactive. Model-use history lives in SQLite; idle pre-copy
uses queued demand and recent use, with no external scheduling service.

Batch `needs.model` remains metadata for placement and pre-copy. A plain command
must use the configured local model file itself; Mycelium does not rewrite its
arguments. A declared environment recipe can install its runtime; pre-copy only
prepares weights. Do not treat pre-copy as environment readiness.

## Requesting software, independently of platform preference

A batch job may use `environment: torch-cuda` from the shared NAS catalog, or:

```yaml
environment:
  lock_file: /queue/nas/my-project/requirements.txt
  python: '3.12'
  platforms: [cuda, xpu]
needs:
  memory_gb: 8
  platform: {preferred: [cuda]}
```

The queue reads and snapshots the lock at submission. Workers install it with
uv into their local cache, prepend the environment's executables to PATH, and
run the ordinary command. Platform preference never bypasses environment
compatibility. The Python client passes these same mappings through `submit`.
Do not submit the caller's laptop-only lock path to a remote queue; the path must
be readable by the queue. Waiting/build failures and logs use the ordinary job
status and `logs` methods. See [environment recipes](environments.md).

## General model servers and joint measurement

Catalog `engine: server` owns a foreground HTTP process with `start_command`,
`health_url`, `ready_timeout`, optional `stop_command`, `stop_timeout`, named
`environment`, `memory_gb`, and `device`/`device_env`. `engine: vllm` is a preset
with served name, context, KV dtype, utilization and extra parser arguments.
Single-file masters or immutable `files` manifests use the same cache and pins.
See [general server contract](guide-servers.md) for full fields and examples.

Call `/models/NAME/PATH` with any method, body and end-to-end headers, plus the
usual fleet token, project and priority. Responses stream; request bodies have
an explicit 8 GiB generic disk-spool limit (OpenAI JSON retains 64 MiB).
The worker streams generic uploads to the engine. Batch `needs.model` remains a scheduling hint, not a
request to launch a server. Queued commands never inherit the fleet token.

`join --device ID` isolates commands/engines to one card and binds CUDA memory
probes. XPU fixed admission cannot detect other owners. Before enabling warm
production pins, run `myce measure` locally in an approved maintenance window
with the exact worker identity/spool, explicit platform/device/budget and model
list. It holds all models together, optionally warms them, writes JSON and text,
and unloads them. Missing native counters fail visibly. See the separate
[B70 weekend measurement](production-test-plan.md#b70-gpu0-weekend-joint-residency-measurement)
checklist; this is not the first-machine Spark acceptance test.
