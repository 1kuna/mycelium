Start with [the agent starting guide](docs/START-HERE.md) when integrating another project.

# Mycelium

One shared queue for GPU batch jobs and OpenAI-format model calls. One Go binary, one SQLite database, plain
commands, and an HTML status page. Everything runs on a laptop with fake GPUs and an explicit fake model engine. The approved spec is [the v2 design](docs/design-proposal.md).
The previous architecture remains archived at `v1-final`; v2 starts fresh.
The manual deployment templates install v2 as `~/.local/bin/myce`, with local
data at `~/.local/share/myce` and distinct `myce` services. `myce install` uses
`~/.local/share/mycelium/ROLE` and macOS `local.mycelium.ROLE` labels instead;
inspect existing paths and service ownership before using it. The old v1 service
was removed from B70 on
2026-10-09; do not reinstall it. Other legacy data remains outside v2 scope.
Use [the ordered founder test checklist](docs/production-test-plan.md) for the first fleet test.

Read [Atlas/Verso integration examples](docs/integrating.md), the
[complete job/CLI reference](docs/job-reference.md), and
[Linux/Mac operations](docs/operations.md).

## Five-minute quick start

You need Go 1.25.6+ and a shell. No GPU, Python, containers, or remote machines
are needed for this quick start.

```sh
go build -o myce ./cmd/myce
./myce dev --workers 2 --token dev-token
```

Leave that terminal open. Dev starts the queue at `http://127.0.0.1:7331` and
two fake workers with 64 GB each. Open that address for the status page.
Use `dev --gpu cuda --environments torch-cuda` to simulate a CUDA machine with
a ready environment, still without hardware. In another terminal in this directory:

```sh
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
cat > job.yaml <<'YAML'
name: hello-fleet
project: demo
priority: experiment
run:
  command: [sh, -c, 'echo "job=$MYCE_JOB_ID attempt=$MYCE_ATTEMPT"; echo "checkpoint=$MYCE_CHECKPOINT_DIR"; echo done']
needs:
  memory_gb: 1
  platform: any
resumable: false
YAML
id=$(./myce submit job.yaml)
./myce wait "$id"
./myce logs "$id"
./myce status
```

Stop dev with Ctrl-C. It plans a leave for each worker, stops resumable jobs,
and waits for non-resumable commands to finish. The queue database (`myce.db`)
and checkpoint folders (`checkpoints/`) persist for the next run. Local worker
spools persist beside the database under `workers/`.

```sh
go test ./... -race
go vet ./...
```

The end-to-end test launches the actual `myce dev` executable, checkpoints an
experiment, preempts it for production, and verifies checkpoint recovery. A second executable test loads an unloaded model
through an OpenAI call, waits for idle unload, then verifies that a second call
loads it again.

## Calling a model

Dev offers a fake `dev-model`; its first call loads it on demand:

```sh
curl http://127.0.0.1:7331/v1/chat/completions \
  -H "Authorization: Bearer dev-token" -H "Content-Type: application/json" \
  -H "X-Myce-Project: demo" -H "X-Myce-Priority: production" \
  -d '{"model":"dev-model","messages":[{"role":"user","content":"Hello"}]}'
```

For Python calls, use a fresh Python 3.10+ environment in this checkout
(`python3.13` below; Apple's `python3` may be 3.9):

```sh
python3.13 -m venv /tmp/myce-adopter-venv
. /tmp/myce-adopter-venv/bin/activate
python -m pip install './clients/python[yaml,openai]'
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:7331/v1", api_key="dev-token",
                default_headers={"X-Myce-Project": "demo", "X-Myce-Priority": "production"},
                timeout=600, max_retries=0)
print(client.chat.completions.create(
    model="dev-model", messages=[{"role": "user", "content": "Hello"}]
).choices[0].message.content)
```

Chat streaming, text completions, and embeddings use the same headers. Models
unload after ten idle minutes or drain for higher-priority work. See the
[model-call guide](docs/guide-model-calls.md) and [catalog example](examples/models.yaml)
for llama.cpp configuration, shared memory scheduling, and recovery behavior.

## Commands

For long jobs, start one detached `myce wait` and end the agent turn; read logs
after it exits. Wait exits `0` for all success, `1` for failed/cancelled jobs,
`2` for usage/auth/API errors, `3` for timeout, and `4` for interruption.
It retries queue/network outages. See [agent waiting](docs/START-HERE.md#waiting-for-long-jobs-agents).

```sh
myce queue --listen 127.0.0.1:7331 --db myce.db --checkpoints /shared/checkpoints
myce join --name laptop --gpu fake --memory-gb 16 --reserve-memory-gb 2 --heartbeat 1s
myce leave --name laptop              # asks wait/stop for non-resumable jobs
myce leave --name laptop --now         # signals every job; keeps checkpoint grace
myce submit job.yaml
myce wait <id> [<id>...] --timeout 2h # wait only; timeout does not cancel
myce status
myce logs <id> --follow
myce cancel <id>
myce dev --workers 1 --memory-gb 8
myce sim --days 7 --seed 1           # GPU-free scheduler replay; docs/simulator.md
```

Use `MYCE_QUEUE` (or `--queue`) for the queue's address. Use `MYCE_TOKEN` for
the shared token on the queue, worker, and CLI. Only dev supplies a default
token. For a fleet, configure your own token, bind the queue to its LAN address,
and mount the same shared NAS checkpoint root on every machine. Queue
`--checkpoints` and worker `--checkpoint-root` can use different local mount
paths; jobs carry relative checkpoint paths on the wire. Create the root
on the mounted NAS before running `queue`; a missing root blocks new jobs.
Use `join --state-dir /durable/local/worker` to choose the worker spool location
(default: a per-queue, per-machine directory in your user cache). Keep it
across worker restarts. Examples for boot services are in [deploy/](deploy/README.md). The HTML page is
read-only and token-protected; HTML, API and model routes require the fleet token. This is a
trusted local-network service; commands run with the worker user's permissions.

`myce join` auto-detects the GPU using the same probe as `myce install worker`, including memory mode and reserve defaults. `--gpu` bypasses detection; `--device` selects a card, and explicit memory flags override detected defaults. Detection failure suggests `--gpu`; multi-GPU CUDA hosts need a device selector. `myce dev` still uses fake inventory.

CUDA workers query live `nvidia-smi` free GPU memory minus
`--reserve-memory-gb`; a positive `--memory-gb` optionally caps that reading before reserve and never replaces the query. Spark CUDA uses Linux
`MemAvailable` when dedicated memory fields
are unsupported; Metal uses macOS `vm_stat` free + inactive memory. XPU/fake
use configured budgets, and `--memory-mode fixed` explicitly selects a budget.
Live readings and fixed budgets are reduced by reserve; a cap applies before reserve. Own batch/model
reservations are accounted for without double subtraction. Unsupported inventory
fails visibly. Use `join --device ID` to select one physical GPU per worker;
CUDA resolves it to a stable UUID for inventory and command/server execution.
On multi-GPU machines, use unique worker names and one selected card per worker.
Without a selector, live CUDA dedicated/auto inventory rejects multiple devices
rather than summing their memory. Admission does not replace Atlas/Verso ownership controls.
Each worker runs one command at a time. Use Ctrl-C or `leave` when reclaiming its
GPU. `--environments torch-cuda,mlx` reports installed, ready environments;
`--models qwen3-32b` remains a batch affinity hint for externally managed models;
it does not make them callable. Use `--model-config` for queue-managed model
loading. `--environments` only reports preinstalled labels; catalog/lock-file
recipes build on demand in the worker cache.

## Job format

```yaml
name: reindex
project: atlas
priority: deadline                 # production | deadline | experiment
deadline: 2026-10-09T18:00:00Z     # required for deadline priority
run:
  command: [python3, reindex.py]
  workdir: /srv/atlas
  env: {PYTHONUNBUFFERED: "1"}
needs:
  memory_gb: 40                    # positive GPU memory; CPU-only jobs stay outside
  platform: {required: [cuda]}     # [cuda] shorthand also works
  model: qwen3-32b                 # optional affinity hint
environment: torch-cuda            # optional catalog or preinstalled environment name
allowed_machines: [spark, pc4090]   # optional allow-list
give_up_at: 2026-10-10T18:00:00Z   # optional expiry for queued jobs
expected_minutes: 90               # planning hint, never a timeout
resumable: true
```

Platform supports `{required: [cuda, xpu]}`, `{preferred: [metal]}`, or `any`
(also `{any: true}`). Preferred platforms are used when a suitable preferred
machine is idle, without preventing another platform from working when needed.
Environment also accepts `{lock_file: /shared/project/requirements.txt, python: "3.12"}`.
The queue snapshots the recipe; eligible workers build it on demand and report
ready content hashes. Without a catalog, `--environments` remains a preinstalled-label
compatibility mode. See [environments](docs/environments.md).

Scheduling uses priority, then model/environment readiness, earliest deadline,
then longest waiting; competing experiment projects first share by weighted recent
usage. Production and deadline jobs may stop lower-priority work when no idle
machine fits. Production gives resumable work 10 seconds to save by default
(`queue --production-stop-grace`) and stops non-resumable commands immediately.
Deadline work stops only resumable commands and gives two minutes. Paused jobs
return to the front of their priority line; non-resumable commands restart from
scratch without consuming an infrastructure-loss retry. Give-up times apply
while waiting, not as a run limit. Loss timeouts depend on priority: production 5s, deadline 20s, experiment 60s
(`queue --production-loss-timeout`, `--deadline-loss-timeout`,
`--experiment-loss-timeout`). Workers default to `join --heartbeat 1s`.
Individual loss is inferred while other workers remain reachable. When a strict majority of workers goes quiet, jobs stay assigned until the
network returns. Queue restarts allow `--restart-grace 3m` for workers to report.
Non-resumable jobs fail after three infrastructure interruptions; resumable jobs
and planned pauses continue from checkpoints. Returning copies reconcile by
newest complete checkpoint, with the loser told to stop and its results rejected.
Command exit failure is shown immediately. NAS access failures block new starts
and appear plainly in status; running jobs must retry their checkpoint saves.

See [the checkpoint guide](docs/guide-resumable-jobs.md) for a Python example.

## HTTP JSON API

Send `Authorization: Bearer <fleet-token>` and `Content-Type: application/json`.
The API uses the same field names as YAML, with platform expanded to an object
and environment expanded to `{ "name": "torch-cuda" }` or `{ "lock_file": "..." }`.

| Method | Path | Body / result |
|---|---|---|
| POST | `/api/jobs` | Job specification → job (including `id`) |
| GET | `/api/status` | `{jobs: [...], machines: [...], warning: "..."}` |
| POST | `/api/jobs/<id>/cancel` | No body required |
| GET | `/api/jobs/<id>/logs?after=0` | Up to 256 `{seq, attempt, text}` chunks; pass last seq to continue |
| POST | `/api/workers/checkin` | `{machine: {id, session, gpu, free_memory_gb, models, environments, checkpoint_unavailable}, running: {id, attempt, progress}, fresh, previous_session}` → assignment / stop / leave |
| POST | `/api/workers/log` | `{machine, session, id, attempt, text, offset}` |
| POST | `/api/workers/complete` | `{machine, session, id, attempt, success, error, interrupted}` |
| POST | `/api/workers/<name>/leave` | `{now: false}` (wait for non-resumable), or `{now: true}` |

Every join uses a new random session ID. Worker check-ins default to once a
second; logs and results go to durable local disk and replay after reconnect.
Log offsets deduplicate lost acknowledgements. Fresh sessions may replace a live
name only with the recorded previous session, after orphan cleanup. Workers
retry outages with exponential backoff capped at 30 seconds, using an injected
clock. No new commands start while disconnected. The SQLite file keeps job states, fleet inventory, and logs across
queue restarts. Never run two queue services against the same database.

General HTTP servers and llama.cpp/vLLM presets share the model lifecycle; see
[server calls](docs/guide-servers.md). Named software recipes build in the local
environment cache. `myce measure` checks joint residency locally and writes JSON
and plain reports; see the [B70 maintenance checklist](docs/production-test-plan.md#b70-gpu0-weekend-joint-residency-measurement).
Quotas beyond experiment concurrency/memory-share caps and redundant queue
services remain deferred. No v1 peer networking,
leases, or replicated state is used.

Project weights, experiment caps, usage history, and the status dashboard are
described in [Sharing GPUs and measuring the pool](docs/guide-fairness-usage.md).
