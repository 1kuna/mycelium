# Mycelium

One shared queue for GPU batch jobs. One Go binary, one SQLite database, plain
commands, and an HTML status page. Everything in step 1 runs on a laptop with
fake GPUs. The approved spec is [the v2 design](docs/design-proposal.md).
The previous architecture remains archived at `v1-final`; v2 starts fresh.

## Five-minute quick start

You need Go 1.25.6+ and a shell. No GPU, Python, containers, or remote machines
are needed for this quick start.

```sh
go build -o myce ./cmd/myce
./myce dev --workers 2
```

Leave that terminal open. Dev starts the queue at `http://127.0.0.1:7331` and
two fake workers with 64 GB each. Open that address for the status page.
Use `dev --gpu cuda --environments torch-cuda` to simulate a CUDA machine with
a ready environment, still without hardware. In another terminal in this directory:

```sh
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
./myce logs "$id" --follow
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
experiment, preempts it for production, and verifies checkpoint recovery.

## Commands

```sh
myce queue --listen 127.0.0.1:7331 --db myce.db --checkpoints /shared/checkpoints
myce join --name laptop --gpu fake --memory-gb 16
myce leave --name laptop              # asks wait/stop for non-resumable jobs
myce leave --name laptop --now         # signals every job; keeps checkpoint grace
myce submit job.yaml
myce status
myce logs <id> --follow
myce cancel <id>
myce dev --workers 1 --memory-gb 8
```

Use `MYCE_QUEUE` (or `--queue`) for the queue's address. Use `MYCE_TOKEN` for
the shared token on the queue, worker, and CLI. Only dev supplies a default
token. For a fleet, configure your own token, bind the queue to its LAN address,
and use the same NAS checkpoint mount path on every machine. Create the root
on the mounted NAS before running `queue`; a missing root blocks new jobs.
Use `join --state-dir /durable/local/worker` to choose the worker spool location
(default: a per-queue, per-machine directory in your user cache). Keep it
across worker restarts. Examples for boot services are in [deploy/](deploy/README.md). The HTML page is
read-only and LAN-visible; all `/api/` routes require the token. This is a
trusted local-network service; commands run with the worker user's permissions.

For step 1, a worker explicitly declares `--gpu cuda|xpu|metal|fake` and the
free GPU memory budget it lends with `--memory-gb`. Each running job's stated
memory is subtracted from that budget on check-in. Lend only memory that other
workloads won't consume; automatic hardware detection and setup are later work.
Each worker runs one command at a time. Use Ctrl-C or `leave` when reclaiming its
GPU. `--environments torch-cuda,mlx` reports installed, ready environments;
`--models qwen3-32b` reports externally loaded models for scheduling affinity.
These flags do not install environments or load models.

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
environment: torch-cuda            # optional ready environment name
allowed_machines: [spark, pc4090]   # optional allow-list
give_up_at: 2026-10-10T18:00:00Z   # optional expiry for queued jobs
expected_minutes: 90               # planning hint, never a timeout
resumable: true
```

Platform supports `{required: [cuda, xpu]}`, `{preferred: [metal]}`, or `any`
(also `{any: true}`). Preferred platforms are used when a suitable preferred
machine is idle, without preventing another platform from working when needed.
Environment also accepts `{lock_file: /shared/project/requirements.lock}`; the
worker reports that exact path in `--environments` when ready. Since environment
setup is deferred, a job waits with a clear reason if no worker reports it ready.

Scheduling uses priority, then model/environment readiness, earliest deadline,
then longest waiting. Production and deadline jobs may stop lower-priority
resumable jobs only when no idle machine fits. Stops allow two minutes to save;
paused jobs return to the front of their priority line. Non-resumable jobs are
never preempted to make room. Give-up times apply while waiting, not as a run
limit. A machine is lost after 60 seconds without a check-in while other workers
remain reachable. When every worker goes quiet, jobs stay assigned until the
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

Model calls, model loading, automatic environment setup, quotas, and redundant
queue services are deferred. No v1 peer networking, leases, or replicated state
is used.
