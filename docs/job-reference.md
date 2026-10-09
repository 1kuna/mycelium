# Job and command reference

See [integration examples](integrating.md) and [operations](operations.md). The
[approved design](design-proposal.md) remains the product spec. Only GPU work
belongs in this queue; CPU-only watching, parsing, and orchestration stay outside.

## Batch YAML

`myce submit job.yaml` accepts exactly one YAML document, at most 1 MiB, and
rejects unknown fields. Its output is the job ID. No field is a hidden shell
command: `run.command` is an argument array. Use `[sh, -c, '...']` explicitly
when shell expansion is needed.

| Field | Required / default | Meaning |
|---|---|---|
| `name` | Required, nonempty | Human-readable job name; not a deduplication key |
| `project` | Required, nonempty | Project label |
| `priority` | Required | `production`, `deadline`, or `experiment`, in that order |
| `deadline` | Required for `deadline`; otherwise absent | RFC3339 time; ordering hint, not an execution timeout |
| `run.command` | Required, nonempty argv | Executable and arguments; installed on every eligible worker |
| `run.workdir` | Absent: worker's working directory | Existing working directory; use an absolute shared or consistently installed path |
| `run.env` | Empty map | String environment overrides, merged with worker environment |
| `needs.memory_gb` | Required, finite and positive | GPU memory reservation in GB, including model/runtime overhead |
| `needs.platform` | Required | `[cuda]` shorthand, `{required: [cuda, xpu]}`, `{preferred: [metal]}`, or `any` / `{any: true}`; exactly one mode |
| `needs.model` | Absent | Batch affinity hint; does not load a model |
| `environment` | Absent | Ready environment name, `{name: torch-cuda}`, or `{lock_file: /shared/project/requirements.lock}`; never both name and lock file |
| `allowed_machines` | Empty: any eligible worker | Machine-name allow-list |
| `give_up_at` | Absent: wait indefinitely | RFC3339 expiry while queued; not a running-job timeout |
| `expected_minutes` | `0` | Rough planning hint, never enforced |
| `resumable` | `false` | Set true only for a command implementing the checkpoint/stop contract |

Platforms are `cuda`, `xpu`, `metal`, and `fake` (laptop tests). A preference
permits other platforms, so the command itself must support them. An environment
is matched against worker `--environments` exactly. Mycelium does not install it.

Attempts get `MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR`. The last
points to an attempt-specific folder beneath the stable job folder on the NAS.
The worker also sets `MYCE_PROCESS_ID` for process ownership. It removes
`MYCE_TOKEN`, even if supplied in `run.env`. These are plain processes with the
worker user's permissions, not a sandbox.

## Scheduling and results

Jobs that fit memory, platform, environment and allow-list are ordered by
priority, model/environment readiness, earliest deadline, then waiting age.
Production/deadline may pause lower-priority resumable work when no idle worker
fits. SIGTERM is sent to the process group, with a two-minute checkpoint grace
before SIGKILL. Non-resumable work is never preempted to make room.

States are `queued`, `running`, `stopping`, `succeeded`, `failed`, and `cancelled`. Status
includes a reason for waiting or failure. A command's own nonzero exit fails
immediately. Non-resumable work fails after three infrastructure losses;
planned pauses do not consume that limit. Resumable work restarts from the latest
complete checkpoint. An attempt is an execution identity, not a retry budget.
Logs and results are accepted only for the current execution identity.

Priority loss timeouts are `production: 5s`, `deadline: 20s`, `experiment: 60s`.
They apply when a worker stops checking in; they are not job execution limits.
A strict-majority worker outage retains assignments, and queue restart allows a three-minute
recovery grace. Partitioned copies can overlap until connectivity returns:
checkpoint isolation and reconciliation do not guarantee exactly-once side effects.

## CLI flags and defaults

All commands accept `--queue` (`MYCE_QUEUE`, otherwise
`http://127.0.0.1:7331`) and `--token` (`MYCE_TOKEN`, otherwise required).
Prefer the environment over a token argument visible in process listings.
Dev alone defaults to `dev-token`. Use `myce <command> --help` for the live parser.
Flags belong before the positional argument except that `logs ID --follow`,
`submit FILE`, and `cancel ID` also work.

| Command / flag | Default | Purpose |
|---|---|---|
| `queue --listen` | `127.0.0.1:7331` | HTTP bind address |
| `queue --db` | `myce.db` | Durable local SQLite file; one queue owner |
| `queue --checkpoints` | `./checkpoints` | Shared root; create on mounted NAS first |
| `queue --restart-grace` | `3m` | Worker reconciliation after queue restart |
| `queue --production-loss-timeout` | `5s` | Production loss detection |
| `queue --deadline-loss-timeout` | `20s` | Deadline loss detection |
| `queue --experiment-loss-timeout` | `60s` | Experiment loss detection |
| `join --name` | Hostname | Unique stable worker name |
| `join --gpu` | Required | `cuda`, `xpu`, `metal`, or `fake` |
| `join --memory-gb` | `0` | Positive configured budget required for XPU/Metal; fake permits zero to simulate no room; CUDA optional positive cap on live free memory before reserve |
| `join --reserve-memory-gb` | `0` | Memory withheld from live CUDA free inventory or configured XPU/Metal/fake budget |
| `join --heartbeat` | `1s` | Worker check-in interval |
| `join --checkpoint-root` | `MYCE_CHECKPOINT_ROOT`, otherwise queue-provided root | Local mount of the same shared NAS root; may differ from queue path |
| `join --state-dir` | Per-queue/per-machine user cache directory | Durable local spool; explicitly set for services |
| `join --environments` | Empty | Comma-separated ready names or lock paths |
| `join --models` | Empty | Comma-separated externally loaded batch affinity hints |
| `queue/join --model-config` | Empty catalog | Same model YAML on queue and workers |
| `queue/join --model-idle` | Catalog value (`10m`) | Override idle unload time |
| `queue/join --model-wait` | Catalog value (`5m`) | Override load/placement wait; not inference duration |
| `join --model-listen` | `127.0.0.1:0` | Authenticated worker proxy bind; engines stay loopback |
| `join --model-advertise` | Local proxy URL | Explicit URL reachable by queue for LAN workers |
| `leave --name` | Hostname | Stop assigning work; ask wait/stop for non-resumable jobs |
| `leave --now` | `false` | Signal all jobs, still allowing checkpoint grace |
| `logs ID --follow` | `false` | Follow until terminal and drain final logs |
| `dev --workers` | `2` | Fake workers, real command runner |
| `dev --memory-gb` | `64` | Per-worker fake budget |
| `dev --gpu` | `fake` | Simulated platform; does not detect hardware |

`dev` also accepts listen/db/checkpoints, environments, models, and model catalog
settings. Its root is local and created automatically. Custom dev catalogs must
use the fake engine. `status`, `submit`, `logs`, `cancel`, and `leave` use the API;
`leave` does not disable a service's automatic restart policy.

## HTTP and models

`GET /api/status`, `POST /api/jobs` (JSON job spec),
`POST /api/jobs/ID/cancel`, `GET /api/jobs/ID/logs?after=SEQ`, and
`POST /api/workers/NAME/leave` (`{"now":false}` or `true`) require bearer auth.
The read-only HTML status page is LAN-visible without authentication.

Model calls support `POST /v1/embeddings`, `/v1/completions`, and
`/v1/chat/completions` (including chat SSE streaming). Required headers are
`Authorization: Bearer TOKEN`, `X-Myce-Project`, `X-Myce-Priority`, and, for
deadline priority, `X-Myce-Deadline` in RFC3339. Body limit is 1 MiB; `model`
must name a catalog entry. Remaining OpenAI JSON is passed unchanged.

Catalog fields: `idle_timeout` defaults to `10m`; `max_wait` to `5m`; `models`
defaults to empty. Each entry requires unique `name`, `engine` (`llama.cpp` or
explicit test-only `fake`), local `path`, positive `memory_gb`, and nonempty
`platforms`. Optional `args` defaults empty. Adapter-owned host/port/model/alias
flags cannot be overridden; default GPU layers is 99. Embedding models need
appropriate engine embedding args. Catalog hashes must match between queue and
workers. Model reservations include loading and draining until unload succeeds.
See [model guide](guide-model-calls.md) for transport retries, stream failures,
engine setup, and the required model-card/inference-contract verification.

## Python client defaults

`mycelium-client` imports as `mycelium`. `Client(url=None, token=None,
timeout=10, poll_interval=0.3)` reads `MYCE_QUEUE` (otherwise loopback:7331)
and requires `MYCE_TOKEN` unless passed explicitly. Time values are seconds.
`submit` accepts a mapping or YAML/path with the optional YAML extra;
`submit_yaml` explicitly parses YAML. `status`, `list`, `get`, and `cancel` use
the same authenticated API. `wait(timeout=None)` waits indefinitely by default
and returns failed/cancelled states too. `logs(follow=False, after=0)` yields
log chunks; retain `seq` to resume. Errors propagate as `APIError` or transport
errors, and client timeouts do not cancel jobs.

`Client.openai(project=..., priority="experiment", deadline=None)` needs the
OpenAI extra, defaults to a 600-second SDK timeout and `max_retries=0`, and
returns the standard SDK client, which must be closed. Deadline priority requires
a deadline. `Checkpoint(directory=None)` defaults to `MYCE_CHECKPOINT_DIR`;
`save(value, name="checkpoint.json")` atomically saves JSON and `load(default=None)`
loads the newest complete JSON. `StopSignal` handles cooperative SIGTERM/SIGINT
on the main thread; callers explicitly save and exit. See the
[client guide](../clients/python/README.md) and [examples](integrating.md).
