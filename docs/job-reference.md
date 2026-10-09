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
| `run.command` | Required, nonempty argv | Executable and arguments; resolved from its environment, then worker PATH |
| `run.workdir` | Absent: worker's working directory | Existing working directory; use an absolute shared or consistently installed path |
| `run.env` | Empty map | String environment overrides, merged with worker environment |
| `needs.memory_gb` | Required, finite and positive | GPU memory reservation in GB, including model/runtime overhead |
| `needs.platform` | Required | `[cuda]` shorthand, `{required: [cuda, xpu]}`, `{preferred: [metal]}`, or `any` / `{any: true}`; exactly one mode |
| `needs.model` | Absent | Batch affinity hint; does not load a model |
| `environment` | Absent | Catalog name, `{name: torch-cuda}`, or `{lock_file: /shared/project/requirements.txt, python: "3.12", platforms: [cuda]}`; never both name and lock file |
| `allowed_machines` | Empty: any eligible worker | Machine-name allow-list |
| `give_up_at` | Absent: wait indefinitely | RFC3339 expiry while queued; not a running-job timeout |
| `expected_minutes` | `0` | Rough planning hint, never enforced |
| `resumable` | `false` | Set true only for a command implementing the checkpoint/stop contract |

Platforms are `cuda`, `xpu`, `metal`, and `fake` (laptop tests). A preference
permits other platforms, so the command itself must support them. An environment restricts eligibility to its supported platforms. Workers build
immutable recipes on demand and report ready content hashes. See
[environments](environments.md) for catalog and project-lock contracts.

Attempts get `MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR`. The last
points to an attempt-specific folder beneath the stable job folder on the NAS.
The worker also sets `MYCE_PROCESS_ID` for process ownership. It removes
`MYCE_TOKEN`, even if supplied in `run.env`. These are plain processes with the
worker user's permissions, not a sandbox.

## Scheduling and results

Jobs that fit memory, platform, environment and allow-list are ordered by
priority, combined environment/model readiness, earliest deadline, then waiting age.
Within a priority tier, both ready beat either ready, which beats cold setup.
At equal combined readiness a loaded model beats a cached one. Placement
includes estimated environment build and model-copy time.
Production/deadline may stop lower-priority work when no idle worker
fits. SIGTERM is sent to the process group, with a two-minute checkpoint grace
before SIGKILL for deadlines. Production gives resumable work 10 seconds by
default and stops non-resumable work immediately, requeueing it to start over
without increasing its infrastructure-loss retry count. Production grace is
configurable with `--production-stop-grace`. Make experiments resumable and
save checkpoints frequently.

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
| `join --memory-gb` | `0` | Positive budget required for fixed/XPU; fake permits zero; live CUDA/Metal optional positive cap before reserve |
| `join --memory-mode` | `auto` | `auto`: CUDA marker detection, Metal unified; `unified`: OS probe; `dedicated`: numeric CUDA; `fixed`: explicit positive budget without probes |
| `join --reserve-memory-gb` | `0` | GiB withheld from live dedicated/unified memory or fixed budget; suggested Spark reserve: 16 |
| `join --heartbeat` | `1s` | Worker check-in interval |
| `join --checkpoint-root` | `MYCE_CHECKPOINT_ROOT`, otherwise queue-provided root | Local mount of the same shared NAS root; may differ from queue path |
| `join --state-dir` | Per-queue/per-machine user cache directory | Durable local spool; explicitly set for services |
| `join --environments` | Empty | Legacy preinstalled labels without a catalog |
| `queue/dev --environment-config` | Empty | NAS environment catalog; recipes resolved at submission |
| `join --environment-root` | Empty | Worker-local catalog directory for relative NAS binaries |
| `join --environment-cache` | `STATE_DIR/environment-cache` | Local software cache |
| `join --environment-cache-bytes` | `21474836480` | 20 GiB active published cache limit; in-use entries protected |
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
deadline priority, `X-Myce-Deadline` in RFC3339. Body limit is 64 MiB; `model`
must name a catalog entry. Remaining OpenAI JSON is passed unchanged.

Catalog fields: `idle_timeout` defaults to `10m`; `max_wait` to `5m`; `models`
defaults to empty. Each entry requires unique `name`, `engine` (`llama.cpp` or
explicit test-only `fake`), NAS master `path`, positive `memory_gb`, and nonempty
`platforms`. Real models also require positive `size_bytes` and a 64-digit
hex `sha256`. `direct_nas` defaults false; enable only for small models.
Optional `args` defaults empty. Adapter-owned host/port/model/alias
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

## Model cache and pins

| Option / command | Meaning |
|---|---|
| `join --model-root DIR` | This worker's NAS master-model folder; relative catalog paths resolve here; evictions go to its `.Trash` |
| `join --model-cache DIR` | Local disk cache; defaults to `STATE_DIR/model-cache` |
| `join --model-cache-bytes N` | Active cache allowance in bytes; default 107374182400 (100 GiB) |
| `queue --production-stop-grace 10s` | Resumable experiment/deadline save allowance when production needs room |
| `model fetch FILE --model-root DIR --url URL --size-bytes N --sha256 HASH` | Explicitly download one Hugging Face master; refuse overwrite |
| `join --model-copy-parallel N` | Concurrent background copies; default 1 |
| `model sync MODEL --to M\|cuda\|metal\|xpu\|all` | Persist a pre-copy request; `--cancel` pauses it and preserves partial bytes |
| `model pin MODEL --project P --machine M` | Hold a production model on M |
| `model pin MODEL --project P --platform cuda` | Hold one copy on an eligible CUDA worker |
| `model unpin MODEL --project P` | Release that project's pin |

Pins optionally accept `--start HH:MM --end HH:MM --weekdays 1,2,3,4,5
--timezone America/New_York`. Weekdays use 0=Sunday through 6=Saturday. Times
use a named IANA zone (default UTC), cannot cross midnight, and exclude the end.
Wait for status to show the assigned slot ready before treating a pin as warm.
Status JSON includes `pins`, worker `cached_models`, `cache_free_bytes`,
`cache_error`, and each job's `started_at` and `start_latency_seconds`.
Latency is submission to the worker startup acknowledgement, using queue time; the real dev timing check also observes
command start. Infrastructure losses remain separate from execution attempts.

Model sync also accepts `POST /api/models/sync` with `{"model":"MODEL","to":"TARGET","cancel":false}`.
A cancelled target suppresses idle copies; a pin still requires its copy.
Status includes `sync_requests`, worker `copy_reports` (state, bytes, total,
error and measured bytes/s), `copy_bytes_per_second`, and `model_copy_seconds`.
Available cache bytes include unwritten copy reservations and are bounded by
both filesystem free space and cache allowance. Loaded affinity precedes cached
affinity; otherwise placement uses remaining NAS bytes / measured speed, with
40 MB/s until measured. This estimates transfer, not engine startup. `needs.model`
is still batch metadata: commands must use a local model path themselves.

## Machine setup

`myce install worker --nas DIR --queue URL [--dry-run]` detects OS, CPU and
GPU, writes private configuration/credentials, copies this binary, and enables a
Linux systemd user service or macOS launchd agent. `myce install queue --nas DIR
--listen ADDRESS [--dry-run]` creates the queue service. No dry run writes files
or invokes service managers. Use `--environment-config FILE` and `--model-config
FILE` for shared catalogs. `--root DIR` changes the local installation root.
GPU/memory overrides are explicit (`--gpu`, `--memory-mode`, `--memory-gb`,
`--reserve-memory-gb`); no unknown GPU or memory is silently accepted.

`myce doctor [--config FILE]` checks GPU visibility, a writable NFS/SMB NAS mount,
authenticated queue access, uv, and at least 1 GiB local free disk space.
`myce uninstall [--config FILE] [--dry-run]` stops/disables the boot service and
moves its files, credentials and local state to recoverable Trash. It leaves NAS
data intact. See [operations](operations.md) for boot/login requirements.

## General model servers and joint measurement

Catalog `engine: server` owns a foreground HTTP process with `start_command`,
`health_url`, `ready_timeout`, optional `stop_command`, `stop_timeout`, named
`environment`, `memory_gb`, and `device`/`device_env`. `engine: vllm` is a preset
with served name, context, KV dtype, utilization and extra parser arguments.
Single-file masters or immutable `files` manifests use the same cache and pins.
See [general server contract](guide-servers.md) for full fields and examples.

Call `/models/NAME/PATH` with any method, body and end-to-end headers, plus the
usual fleet token, project and priority. Responses stream; request bodies have
an explicit 64 MiB limit. Batch `needs.model` remains a scheduling hint, not a
request to launch a server. Queued commands never inherit the fleet token.

`join --device ID` isolates commands/engines to one card and binds CUDA memory
probes. XPU fixed admission cannot detect other owners. Before enabling warm
production pins, run `myce measure` locally in an approved maintenance window
with the exact worker identity/spool, explicit platform/device/budget and model
list. It holds all models together, optionally warms them, writes JSON and text,
and unloads them. Missing native counters fail visibly. See the separate
[B70 weekend measurement](production-test-plan.md#b70-gpu0-weekend-joint-residency-measurement)
checklist; this is not the first-machine Spark acceptance test.
