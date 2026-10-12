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
| `deadline` | Required for `deadline`; optional otherwise | RFC3339 time; ordering hint, not an execution timeout |
| `run.command` | Required, nonempty argv | Executable and arguments; resolved from its environment, then worker PATH |
| `run.workdir` | Absent: worker's working directory | Existing working directory; use an absolute shared or consistently installed path |
| `run.env` | Empty map | String environment overrides, merged with worker environment |
| `needs.memory_gb` | Required, finite and positive | GPU memory reservation in GB, including model/runtime overhead |
| `needs.host_memory_gb` | Absent / `0`: GPU-only admission on uncapped hosts | Additional host RAM in GiB, including runtime/activation peaks; capped hosts require a positive declaration; qualified Linux services enforce it plus worker slack |
| `needs.platform` | Required | `[cuda]` shorthand, `{required: [cuda, xpu]}`, `{preferred: [metal]}`, or `any` / `{any: true}`; exactly one mode |
| `needs.model` | Absent | Batch affinity hint; does not load a model |
| `environment` | Absent | Catalog name, `{name: torch-cuda}`, or `{lock_file: /shared/project/requirements.txt, python: "3.12", platforms: [cuda]}`; never both name and lock file |
| `allowed_machines` | Empty: any eligible worker | Machine-name allow-list |
| `start_by` | Production: submission + 150 seconds; absent for other priorities | Absolute RFC3339 first-start deadline, production batch only; loss recovery gets a fresh original-length window |
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
At equal combined readiness a loaded model beats a cached one. Competing
experiment projects first share by weighted recent usage. Placement
includes estimated environment build and model-copy time.
Production/deadline may stop lower-priority work when no idle worker
fits. SIGTERM is sent to the process group, with a two-minute checkpoint grace
before SIGKILL for deadlines, which only preempt resumable work. Production
gives resumable work 10 seconds by default and stops non-resumable work immediately, requeueing it to start over
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
`submit FILE`, `cancel ID`, and `wait ID [ID...] --timeout DURATION` also work.

| Command / flag | Default | Purpose |
|---|---|---|
| `queue --listen` | `127.0.0.1:7331` | HTTP bind address |
| `queue --upload-spool-bytes` | `17179869184` | Positive total held model-upload bytes (16 GiB); full budget returns HTTP 507 |
| `queue --upload-spool-dir` | OS temporary directory | Local model-upload spool location; files removed after completion/abort |
| `queue --db` | `myce.db` | Durable local SQLite file; one queue owner |
| `queue --checkpoints` | `./checkpoints` | Shared root; create on mounted NAS first |
| `queue --restart-grace` | `3m` | Worker reconciliation after queue restart |
| `queue --production-loss-timeout` | `5s` | Production loss detection |
| `queue --deadline-loss-timeout` | `20s` | Deadline loss detection |
| `queue --experiment-loss-timeout` | `60s` | Experiment loss detection |
| `join --name` | Hostname | Unique stable worker name |
| `join --host-id` | OS hostname | Physical host identity; every per-card worker on the same machine must use the same stable value |
| `join/install worker --host-memory-budget-gb` | `0` | Aggregate per-host admitted RAM ceilings; positive budgets reject zero host declarations and retain unconfirmed release charges |
| `join/install worker --memory-ceiling-slack-gb` | `0.125` | Extra GiB per declared ceiling; included in host admission |
| `join/install worker --memory-swap-max-bytes` | `0` | Swap allowance per native attempt; managed Docker uses optional run-spec total RAM-plus-swap values |
| `join --reserve-host-memory-gb` | `4` | RAM withheld from observed available memory; configure OS/other-workload headroom; unified uses the larger host/GPU reserve once |
| `join --device` | Sole detected CUDA card / platform default | One physical card per worker; CUDA resolves to a stable UUID for inventory/execution; select explicitly on multi-GPU hosts |
| `join --gpu` | Auto-detected | Override with `cuda`, `xpu`, `metal`, or `fake`; same detection as `install worker` |
| `join --memory-gb` | Detected budget; `0` with explicit `--gpu` | Positive budget required for fixed/XPU; fake permits zero; live CUDA/Metal optional positive cap before reserve |
| `join --memory-mode` | Detected mode; `auto` with explicit `--gpu` | `auto`: CUDA marker detection, Metal unified; `unified`: OS probe; `dedicated`: numeric CUDA; `fixed`: explicit positive budget without probes |
| `join --reserve-memory-gb` | Detected reserve; `0` with explicit `--gpu` | GiB withheld from live dedicated/unified memory or fixed budget; suggested Spark reserve: 16 |
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
| `resolve-docker --state-dir DIR --attempt-id ID --confirm-gone` | Explicit offline assertion | Record externally verified release of a stuck managed attempt; see [operator recovery](operations.md#docker-cleanup-custody) |
| `leave --now` | `false` | Signal all jobs, still allowing checkpoint grace |
| `wait ID [ID...] --timeout DURATION` | Unlimited | Wait for every final state; timeout never cancels jobs |
| `logs ID --follow` | `false` | Follow until terminal and drain final logs |
| `dev --workers` | `2` | Fake workers, real command runner |
| `dev --memory-gb` | `64` | Per-worker fake budget |
| `dev --gpu` | `fake` | Simulated platform; does not detect hardware |

`dev` also accepts listen/db/checkpoints, environments, models, and model catalog
settings. Its root is local and created automatically. Custom dev catalogs must
use the fake engine. `status`, `submit`, `logs`, `wait`, `cancel`, and `leave` use the API;
`leave` does not disable a service's automatic restart policy.

`wait` prints each full ID, final state (`succeeded`, `failed`, `cancelled`),
and reason once. It polls every three seconds and backs off to ten seconds
through network errors, queue restarts, HTTP 408/429 and server errors. Use full
submitted IDs; unknown IDs and other HTTP client errors fail immediately.
Exit codes: `0` all succeeded (also help), `1` any failed/cancelled, `2`
usage/auth/API error, `3` timeout, `4` interrupted. `--timeout` must be positive;
omit it to wait indefinitely. Timeout/interruption leaves the jobs running.
Run one wait detached for long jobs; see [the agent workflow](START-HERE.md#waiting-for-long-jobs-agents).

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
defaults to empty. Each entry requires unique `name`, `engine` (`llama.cpp`,
`server`, `vllm`, or explicit test-only `fake`), NAS master `path`, positive `memory_gb`, optional finite nonnegative `host_memory_gb` (additional CPU-side RAM in GiB), and nonempty
`platforms`. Real models also require positive `size_bytes` and a 64-digit
hex `sha256`. `direct_nas` defaults false; enable only for small models.
Optional `max_in_flight` defaults to `0` (unlimited), otherwise a positive
integer caps simultaneous requests per loaded copy; excess calls wait by
priority/deadline. Optional `args` defaults empty. For llama.cpp, adapter-owned host/port/model/alias
flags cannot be overridden; default GPU layers is 99. Embedding models need
appropriate engine embedding args. Server/vLLM fields and multi-file manifests
are described in [general servers](guide-servers.md). Catalog hashes must match
between queue and workers. Model reservations include loading and draining until unload succeeds.
See [model guide](guide-model-calls.md) for transport retries, stream failures,
engine setup, and the required model-card/inference-contract verification.

## Python client defaults

`mycelium-client` imports as `mycelium`. `Client(url=None, token=None,
timeout=10, poll_interval=0.3)` reads `MYCE_QUEUE` (otherwise loopback:7331)
and requires `MYCE_TOKEN` unless passed explicitly. Time values are seconds.
`submit` accepts a mapping or YAML/path with the optional YAML extra;
`submit_yaml` explicitly parses YAML. `status`, `list`, `get`, and `cancel` use
the same authenticated API. `wait(timeout=None)` waits indefinitely by default
and returns failed/cancelled states too. A wait timeout does not cancel the job.
`logs(follow=False, after=0)` yields log chunks; retain `seq` to resume. Errors propagate as `APIError` or transport
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
Installer defaults are `~/.local/share/mycelium/ROLE` (binary/config/state) and
macOS `local.mycelium.ROLE` labels; these differ from the manual deploy templates.
GPU/memory overrides are explicit (`--gpu`, `--memory-mode`, `--memory-gb`,
`--reserve-memory-gb`); no unknown GPU or memory is silently accepted.

`myce doctor [--config FILE]` checks GPU visibility, a writable NFS/SMB NAS mount,
authenticated queue access, uv, Linux user lingering, and at least 1 GiB local
free disk space. Linux doctor fails if lingering is disabled.
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

`measure --warmup-parallel N` defaults to `1` (warm each model after loading it).
With `N > 1`, load all selected models first, then run at most N warm-up
requests concurrently. `--load-timeout DURATION` defaults to `5m` per model;
both flags require positive values. Each model reports `load_seconds` and
`warmup_seconds`; the report also includes the warm-up phase's wall time,
parallel limit, load timeout, and combined absolute device/host peaks. Concurrent
warm-up peaks are joint observations, not attributed per-model deltas. Failure
cancels all warm-ups, waits for them to exit, and unloads every attempted engine.

## Production start deadline

Production jobs must acknowledge command startup before `start_by`. Set an
absolute RFC3339 timestamp, for example `start_by: 2026-10-12T14:00:30Z`.
If omitted, the queue saves submission time plus **150 seconds**: the ordinary
10-second experiment grace, two-minute Docker cleanup escalation, and a small
reporting margin. This is a bounded availability policy, not a native startup
latency guarantee. A shorter caller-owned deadline can trigger fallback sooner.
Unstarted retries, re-delivery and queue restarts never renew it. Older unfinished
production records without the field receive the same default from their original submission.

`give_up_at` keeps its queued-only meaning; the earlier applicable expiry wins.
`deadline` remains an ordering hint. A command that has started may finish after
`start_by`. After a started attempt loses its machine (including worker restart),
the recovery attempt gets a fresh window from the queue's loss-detection time:
loss time plus the original `start_by` minus submission time. That original window
length is durable and does not grow across repeated losses. Brief outages can
resume; a recovery attempt that does not start within its window fails with
`production start deadline exceeded: <blocker>` so the caller can run fallback.
Planned stops are not production loss recovery and do not renew this budget.
`start_by` covers production **batch jobs only**. Model HTTP calls rely on their
own request timeouts and the separate `max_wait` contract; they do not honor
`start_by`. Models loaded inside a batch command are outside its start hand-off.

Status, the status page and the job record show the blocker and seconds left.
Queued production fails immediately when every known eligible host is disabled
by the operator. Unknown inventory may still join within the deadline. Pending stops,
unconfirmed Docker cleanup, worker recovery, environment preparation and capacity
can all exhaust the budget. At `now >= start_by`, an unstarted job fails with
`production start deadline exceeded: <blocker>`; a past deadline fails on submit
without preempting anything. `myce wait` exits **1** and prints the reason; Python
`Client.wait()` returns the failed record with its reason.

Assignment alone is not startup. Before executing a production batch command,
the worker asks the queue to durably record its exact attempt as started. The
queue serializes that hand-off with expiry in one SQLite transaction: at the
deadline expiry wins; a previously accepted start is not failed by `start_by`.
This records an accepted hand-off, not proof of successful physical exec or
model readiness. A private exec-helper pipe holds plain-command execution until
the positive reply; managed Docker uses the same confirmation before its start
operation. An unreachable queue, failed durable write, duplicate start, stale
attempt or cancellation refuses execution. The worker stores launch intent before
asking, so restart reconciles/cleans that attempt and replays buffered results
instead of executing it again. A lost positive reply conservatively leaves the
attempt started in the queue but unexecuted; reconciliation handles recovery.
Deploy the matching queue and workers together. Check-in requires the durable-start
protocol marker; older workers receive HTTP 426 before assignment. Production
workers also require an explicit matching durable-start answer and `start_by`,
so an older queue cannot authorize execution. Shared and instance disable flags
are checked before and after the start request; cleanup retains attempt custody.
Late result messages cannot
revive failure. Assigned expiry sends an attempt-specific kill and retains release custody and memory accounting;
status says `release pending` until the matching cleanup acknowledgement.
Experiment stops and Docker cleanup continue independently after production fails.
Failure does **not** authorize a fallback to reuse uncertain resources.

Expiry runs on queue ticks and API mutations (including status), so observation
adds tick/poll delivery latency. Queue downtime or local storage failure cannot
promise timely durable failure; the caller still owns its outage watchdog and safe
fallback. Do not downgrade to workers/queues that ignore `start_by`.

## Physical-host production hold

Opt in with `queue --host-config hosts.yaml` (also accepted by `dev` and `sim`):

```yaml
hosts:
  b70:
    production_hold: true
    production_idle: 180s
```

`production_hold` defaults **false**. When enabled, `production_idle` defaults
180 seconds, accepts zero, and rejects negative/invalid durations. Each card's
worker must use the same `join --host-id b70` and a distinct `--name` (for example,
`b70-0` and `b70-1`). `dev --host-id b70 --workers 2` groups its fake workers too.
Simulation fleet YAML can include the same `hosts` mapping; `sim --host-config`
overrides it. Effective host policies and hold state live in SQLite. Omitting the
file on restart retains the saved policy; an explicitly different policy fails
startup. There is no hot reload or implicit policy relaxation.

A feasible production command/model request selects one physical host before
reclaiming anything. On an enabled host, it closes every lower-priority command,
model-call/load and environment-preparation admission path, even if production
only needs one card. All lower-priority commands stop concurrently. Resumable
commands get `--production-stop-grace` (default 10 seconds); non-resumable ones
are killed immediately and requeued without consuming an infrastructure retry.
Repeated demand cannot extend a stop deadline. Unpinned lower-priority models
drain; their outstanding calls get the same grace, then fail with
`preempted by production host hold` rather than being silently replayed.
Lower-priority background copies/builds are canceled and their reported cleanup
must settle. Environment builds required by queued production continue during
the hold.
Production waits for the entire host's lower-priority allocations to release.

The grace bounds cooperative stopping, **not successful recovery from an
unresponsive worker/driver**. After grace, pending release becomes visibly
unknown and still blocks production. Worker loss, stop expiry, a failed process
cleanup, cancellation or queue restart never acts as a release acknowledgement.
Attempt/session-specific results can settle old custody without changing a new
attempt. A fresh worker must have cleaned its old processes before reconciling
its previous session. Reopened model slots require measured in-flight counts;
older workers without those reports cannot clear a recovering hold. A replacement
model worker must acknowledge cleanup of the prior session, even after worker
loss. Forced-stop results retain `last_stop_outcome`; periodic NAS checkpoints
remain available. A checkpoint save blocked on NAS I/O is force-stopped at the
same grace deadline; production without `resumable: true` uses local scratch
and can start after the host's actual release acknowledgements even while the
NAS is unavailable. Displaced experiments wait for checkpoint storage to recover,
then resume from the last complete checkpoint generation. NAS reconciliation
cannot keep a late experiment copy alive on the held host.
These are process/model cleanup acknowledgements, not native GPU-driver proofs.

Stopped experiments return to the shared priority line with a fresh attempt and
may resume immediately on **any** eligible worker, including Spark. Only
production has a selected `production_host`; an experiment never inherits it.
Platform, environment, memory and both machine allow-lists still apply. Verso
must supply a cross-platform command and validate its checkpoint's scientific
identity; Mycelium copies checkpoint files without interpreting them.

Queued/running/preparing/stopping production demand shares the hold. Cooldown
starts when the final demand and all cleanup obligations settle. At 180 idle
seconds lower-priority admission reopens. A new submission committed at the
boundary wins over expiry; backward clock movement postpones reopening. Worker
poll/tick delivery adds latency. Unpinned idle production models stay resident
during cooldown and can drain immediately when it ends. Existing pins are never
automatically removed: they may prevent production placement or experiments
from using all memory. A resident pinned model still refuses experiment calls
during the hold. Pin loads also obey project restrictions and release gates.

Status/API `host_holds` and CLI/web status show, for example,
`b70: production hold; stopping 2 attempts; grace remaining 7s`,
`production idle; experiments resume in 179s`, or
`release unknown: worker b70-1`. Blocked experiments show
`host b70 reserved for production`; pinned capacity conflicts identify the pin.
This is observable GPU demand, not a CPU publication/session lock. Production
startup expiry and managed Docker are separate contracts; this option alone does
not provide cgroup enforcement or a native device fence. Do not downgrade a database with active holds
or mix old workers while depending on hold recovery.

## Attempt placement environment

Every command attempt receives worker-owned `MYCE_PLATFORM` (`cuda`, `xpu`,
`metal`, or `cpu-fake`), `MYCE_DEVICES` (the worker's selected `--device` string,
empty when no device is selected), and `MYCE_MACHINE` (worker name, not host ID).
Queue-managed model servers receive the same three variables on every load.
`run.env` and catalog overrides cannot change them. Existing CUDA/XPU visibility
selectors still apply; these variables describe placement, not a hardware fence.
`MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR` retain their existing
command-only contract. `MYCE_TOKEN` remains removed from payload environments.

## Project machine allow-list

The existing `queue/dev --project-config projects.yaml` accepts:

```yaml
verso:
  weight: 1
  allowed_machines: [b70-0, b70-1]
study:
  weight: 1
  # Unset/empty allowed_machines means unrestricted.
```

This restriction covers **every priority**, command and model call (including
pin placement). It intersects a command's own `allowed_machines`; weights and
experiment caps keep their existing meaning. An empty intersection fails
submission with `project verso allowed_machines has no overlap with job
allowed_machines`. If every permitted worker is known and structurally
incompatible with the required platform, device, capacity or environment,
submission fails with `no compatible machine in project verso allowed_machines`.
This check uses last-known inventory, not current free memory: busy/offline
workers can return, and unreported machine names are allowed to join later.
Resubmit after correcting restrictions or updating incompatible inventory.
Policies persist across restart when the configuration flag is omitted.

For a small synthetic example run:

```sh
myce sim --fleet examples/sim/production-hold/fleet.yaml \
  --workload examples/sim/production-hold/workload.yaml --days .01 --seed 1 \
  --json /tmp/production-hold-sim.json
```

Two B70 experiments run for 600 seconds each; a 60-second production command
arrives at second 60. Both experiments are displaced, one continues on Spark,
and the other returns after cooldown. Speeds, admission budgets and checkpoint
behavior are synthetic; this is not a native throughput/latency qualification.

## Managed Docker batch commands

`run.docker: true` (default `false`) opts a **direct** `docker run` or
`docker container run` argument array into worker-owned containment. The worker
converts it to `docker create --pull=never` followed by `docker start`, injects
`--init`, a private name, `--label myce.attempt=<opaque attempt ID>`,
`--label myce.worker=<worker ID>`, and a private spool `--cidfile`. Use an
already-local immutable `IMAGE@sha256:<64 hex digits>`; no image is pulled.
Do not put labels/name/cidfile in the submitted command: the worker owns them.

```yaml
name: docker-experiment
project: verso
priority: experiment
run:
  docker: true
  command: [docker, run, --device, /dev/dri/renderD128, "IMAGE@sha256:REPLACE_WITH_64_HEX_DIGITS", python, /app/experiment.py]
needs:
  memory_gb: 8
  host_memory_gb: 6
  platform: [xpu]
resumable: true
```

The example's image is a substitution marker, not a runnable image. Supported
options before the image are `--init`, explicit `--env`/`-e KEY=value`,
`--volume`/`-v`, `--device`, `--gpus`, `--entrypoint`, `--workdir`/`-w`, and
`--user`/`-u`. All other options are rejected before contacting Docker, including
detach, auto-remove, restart policies, privileged mode, label/name/cidfile
changes, daemon/context selection and Docker socket mounts. Image arguments
are passed unchanged. The payload must forward TERM and implement the ordinary
checkpoint contract; `--init` is not a checkpoint implementation.

The container receives `MYCE_ATTEMPT_ID` (opaque, unique across attempts),
`MYCE_DOCKER_LABEL` (`myce.attempt=<ID>`), `MYCE_JOB_ID`, `MYCE_ATTEMPT`,
placement environment, selected-device environment, and `MYCE_MEMORY_LIMIT_BYTES`
when host RAM is declared. The adapter installs and inspects this limit before
start; payload arguments cannot override it. `MYCE_CHECKPOINT_DIR`
is `/myce/checkpoint`, bind-mounted from the worker's prepared attempt directory.
`run.env` ordinary entries are also explicit container env; `MYCE_TOKEN` is
removed from the Docker client and never forwarded. MYCE identity overrides
in Docker `--env` are rejected. `CUDA_VISIBLE_DEVICES`, `ONEAPI_DEVICE_SELECTOR`
and `ZE_AFFINITY_MASK` are worker-owned: neither `run.env` nor Docker `--env`
may override them. Configure the worker device instead. GPU device mappings and image/library behavior
still need native qualification. Declared host RAM ceilings are enforced through
worker-owned Docker memory flags and verified inspection; device isolation still
requires qualification.

The worker pins the daemon endpoint from its own `DOCKER_HOST` (default
`unix:///var/run/docker.sock`) in its custody record; `DOCKER_CONTEXT` is
unsupported. Configure each worker for its intended daemon. Workers retain
stopped containers, cidfiles and spool records as evidence. Docker output is
collected into the ordinary job log after the container settles.

For an existing **unmanaged** wrapper (`run.docker: false`), a cooperative
container launch would be `docker run --label "$MYCE_DOCKER_LABEL" ...`, but
that wrapper must supply a unique label itself **and** use
`--memory "$MYCE_MEMORY_LIMIT_BYTES" --memory-swap "$MYCE_MEMORY_LIMIT_BYTES"`
for the default no-swap contract. Mycelium cannot prove custody
for hidden shell/nested Docker calls or infer their labels from a process tree.
Use managed direct argv for production handover. A managed job cannot forget
its label; inspection mismatches remain visibly unconfirmed and no unrelated
container is signaled. Docker-backed catalog model servers are not covered by
this batch adapter; keep them out of a containment-qualified handover.

### Private networks and worker-owned sidecars

Add `run.docker_network: private` to a managed batch. The worker creates one
`myce-<opaque attempt ID>` bridge with `--internal` and the exact `myce.attempt`
label. Each container joins only that network and receives
`MYCE_DOCKER_NETWORK`, attempt/checkpoint identity, and placement/device selectors.
No outbound option is offered: models and images must be local and work offline.
Without this field the managed container uses `--network none`.

`run.docker_sidecars` is an optional ordered list with unique lowercase DNS
`name` (1–63 letters/digits/hyphens; `main` is reserved), direct Docker `command`,
positive `memory_gb`, optional `env`, optional `memory_swap`, and optional
`readiness: {port, path, timeout}`. Main retains `run.command` and declares
positive `run.docker_memory_gb`. Sidecars require this main declaration and the
private network. The worker starts sidecars in order, awaits each configured
HTTP GET readiness (2xx), then starts main. DNS aliases are the declared names
and `main`. Jobs have no Docker socket and cannot launch containers themselves.
Readiness probes use the inspected container IP from the worker, without proxies
or redirects. The worker must reach the daemon's bridge IP (normally local Linux);
Docker Desktop/remote bridge reachability is not assumed. Unreachable probes fail
at their positive declared timeout, measured with the injected Clock.

Each container's limit must individually fit `needs.host_memory_gb`; their sum
may exceed it. Admission and the host budget charge the job's measured
simultaneous need, rather than summing peaks at different times. Each Docker
`--memory` limit is supplied and verified separately. This is not a shared kernel
ceiling: overlapping peaks can exceed the declared simultaneous need, so qualify
the actual workload overlap.

Main's `run.docker_memory_swap` and sidecar `memory_swap` are optional GiB values
for Docker's total RAM plus swap limit (`--memory-swap`), at least the container's
RAM limit. Equal to RAM explicitly forbids swap. When omitted, the worker leaves
the flag unset: Docker's daemon default applies (normally RAM plus an equal swap
allowance when host swap is available). Native `--memory-swap-max-bytes` still
defaults to zero for native cgroups; it does not configure managed Docker.
Swap-free Verso production is not qualified.

A Verso-shaped production batch (replace placeholders with real local digests
and use measured memory/device mappings):

```yaml
name: verso-recording-batch
project: verso
priority: production
run:
  docker: true
  docker_network: private
  docker_memory_gb: 24
  # docker_memory_swap: 28  # optional total RAM plus swap, GiB
  command: [docker, run, --device, /dev/dri/renderD128, "PIPELINE@sha256:REPLACE_WITH_64_HEX_DIGITS", python, /app/process.py]
  env:
    SORTFORMER_URL: http://sortformer:8080
    NAMING_URL: http://naming:8080
  docker_sidecars:
    - name: sortformer
      memory_gb: 16
      command: [docker, run, --device, /dev/dri/renderD128, "SORTFORMER@sha256:REPLACE_WITH_64_HEX_DIGITS", python, /app/server.py]
      readiness: {port: 8080, path: /health, timeout: 120s}
    - name: naming
      memory_gb: 12
      command: [docker, run, --device, /dev/dri/renderD128, "NAMING@sha256:REPLACE_WITH_64_HEX_DIGITS", python, /app/server.py]
      readiness: {port: 8080, path: /health, timeout: 120s}
needs:
  memory_gb: 24
  host_memory_gb: 24.5
  platform: [xpu]
resumable: false
```

These memory values illustrate nonsummed peaks, not qualification evidence.
Verso's CPU api/worker/render services remain outside Mycelium. This is a batch
lifecycle, not a Docker-backed catalog model server.

Main's exit code determines the result; early sidecar exit fails the job. Main
exit stops remaining sidecars with ordinary grace, then KILL. Stop, restart,
loss reconciliation and `myce disable` stop all labeled containers, confirm
nonrunning/absent execution, disconnect owned stopped endpoints, remove labeled
networks, and confirm network absence. Stopped containers remain evidence;
networks do not. RAM custody stays held until both proofs succeed. Late containers
are rescanned. Stuck removal uses existing cleanup status, the two-minute
`docker_cleanup_escalated` event and `myce resolve-docker`; that assertion must
also cover network absence.

Publishing host ports (including publish-all), host networking, joining other
networks, socket mounts, privileged mode and submitted network/alias/lifecycle
flags remain forbidden. Inspected violations stop the whole attempt with a
specific reason. Labels are inspected before removal; unlabeled networks and
unrelated containers are never touched.
