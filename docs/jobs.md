# Jobs and caller lifecycle

Run `myce run -- CMD` for a single installed command. For reusable project
work start with [an adapter](adapter.md). This reference covers
[batch fields](#batch-yaml), [keyed intent and cancellation](#keyed-submission-and-cancellation),
[waiting](#cli-flags-and-defaults), [fan-out](#declared-independent-pieces) and
[dependencies](#start-a-consumer-when-one-piece-is-accepted). CPU and GPU jobs use
the same queue. The caller owns business cancellation and publication fences;
queue success is attempt-specific, with exact cleanup and accepted output checks.

See [integration examples](jobs.md) and [operations](operations.md). The
[approved design](design-proposal.md) remains the product spec. CPU commands and GPU work use the same queue; callers retain orchestration.

## One-line commands

```sh
myce run -- echo hello
myce run --gpu cuda --mem 8G --host-mem 4G --project demo -- python task.py
myce run --cpu 2 --mem 8G --detach -- python task.py
```

`run` uses the ordinary submission API and waits with transient status-read
retry. It sends command arguments literally; use `sh -c` explicitly for shell
syntax. Defaults are CPU-only, one logical core, 1 GiB host RAM, project
`default`, class `experiment`, and non-resumable execution. With `--gpu
cuda|xpu|metal`, `--mem` means accelerator RAM and `--host-mem` is additional
host RAM (default 1 GiB). `G`/`GiB` and `M`/`MiB` use binary units; a bare
number is GiB. `--name`, `--class`, legacy `--priority` and RFC3339 `--deadline`
cover common scheduling declarations. Deadline priority requires a deadline.

Status changes go to stderr; command logs go to stdout. A successful command
exits zero; a recorded positive command exit code is propagated, and other
failures or cancelled jobs exit 1. Ctrl-C requests durable cancellation by
submission key and exits 130; the acknowledgement does not prove a running
process has finished cleanup. An unconfirmed cancellation prints the exact
project/key for recovery through the cancellation API. `--detach` prints only
the job ID and leaves the job running.

An installed worker's private joining authority supplies the queue credential
and TLS certificate pin automatically, with the installed queue config as a
fallback. `--config` selects another installation; `--joining-file` or
`MYCE_JOINING_FILE` selects private joining authority explicitly. `--queue`
changes its endpoint without removing the pin. Explicit `MYCE_TOKEN` (or
`--token`) and `MYCE_QUEUE` select the existing direct client, useful for dev.
Use YAML below for shared-storage workdirs, environments, file mappings,
checkpoints and other advanced options.

For repeatable project commands with frozen source, recoverable JSON receipts and
a detached completion notifier, use `myce run --profile NAME`; see
[agent offload](adapter.md).

## Batch YAML

`myce submit job.yaml` accepts exactly one YAML document, at most 1 MiB, and
rejects unknown fields. Its output is the job ID. No field is a hidden shell
command: `run.command` is an argument array. Use `[sh, -c, '...']` explicitly
when shell expansion is needed.

Check a YAML or JSON job before submission with `myce spec check job.yaml`.
This uses submit's strict decoder and `Spec.Validate()` offline, without fleet
authority, network requests, uploads or fleet changes. Exit status is 0 for a
valid spec, 1 for an invalid or unreadable spec, and 2 for incorrect usage.
Errors identify the file and rejected field or rule. This checks static validity
only: live eligibility, admission and qualification, registered extensions,
input availability and current fleet policy still require live checks.

| Field | Required / default | Meaning |
|---|---|---|
| `name` | Required, nonempty | Human-readable job name; not a deduplication key |
| `submission_key` | Optional | Project-scoped 1–128 ASCII `[A-Za-z0-9._:-]`; identical spec replays the same job; see [identity/cancellation](design-proposal.md#submission-identity-and-cancellation-80) |
| `project` | Required, nonempty | Project label |
| `priority` | Required | `production`, `deadline`, or `experiment`, in that order |
| `logs` | Omitted / `keep` | `none` discards command stdout/stderr; no queue log rows or worker attempt `.log` file |
| `deadline` | Required for `deadline`; optional otherwise | RFC3339 time; ordering hint, not an execution timeout |
| `run.command` | Required, nonempty argv | Executable and arguments; resolved from its environment, then worker PATH |
| `run.workdir` | Absent: managed `{work}` | Native cwd must resolve within the private attempt directory; stage source/data as inputs or a profile |
| `run.env` | Empty map | String overrides over the worker's safe environment allowlist; attempt/device/temp controls remain worker-owned |
| `needs.cpu_cores` | Default one | Admitted logical cores for the whole attempt, including sidecars; omit GPU selectors for CPU-only work |
| `needs.memory_gb` | Finite and positive unless `memory: auto` | Host RAM GiB for CPU-only work; GPU memory for GPU work; optional hint with auto |
| `needs.host_memory_gb` | Absent / `0`: GPU-only admission on uncapped hosts | Additional host RAM in GiB, including runtime/activation peaks; capped hosts require a positive resolved envelope; qualified Linux services enforce it plus worker slack |
| `needs.memory` | Omitted: declared reservation | `auto`: local accepted peaks, cross-machine estimate, optional hint, or guarded exploratory first run; resolved once per attempt |
| `needs.platform` | GPU work: required; CPU input: omitted or `cpu` | `[cuda]` shorthand, `{required: [cuda, xpu]}`, `{preferred: [metal]}`, or `any` / `{any: true}`; exactly one mode |
| `needs.model` | Absent | Batch affinity hint; does not load a model |
| `needs.network` | Omitted / `false` | Native payload outbound opt-in. `true` requires a host advertising protected egress or submission is refused; reserved sidecar loopback and preparation need no opt-in. [Platform limits](native-isolation.md) apply, including fallback's partial network isolation. |
| `environment` | Absent | Catalog name, `{name: torch-cuda}`, or `{lock_file: /shared/project/requirements.txt, python: "3.12", platforms: [cuda]}`; never both name and lock file |
| `targets` | Empty: any OS/arch | Exact `{os, arch, platform}` alternatives, enforced at dispatch and launch |
| `require_target` | `false` | Refuse a new submission without an online qualified target of sufficient total admitted capacity; busy qualified targets may queue |
| `allowed_machines` | Empty: any eligible worker | Machine-name allow-list |
| `start_by` | Production: submission + effective start window (readiness for dependents); absent for other priorities | Absolute RFC3339 first-start deadline, production batch only; takes precedence over `start_window`; loss recovery gets a fresh original-length window |
| `start_window` | Production: `150s`; absent for other priorities | Relative Go duration string, `0 < duration <= 1h`, production batch only; inherited by physical fan-out children |
| `give_up_at` | Absent: wait indefinitely | RFC3339 expiry while queued; not a running-job timeout |
| `expected_minutes` | `0` | Rough planning hint, never enforced |
| `resumable` | `false` | Set true only for a command implementing the checkpoint/stop contract |

Platforms are `cuda`, `xpu`, `metal`, `cpu`, and `fake` (laptop tests). A preference
permits other platforms, so the command itself must support them. An environment restricts eligibility to its supported platforms. Workers build
immutable recipes on demand and report ready content hashes. See
[environments](environments.md) for catalog and project-lock contracts.

Attempts get `MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR`. The last
points to an attempt-specific folder beneath the stable job folder on the NAS.
The worker also sets `MYCE_PROCESS_ID` for process ownership. It removes
`MYCE_TOKEN`, even if supplied in `run.env`. These are plain processes with the
worker user's permissions, not a sandbox.

A job does not inherit the worker's whole environment. It starts from `PATH`,
`HOME`, `USER`, `LOGNAME`, `LANG`, `LC_*`, `TZ`, `TMPDIR` and the GPU runtime
controls the installer saves for the service (`LD_LIBRARY_PATH`, `CUDA_*`,
`ZE_*`, `ONEAPI_*`, `SYCL_*`), and the proxy and CA settings `HTTPS_PROXY`,
`HTTP_PROXY`, `NO_PROXY`, `ALL_PROXY` (and their lowercase forms),
`SSL_CERT_FILE`, `SSL_CERT_DIR`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE` and
`NODE_EXTRA_CA_CERTS`. Any proxy or CA setting whose value contains `@` is dropped
and the worker logs the variable name once, without its value. Mycelium then
adds `run.env`, environment activation, device and placement selectors and the
`MYCE_*` attempt variables. Session variables such as `SSH_AUTH_SOCK`,
`DISPLAY`, `DBUS_*` and `XDG_RUNTIME_DIR`, and the worker's own `MYCE_QUEUE` and
`MYCE_JOINING_FILE`, are dropped; pass anything else a job needs, including an
authenticated proxy, through `run.env`. The same base applies to native
helpers, extension setup/check/run, environment builds and model servers.
Managed Docker containers receive only explicit `--env` values.

Environment builds also keep these operator uv settings: `UV_LINK_MODE`,
`UV_CONCURRENT_BUILDS`, `UV_CONCURRENT_DOWNLOADS`, `UV_CONCURRENT_INSTALLS`,
`UV_CONCURRENT_CACHE_READS`, `UV_COMPILE_BYTECODE`, `UV_NATIVE_TLS`,
`UV_SYSTEM_CERTS`, `UV_INDEX_STRATEGY`, `UV_INDEX`, `UV_DEFAULT_INDEX`,
`UV_INDEX_URL`, `UV_EXTRA_INDEX_URL`, `UV_FIND_LINKS`,
`UV_PYTHON_INSTALL_MIRROR` and `UV_TORCH_BACKEND`. An index, find-links or
mirror setting whose value contains any `@` is dropped. uv credentials
(`UV_INDEX_*_USERNAME`, `UV_INDEX_*_PASSWORD`, `UV_PUBLISH_*`,
`UV_KEYRING_PROVIDER`, `UV_GITHUB_TOKEN`) are never forwarded, because recipe
build code is arbitrary.

## CPU example

```yaml
name: cpu-shard
project: example
class: experiment
run:
  command: [python3, compute.py]
needs:
  cpu_cores: 2
  memory_gb: 4
```

For CPU-only work `memory_gb` is host RAM. The stored job lowers this to zero
GPU memory and `host_memory_gb: 4`, with explicit CPU platform. GPU work keeps
its existing memory meanings and can add `cpu_cores`. Several worker views on
one physical host share a single CPU/RAM ledger. R1 runs one attempt per worker.
See [CPU capacity and allowances](operations.md#cpu-capacity-and-allowances-r1).

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
immediately. Production infrastructure interruptions requeue at most twice,
then fail with `infrastructure_retries_exhausted`;
`queue --production-infrastructure-retries` configures the nonnegative limit (values above two are clamped to two).
The durable optional `infrastructure_retries` job/status field counts requeues used
(omitted at zero). Worker/queue recovery, worker interruption/release, leave and reboot
share that budget; release acknowledgements do not refund it. Production OOMs
and job-caused command/container failures fail without retry. Experiments and
deadline work keep the three-loss limit; their planned pauses do not consume it. Resumable work restarts from the latest
complete checkpoint. An attempt is an execution identity, not a retry budget.
Logs and results are accepted only for the current execution identity.

Use top-level `logs: none` for private batch output, matching the model/server
catalog field. Both streams go directly to the OS null device, so large output
cannot fill an undrained pipe or reach the worker's journal. The queue also
discards log chunks for that job after checking execution identity. Success,
failure (including the command exit-status diagnostic), cancellation and lifecycle
timestamps remain available through the existing result/status paths. Managed
Docker uses the `none` logging driver for every member, skips log collection and
private stderr tails, and preserves exit/cleanup evidence through recovery.
Omitted or `logs: keep` retains existing capture and retention behavior. This
does not remove older logs or control files/telemetry written by the payload or
environment-build logs held under their own environment records. These logs are
not copied into a `logs: none` job or returned by its logs endpoint. `myce status`
shows the log policy; `myce logs` reports when logs are disabled. Keep submitted
metadata non-sensitive.

Workers advertise `capabilities: [logs_none]`. The queue leaves `logs: none`
jobs and server loads waiting, with a capability reason, until a compatible
worker checks in. Omitted/`keep` entries use normal log capture; all workers still need the current wire protocol.

Roll workers and queue together. An old queue silently treats `logs: none`
jobs as `keep`, and an old worker refuses saved state containing the field.
Upgrading the queue first drains live `logs: none` servers on older workers;
they cannot load or serve calls again until their host's worker is upgraded.
Upgrade queue and workers together; first stop and drain incompatible work.
Do not downgrade while `logs: none` jobs or server entries are live; first stop
and drain them with the compatible versions.

Priority loss timeouts are `production: 5s`, `deadline: 20s`, `experiment: 60s`.
They apply when a worker stops checking in; they are not job execution limits.
A strict-majority worker outage retains assignments, and queue restart allows a three-minute
recovery grace. Partitioned copies can overlap until connectivity returns:
checkpoint isolation and reconciliation do not guarantee exactly-once side effects.

## CLI flags and defaults

Queue API clients accept `--queue` (`MYCE_QUEUE`, otherwise
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
| `queue --production-infrastructure-retries` | `2` | Maximum production infrastructure requeues (clamped to 2; zero disables) |
| `queue --production-loss-timeout` | `5s` | Production loss detection |
| `queue --deadline-loss-timeout` | `20s` | Deadline loss detection |
| `queue --experiment-loss-timeout` | `60s` | Experiment loss detection |
| `join --name` | Hostname | Unique stable worker name |
| `join --host-id` | OS hostname | Physical host identity; every per-card worker on the same machine must use the same stable value |
| `host placement HOST --last-resort=true\|false` | `false` | Queue-owned placement policy; ordinary admissible hosts first, unchanged MY-18 order within each group, immediate fallback without waiting or reservation; may change while enabled. For model calls, a ready or loading engine on a last-resort host beats a cold ordinary host (readiness before value). |
| `join/install worker --host-memory-budget-gb` | `0` | initial queue allowance input; subsequent changes use `host budget HOST` while drained/disabled |
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
| `resolve-docker --state-dir DIR --attempt-id ID` | Verified offline reconciliation | Reconcile exact registered custody under the worker spool lock; unknown operation disposition stays charged; see [operator recovery](operations.md#docker-cleanup-custody) |
| `leave --now` | `false` | Signal all jobs, still allowing checkpoint grace |
| `wait ID [ID...] --timeout DURATION` | Unlimited | Wait for every final state; timeout never cancels jobs |
| `logs ID --follow` | `false` | Follow until terminal and drain final logs |
| `dev --workers` | `2` | Fake workers, real command runner |
| `dev --memory-gb` | `64` | Per-worker fake budget |
| `dev --gpu` | `fake` | Simulated platform; does not detect hardware |

Host policy status: `host status HOST` returns `policy.last_resort` (boolean).
The status API's machine `last_resort` field is present when true; CLI and web
status also label last-resort hosts. Queued job `reason` and assigned attempt
`timing.placement_reason` explain fallback when no ordinary host can admit the
work now in the current plan, alongside the existing value ranking. Reasons are
diagnostic text, not stable machine-readable codes. The policy is keyed by
physical host, persists across queue restarts and worker sessions, and does not
alter fairness, tiers, preemption, eligibility or cleanup custody.

`dev` also accepts listen/db/checkpoints, environments, models, and model catalog
settings. Its root is local and created automatically. Custom dev catalogs must
use the fake engine. Explicit `myce dev --checkpoints X` selects NAS-mode
checkpoints even with `--file-store`, matching the real queue.
`status`, `submit`, `logs`, `wait`, `cancel`, and `leave` use the API;
`leave` does not disable a service's automatic restart policy.

`wait` prints each full ID, final state (`succeeded`, `failed`, `cancelled`),
and reason once. It polls every three seconds and backs off to ten seconds
through network errors, queue restarts, HTTP 408/429 and server errors. Use full
submitted IDs; unknown IDs and other HTTP client errors fail immediately.
Exit codes: `0` all succeeded (also help), `1` any failed/cancelled, `2`
usage/auth/API error, `3` timeout, `4` interrupted. `--timeout` must be positive;
omit it to wait indefinitely. Timeout/interruption leaves the jobs running.
Run one wait detached for long jobs; see [the agent workflow](jobs.md#cli-flags-and-defaults).

## HTTP and models

Batch `POST /api/jobs` supports project-scoped `submission_key`.
`POST /api/jobs/cancel` cancels by `{project, submission_key}`;
`POST /api/jobs/ID/cancel` cancels by ID. Replies are durable typed
acknowledgements. See [submission/cancellation](design-proposal.md#submission-identity-and-cancellation-80)
and [examples](jobs.md#keyed-submission-and-cancellation).

`GET /api/status`, `GET /api/jobs/ID`, `POST /api/jobs` (JSON job spec),
`POST /api/jobs/ID/cancel`, `GET /api/jobs/ID/logs?after=SEQ`, and
`POST /api/workers/NAME/leave` (`{"now":false}` or `true`) require bearer auth.
The read-only HTML status page also requires authentication: use HTTP Basic
username `myce` with the fleet token as the password.

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
are described in [general servers](services.md). Catalog hashes must match
between queue and workers. Model reservations include loading and draining until unload succeeds.
See [model guide](services.md) for transport retries, stream failures,
engine setup, and the required model-card/inference-contract verification.

## Retained per-job ledger

`GET /api/jobs/ID/ledger?after=0&limit=100` is an authenticated, read-only
batch execution/custody receipt. Use the full job ID. `myce ledger ID [--after N]
[--high-water N] [--limit N]` prints one page as JSON, using the same installed
client configuration as `status`. Go: `Client.JobLedger(ctx, id, after,
highWater, limit)` (`highWater = -1` starts a snapshot). Python:
`Client.ledger(id, after=0, high_water=None, limit=100)`.

Job GET and Python `get()` include `ledger: {complete, high_water}` as a small
summary; entries are returned by the ledger endpoint. The supported wire types
are [Job](schemas/job-v1.schema.json) and
[ledger page](schemas/job-ledger-v1.schema.json).

A page contains `job_id`, `complete`, `high_water`, `next`, `has_more`, and
`entries` (always an array). Omit `high_water` on the first request to capture
the last durable sequence, then pass that same value with `after=next` until
`has_more=false`. Entries through that high-water mark are immutable; new
assignments or custody generations cannot extend that snapshot. An empty
snapshot has `high_water=0`, `next=0`, `has_more=false`. The default page size
is 100, maximum 256. Invalid cursors return HTTP 400
`job_ledger_cursor_invalid`. Sequences are local to the job, start at 1, and
never reset during retention. No total row cap applies.

Each entry includes `sequence`, queue-clock `at`, `attempt`, `machine`,
`session`, `kind`, `reason`, and `release` (`pending` or `confirmed`).
`execution_id` is the queue's existing execution identity rendered as
`JOB_ID/ATTEMPT`. Optional `process_id` is the worker's durable native process
identity (`MYCE_PROCESS_ID`), shared across custody generations in that execution;
it is unknown at assignment and omitted then. `state` is the queue state at that
transition. Reasons explain the transition using existing
sanitized queue diagnostics; reason text is not a machine contract.

Kinds are `assigned`, `started`, `outcome`, `released`, `custody_reserved`,
`custody_started`, and `custody_released`. Assignment does not prove execution.
`started` records the existing durable Started authorization before payload
execution, not proof that a payload reached its first instruction. Native
custody is reserved durably before even the gated supervisor starts;
`custody_started` records the created supervisor before payload gates open.
Custody entries carry a distinct `custody_id`, optional `custody_start`, and
optional `custody_end`. Started custody also records `supervisor_pid` and
`supervisor_birth` from the existing process receipt, so PID reuse cannot merge
generations. The timestamps are queue observations, avoiding
cross-machine clock comparisons; absent timestamps are unknown, never zero.
A reserve followed directly by release can mean launch failed or the worker
restarted before reporting its start. Port-collision preparation retries keep
the same attempt/execution ID and native process identity but receive distinct custody IDs. Each old group
is verified empty and its release committed before replacement. Final cleanup
and restart reconciliation reuse the existing exact copy-release authority to
settle every outstanding generation; diagnostics never authorize release.

`complete` describes historical coverage, independently of `has_more`.
Jobs created with ledger support have complete coverage. Older retained jobs,
and native attempts reported without custody receipts by an older worker,
report `complete=false`: earlier native generations cannot be reconstructed
from the bounded diagnostic arrays. An incomplete ledger must not be treated
as proof of all historical execution. Docker jobs retain attempt identities and
release transitions; native custody generations describe native supervisors.

Retention follows [queue history pruning](design-proposal.md#submission-identity-and-cancellation-80):
keep the entire ledger with its job, with no independent row eviction. Released
terminal jobs remain for at least 168 hours after `finished_at`; longer configured
result/log retention and cleanup backlog extend this. Never prune unresolved
custody. A keyed job's complete ledger remains readable by ID during its extra
30-day retained replay-answer window, even after ordinary job GET stops finding
it. An unkeyed ledger expires with its job; a keyed ledger expires with the full
replay answer. Thereafter this endpoint returns HTTP 410
`job_ledger_unavailable`, also used for an unknown ID. Permanent expired-key
digests do not retain ledger rows. Expired history is never returned as an empty
complete page. The four-entry `attempt_history` and 32-entry `memory_attempts`
remain bounded diagnostics with their existing contracts.

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
[client guide](../clients/python/README.md) and [examples](jobs.md).

## Model cache and pins

| Option / command | Meaning |
|---|---|
| `join --model-root DIR` | This worker's NAS master-model folder; relative catalog paths resolve here; evictions go to its `.Trash` |
| `join --model-cache DIR` | Local disk cache; defaults to `STATE_DIR/model-cache` |
| `join --model-cache-bytes N` | Active cache allowance in bytes; default 107374182400 (100 GiB) |
| `queue --production-stop-grace 10s` | Resumable experiment/deadline save allowance when production needs room |
| `model fetch FILE --model-root DIR --url URL --size-bytes N --sha256 HASH` | Explicitly download one Hugging Face master; refuse overwrite |
| `model approve-local NAME --path FOLDER --approved-root ROOT --model-approval-root DIR` | Approve an in-place folder on this host; emit a portable manifest |
| `join --model-approval-root DIR --secret-root DIR` | Local approval records and named secret files; see the server-entry guide |
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
Linux systemd user service or macOS launchd agent. Installation starts workers under queue-owned maintenance policy; disable admission on the queue before upgrading. See [installer overlay/rollback](operations.md#install-and-upgrade). `myce install queue --nas DIR
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
See [general server contract](services.md) for full fields and examples.

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
[native measurement checklist](development.md).

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
For a relative budget, declare **`start_window: 600s`** on the job or the selected
entry in `.mycelium/adapter.yaml`. It accepts a Go duration string (for example
`600s` or `10m`), strictly positive and at most **1 hour**. The cap bounds the
time production can wait on unavailable capacity or stuck preparation while
allowing substantial contention; choosing a longer window delays failure by that
budget. Zero, negative, malformed, overflowing and over-cap values are rejected.
It is a startup budget, not a runtime limit. Experiments have no start deadline
and must omit both fields. If both are supplied, the explicit absolute `start_by`
wins, including for every physical fan-out child.

Without `start_by`, the queue saves submission time plus `start_window`, which
defaults to **150 seconds**: the ordinary
10-second experiment grace, two-minute Docker cleanup escalation, and a small
reporting margin. This is a bounded availability policy, not a native startup
latency guarantee. A shorter caller-owned deadline can trigger fallback sooner.
Unstarted retries, re-delivery and queue restarts never renew it. Older unfinished
production records without the field receive the same default from their original submission.
For a dependent job, its generated window starts at dependency readiness; for
each physical fan-out child it starts at that child's assignment. The declared
length survives both transitions. A logical fan-out parent's generated cutoff
does not expire its capacity wait, even before its first child is assigned. Use
an explicit absolute `start_by` or `give_up_at` to bound that orchestration wait.
No preparation step or unstarted retry renews a physical child's cutoff.
Retained jobs from before this declaration existed preserve their stored cutoff
as absolute: older records did not record whether it was generated or submitted.

Job GET exposes the authored duration at `spec.start_window`, the current cutoff
at `spec.start_by`, and the effective original length at top-level `start_window`
in **nanoseconds** (`600000000000` for 600 seconds). The effective length is
available before command startup, including while dependencies block.

`give_up_at` keeps its queued-only meaning; the earlier applicable expiry wins.
`deadline` remains an ordering hint. A command that has started may finish after
`start_by`. After a started attempt loses its machine (including worker restart),
the recovery attempt gets a fresh window from the queue's loss-detection time:
loss time plus the original effective window (for an absolute deadline, the
original `start_by` minus submission time). That original window
length is durable and does not grow across repeated losses. Brief outages can
resume; a recovery attempt that does not start within its window fails with
`production start deadline exceeded: <blocker>` so the caller can run fallback.
Production worker interruption/release uses the same bounded recovery window. Planned
leave keeps its original start deadline; both use the infrastructure retry budget.
`start_by` covers production **batch jobs only**. Model HTTP calls rely on their
own request timeouts and the separate `max_wait` contract; they do not honor
`start_by`. Models loaded inside a batch command are outside its start hand-off.

Status, the status page and the job record show the blocker and seconds left.
New queued production fails immediately when every known eligible host is disabled
by the operator. An interrupted production job may wait for reopening until its
recovery start window expires. Unknown inventory may still join within the deadline. Pending stops,
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
so an older queue cannot authorize execution. Current queue maintenance is checked at Started; cleanup retains exact attempt
custody across later maintenance changes.
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
  example-host:
    production_hold: true
```

`production_hold` defaults false. Each per-card worker uses the same physical
`--host-id` and a distinct `--name`. Production selects an eligible host and
stops lower-priority commands, models and preparations across its cards.
Resumable work receives the original production save grace; non-resumable work
restarts from scratch without an infrastructure loss. All lower-priority release
acknowledgements must settle before production starts. Unknown custody remains
charged across stop expiry, worker loss and queue restart; elapsed grace never
proves release. Pins remain protected and can prevent placement.

There is **no idle production cooldown**. Once the final production demand and
cleanup settle, ordinary admission resumes. Displaced experiments rejoin the
shared queue and can run on any eligible machine; they do not inherit production's
host pin. Queue host maintenance and consumer cancellation are separate
contracts, described in [operations](operations.md#queue-maintenance).
No CPU publication/session lock is implied by a GPU production hold.

## Attempt placement environment

Every command attempt receives worker-owned `MYCE_PLATFORM` (`cuda`, `xpu`,
`metal`, or `cpu-fake`), `MYCE_DEVICES` (the worker's selected `--device` string,
empty when no device is selected), and `MYCE_MACHINE` (worker name, not host ID).
Queue-managed model servers receive the same three variables on every load.
`run.env` and catalog overrides cannot change them. Existing CUDA/XPU visibility
selectors still apply; these variables describe placement, not a hardware fence.
`MYCE_DEVICES` contains physical selectors, not framework-local indices. XPU
`ZE_AFFINITY_MASK=1` is paired with `ONEAPI_DEVICE_SELECTOR=level_zero:0`: use
`xpu:0` inside that process. A mask `0,1` uses `level_zero:0,1`. CUDA also
renumbers `CUDA_VISIBLE_DEVICES`: one selected physical card is `cuda:0`.
`MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR` retain their existing
command-only contract. `MYCE_TOKEN` remains removed from payload environments.

## Project machine allow-list

The existing `queue/dev --project-config projects.yaml` accepts:

```yaml
transcribe:
  weight: 1
  allowed_machines: [gpu-host-a, gpu-host-b]
study:
  weight: 1
  # Unset/empty allowed_machines means unrestricted.
```

This restriction covers **every priority**, command and model call (including
pin placement). It intersects a command's own `allowed_machines`; weights and
experiment caps keep their existing meaning. An empty intersection fails
submission with `project transcribe allowed_machines has no overlap with job
allowed_machines`. If every permitted worker is known and structurally
incompatible with the required platform, device, capacity or environment,
submission fails with `no compatible machine in project transcribe allowed_machines (last-known platform, capacity and environment)`.
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
and the other returns after production and cleanup settle. Speeds, admission budgets and checkpoint
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
project: transcribe
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
`--user`/`-u`, plus repeatable `--group-add <gid|name>` (also `--group-add=value`)
for supplemental device groups with non-root images. Group values must be GIDs
from 0 to 2147483647 or group names starting with a letter/underscore, followed
by letters, digits, underscores, dots or hyphens, with an optional trailing `$`.
All other options are rejected before contacting Docker, including
detach, auto-remove, restart policies, privileged mode, label/name/cidfile
changes, daemon/context selection and Docker socket mounts. Image arguments
are passed unchanged. The payload must forward TERM and implement the ordinary
checkpoint contract; `--init` is not a checkpoint implementation.

Plain batches can declare hashed `files` and `outputs` with the
[`docker_files_v1` contract](files.md#managed-docker-files): private
read-only `/myce/inputs`, writable `/myce/work`, and current-attempt verified
`output_refs`. The worker owns all mounts, numeric UID:GID, working directory
and environment for these jobs; the authored volume/env/workdir/user/group
options above are rejected. Image `USER` is overridden, capabilities are dropped
and privilege escalation is disabled. Native environments, `shared_inputs` and
NAS input mixing remain unsupported. Main and managed sidecars share these
attempt roots. Outputs are captured only after every writer has settled and
retained across upload/acknowledgement failures and restart.

The container receives `MYCE_ATTEMPT_ID` (opaque, unique across attempts),
`MYCE_DOCKER_LABEL` (`myce.attempt=<ID>`), `MYCE_JOB_ID`, `MYCE_ATTEMPT`,
placement environment, selected-device environment, and `MYCE_MEMORY_LIMIT_BYTES`
when host RAM is declared. The adapter installs and inspects this limit before
start; payload arguments cannot override it. `MYCE_CHECKPOINT_DIR`
is `/myce/checkpoint`, bind-mounted from the worker's prepared attempt directory.
For batches without managed files/outputs, `run.env` ordinary entries are also
explicit container env; `MYCE_TOKEN` is
removed from the Docker client and never forwarded. MYCE identity overrides
in Docker `--env` are rejected. `CUDA_VISIBLE_DEVICES`, `ONEAPI_DEVICE_SELECTOR`
and `ZE_AFFINITY_MASK` are worker-owned: neither `run.env` nor Docker `--env`
may override them. Configure the worker device instead. Docker `--device`
mappings are forwarded as declared; Mycelium does not add or translate render
nodes. Keep the Intel card inventory visible in the container
(for example, pass both `/dev/dri/renderD128` and `/dev/dri/renderD129` for a
two-card host) when using the host physical affinity mask. Passing only one card
can renumber the container inventory before the mask is applied; do not assume
that a host mask `1` still identifies it. After the mask, SYCL/PyTorch use local
index zero for a single card. GPU mappings and image/library behavior still
need native qualification. Declared host RAM ceilings are enforced through
worker-owned Docker memory flags and verified inspection; device isolation still
requires qualification.

The worker pins the daemon endpoint from its own `DOCKER_HOST` (default
`unix:///var/run/docker.sock`) in its custody record; `DOCKER_CONTEXT` is
unsupported. Configure each worker for its intended daemon. Workers retain
stopped containers, cidfiles and spool records as evidence. Main Docker output is
collected into the ordinary job log after the container settles.
Every settled main/sidecar also retains its exit code and private stderr tail
(last 256 lines, at most 64 KiB) in the attempt record and mode-0600
`<identity>.docker-evidence.json`. Sidecar stderr is excluded from queue logs,
results and public status; this private evidence survives container retention.
Stopped containers become eligible for removal 24 hours after result
acknowledgement by default. See [Docker cleanup custody](operations.md#docker-cleanup-custody)
for one-time reconciliation of retained containers from retired worker identities.

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
optional positive `memory_gb`, optional `env`, optional `memory_swap`, and optional
`readiness: {port, path, timeout}`. Main retains `run.command` and may declare
`run.docker_memory_gb`. Sidecars require the private network. The worker starts sidecars in order, awaits each configured
HTTP GET readiness (2xx), then starts main. DNS aliases are the declared names
and `main`. Jobs have no Docker socket and cannot launch containers themselves.
Readiness probes use the inspected container IP from the worker, without proxies
or redirects. The worker must reach the daemon's bridge IP (normally local Linux);
Docker Desktop/remote bridge reachability is not assumed. Unreachable probes fail
at their positive declared timeout, measured with the injected Clock.

Explicit member limits must individually fit `needs.host_memory_gb`; their sum
may exceed it. A group omitting any member cap requires a shared local cgroup v2
parent with a cgroupfs Docker daemon and delegated worker memory. Its aggregate
parent limit includes all members and forbids swap; omitted member caps inherit
that envelope. Unsupported daemons refuse explicitly with the native/capped
alternatives. Existing capped groups retain **member-only** enforcement and
their existing swap settings. The B70 Docker driver remains unverified until
the separately authorized deployment check.

Main's `run.docker_memory_swap` and sidecar `memory_swap` are optional GiB values
for Docker's total RAM plus swap limit (`--memory-swap`), at least the container's
RAM limit. Equal to RAM explicitly forbids swap. When omitted, the worker leaves
the flag unset: Docker's daemon default applies (normally RAM plus an equal swap
allowance when host swap is available). Native `--memory-swap-max-bytes` still
defaults to zero for native cgroups; it does not configure managed Docker.
These legacy member-only swap settings do not weaken a shared parent's zero
swap limit. Swap-free production transcription requires separate native qualification.

A transcription production batch (replace placeholders with real local digests
and use measured memory/device mappings):

```yaml
name: transcribe-recording-batch
project: transcribe
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
The application's CPU api/worker/render services remain outside Mycelium. This
is a batch lifecycle, not a Docker-backed catalog model server.

Main's exit code determines the result; early sidecar exit fails the job. Main
exit stops remaining sidecars with ordinary grace (normally two minutes).
Each exact container gets at most one TERM attempt, with permanent intent saved
before delivery. Failed/uncertain TERM waits for idempotent, retried KILL at the
original deadline; worker restart never renews grace or resends recorded TERM.
Existing forced containment/operator cleanup remains immediate. Label-only orphans
instead get one bounded `docker stop --time` per worker process lifetime; their
report records that no attempt exit/stderr evidence was retained.

A restart during grace succeeds with one attempt and no loss only when main exit
zero was already durable and the full recovery path completes: cleanup, private
evidence, network removal, durable release, output finalization and result save.
A restart before durable main completion remains a loss and retries. Released
custody without a saved result also remains interrupted and retries; unexpected
private-network disconnection remains a containment failure. Replayed results
must match the exact job, attempt, machine and session.
See [Docker cleanup custody](operations.md#docker-cleanup-custody) for delivery
outcomes, private-evidence review and recovery.

Stop, restart, loss reconciliation and queue disable-stop stop all labeled containers, confirm
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

## v3 extension jobs

Jobs can describe a `kind` (with `args`) or a `program`
(`type`, `file`, `args`), plus NAS-relative `inputs`, `outputs` and optional
`size_hint`. Their commands and hardware reservations come from a qualified,
frozen extension version. Plain command forms above remain supported. See
[write an extension](adapter.md) for the common YAML/JSON/Python
form, setup/check qualification and per-attempt output contract.

Native server entry placement, `env`/`secret_env`, local model approval and
OpenAI client defaults: [operator configuration](services.md#shared-entries-local-secrets-and-approved-model-folders).

`GET /api/extensions` lists extension metadata and may include an optional
`files_ref` bundle reference (`sha256` and `size_bytes`) instead of embedded
files. Job GET replies continue to omit extension payloads.

### Measured job envelopes

Native main and sidecars share one Linux attempt cgroup. Sidecar `memory_gb`
and main `run.memory_gb` are optional additional member caps; omit them to let
members share the job envelope. Sample whole-group anon + kernel + shmem
from `memory.stat` every 200 ms and retain the total `memory.peak`. Learned host
reservations for Linux CPU work use a complete non-reclaimable peak (anon +
kernel + shmem + unevictable + file_dirty + file_writeback) plus the largest of
the observed attempt-peak range, 25% of the retained peak, and the snapshot drift
floor (128 MiB). At least two comparable complete measurements are required for
every contributing local observation; two samples establish a range, not tail
confidence. All observed resident peaks remain a floor. Kernel/shmem, locked
and dirty/writeback pages are conservatively retained; clean file cache is
excluded. Discrete Linux xe/XPU work can use that same margin only with
complete per-attempt driver and whole-host evidence too. `memory_peak.gpu_host`
retains deduplicated DRM client system/GTT allocations plus independently read
xe debugfs GTT-manager usage for every participating card (deliberately counting
overlap between both sources and cgroup charges), and the whole-host MemAvailable drop from an
empty-cgroup, prelaunch baseline. At each synchronized sample, outside-cgroup
use is conservatively estimated as max(driver allocations, host drop minus
current non-reclaimable use, zero); `outside_gb` retains its maximum. Never
subtract independent lifetime peaks. The base is non-reclaimable peak plus
outside peak, also floored by the whole-host drop. Independent component,
resident and driver maxima remain floors. At least two complete attempts per
contributing local observation are required; `gpu_host_range` records the range separately from CPU components.
Host swap growth/swap-out or OOM during observation rejects GPU samples too.
Resident/component and host-pressure sampling remain independent of driver I/O.
Every driver collection must finish within one 200 ms resident sampling period;
a gap or final age beyond two periods makes GPU evidence incomplete. A blocked
driver read cannot prevent resident sampling or indefinitely delay attempt
completion. Missing DRM client identity rejects the whole GPU observation, even
when other clients are readable. Runtime debugfs reads use the worker's existing
permissions, or an operator-configured `--gpu-telemetry-helper` supplies fresh
GTT-manager text through the same parser. The helper is an exact executable path,
not a job command; see [its contract](operations.md#optional-privileged-gpu-telemetry-source).
Missing, unreadable, stale or malformed GTT-manager evidence keeps conservative sizing. Absolute driver allocations are
retained independently of net host deltas, which unrelated RAM release can mask.
Other GPU drivers, CUDA, unified memory, older/incomplete samples and transfers
retain the larger resident/total peak times 1.25. Missing readings are unknown,
not zero. Sampled host/DRM counters remain observations, not instantaneous
bounds; native coexistence still requires workload output and pressure proof.
Any pressure still invalidates comparable memory evidence and its range. macOS remains
admission-only and does not promise a kernel RAM ceiling. There, the worker learns the summed RSS of the attempt's process
groups every 500 ms (single-command jobs: at least main's exit max RSS), recorded
as unenforced evidence. It omits compressed pages and members that left the group,
so it can read low. It is never marked constrained and never invalidates learned
peaks; a real OOM still does. Darwin Metal jobs still explore. Linux peaks keep `memory.events` max/high/OOM counts beside them. OOM is a job failure with an explicit resource-limit cause.

For qualified extension work, opt into automatic sizing. A submitter hint is
optional:

```yaml
extension: example.stage@1
needs: {memory: auto}
workload: {input_shape: recording-v1, units: 1500}
# Optional: size_hint: {host_memory_gb: 24, memory_gb: 8}
```

A matching variant reference enables accepted slice-1 observations. Prefer
complete local CPU or discrete xe host evidence plus the margin above
(other GPU and legacy local peaks × 1.25), then another machine's peaks for
the same recipe/work/shape
× 1.25 × 1.5 for the same OS/architecture/accelerator/memory-layout family, or
× 1.25 × 2 for another family. Cross-machine estimates are labelled in status.
Unknown, constrained, stale-identity or failed measurements, and peaks
recorded more than 30 days ago, are excluded. A peak's age is its own and does
not follow the throughput measured at the same point; main-process RSS alone is not a whole-attempt observation. Next use
the optional hint. Otherwise an exploratory first run needs at least half the
host allowance (physical RAM if uncapped) available and no protected attempt or
model residency charging a quarter of it. Its envelope uses the available
allowance after existing reserves, charges and attempt slack. Existing priority
and release rules still apply. Accepted exploratory peaks train later runs.
Input-size/duration scaling is a follow-up. The chosen
source, reservation and enforcement mode appear in `memory_envelope` on the job.
Finished attempt facts are also stored in `memory_attempts` on the job GET,
without joining `work_curves`. Each entry has exact `attempt`, `machine`,
`session`, the envelope it ran with (`memory_envelope`), and its observed
`memory_peak` when measured. The peak includes `host_gb`, `cgroup_gb`,
`device_gb`, Linux `swap_gb` (`memory.swap.peak`), `events`
(`high`, `max`, `oom`, `oom_kill`, `oom_group_kill`), and `constrained`
(omitted false). Optional readings are absent when unavailable, distinct from
measured zero. Event counts and swap peaks are from the hierarchical attempt
parent, including main and sidecars, with maxima retained across samples.
`memory.swap.current` still signals pressure but is not reported as a peak.
Constrained and failed observations remain on the attempt even when learning
rejects them, including constrained first runs. Records survive queue restart
and retain at most 32 rows (the first plus the latest 31), independently of
diagnostic history. `memory_attempts_dropped` counts evicted rows (omitted zero).
Prelaunch attempts without a peak do not create rows or count as dropped.
Release-only kill-time acknowledgements preserve their observations. Historical
readings cannot be reconstructed. Go `Client.GetJob` returns
`Job.MemoryAttempts`; Python `Client.get` returns the same array in its dict.
GPU counters are process-scoped sampled lower bounds, using NVIDIA compute-process
accounting or XPU DRM resident VRAM deduplicated by device/client on Linux.
Unavailable counters remain unknown, including unsupported Metal per-attempt
counters. Until Spark accounting is qualified, measured unified host/GPU peaks
are added with 25% headroom; an unknown device peak prevents an exact estimate.
GPU queries run every five seconds; empty rows and transient timeouts retry,
while unsupported or unparseable accounting stops sampling. When only host
memory was measured, the source names the retained GPU recipe default.
The existing driver/system reserve and attempt slack still apply. Measurements
never override an existing attempt or opt an explicitly declared job into auto.
Resumed attempts may use accepted estimates but do not train them. Peaks are
constrained by OOM, swap-out or anonymous workingset refault. `memory.events`
`max > 0` or `high > 0` also constrains a peak, including capped-member pressure
and file-backed working sets with low resident-only use. Older-worker results
and stored curves with these counters cannot supply accepted memory estimates.
This supersedes #184's cache exception; file-heavy jobs may reserve more or
explore again to avoid repeatedly learning a binding ceiling. Ceiling-bound peaks
from explicit-memory runs cannot seed auto estimates. An auto OOM or constrained
attempt clears comparable learned memory across machines without forbidding the
work. For a measured-need run, submit the same comparable work with `memory: auto`
and no `size_hint`: the next run explores available headroom, with source
`exploratory (previous peak constrained)` when a constrained peak invalidated
the measurement. Omit binding optional
member caps so the job can measure its need within the shared envelope. A
successful unconstrained run teaches a new peak. Ceiling pressure alone leaves
the job outcome unchanged; an OOM retains a job-local host floor for one retry
with 25% more room, without persisting a floor for later jobs.
Shared-envelope Docker attempts end on worker-unit stop.
Verified stopped members with a vanished parent release custody with unknown
memory/cause and supply no learning data. A surviving parent's post-exit reading
cannot train an estimate when the running sampler was lost during restart.

### Per-piece dependencies

`after: {job: FULL_PARENT_ID, piece: KEY, outputs: {LOCAL_INPUT: PRODUCER_OUTPUT}}`
predeclares one consumer of an accepted fan-out piece in the same project. Use an
ordinary command or extension job; see [fan-out dependencies](jobs.md#start-a-consumer-when-one-piece-is-accepted)
for validation, exact-attempt binding, deadlines, cancellation and retention.
`queued` includes blocked jobs; inspect structured `dependency.state` and
`dependency.execution`, rather than parsing `reason`. Parent failure preserves
accepted independent pieces; explicit cancellation aborts unfinished consumers.


## Keyed submission and cancellation

Start the [laptop dev queue](START-HERE.md#five-minute-start). Persist the
key and identical spec in the consumer before the first request. For this demo:

```sh
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
cat > job.json <<'JSON'
{"name":"keyed-demo","project":"demo","submission_key":"demo-operation-1","priority":"experiment","run":{"command":["sh","-c","echo done"]},"needs":{"memory_gb":1,"platform":{"any":true}},"resumable":false}
JSON
curl --fail-with-body "$MYCE_QUEUE/api/jobs" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @job.json > first.json
# Retry the stored identical request after a lost response, while intent is open:
curl --fail-with-body "$MYCE_QUEUE/api/jobs" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @job.json > retry.json
```

Both replies have the same `id`; identity echoes in `spec.project` and
`spec.submission_key`. A changed spec under that key is HTTP 409
`submission_key_conflict`. Terminal replies never rerun a job. This does not
promise exactly-once external effects; keep writes attempt-safe.

When the consumer closes its operation, fence late publication and **cancel**
its stored keys. Never recover a closed operation by resubmitting:

```sh
curl --fail-with-body "$MYCE_QUEUE/api/jobs/cancel" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  -d '{"project":"demo","submission_key":"demo-operation-1"}' > cancellation.json
```

Persist and verify the exact project/key echo and `will_not_run: true`.
Wait for `release: confirmed`, or `not_applicable` when never attempted.
`pending` means an already assigned attempt may still run briefly and remains
unreleased. Terminal state alone does not prove every attempt released.
Retry cancellation to recover a lost acknowledgement. An unseen key creates
an `absent` cancelled reservation: its first later valid submit can only bind a
minimal terminal cancelled job. It never executes or conflicts on spec.

An unbound reservation lasts 30 days. After expiry submit returns HTTP 410
`submission_key_expired`; cancel returns `unknown` and null release. Genuinely
unseen keys return `absent`. Unknown/expired coverage blocks the consumer's
reopen. Job-bound keys remain for the job lifetime, plus retention after removal.
For isolated dev expiry tests use `myce dev clock advance 30d`; it has no production
route. See the [full submission/cancellation contract](design-proposal.md#submission-identity-and-cancellation-80)
and [wire schemas](schemas/job-v1.schema.json).

## Maintenance

Close consumer publication intent and persist exact cancellation acknowledgements
independently. Use `myce host drain HOST` or `host disable HOST --stop --wait` for
queue maintenance. Inspect exact pending cleanup and worker freshness, then use
`host resume HOST` when authorized. Queue outage leaves maintenance unavailable;
ordinary service tools have no queue release guarantee. Check the application's
own runtime/model identity and health before directing traffic to it. Integrate
through the submission API; no local host-control client is required.

For a Python caller use [Client](../clients/python/README.md); resumable commands
use [the checkpoint contract](files.md). Dependency recipes,
server-local named secrets and immutable model sources are in
[environments](environments.md) and [servers](services.md).


## Declared independent pieces

The [slice 4 amendment](design-proposal.md#declared-fan-out-and-batch-planning-slice-4-107-approved-2026-10-24)
defines this contract. It uses ordinary extension jobs and existing artifact
transport. Applications supply homogeneous independent pieces and own semantic
splitting and reduction. Mycelium plans, retries and gathers them.

Declared calls fall back to serial admission when undeclared calls share the slot.

```yaml
name: parameter-sweep
project: research
priority: experiment
extension: simulator@1
workload: {input_shape: fixed-v1, units: 100}
files: {}                       # immutable common input manifest, or NAS inputs
outputs: {metrics.json: results}
fan_out:
  max_concurrency: 8             # ceiling, not a fixed batch size
  pieces:
    - {key: first, args: ["--seed", "1"]}
    - {key: second, args: ["--seed", "2"]}
```

Keys must be unique clean names; there are at most 256 pieces. `workload.units`
describes **one** piece, using the matching variant reference's unit and shape.
The existing `resumable` contract applies to each fixed wave. Arbitrary programs,
dynamic enumeration, general stage graphs and timed input accumulation are outside
this slice. Whole-parent dependencies below are the bounded #180 extension. Common inputs are staged through the normal file-store/NAS path for each
wave; arguments select the relevant data. No private input paths go in curves.

A variant opts into measured wave execution by adding `run` to its reference
sweep, alongside `command`, `counts` and `max_concurrency`:

```yaml
sweeps:
  extension:simulator:
    command: [python3, "{extension}/reference.py"]
    run: [python3, "{extension}/pieces.py"]
    counts: [1, 2, 4, 8]
    max_concurrency: 8
```

The author promises that `run` uses the same concurrency mode and shape measured
by the correctness-checked reference. No measured point grants resource capacity.
The queue conservatively scales per-piece CPU/RAM/device floors, includes measured
high-water memory and charges shared hosts once. Existing priority, fairness,
placement and owner controls still apply. Short groups and tails use smaller
measured counts. Without a safe matching wave recipe/curve, each piece uses the
ordinary serial command and its own arguments.

For a measured wave the worker sets `MYCE_BATCH_CONCURRENCY` and
`MYCE_BATCH_MANIFEST=.myce-batch.json`. This small JSON file is in the attempt's
working directory and contains `parent`, `pieces` (key and args), `concurrency`,
common `args`, `inputs`/`files` names and keyed `outputs` names. Write each declared
output under `KEY/OUTPUT_NAME`, e.g. `first/metrics.json`. The worker publishes
these through the existing attempt-specific verified artifact path. The manifest
name is reserved. A wave owns one command slot, including on GPUs.

The returned parent job's `fan_out` has a dispatch cursor, child job IDs and
`results` keyed by original piece key. Each result names the accepted execution
and its output references. The parent succeeds only after every piece succeeds;
its final `output_refs`/`output_files` use `KEY/OUTPUT_NAME`. A failed wave fails
the parent. An OOM forbids its exact measured count and fails the wave; it does
not qualify the same occupancy for another attempt. Infrastructure loss and planned interruption use existing retry
policy on that wave alone. Membership and measured count stay fixed across
retries, preserving the recipe's vector-batch semantics. A retry waits if no
component can safely admit that count and its resource envelope. Late attempts
cannot replace accepted results. Parent cancellation stops its children and
reports outstanding cleanup. Persisted membership/results survive queue restart.

## Observing parents and native children

Save the parent job ID returned by submission. For a keyed submission, bind that
ID using the exact `spec.project` and `spec.submission_key` echo in the Job reply
([identity contract](design-proposal.md#submission-identity-and-cancellation-80)).
Read the parent with authenticated `GET /api/jobs/{id}` (Go `GetJob`/`Wait`, Python
`get`/`wait`). Its top-level `fan_out.children` lists the native child job IDs
materialized so far; more can appear as dispatch proceeds. Fetch those jobs by ID.
Each child's top-level `batch.parent` is the parent job ID, and `batch.pieces`
identifies the original piece keys in that fixed wave. A child has its own job
ID and no `spec.submission_key`; the parent's key is not copied to children.
One child may execute several pieces, so do not assume one child per piece.

An assigned child's top-level `extension` reports the selected `name`, `version`
and frozen `digest`. This identifies the executable selection, not the parent:
many unrelated jobs can select the same extension. Use `fan_out.children` and
`batch.parent` for the relationship, and `fan_out.results` for accepted per-piece
executions and outputs. An unassigned logical parent may omit top-level
`extension` because selection occurs when a child is assigned.

GET `spec` is a redacted view, not the original submission. As documented in the
[status/Job view contract](status.md), GET retains payload
redaction while echoing project/key identity. It omits `spec.extension`,
`spec.extension_digest` and `spec.fan_out`, including the declaration's piece
arguments; observe top-level progress and child selection instead. Retain the
submitted declaration if you need arguments before dispatch. Use
`cancellation_scope.pieces` for every declared key, including undispatched keys;
`fan_out.children` lists only materialized waves.

## Abort and wait for release

Cancel the parent once with `myce cancel PARENT` or
`myce cancel --project PROJECT --key KEY`. Both routes fence all declared pieces
and registered dependents, including blocked/ready consumers and retried waves.
Valid future consumers of the retained cancelled parent are cancelled on admission.
Terminal results remain intact; cancellation adds durable exclusion metadata.

Read `GET /api/jobs/PARENT` (Python `client.get(parent_id)`) to observe
`cancellation.release`, or use `myce status PARENT`, which prints cancellation,
pieces and members. Wait until release is `confirmed` or `not_applicable`;
terminal job state alone does not establish release. `pending` covers all old
and current producer/consumer copies until exact cleanup. The logical parent
reports `confirmed` after attempted descendants settle even though its own
`release_confirmed` stays absent. Never-attempted cancelled members explicitly
report `not_applicable`, without manufacturing a physical release.

`cancellation_scope.pieces` includes each `key`, `materialized`, `accepted`,
optional `wave_id`, `will_not_run`, aggregate `release`, and registered dependent
IDs in `dependents`. Its `members` lists all retained wave/dependent jobs with
`job_id`, `state`, `will_not_run` and aggregate `release`. Empty lists are explicit.
Before abort, release only describes attempts so far; inspect `will_not_run`
for exclusion. No child cancellation loop is needed.

Scope survives restart and includes future registered consumers on later reads.
It enumerates retained records: jobs are not automatically removed; keyed
tombstones expire 30 days after explicit removal, followed by permanent digest
markers. Once the source is unavailable, GET cannot enumerate it and new
dependencies are rejected. Artifact expiry does not erase retained scope or
settle custody. Keep parent identity and acknowledgement locally across outages.

## Binding model-call curves

An extension-command sweep alone does not qualify server calls. Compute the
exact server configuration digest with the read-only command:

```sh
myce model measurement-digest MODEL --model-config models.yaml --environment-config environments.yaml
```

The reference sweep must declare that digest as `server_digest` and actually
exercise that exact model/server, shape, environment and cache/context settings.
The model catalog's `measurement` declares `extension_digest`, `work` (the exact
reference work type) and the same `server_digest`. Changing the model bytes,
command, settings, bounds or resolved environment invalidates the binding.
Existing variant correctness qualification and exact component/fingerprint,
allowance, freshness, OOM and measured-memory checks remain mandatory.

Requests opt in with generic headers `X-Myce-Fan-Out` (group key),
`X-Myce-Input-Shape` and `X-Myce-Work-Units`. Compatible pending calls in the same
project, class/tier, model, shape and size can use measured engine occupancy,
bounded by configured `max_in_flight`. A running wave's chosen occupancy is not
increased by later arrivals; a tail can use fewer slots. Unknown binding means
serial admission for declared calls. Requests without the declaration keep
their existing routing/caps, including zero-as-unlimited. Model calls retain
their existing ephemeral request/retry/streaming semantics; this is not a durable
workflow API or a second batching timer.

## Laptop simulator comparison

```sh
myce sim --fleet examples/sim/fleet.yaml --workload examples/sim/declared-fan-out/workload.yaml --what-if declared-batch --days 7 --seed 1 --json sim-results.json
```

This compares unsplit, naive split and declared execution using identical arrival
draws and policy. The added curves are explicitly synthetic work-units/second,
not measurements of any fleet. Execution timers use these rates while the real
queue chooses placement/count and handles preemption. The JSON records the
paired mean makespan gain, worst individual batch regression, production-p95
delta and gate result. A failed gate writes the JSON and exits nonzero. Each
comparison uses the same synthetic component inventory and shared host ledger;
virtual per-component workers retain the simulator's single-assignment transport.
Native qualification and
deployment require separate authorization.

## Start a consumer when one piece is accepted

Submit the producer first, then predeclare an ordinary next-stage job for each
known piece. The queue owns readiness even after the submitter exits:

```yaml
name: consume-first
project: research
submission_key: operation-42.consume.first
extension: summarizer@1
after:
  job: FULL_PRODUCER_JOB_ID
  piece: first
  outputs: {input.json: metrics.json}
```

`outputs` maps the consumer's local input name to the producer's declared output
name. The full parent ID, project, piece and output names are checked at submit.
One selected-piece edge is supported; it cannot be combined with `fan_out`, checks, programs,
Docker or a caller-selected working directory. Dependency inputs cannot collide
with other inputs or outputs, or mix stored-file and NAS input transports.

The wire state stays `queued`. `dependency.state` is `blocked` until acceptance,
then `ready`; `dependency` records the parent, piece, exact producing `execution`,
`ready_at`, and immutable `files` or NAS `inputs`. The submitted `spec.after`
remains unchanged. A measured wave accepts all its pieces together after its
complete output manifest is verified. Availability means normal scheduling
eligibility; it reserves no resources. Consumers stage and verify the bound input
through the existing artifact transport before command execution.

The generated production start deadline and runnable wait age start at readiness.
Declare `start_window: 600s` on a production consumer job to give it a ten-minute
relative budget. Omission keeps the `150s` default; experiments omit it and have
no start deadline. The declaration also works on a selected adapter entry in
`.mycelium/adapter.yaml`. It accepts positive Go duration strings up to `1h`;
the cap keeps unavailable capacity/preparation waits bounded while allowing
contention. Longer budgets delay startup failure accordingly.
Explicit `start_by`, `give_up_at` and `deadline` remain absolute. Accepted pieces
survive sibling failure; consumers of unaccepted pieces fail with structured
`diagnostic_code: upstream_failed`. Cancelling the parent is a pipeline abort,
including after gather: unfinished consumers stop, future consumers are cancelled,
and the cancellation acknowledgement stays `release: pending` until all exact
producer/consumer copies are released. Terminal results remain intact. Cancelling
one consumer has no reverse effect.

Producer gather still requires every producer piece and does not wait for
consumers. Save the consumer IDs if the application needs all downstream results.
Submission keys make an interrupted finite submission list safe to resume; they
do not make the pipeline atomic. Partial parent results and bound consumer inputs
use their existing artifact retention windows. A new binding to an expired result
returns HTTP 410 `result_expired`; an existing key still replays its original job.

## Start a consumer after every producer piece succeeds

Omit `after.piece` to wait for the whole fan-out parent. Map its gathered
`KEY/OUTPUT_NAME` names to explicit local inputs. Even when only one output is
mapped, every declared producer piece must succeed before the consumer is ready:

```yaml
name: reduce
project: research
run: {command: [sh, -c, 'cat first/input second/input > combined']}
needs: {platform: cpu, cpu_cores: 1, memory_gb: 0.1}
after:
  job: FULL_PRODUCER_JOB_ID
  outputs: {first/input: first/metrics.json, second/input: second/metrics.json}
outputs: {combined: results}
```

The consumer may itself declare `fan_out`:

```yaml
name: next-stage
project: research
class: production
start_window: 600s
extension: summarizer@1
workload: {input_shape: fixed-v1, units: 100}
after:
  job: FULL_PRODUCER_JOB_ID
  outputs: {first/input: first/metrics.json, second/input: second/metrics.json}
outputs: {summary.json: results}
fan_out:
  max_concurrency: 8
  pieces:
    - {key: first, args: [--input, first/input]}
    - {key: second, args: [--input, second/input]}
```

Inputs are explicit immutable common inputs, staged for each wave. Piece args or
the existing batch manifest select the keyed paths; there is no implicit key
substitution or input filtering. A measured wave's manifest lists these bound
inputs along with submitted common inputs. Register subsequent consumers against
this stage's parent ID, then wait only on the final job. No caller-side gather wait
is needed. Membership and accepted bindings survive queue restart.

The logical consumer's generated deadline is recorded at readiness but does not
expire the parent while it waits for capacity, including before the first wave.
Each executing child inherits `start_window` and gets that full relative budget
from its own assignment, including later waves. An explicit absolute `start_by` takes
precedence and is preserved by every child instead. If it expires before all
pieces are assigned, the tail never dispatches. Materialized children keep their
own startup/recovery windows and accepted commands may finish; the parent fails
after those children settle.
After a started child loses
its machine, recovery gets the original window length from loss detection;
unstarted retries keep their cutoff. Job GET shows `spec.start_window`, current
`spec.start_by`, and effective top-level `start_window` in nanoseconds. See
[production start deadline](jobs.md#production-start-deadline).

For this form `dependency.executions` records the exact accepted producing
execution for each mapped source key. `dependency.execution` is zero-valued;
it applies only to the selected-piece form. Parent failure fails a blocked
consumer with `upstream_failed` without an attempt. Cancellation propagates through
downstream parents, their children and ordinary consumers, and release includes
every exact attempt. Terminal results remain intact. Existing readiness clocks,
absolute deadlines, expiry and transport checks still apply.

Only one already submitted fan-out parent in the same project may be named.
Ordinary-job sources, multiple-parent joins, forward references, dynamic piece
creation, timed accumulation and atomic graph submission remain out of scope.

## Per-recording stage chains

For an ASR → separation → naming → render pipeline, submit a separate fan-out
parent for **each stage of each recording**. Naming can be a one-piece fan-out;
render is the ordinary sink. Submit stages in order using their returned parent
IDs, then wait only on render. Different recordings' chains can progress
independently.

A stage-wide fan-out across recordings cannot feed independent per-recording
downstream stages as each recording becomes ready: `after.piece` supports only an
ordinary sink, ordinary results cannot be upstream sources, and a whole-parent
edge waits for every recording in that parent. Keep the parent boundary at one
recording when you need per-recording progress.

This illustrative recipe uses two known chunks and extension-defined filenames,
arguments and workload shapes. Supply the actual extension recipes and immutable
ASR inputs for the recording; Mycelium does not discover chunks:

```python
project, recording = "research", "recording-1"

def stage(name, pieces, outputs, *, after=None, files=None):
    return client.submit({
        "name": f"{recording}-{name}", "project": project,
        "submission_key": f"operation-42.{recording}.{name}",
        "extension": f"{name}@1",
        "workload": {"input_shape": f"{name}-v1", "units": 1},
        "files": files or {}, "outputs": outputs,
        "fan_out": {"max_concurrency": 8, "pieces": pieces},
        **({"after": after} if after else {}),
    })

asr = stage("asr", [
    {"key": "a", "args": ["--input", "a.wav"]},
    {"key": "b", "args": ["--input", "b.wav"]},
], {"words.json": "results"}, files=recording_files)
separation = stage("separation", [
    {"key": "a", "args": ["--input", "a/input"]},
    {"key": "b", "args": ["--input", "b/input"]},
], {"segments.json": "results"}, after={"job": asr["id"], "outputs": {
    "a/input": "a/words.json", "b/input": "b/words.json"}})
naming = stage("naming", [
    {"key": "whole", "args": ["--input", "a.json", "--input", "b.json"]},
], {"labels.json": "results"}, after={"job": separation["id"], "outputs": {
    "a.json": "a/segments.json", "b.json": "b/segments.json"}})
render = client.submit({
    "name": f"{recording}-render", "project": project,
    "submission_key": f"operation-42.{recording}.render", "extension": "render@1",
    "after": {"job": naming["id"], "outputs": {"input.json": "whole/labels.json"}},
})
result = client.wait(render["id"])
```

Repeat those submissions with a distinct recording identity/key namespace for
each recording, registering every chain before waiting on the final sink IDs.
