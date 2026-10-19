# Mycelium v3: approved design

Approved design, revised 2026-10-19 by the maintainer decisions in [#80](https://github.com/1kuna/mycelium/issues/80). Queue maintenance, keyed submission, cancellation and installer contracts below are implemented. This is the only product spec; native qualification and deployment remain separate from laptop implementation evidence.

## Why a redesign

v1 tried to do too much at once and never thought about how people would use or test it. It was too heavy to run in production and too hard to test. Mycelium keeps one rule above all others: **every feature must be easy to use, and easy to test on a laptop with no GPU.** If a feature can't meet that bar, it waits.

## What it is

One shared queue for production, research and future projects. The approved extension direction includes CPU and accelerator workloads through the same execution interface. **Current admission remains GPU-only:** CPU/core reservations and CPU-only workers are a later #98 resource slice; E1 does not enable them. Laptop tests use fake GPU inventory. Projects submit jobs. Machines pull the most important job they can run. No machine sits idle because its "own" project is quiet.

## The parts

| Part | Where it runs | What it does |
|---|---|---|
| Queue service | One always-on machine, on its CPU | Holds the jobs in one local database file. Decides which job goes where. Serves a small HTTP API and one status page. |
| Worker | Every GPU machine that is in the fleet right now | Reports its free memory, GPU type and loaded models. Asks for work, runs jobs, sends back logs and results. |
| `myce` command | Anywhere | Submit, watch, cancel and read logs. `myce join` starts a worker on this machine. |

- **Dedicated machines** offer their GPUs whenever available. **Shared machines** can drain or disable queue admission when another owner needs them; that owner controls its own work and checks release before reopening Mycelium.
- **Machines that come and go:** running `myce join` adds a machine; stopping the worker removes it. Daily-use participation can wait for owner inactivity, safe heat and external power.
- All machines are on one local network. Workers use a configured queue address or authenticated joining; discovery finds addresses but does not establish trust. A privately provisioned join credential keeps strangers out.
- Single Go binary, as in v1. There is no peer network, no leader election and no replicated state. If the queue service restarts, queued and running batch jobs survive in its database file, and workers wait and reconnect. Model calls that were in flight are not replayed and fail; callers must retry them.

## A job

```yaml
name: example-reindex
project: example
class: experiment           # arbitrary job class; legacy priority is accepted
deadline: null              # a time, required when priority is "deadline"
run:
  command: ["python", "reindex.py", "--batch", "7"]
  workdir: /srv/example
  env: {}
needs:
  memory_gb: 40
  platform: [cuda]          # any of: cuda, xpu, metal
  model: qwen3-32b          # optional; used to group jobs that share a model
expected_minutes: 90        # rough guess; used for planning, never enforced
resumable: true             # true = the job saves checkpoints and can be stopped and picked up later
```

The queue gives every job its job id, `MYCE_CHECKPOINT_DIR` and attempt number. With NAS configuration, assignments are NAS-relative and each worker resolves them under its own `--checkpoint-root`. Without a NAS, the file store transports verified checkpoint generations. A resumable job only has to (1) save to the checkpoint folder now and then, (2) save and exit when it gets a stop signal, and (3) pick up from the checkpoint folder when it starts. Only durable, complete progress can resume on another machine; see the resumable-jobs guide.

## How the queue decides

When a worker asks for work, the queue picks, from the jobs that fit this machine's free memory and GPU type:

1. the highest owner-configured project/class tier first; without a configured policy, production, then deadline, then the catchall (experiment);
2. within that tier, the fitting project with the greatest weighted recent-usage deficit; equal usage prefers fewer active allocations per weight;
3. within that project, a model already loaded, then cached locally, then needing a NAS copy;
4. then the earliest deadline and the job that has waited longest.

**Making room.** Production is checked first on every check-in and immediately
on submission. When no idle fitting machine has room, stop lower-priority work
right away through an authenticated long-poll notification, without waiting for
its next check-in. Resumable work receives SIGTERM and 10 seconds by default to
save; the queue's `--production-stop-grace` configures that duration. Deadline
work still allows 2 minutes and only stops resumable lower-priority jobs.
Production also stops non-resumable lower-priority work immediately. Stopped
jobs return to the front of their priority line without consuming an
infrastructure retry; non-resumable jobs start over with a new execution attempt.
**Make experiments resumable and save checkpoints often**: production may arrive
at any moment, and a power cut offers no grace at all. Status reports production
submission-to-start acknowledgement latency; the laptop dev test exercises the actual process
start under 15 seconds, excluding model load time.


**Owner priority tiers (P1, #98).** The durable SQLite policy contains ordered,
uniquely named tiers of `project/class` selectors using Go `path.Match` globs.
First match wins; the final tier includes `*/*`. One comparator replaces legacy
rank decisions in placement and existing preemption. `myce policy get [--yaml]`
and `myce policy set policy.yaml --if-revision N` expose an atomic revision CAS;
HTTP conflicts return 409. Jobs declare immutable `class` (default experiment),
legacy priority normalizes at admission, and conflicting declarations fail.
Project/class identifiers cannot contain `/`. Status shows current tier,
selector and policy revision. Policy changes rerank existing jobs on subsequent
planning decisions without changing keyed replay or legacy reliability behavior.
An already-assigned attempt is treated like a running attempt: a policy change
alone does not revoke its assignment or prevent it from starting, even if a
higher-tier job is now waiting. Policy changes apply to future placement
decisions; reclaiming already-assigned attempts belongs to P3 preemption.
P2 admission fairness applies across all classes within a tier. P3 expanded
preemption/retry semantics remain a later slice; current eligibility and
stop/retry rules still apply. See
[the policy guide](guide-priority-policy.md) for format and API details.

**Optional physical-host takeover.** A queue host policy may opt into whole-machine production takeover, default off. Production selects one eligible physical host; all lower-priority work on that host stops safely, regardless of how much room the request needs. Stopped experiments rejoin the shared queue and resume on any compatible machine, without an infrastructure retry. Production admission waits for all lower-priority cleanup acknowledgements. Unknown cleanup survives loss and restart; elapsed stop grace never proves release. Pins remain protected.

There is **no idle production cooldown** after the final demand and cleanup settle. Lower-priority work can run again; new production preempts through the queue. Production takeover covers submitted GPU work, not CPU publication. Project machine allow-lists apply at every priority and to model calls; commands and servers receive their placement in worker-owned environment variables.

One measured physical-host memory allowance covers simultaneous production demand, other admitted work, sidecars, launch overhead and headroom. Its numeric value comes from component measurements, not this spec. Keep aggregate accounting and cgroup/Docker enforcement. Do not rewrite budgets on every ownership switch: queue maintenance excludes new work while another owner uses the host.

**Machines leaving.** Workers pull jobs, so a job is never tied to a machine until it starts. That means there is no list of waiting jobs per machine to clean up. Leaving works like this:

- **Planned leave** (`myce leave`, or Ctrl-C on the worker). The machine stops taking new jobs at once. Resumable jobs get the stop signal, save to the NAS, and go back to the front of their line. For jobs that can't be resumed, `myce leave` asks whether to wait for them to finish or to stop them now and put them back in the queue. `myce leave --now` skips the question and stops everything.
- **Sudden loss** (unplugged, asleep, network drop): without a check-in, the queue marks the machine gone after 5 seconds for production work, 20 seconds for deadline work, or 60 seconds for experiments (defaults based on the machine's active job priority). Its running jobs go back in the queue. Resumable jobs continue from their last checkpoint. Others start over. Experiments and deadline jobs fail loudly after three counted infrastructure losses; their planned stops to make room or leave do not count. Production uses the bounded infrastructure retry budget below, including planned host release or leave. Model calls that were in flight are retried on another machine, and models loaded there are marked as gone.
- **Late return.** A network split can briefly leave the same job running on two machines. When contact returns, the queue keeps the resumable copy with the newest complete checkpoint, or the current attempt when there is no newer checkpoint, and tells the other copy to stop. Jobs must make output writes safe to repeat.
- **Jobs no remaining machine can run.** These are jobs pinned to the machine that left, or jobs that need a GPU type no current machine has (for example, Apple-only work when every Mac is away). They stay in the queue, and `myce status` says plainly why each one is waiting: "no machine in the fleet can run this right now (needs metal)". A job can set a give-up time, after which it is marked failed instead of waiting forever.

**Power and network loss are normal, not rare.** Every part must survive them without losing durable intent or treating uncertainty as release.

- **The queue machine loses power.** The database is written safely (every change is fully saved to disk before it's confirmed), so a power cut never corrupts it or loses a submitted job. The queue starts automatically when the machine boots. After a restart it does *not* immediately declare every job lost. It waits a few minutes for workers to reconnect and report what they're still running or have finished, and only then puts back in line the jobs nobody reported.
- **A worker machine loses power.** This is handled as a sudden loss: its jobs go back in line after the priority-based timeout above (5, 20, or 60 seconds). On the always-on machines, the worker starts automatically at boot. On startup it cleans up any job processes left over from before the power cut, and then reports in fresh.
- **A worker loses network but keeps power.** It keeps running its current jobs, saves their logs and results locally, and keeps trying to reconnect. It does not start new jobs. When it reconnects it sends everything it saved. If the queue already gave a job to another machine, the queue reconciles the overlapping copies: the resumable copy with the newest complete checkpoint keeps running, otherwise the current attempt does, and the other stops. Output writes must be safe to repeat.
- **The local network drops, or the queue machine loses network.** If a strict majority of workers (including all of them) go quiet at once, the problem is almost certainly the network, not every machine at the same time. So the queue does not put everything back in line. It waits until machines reconnect, or until it can confirm specific machines are really gone.
- **Configured storage is unreachable.** NAS-backed resumable jobs wait for their checkpoint folder; file-store-backed jobs require their declared inputs and durable checkpoint transfers. Non-resumable production with local inputs starts with local scratch and does not require a NAS. NAS probes and checkpoint reads/preparation have short bounded waits outside the queue lock. A displaced experiment that cannot save within its grace is force-stopped and reported interrupted; production proceeds after memory release, and the experiment later resumes its last durable save. Running jobs otherwise keep going and retry their saves. Status explains the storage failure; local-only progress never masquerades as portable progress.
- **Saved progress is never half-written.** The resumable-jobs guide requires writing each checkpoint to a temporary file and then renaming it, so a power cut mid-save leaves the previous checkpoint intact. When resuming or reconciling copies, a job uses the greatest monotonic generation recorded atomically in its checkpoint; file times and clocks on other machines do not determine progress. Current execution/session/attempt identity and a complete checkpoint generation are required. Historical unsequenced saves need offline migration or remain held; the runtime does not guess their ownership or rank them as a fallback.
- **Tests:** each of these cases has a test using fake time and pretend machines: queue restart, worker restart, worker cut off from the network, the whole network dropping, and the NAS going away.

**Data stays local.** Every machine in the fleet is on-premises, so job data never leaves our machines. A job can also list the specific machines it is allowed to run on.

**Production infrastructure retries (#80, agreed with Verso 2026-10-19).** An interrupted production batch job requeues at the front of its priority line under the same job ID and `submission_key`, at most **2** times (`queue --production-infrastructure-retries`, a nonnegative limit clamped to two; zero disables requeue). Worker restart or loss, an unreported attempt after queue restart, undelivered/lost results, worker interruption/release, planned leave and reboot all use the existing requeue path. A queue restart that reconnects a still-current running command or its buffered result does not interrupt it. The third interruption fails with `infrastructure_retries_exhausted` and a terminal timestamp. The durable job counter `infrastructure_retries` records requeues used and is an additive optional integer in status and job-v1 (omitted at zero). It survives restart; release evidence cannot refund or revive an exhausted production budget.

Classification uses the cause for both native and Docker attempts. A Mycelium stop (worker interruption/release, planned leave, worker shutdown or stop event), worker/daemon loss with an unobserved outcome, or a readiness wait aborted by that infrastructure interruption uses the retry budget. A container killed or missing during an issued stop or recovery is infrastructure. An observed independent nonzero exit, OOM (including an extension job), sidecar self-exit during readiness, or readiness timeout without an infrastructure stop fails production immediately and consumes no infrastructure retry. A SIGKILL without stop/recovery evidence is a stage failure, with the ambiguity recorded in status. Cancellation takes precedence: once `will_not_run: true` is acknowledged, no interruption can requeue the job. Superseded production attempts cannot recover ownership or publish a late result; exact release evidence can still discharge their custody. At most one attempt's result is published. Commands and output writes must still be safe to repeat; external side effects are not exactly-once. Experiments retain their existing retry, checkpoint recovery and planned-stop behavior.

A production attempt that had started gets its original-length start window from interruption detection, including a worker interruption/release. Planned leave keeps its original start deadline. A requeued job may wait for its maintained host to resume within that window; a new production submission to only disabled eligible hosts still fails immediately. Unstarted attempts do not renew their start deadline.

## Queue-owned host maintenance

Maintenance is a durable queue record keyed by the physical `host_id`, shared by
all of its GPU workers. It survives checkin, queue restart and worker rejoin.
It is independent of the worker's `disabled` report and project/job allowlists.

```sh
myce host drain HOST [--wait] [--timeout 30s]
myce host disable HOST [--stop] [--wait] [--timeout 30s]
myce host resume HOST
myce host status HOST [--wait] [--timeout 30s]
myce host budget HOST --memory-budget-gb 64
```

These commands use the authenticated queue (`MYCE_QUEUE`, `MYCE_TOKEN`, or
`--queue`/`--token`), run as an ordinary account and print JSON. They require no
local exclusion proof or consumer-specific integration. Drain and disable both
block new placement, preparation and Started authorization across the host.
Drain lets already-authorized attempts finish. An assigned attempt that has not
passed Started is withdrawn through the existing stop channel; it retains its
preparation/create custody until exact cleanup. `disable --stop` additionally
stops every kind of host work through the same obligation enumeration used by
wait: batch attempts and their sidecars, exact model calls and engines, and
session-owned environment builds and model preparations. They share a persisted
stop deadline of two minutes, which repeated stop requests cannot extend.
Engine unload is forced at that deadline even with in-flight calls. Drain and
disable without stop let existing preparations finish while admitting no new
ones. Admission acceptance or deadline expiry is not evidence of physical
release; wait still requires exact cleanup. Resume restores eligibility
transactionally.

Queue-requested stops carry `planned_host_maintenance` on the exact copy. They
requeue the same job without consuming production infrastructure retries or
infrastructure losses, including when cleanup, loss detection or restart happens
later. The cause never transfers to the next attempt. Cancellation still wins;
observed independent command failure, OOM or sidecar/readiness failure still
fails production immediately. Started attempts get their original-length start
window on requeue; an unstarted attempt keeps its deadline. Retries can wait on
the maintained host or use another eligible host, subject to ordinary deadlines.
The earlier two-infrastructure-retry contract still applies to other causes.

The queue bootstraps the memory allowance once from the first worker report,
including zero for an uncapped laptop. Subsequent reports cannot change it.
`host budget` or `drain|disable --memory-budget-gb GiB` sets a positive allowance
only while drained/disabled. Both device workers receive the policy and must
report it consistently before new work starts. Lowering it keeps existing
attempt charges and exposes aggregate RAM debt; it does not resize a running
attempt's ceiling or release resources. Qualification remains `UNQUALIFIED`
until separately authorized native measurement. Docker main/sidecar ceilings
and total per-attempt charges are unchanged.

Status reports policy, known worker freshness/device identities,
allowance/mismatch, aggregate RAM/debt, attempt identities, preparations,
sidecars/engines and pending releases/daemon cleanup. All previously known
workers remain required after loss or restart; a never-reported host has unknown
health. Plain status prints the snapshot and exits nonzero for incomplete health. `--wait` exits zero only while drained/disabled with fresh, consistent
worker evidence and zero known attempts, preparations, sidecars, releases and
cleanup. Queue outage, missing/stale reports and timeout cannot satisfy it or
discharge custody. This is queue-observed quiescence, not an offline hardware
handoff guarantee. Offline intervention uses separately authorized service and
container tools after inspecting owned work.

Queue maintenance is the sole host admission authority. There is no local intent,
worker registry, host revision/CAS, last-close, exclusion observer, session-wide
discharge, privileged controller, rest envelope or reconcile timer. Ordinary
binary execution as root is refused before reading flags, configuration or secrets.
Each worker uses its own spool and reports exact session/attempt custody. Queue
policy reaches workers through the existing authenticated checkin reply. There
is no local allowance configurator or local-budget fallback.

## Using it

```
myce submit job.yaml        # prints the job id
myce status                 # what's running where, what's waiting, and why
myce logs <id> --follow
myce cancel <id>
myce join                   # turn this machine into a worker
myce leave [--now]          # take it out of the fleet
```

The same actions are available as plain HTTP calls, so projects can submit from code. The status page shows the same information as `myce status`.

These GPU-work and setup features are implemented now:

- **Any GPU work, not just batch jobs and OpenAI model calls.** GPU commands run as batch jobs; general HTTP model servers keep their existing interfaces and are loaded and unloaded by the queue.
- **GPU preference.** A job either requires a platform (`cuda`, `xpu`, `metal`/MLX), or states a preferred one, or doesn't care. If it doesn't care, it goes to any machine that can run it.
- **Environments.** A job names the software it needs, either a named environment (like `torch-cuda` or `mlx`) or a lock file. Workers report which environments they already have ready. The queue prefers a machine whose environment is ready, the same way it prefers a machine with the model already loaded. If none has it ready, a machine that *can* build it does so first.
- **Automatic setup.** `myce install worker` detects the GPU and operating system and installs the worker and its boot service; `myce install queue` sets up the queue service. Matching software recipes build on demand, rather than all being installed up front. Bare `myce join` uses the same GPU detection for a foreground worker; explicit GPU and device flags override it.
- **Model reservations are implemented now:** project-owned pins can hold a model continuously or within explicit weekday/time-zone windows. See Models below.

## Host admission and release (#80)

Queue maintenance serializes placement and Started with cancellation and durable
queue state. Every batch payload needs the current exact-attempt Started response.
A grant already accepted can race a later drain/disable; it retains its charge and
stops through the ordinary queue protocol when requested. No offline file or root
proof can supersede that queue transaction.

Known executions, preparations, model engines/calls and pending daemon operations
remain visible through `host status HOST`. `HostRelease` and the work enumeration
retain unknown custody after loss, stale reports or restart. Only exact completion,
release acknowledgement or cleanup of the immediately previous worker session
settles that custody. Timeouts, an empty inventory and service exit alone do not.
The same cleanup handles native groups and Docker main/sidecar/network resources.

Drain before an upgrade; use `disable --stop --wait` when active work must stop.
Resume explicitly when ready. There is no offline handoff certification: during
queue outage use separately authorized service/container tools after inspecting
owned work, preserve pending results, then reconnect for exact reconciliation.

## Submission identity and cancellation (#80)

`POST /api/jobs` and submit clients accept optional `submission_key`, unique within `project`. When supplied it must be 1–128 ASCII characters from `[A-Za-z0-9._:-]`; submit and cancel-by-key reject an invalid key with HTTP 400 `submission_key_invalid` before lookup or mutation. Omitting the key on submit preserves unkeyed behavior; an explicitly empty or null key is invalid. Without it, each submission is a new job. Atomically persist key, canonical client spec and job ID before replying. Identical repeated/concurrent submissions return the existing ID, even for terminal jobs; they never resurrect work. Compare client values ignoring object-key order and excluding the key and server-generated IDs, timestamps and default start deadline. An explicit client deadline remains part of the spec. A different spec returns HTTP 409 `submission_key_conflict` without mutation.

**Cancelled reservations take precedence.** The first later valid POST against a spec-less cancelled reservation binds a minimal terminal `cancelled` job with an ID and returns the normal Job shape, whatever spec was submitted: no comparison, no 409, no assignment and no execution. Preserve cancellation time. Later submissions return that cancelled job regardless of spec. Submit and cancel-by-key for the same `(project, submission_key)` each execute in one SQLite transaction under the queue's single update lock, serializing with each other. If submit commits first, cancel acts on its job; if cancel commits first, submit gets the reserved cancelled job.

| Retained identity | Lifetime and answer |
| --- | --- |
| Job key, spec fingerprint and ID | Entire job lifetime plus 30 days after removal; return the original answer within retention. |
| Cancelled reservation with no job | 30 days from creation; later submit can bind only a cancelled job. |
| SHA-256 digest marker of `(project, key)` | Retained indefinitely after full-record expiry. Submit returns HTTP 410 `submission_key_expired`, never creating a job; cancel-by-key returns `unknown`. |
| No record, with intact durable marker store | Genuinely never seen; cancel-by-key records an `absent` reservation. A cache miss alone never proves absence. |

A key's acknowledged `will_not_run: true` remains true permanently, including after expiry. While a consumer operation is open, recover a lost response by retrying its stored identical spec/key within retention. Once closed, cancel by key only, never resubmit. An expired/unknown answer leaves consumer coverage unresolved and blocks its reopen. Consumers own key generation, stored specs, operation intent and acknowledgement persistence. This promises no exactly-once external effects; keep output writes safe to repeat.

### Job reply contract

The generated [Job reply schema](schemas/job-v1.schema.json) describes actual `Job`/`Spec` JSON output, including the minimal reserved terminal job. Identity echo is **`spec.project` and `spec.submission_key`**, never top-level fields. Stable contract fields are annotated `x-contract: "stable"`: job ID/state, project/key echo, execution identity (`machine`, `session`, `attempt`), recorded release confirmation, recorded terminal time (`finished_at`), durable cancellation time (`cancelled_at`), and optional production requeues used (`infrastructure_retries`). The state enum is `queued`, `running`, `stopping`, `succeeded`, `failed`, `cancelled`; preparation is an assigned `running` job with no `started_at` yet. Optional identity/time/release fields follow the Go JSON omission rules: absent `release_confirmed` is not confirmation; queued/reserved jobs omit machine/session, and a reserved job has attempt 0. A reservation's minimal spec preserves only project/key identity: zero/empty command, priority, needs and environment fields are valid **reply** data, not a valid new runnable submission. `reason`, progress, placement, accounting and other projections are diagnostic (`x-contract: "diagnostic"`); clients must not parse reason text or infer release from it. Diagnostics may grow. Terminal state/results stay intact when cancellation adds durable exclusion metadata.

Schemas describe wire shape, not proof of physical release or permission to consume results. Go tests regenerate the Job schema from the actual types and validate real marshalled jobs, cancellation acknowledgements and HTTP error responses. Examples cover queued, running, succeeded and reserved cancelled replies.

### Cancellation acknowledgement

Cancel by ID with `POST /api/jobs/{id}/cancel`, or by key with `POST /api/jobs/cancel` and `{project, submission_key}`. Both are idempotent and return `{job_id, project, submission_key, state, will_not_run, release, acknowledged_at}`. `job_id` and `submission_key` may be null where no binding exists. `acknowledged_at` echoes the stored first `cancelled_at` (or reservation cancellation time) in UTC RFC3339; retries, reopen and reservation binding preserve it. It is null for an expired digest marker with no retained timestamp. `will_not_run: true` durably means no **new** attempt is created or assigned after the acknowledgement. An attempt already assigned before the acknowledgement may still begin briefly; the worker stops it at its next check, and `release` stays `pending` until release is proven. Consumers that need execution exclusion wait for `release: "confirmed"` (or `not_applicable` when never attempted). Every batch command requires the current attempt-fenced Started acknowledgement before its payload executes; Docker and engines retain exact execution custody and queue-authorized plans. `release` is `not_applicable`, `pending` or `confirmed`, or null when no release fact can be established. For a retained job, release always covers **every known attempt**, regardless of whether the job is queued, active, terminal or removed: `not_applicable` only when it never had an attempt; `pending` while any known attempt has unproven release (its charge remains); `confirmed` once all known attempts are proven released. In particular, a requeued job can still have a lost, unreleased attempt; cancelling it returns `pending`, then a repeated cancellation returns `confirmed` after that exact release is proven. Cancellation never creates release custody or placement blockers; runtime tracking owns charges. Fresh rejoin records release only for copies from the immediately previous session (`PreviousSession`, matching the recorded old session), even without a retained release record; older sessions remain unresolved. Pre-v3 persisted queue state is refused and never contributes inferred release evidence.

| State at cancellation | Acknowledgement |
| --- | --- |
| queued | Atomically `cancelled`; `will_not_run: true`, release follows the all-attempt rule above. |
| preparing / running / stopping | Persist cancel intent and issue stop. `will_not_run: true` immediately prevents retry/requeue. `release: "pending"` until proven, then `confirmed`; charges remain meanwhile. |
| terminal: succeeded / failed / cancelled | State/results unchanged; `will_not_run: true`, release as recorded. Consumers decide not to consume results. |
| removed, retained tombstone | `state: "removed"`, retained ID/key, `will_not_run: true`, release as recorded. |
| never-seen key | Atomically create cancelled reservation; exact project/key echoed, `job_id: null`, `state: "absent"`, `will_not_run: true`, `release: "not_applicable"`. |
| expired key, digest marker only | `job_id: null`, `state: "unknown"`, `will_not_run: true`; historical execution/release coverage is unknown, so `release: null`. Consumer coverage remains unresolved. |
| unknown ID, no retained tombstone | HTTP 404 `job_unknown`, requested ID, `state: "unknown"`, `project: null`, `submission_key: null`, `will_not_run: null`, `release: null`, no acknowledgement (`acknowledged_at: null`). Keep the obligation open. |

The [cancellation acknowledgement schema](schemas/cancel-ack-v1.schema.json) covers successful replies; the [HTTP error body schema](schemas/error-v1.schema.json) covers 404 `job_unknown`, 400 `submission_key_invalid`, 409 `submission_key_conflict` and 410 `submission_key_expired`. HTTP status is outside the body. The 404 body contains `code` and sanitized `error` plus the non-acknowledging nullable cancellation fields above; 400/409/410 contain `code` and sanitized `error`. All ack fields remain present, including null release for expired coverage. Examples under `docs/schemas/examples/` cover every acknowledgement row and each error.

Reservations, tombstones and markers follow the retention table. Accept a key reservation only when the exact project/key is echoed. During queue outage, preserve exact local attempt/release evidence and pending cancellation intents; send them through the API on reconnect. Queue maintenance cannot substitute for cancellation acknowledgements.

## Upgrade and migration (#80)

v3 is the only runtime; no old worker-report, capability/profile, empty-attempt or broad host-cleared fallback. Queue and workers must upgrade together. Old or missing worker/report protocol headers, including checkpoint reports and worker events, receive HTTP 426. Exact execution ownership and release evidence remain mandatory across restart/loss; a new report never silently clears omitted obligations.

**Old formats are refused; operators archive and install fresh.** Old queue state is not imported. Stop the old services and prevent their restart, archive their queue/worker data directories and shared host-control directory, preserve configuration and secret references, then install matching current binaries and units. All current queue, worker and cache state roots carry `.myce-format` with `v3` and a newline. One startup gate checks every root before any filesystem write: absent or empty roots receive a fsynced marker first; non-empty roots without that exact marker return `legacy_state_present` naming the root. No content-specific legacy detection or migration of pre-marker development state is supported. There is no in-place or offline converter and no dual runtime decoder. Preserve archives, checkpoints and unique evidence; this rule does not authorize deleting them or changing production outside the queue.

Installation overlays explicit options on saved current configuration, preserving
queue address, NAS, names, Docker endpoint, GPU environment and secret references.
Matching binaries and service definitions replace each selected installation.
There is no local start condition, stage-only host gate or timer installation.
Linux workers start as ordinary-account services with `Restart=on-failure`,
`Delegate=memory`, `KillMode=control-group` and bounded stop. Installer rollback
restores files, modes and actual manager registration/activity, and reports any
incomplete restoration. Use queue maintenance to keep admission disabled across
an upgrade. Saved configurations containing removed local-control fields must
be rendered in the current schema; preserve the old config and secret references.

## Testing

Keep laptop tests for queue policy across both device workers, Started/cancel
races, queue and worker restart, stale/missing reports, unknown release custody,
RAM/debt and sidecar limits, exact Docker stop/recovery/result, files and privacy.
Use injected clocks for queue decisions. `myce dev` runs the real API and command
runner with fake GPU inventory; standalone e2e checks a command effect, exact
release, queue maintenance and complete process/listener teardown.

Linux daemon, GPU, memory-enforcement, manager and reboot checks are separately
authorized native acceptance; laptop fakes do not qualify hardware. See
[component probes](guide-component-probes.md) and [native checks](production-test-plan.md).

## Scope and later work

Supported v3 features include plain batch commands, queue-managed model calls and HTTP servers, software environments, production pins, fair sharing, enforced memory ceilings and opt-in managed Docker commands. Capability profiles, authenticated discovery/joining, qualified extensions, polite daily-use participation, optional measured placement and verified file transport are also implemented. An extension describes a kind of work or program; qualification is tied to its exact version and a matching native baseline. Hardware facts or benchmark timing alone do not qualify it. Unknown required participation signals withhold work; unsupported hardware is reported rather than silently using the CPU.

Explicit NAS roots remain supported for models, shared files and checkpoints. The queue file store also supports jobs without a NAS. Ordinary commands and servers do not need to become extensions. These are supported v3 paths, not a second runtime or a reason to restore older worker/report protocols.

CPU-only scheduling is approved direction under #98, with implementation deferred to its resource slices; multi-machine/gang work and vision expansion remain deferred under #17, #22 and #69. Automatic dependency detection for arbitrary projects, automatic memory estimation and a backup queue service also wait. Jobs must name their software recipe or lock file and declare their needs.

## Product decisions

1. **GPU work only.** CPU callers, file watching and publication stay outside the queue.
2. **The queue owns GPU work.** Apps submit commands or model calls instead of keeping their own GPU processes between jobs. An unpinned idle model is unloaded after its configured interval (default 10 minutes), or when higher-priority work needs the memory. Explicit production pins are the reservation mechanism.
3. **Progress can move.** NAS checkpoints or verified file-store checkpoint generations allow a resumable job to continue on another compatible machine. Outputs must be safe to repeat; attempt-specific results reject stale executions.
4. **One local network, one queue.** No peer network, leases, leader election or replicated registry.
5. **Plain commands by default.** Software must exist or be built on the worker. Managed Docker containment is an explicit supported Linux command option, not a container scheduler.

## Models: NAS masters, local cache, and production pins (maintainer, 2026-10-09)

The NAS holds the master copy of every model file. `myce model fetch` explicitly
downloads an approved Hugging Face HTTPS file into a NAS folder, verifying its
expected byte count and SHA-256. Loading never fetches from the internet.
The catalog `path` names the NAS master: preferably relative to each worker's
`--model-root`, or an absolute NAS mount path when all workers share that path.
Real models require `size_bytes` and `sha256`. Keep immutable masters and update
the catalog when changing weights.

Each worker has a local `--model-cache` directory (default under its state
folder) and `--model-cache-bytes` limit (default 100 GiB). First use checks local
free space and cache capacity, moves least-recently-used unloaded copies to
Trash, writes a `.partial` file, verifies size and SHA-256, syncs, and renames.
Loaded/loading/draining models are protected from cache eviction. Restart retains
interrupted copies and resumes from their byte offset after rehashing the prefix. With `--model-root`, Trash lives under the NAS root;
cross-filesystem eviction saves and verifies a recovery copy before finishing
the move. Without a model root, user Trash is used; it may not release local
blocks, in which case the load refuses clearly. Never empty Trash automatically.
Worker check-ins report verified cached model names, available cache bytes (the lesser of
filesystem free bytes and remaining cache allowance), and errors. Status exposes
those reports. Copy and verification time are part of model load time. Large
models always load from local disk. `direct_nas: true` is an explicit per-model
exception intended for small models; verify it before reading the NAS master.

`myce model pin MODEL --project PROJECT --machine MACHINE` holds a production
model on one worker. `--platform cuda|metal|xpu` instead chooses one eligible
worker of that platform, retaining the assignment while healthy. Pins are stored
in SQLite and shown in status. They reserve the model's GPU memory and prevent
idle unloading, priority reclamation, and disk-cache eviction while loaded.
`myce model unpin MODEL --project PROJECT` releases protection. Optional
`--weekdays 1,2,3,4,5 --start 08:00 --end 20:00 --timezone America/New_York`
implements reservations now. Windows are start-inclusive/end-exclusive and must
not cross midnight. Initial pin warmup and unavailable hardware remain visible;
a ready pin serves calls without a model load wait. A pin cannot promise
availability while its machine or NAS is down.

### Pre-copying (maintainer, 2026-10-09)

`myce model sync MODEL --to machine|platform|all` persists a request to copy NAS
masters before use; `--cancel` pauses that target request without discarding
partial bytes. Pins warm/protect every possible landing worker's disk cache,
including before a scheduled window. Idle workers pre-copy queued and recently
used models, most-needed first. Real jobs, engine loads and model calls pause
background copies. One copy per worker is the default, configurable with
`--model-copy-parallel`. Cache limits include partials and full active copy
reservations; background eviction never displaces pinned, loaded, active or
more-used models. Final size and SHA-256 verification precede atomic publication.
Status reports progress, errors and measured achieved copy speed. Placement
estimates remaining NAS bytes divided by that worker's measured speed (40 MB/s
until measured), prefers verified cached models, and keeps engine startup time
separate from this transfer estimate. Everything remains laptop-testable.

## Fair sharing, usage history, and status (maintainer, 2026-10-09)

Within each owner-configured tier, competing runnable projects share capacity
by configurable weight (equal by default), using recent usage with a 24-hour
half-life. Select the greatest fair-share deficit before affinity/wait tie-breaks;
equal usage favors fewer active allocations per weight. A sole project can use
every compatible worker. Non-fitting work does not block useful backfill.
Stable tier/resource-family/project accounting persists across restart. GPU work
uses normalized allocated GPU-seconds, including batch preparation and model
loads charged once to their owner. CPU core-seconds remain a separate family for
the later CPU admission slice. Optional all-class project concurrency caps live
in the revision-CAS policy; legacy experiment concurrency and GPU memory-share
caps retain their scope. Status and the simulator use the dispatch planner.
This is admission fairness, with no same-tier interruption or bounded-wait
promise for indefinite allocations. Strict higher tiers may starve lower ones.

The same SQLite queue keeps minute-level device/project/job/model allocation
samples for seven days, then hourly totals through 90 days. Busy allocation,
idle time, memory, pins, preemptions and production start latency distinguish
active pool use from reservations and unknown downtime. `myce usage` reports
these totals. A self-contained responsive status page shows the fleet, queue,
seven-day SVG charts and problems. See [the guide](guide-fairness-usage.md) for
configuration, accounting definitions and known measurement limits.

## Docker command containment (maintainer, 2026-10-11)

Opt-in `run.docker` contains a supported,
direct Docker batch command. One worker-owned create/start request, immutable
attempt labels and durable cleanup custody keep daemon-owned containers within
the existing worker/SQLite accounting contract. Unconfirmed release retains
the physical host's exclusion across loss/restart. Plain commands remain the
default; no container scheduler, peer network or extra service is introduced.
The exact supported subset and limits are in [job reference](job-reference.md#managed-docker-batch-commands)
and [operations](operations.md#docker-cleanup-custody).

## Per-attempt RAM ceilings

Declared batch/model host RAM is enforced on Linux cgroup v2 workers with
systemd memory delegation. Each execution remains inside its worker service's
cgroup, so ordinary service stop still contains every native descendant.
Workers report unsupported enforcement plainly and keep laptop admission-only
execution. An optional physical-host budget caps the sum of declared ceilings,
including configurable slack, and retains unconfirmed allocations after loss.
Managed Docker batches install and inspect daemon-side memory limits. OOM is a
normal failure rather than infrastructure loss. The configuration, unsupported
cases and qualification limits are in [operations](operations.md#host-ram-admission-and-ceilings).

## Files with or without a NAS

The queue can store SHA-256-identified files and
serve resumable HTTP transfers, with bounded caches on execution hyphae. Inputs
and models are explicit; outputs and checkpoint generations are published only
after verified durable transfer. Checkpoint commands still use
`MYCE_CHECKPOINT_DIR`; local-only progress cannot resume elsewhere. The single
SQLite queue, plain commands, injected scheduling clock and attempt ownership
remain. Explicit NAS configuration remains supported. One store is not a backup.
See [files without a NAS](guide-files-v3.md) for configuration and retention limits.

## Shared native server entries (#68)

Server entries may set `project` and `allowed_machines`. Models stay shared
across projects; owner and caller placement policies intersect. Optional `env`
contains non-secret literals; `secret_env` references worker-local named secrets,
read only at launch from private mode-0600 files, with `logs: none` required.
An explicit per-folder/per-host `local_source` approval permits in-place model
use, including HF blob symlinks within the approved root. Fingerprint once,
check metadata subsequently, and require explicit reapproval after changes.
Cheap approval records are checked before directory walks, and completed results
survive sibling timeouts. Unknown blocks new loads/pre-copy and preserves ready
routing for at most five continuous minutes under the queue Clock; definite reports
reset that timer. Expiry, verified changes and revocation drain with custody until
unload acknowledgement. Mid-load draining on unknown is accepted. Check-ins carry
the approval state; all workers must use the current report/start protocol. Missing required approval evidence blocks new loads; no older-worker fallback supplies it.
This opt-in source is an exception to the normal verified-copy contract;
ordinary copied-model custody is unchanged. Authenticated header-less `/v1`
calls default to the entry owner and `experiment`; a per-project production
default requires explicit operator opt-in by the effective caller project, even
when calling another owner's entry. Existing entries retain defaults.
See [operator configuration](guide-servers.md#shared-entries-local-secrets-and-approved-model-folders).

## Log opt-out rollout (#68 review decision)

Workers advertise `logs_none` in `capabilities`; the queue assigns
`logs: none` batches and server entries only to those workers. Waiting status
names the missing capability. Environment build output remains in environment
records, without per-job copies or job-log fallback text for opted-out jobs.
Roll queue and workers together; do not downgrade with opted-out work live.
A queue-first upgrade drains live opted-out servers until their workers upgrade.
See [log policy and downgrade behavior](job-reference.md#scheduling-and-results)
and the [native proof checklist](guide-servers.md).


## Extension composition (E1, #98)

An extension wraps a project's existing setup and commands. Applications keep
that setup and use the queue's submit/wait or model endpoint seam. Mycelium
provides execution, environment and placement mechanisms, not application
packages or project-specific rewrites.

Jobs may select `extension: name@version` and optionally `extension_digest`, a
lowercase SHA-256 of the frozen folder. Digest-only selection is also accepted.
The name/version and digest must agree. A bare name is accepted when exactly
one version is registered; otherwise the caller must pin a version or digest.
First admission persists the resolved name/version, digest and extension
snapshot in the existing SQLite job. Submission-key replay occurs before
resolution, retaining that snapshot and class after registry changes or restart.
An unknown explicit reference is rejected. Legacy kind/program routing retains
its existing wait-for-registration behavior. Registry and job snapshots own
their bytes; caller mutations cannot change registered content.

An explicitly selected extension supplies `default_class` when the caller
omits both class and legacy priority. The caller still supplies project, inputs,
arguments, allowed machines and optional size hints. It cannot override
extension-owned commands, environments, workdir, env, or resource floors.

An extension may retain its old `platforms`, memory floors, `run: [argv...]`,
setup/check and baselines, or declare complete `variants`. A variant has an
exact `target: {os, arch, platform}` (Go OS/architecture names and the existing
cuda/xpu/metal/fake backend), named `environments`, `needs`, `execution`, and
its own setup/check and baselines. Targets are unique. No field inheritance,
recipe merging, backend fallback or implicit OS emulation occurs. Legacy
execution fields cannot accompany variants. Qualification reports for variants
include the exact target as well as folder digest, work type and fingerprint.
Explicit jobs without kind/program use baseline key `extension:NAME`;
kind/program jobs retain their existing baseline keys. Missing baselines never
qualify. Explicit setup and check remain required on every worker.

```yaml
name: example.stage
version: '1'
purpose: Wrap the existing stage command
default_class: experiment
variants:
  - target: {os: linux, arch: amd64, platform: cuda}
    environments:
      main: {backend: uv, python: '3.12', lock_file: locks/requirements.txt}
    needs: {memory_gb: 4, host_memory_gb: 8}
    execution:
      main:
        environment: main
        command: ['{env.main.python}', '{extension}/adapter.py']
        workdir: '{work}'
    # Supply target-specific check argv and bundled native baselines to qualify.
```

Environment references name a member of that variant's `environments` map.
E1 supports the existing uv requirements or uv project lock snapshots, with
`backend: uv` (also the default), Python version and bundled relative lock
path. A uv project includes both uv.lock and pyproject.toml. Immutable recipe
keys include lock bytes, Python, backend and exact OS/arch/platform, excluding
private mount paths. The existing environment cache, exclusive build locks,
atomic publication and shared execution references remain authoritative.
`backend` leaves room for a later pixi adapter; unsupported backends are rejected
and never executed as uv. E1 adds no solver, driver installer or package catalog.
For variant setup/check, pass `--environment-cache` (and optionally
`--environment-cache-bytes`) to the existing extension CLI. Both qualification
and jobs acquire the same immutable recipe through the existing cache builder.
The existing worker environment preparation may populate its configured cache;
automatic extension distribution/setup/qualification is E2 and is not enabled.

Ordinary jobs may use `execution.main` instead of `run`. Main contains command,
workdir, env and an optional environment reference (an existing catalog name
for an ordinary job). Both forms lower through one adapter into `RunSpec`, the
existing run wire contract and worker runner. Old extension argv becomes a
single main member through this adapter too. Literal substitutions cover
`{extension}`, `{cache}`, `{work}`, `{env.NAME.python}` and `{env.NAME.root}`;
argv and environment values are never shell-interpolated. Existing staged
input files remain available beneath `{work}`. No workflow graph, application
configuration patch language or second extension executor is introduced.
Current admission conservatively uses the maximum declared memory floors
across candidate variants plus caller size hints; target-specific resource
admission is left to the resource slices. This may withhold a smaller host,
but never lowers a reservation below the selected recipe's floor.

Native job groups use `execution.sidecars` or `run.sidecars`: name, command,
optional environment/env, positive finite member host-RAM declaration, optional
port and HTTP readiness path/timeout. An absent port means a plain helper;
zero allocates a loopback port and a positive port reserves that explicit port.
Members inherit main's workdir and environment when no environment is named.
All environments are acquired before payload launch and retained through cleanup.
Member RAM declarations must fit `needs.host_memory_gb`, the whole simultaneous
host reservation; they are not added as separate reservations. D4 accounts for
these bounds but does not enforce child caps. Main member RAM caps remain
rejected until D5. Docker sidecars retain their existing behavior.

The worker records a process-group anchor and gated member intent/PID/birth
before exec, authorizes every member through the shared exact-attempt launch
fence, waits for sidecars in declaration order, then obtains main's Started
acknowledgement. Preparation never renews the queue start window. Heartbeats
continue during readiness and report Started only after main's handoff.
Member exit (including code zero) before main finishes or readiness timeout is
an attempt stage failure; infrastructure stops use the existing cause classifier
and requeue policy. Cancellation during preparation closes subsequent launch
gates. Main receives checkpoint grace while helpers remain available, then the
whole group is killed, reaped and verified empty before completion. Unconfirmed
cleanup retains release custody. Restart reuses the existing attempt group scan.

Ports bind `127.0.0.1`; readiness accepts HTTP 2xx, without proxies or redirects,
and verifies listening-process group ownership (Linux procfs, macOS lsof).
An occupied port or unverifiable listener fails preparation. Loopback is not a
private-network sandbox. Endpoint env and literal substitutions, examples and
platform limits are described in [Native sidecars](native-sidecars.md).
All members share worker-selected GPU masks, receive no `MYCE_TOKEN`, and honor
`logs: none`. CPU admission, multi-GPU vectors, child cgroups, assets/archive
expansion and native Windows execution remain separate slices.
