# Mycelium v3: approved design

Approved design, revised 2026-10-20 by the maintainer decisions in [#80](https://github.com/1kuna/mycelium/issues/80) and the machine-instance decision in [#100](https://github.com/1kuna/mycelium/issues/100). Queue maintenance, keyed submission, cancellation and installer contracts below are implemented. Package M is an approved amendment awaiting implementation and qualification. This is the only product spec; native qualification and deployment remain separate from laptop implementation evidence.

## Why a redesign

v1 tried to do too much at once and never thought about how people would use or test it. It was too heavy to run in production and too hard to test. Mycelium keeps one rule above all others: **every feature must be easy to use, and easy to test on a laptop with no GPU.** If a feature can't meet that bar, it waits.

## What it is

One shared queue for production, research and future projects. The approved extension direction includes CPU and accelerator workloads through the same execution interface. CPU-only commands now declare logical cores and host RAM and may run on GPU-less Linux/macOS workers. Laptop tests use real CPU commands and optional fake GPU inventory. Projects submit jobs. Machines pull the most important job they can run. No machine sits idle because its "own" project is quiet.

## The parts

| Part | Where it runs | What it does |
|---|---|---|
| Queue service | One always-on machine, on its CPU | Holds the jobs in one local database file. Decides which job goes where. Serves a small HTTP API and one status page. |
| Worker | Every participating physical machine | One process reports its GPU devices, CPU/RAM and loaded models; runs exact attempts, sends logs and results. See the M amendment for migration from per-GPU workers. |
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

**Making room.** The highest fitting owner tier is checked on submission and
check-in through the shared planner. Prefer compatible free capacity; otherwise
stop only useful lower-tier work, including non-resumable commands, native
sidecar groups, model calls/loads, pinned engines and environment builds. Idle
unpinned same-tier model cache may be evicted; active same-tier work is never
preempted. Resumable commands receive SIGTERM and ten seconds by default to save;
non-resumable work stops immediately. Deadline demand preserves non-resumable
commands and gives resumable commands the existing 120-second grace.
`--preemption-stop-grace` sets the other priority-stop
grace (`--production-stop-grace` is a compatibility alias). After two planned
preemptions, each resumed batch, build or desired engine gets at least that grace
of runtime before another priority stop. Strict higher tiers may still starve
lower tiers before admission.

Each planned stop carries `priority_preemption`, exact demand/victim identity
and policy match/revision. Stop intent never proves delivery or release. D5's
recorded member facts classify interruption; independent exit/OOM remains a
failure. Cancellation wins over completion, and completion observed before stop delivery
wins over preemption. A checkpoint handler exiting zero after a delivered
priority stop is an interruption, not a completed attempt.
Confirmed interruption requeues the same logical job and submission key without
charging infrastructure retries or losses, including production victims. Resume
cooperative jobs from their last durable checkpoint; restart other jobs in a new
attempt. Stop deadlines authorize kill, and exact cleanup still gates replacement.

Pins remain durable desired state and return when capacity permits; higher tiers
can displace their engines. Streaming calls return a typed `priority_preemption`
interruption (OpenAI SSE error, or `X-Myce-Error-Code` trailer on generic streams)
and are never replayed after output starts. Model routing accepts `X-Myce-Class`
or legacy `X-Myce-Priority`, with conflicting declarations rejected.

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
An already-assigned attempt can be reclaimed before Started when the normal
planner selects useful higher-tier demand. Cached assignments and preparation
offers cannot bypass that stop fence. P2 admission fairness applies across all
classes within a tier. See
[the policy guide](guide-priority-policy.md) for format and API details.

**Optional physical-host takeover.** A queue host policy may opt into whole-machine production takeover, default off. Production selects one eligible physical host; all lower-priority work on that host stops safely, regardless of how much room the request needs. Stopped experiments rejoin the shared queue and resume on any compatible machine, without an infrastructure retry. Production admission waits for all lower-priority cleanup acknowledgements. Unknown cleanup survives loss and restart; elapsed stop grace never proves release. Pins in the same or a higher tier remain protected.

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

Classification uses Mycelium-recorded facts for native, native-group and Docker attempts. Infrastructure evidence is a durably recorded stop or kill delivered to that member before its exit was observed; loss of observation; a Mycelium-side pre-execution or launch refusal/failure (including authorization, exec/spawn or environment preparation infrastructure failure); or Mycelium's own probe proving that an allocated readiness port belongs to a process outside this group when readiness fails. Exit codes, payload stderr, stop intent and signals recorded after exit observation are never infrastructure evidence. Independent member exits, OOM (including an extension job), and readiness timeouts without such evidence fail production immediately without an infrastructure retry. A successful terminal outcome is preserved. Cancellation takes precedence: once `will_not_run: true` is acknowledged, no interruption can requeue the job. Superseded production attempts cannot recover ownership or publish a late result; exact release evidence can still discharge their custody. At most one attempt's result is published. Commands and output writes must still be safe to repeat; external side effects are not exactly-once. Experiments retain their existing retry, checkpoint recovery and planned-stop behavior.

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

v3 is the only runtime; no pre-v3 worker-report, capability/profile, empty-attempt or broad host-cleared fallback. Old or missing worker/report protocol headers, including checkpoint reports and worker events, receive HTTP 426. Within host-launch-v3, current one-attempt workers remain compatible with the M1 keyed wire extension below; upgrade the queue before keyed workers. Exact execution ownership and release evidence remain mandatory across restart/loss; a keyed report never silently clears omitted obligations.

**Pre-v3 formats are refused; operators archive and install fresh.** Pre-marker queue state is not imported. Stop the old services and prevent their restart, archive their queue/worker data directories and shared host-control directory, preserve configuration and secret references, then install matching current binaries and units. All current queue, worker and cache state roots carry `.myce-format` with `v3` and a newline. One startup gate checks every root before any filesystem write: absent or empty roots receive a fsynced marker first; non-empty roots without that exact marker return `legacy_state_present` naming the root. No content-specific legacy detection or migration of pre-marker development state is supported. There is no pre-v3 converter or dual runtime decoder. The separately gated M4 conversion below starts from production's marked v3 state. Preserve archives, checkpoints and unique evidence; this rule does not authorize deleting them or changing production outside the queue.

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

CPU-only scheduling is implemented by R1 (#98); the M amendment below defines one machine worker and concurrent single-device attempts; multi-machine/gang work and vision expansion remain deferred under #17, #22 and #69. Automatic dependency detection for arbitrary projects, automatic memory estimation and a backup queue service also wait. Jobs must name their software recipe or lock file and declare their needs.

## Product decisions

1. **CPU and GPU compute.** CPU commands and accelerator workloads share the queue. Applications retain file watching, submission and publication.
2. **The queue owns GPU work.** Apps submit commands or model calls instead of keeping their own GPU processes between jobs. An unpinned idle model is unloaded after its configured interval (default 10 minutes), or when higher-priority work needs the memory. Explicit production pins are the reservation mechanism.
3. **Progress can move.** NAS checkpoints or verified file-store checkpoint generations allow a resumable job to continue on another compatible machine. Outputs must be safe to repeat; attempt-specific results reject stale executions.
4. **One local network, one queue.** No peer network, leases, leader election or replicated registry.
5. **One machine is one hypha.** A worker owns the machine and selects its components; CPU/RAM capacity is shared once. Existing per-GPU workers transition through package M.
6. **Plain commands by default.** Software must exist or be built on the worker. Managed Docker containment is an explicit supported Linux command option, not a container scheduler.

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
loads charged once to their owner. CPU work uses allocated core-seconds in a separate family, with the same
decay and weighted planner. GPU work also reserves host cores/RAM for admission. Optional all-class project concurrency caps live
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
cuda/xpu/metal/cpu/fake backend), named `environments`, `needs`, `execution`, and
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
Admission evaluates each qualified variant's own CPU/GPU/RAM floors plus
caller size hints and learned needs for that backend. A host is eligible when
one matching variant fits; dispatch reserves that selected variant's resources.

Native job groups use `execution.sidecars` or `run.sidecars`: name, command,
optional environment/env, positive finite member host-RAM declaration, optional
port and HTTP readiness path/timeout. An absent port means a plain helper;
zero allocates a loopback port and a positive port reserves that explicit port.
Members inherit main's workdir and environment when no environment is named.
All environments are acquired before payload launch and retained through cleanup.
Member RAM declarations must fit `needs.host_memory_gb`, the whole simultaneous
host reservation; they are not added as separate reservations. Delegated Linux
cgroup v2 enforces the aggregate parent and each member leaf before exec.
Main RAM is optional (`execution.main.memory_gb` or `run.memory_gb`); when
omitted its leaf uses the parent cap. A separate control leaf contains the
supervisor. Member OOM fails the stage and identifies that member; cleanup
verifies the whole subtree and removes leaves before releasing the parent.
macOS and nondelegated foreground workers remain admission-only; requiring
memory ceilings refuses hosts without delegation. Docker sidecars retain their
existing behavior.

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
`logs: none`. Concurrent attempts, multi-GPU vectors, assets/archive
expansion and native Windows execution remain separate slices.


## One machine, one worker (M0–M4, #100; decision 2026-10-20)

The **physical machine is the hypha**. One worker process owns its GPU device
inventory, logical CPU cores, host RAM and, later, other components. maintainer rejected
one Mycelium instance per GPU. This amendment defines package M; R1, P3 and
D5 are its implementation base. M0 is the approved target, not a claim that
M2–M4 or native qualification have shipped. Until migration, existing
per-GPU installations use the compatibility rules below. This section governs
that transition where older guides describe per-GPU workers.

### Inventory and the shared ledger

A machine has one durable ID (`b70` in examples), equal to its physical
`host_id`, one live worker session and one spool/lock. A second machine worker
for the same host is refused; a reconnect uses the existing session/recovery
fences. Each device inventory entry has a host-local string ID (`"0"`, `"1"`),
platform, hardware identity, informational driver/name fingerprint, runtime
selector, memory mode, total,
reserve, effective budget and measured/free-memory evidence. IDs are persisted
against hardware identity, not silently reassigned when discovery order changes.
Identity is the PCI BDF, Level Zero UUID or CUDA UUID; driver version and
name are informational and never decide whether the hardware identity matches.
For XPU, use the PCI BDF or Level Zero UUID. The worker forces
`ZE_ENABLE_PCI_ID_DEVICE_ORDER=1` before its own discovery and in every launched
member, including engines, helpers and containers. Re-verify the runtime
ordinal-to-identity mapping at every worker start and before each member start;
a mismatch or ambiguity withholds that device with typed reason
`device_identity_mismatch` while the mapping differs. A probe error or empty
startup probe uses `device_probe_unavailable`, withholding accelerators without
stopping CPU work whose select-none visibility is qualified. Re-evaluate every
probe: a matching saved identity restores the device, even after restart; a
persisted reason never outlives its condition. A true replacement at a saved
ordinal stays withheld while it differs. To accept a replacement deliberately,
drain/stop and settle the old worker's custody, preserve its spool, inspect the
new mapping and re-run install/`myce join` with a fresh spool (`--state-dir`) to
record that hardware identity. Requalify before reopening admission. Its old
obligations remain. CUDA already binds by UUID through `DeviceBinding`; preserve
that binding rather than launching against an unverified discovery ordinal.
Mixed-platform inventory is valid. CPU-only hosts have an empty GPU inventory,
not a fake GPU. IDs are canonical nonnegative decimal strings (no leading zero except `"0"`), unique
across the host's accelerator families; runtime ordinals may differ from IDs.

Reuse detection, capability profiles and device probes to enumerate every
supported card. Preserve the XPU hotfix **per device**: fixed mode uses the
operator's `--memory-mode fixed` / `--memory-gb` budget and reserve, including
capability and free-memory reports. It never substitutes host RAM, sums two
cards into one capacity, or overwrites a fixed budget with unreliable live XPU
telemetry. Unknown mandatory facts withhold work on the affected component.
On unified devices (GB10/Metal included), `--memory-gb 0` uses measured memory
for capacity; a nonzero operator allowance caps it, and live telemetry and the
reserve still apply below that cap. Missing capacity/budget uses
`device_budget_unavailable`, never `device_identity_mismatch`.
Shared CPU/RAM facts and participation policy are sampled once per machine.

A worker's comma-separated `--device` allowance restricts inventory to exactly
those cards: unlisted cards are never reported or allocated. Omitted means all
cards. After a successful probe, any unmatched entry reports
`device_selector_unmatched` in status (`inventory_reason`); startup refuses an
allowance with no matching cards. A partial match keeps only the matched cards available and
retains that diagnostic. This machine-wide allowance may contain multiple cards
while each attempt retains its one-card bound.

R1's queue-owned host ledger remains authoritative. Every allocation contains
host cores, host RAM and zero or one selected GPU with its VRAM charge in M.
Assignment reserves these resources atomically in SQLite before delivery;
preparation, Started work, stopping work, model residency and unconfirmed
releases retain their charges. A current attempt and its pending release are
charged once. Local worker checks mirror these charges before launch, but create
no second scheduler or independent allowance. Lowered allowances expose debt
and block new allocation rather than resize running ceilings.

Keep the measured physical-host RAM allowance, owner reserve, per-allocation
slack and live-RAM refusal. Dedicated GPUs charge additional host RAM; unified
memory charges GPU plus additional host RAM once in the shared physical pool.
CPU counts/reserve are applied once, never per card. Native sidecars fit within
the attempt's declared whole-group RAM/cores. Environment/model preparation
keeps R1's bounded host preparation/core hand-back accounting; copy parallelism
and preparation limits are machine-wide. No capacity is refunded by omission,
elapsed grace, an empty inventory or service exit.

### Concurrent execution and containment

The worker holds a keyed collection of supervisors and durable attempt records,
not a singular current job. Every member uses `(machine, session, job, attempt)`;
results, logs, checkpoint commits, member launch, Started, stop delivery and
release remain exact-attempt operations. One slow preparation or result upload
must not block the neighbor's heartbeat, cancellation or control. Bulk I/O stays
outside the queue lock. Spool reopen recovers every attempt and buffered result
before a fresh session admits work; omission never proves release.
One heartbeat covers machine liveness; use the shortest existing loss timeout
among active work: production **5 s**, deadline **20 s**, experiment **60 s**
(the defaults in `queue.go`, including `machineLossTimeout`). Thus an experiment
sharing a machine with production inherits the 5 s machine timeout. One worker
stall past that threshold costs **every active production attempt one
infrastructure retry**; per-device processes would have separated that failure
domain. Accept this shared fate to retain fast production loss detection and
one machine controller, rather than silently lengthen production recovery.
Bound its impact with independent nonblocking heartbeat/control paths, bulk I/O
outside them, and the existing maximum of two infrastructure requeues per
production job. Unknown custody stays charged until exact release; a timeout
never permits overlapping replacements. M3 fake-clock tests cover mixed-tier
loss, all affected counters and the terminal retry bound. Machine loss applies
to every owned attempt separately, retaining each one's cause, retry counter
and unknown release charge. Failure or completion of one attempt alone never
marks the machine or neighbors lost.

M admits at most **one batch attempt per GPU device**. CPU-only attempts may run
alongside it and one another when the shared cores/RAM and existing owner or
`max_jobs` caps fit; absent a cap, resource allowances bound concurrency.
Existing model engines may share a device's remaining memory under their usual
admission/priority rules; a batch slot does not invent exclusive ownership of
all VRAM. This preserves model behavior while making independent batch jobs on
different GPUs the default. An attempt never migrates devices in place.

Reuse `WithModelDevice` and the existing command/group adapter. Construct a
private environment per attempt without changing the worker's process-wide
environment. CUDA receives the chosen UUID in `CUDA_VISIBLE_DEVICES`; XPU
receives its verified physical `ZE_AFFINITY_MASK` and renumbered
`ONEAPI_DEVICE_SELECTOR` (one assigned card is in-job device 0). Metal retains
device 0. Explicitly hide unselected accelerator families, rather than deleting
their selectors and exposing the runtime defaults. For **CPU attempts**, set
`CUDA_VISIBLE_DEVICES` to the empty string and set both `ZE_AFFINITY_MASK` and
`ONEAPI_DEVICE_SELECTOR` to explicit select-none masks for the installed XPU
runtimes. Unset or empty XPU selectors are not evidence of no visible devices.
Qualify those select-none semantics with actual runtime enumeration; inability
to hide a family withholds CPU execution with typed reason
`cpu_accelerator_isolation_unavailable`. This is an M2 correctness fix: today's
`worker.go` / `WithModelDevice` CPU path only hides CUDA; R1 supplies no XPU
select-none masks. Main, native sidecars, helpers and managed Docker members
see the same selected device, or no accelerator for CPU work, and all inherit
the forced PCI ordering above. Caller/extension env cannot widen it, and no
payload inherits `MYCE_TOKEN`. These are cooperative device controls, not a
security sandbox against arbitrary hostile code.

Reuse D5's delegated service subtree: a worker control leaf, a distinct parent
for each attempt, and that parent's control/main/sidecar leaves. Install and
verify aggregate and member ceilings before exec. Attempt kill/OOM/cleanup acts
only on that exact subtree (or exact labeled Docker resources), never on the
worker service or a sibling. Engines have their own execution subtrees. Keep
`Delegate=memory`, `KillMode=control-group` and bounded service stop: stopping the
whole service intentionally contains all descendants. RAM enforcement remains
admission-only on unsupported laptop platforms, and CPU core counts remain
admission reservations, not a newly promised CPU-time limit. A required ceiling
still refuses unsupported hosts.

### Wire protocol and placement

This is the single M1/M2/M3 protocol and placement contract. The existing
`host-launch-v3` endpoints negotiate one opt-in keyed revision,
`attempt_protocol: keyed-attempt-v1`, by request and reply echo. `attempts` is a
collection of unique `(id, attempt, session)` reports or directives from M1
onward, with no wire cardinality bound. Reports carry heartbeat/progress/Started;
directives carry assignment/hold/stop/kill/grace and P3 provenance. Nested
assignments retain [job-v1](schemas/job-v1.schema.json) and match their key and
reporting machine. Stop events add session fencing.

M1 admits at most one attempt per worker as a **queue admission rule**,
including recovered copies, not a collection wire bound. M1 workers reject
replies with multiple assignments; multiple stop/hold decisions are valid. The
local supervisor selects its owned controls from the full collection. Results,
release-only acknowledgements, logs, checkpoint publication and Started retain
their exact identity fields, ownership checks and session fences. Omission never
proves release; exact acknowledgement or the existing immediately previous-session
cleanup rule is required. When a stop names the worker's own session and a key
it does not hold, the worker sends an exact release-only acknowledgement for
that key, including Docker assignments it never received. An exact release-only
acknowledgement advances a native stop to requeue/cancellation immediately; for
Docker, the keyed check-in also requires that key to be absent from the report.
Omission alone never proves container cleanup. Restart retains
saved keys and buffered results until acknowledged.

M2 adds `machine.devices` as an additive field to that same keyed revision,
replaces local singular execution state with keyed collections, and enables
concurrent execution in its assignment/protocol harness. CPU-only inventory is
empty. Device entries are the GPU capacity authority when present; old scalar
GPU/device fields then serve only compatibility diagnostics, never a second
allocation pool. Host CPU/RAM remains on the machine report. Each directive's
nested job records its selected device; controls retain M1's exact attempt key,
not a device-wide stop. The queue keeps M1's one-attempt admission rule through
M2; M3 enables multiple assignments using the existing collection.

No new revision negotiation is needed. The queue's strict request decoder
(`DisallowUnknownFields` in `http.go`) rejects fields it does not understand.
These existing guards fail clearly rather than silently drop inventory or
neighbor controls. An
incompatible peer must preserve custody and request the compatible upgrade;
it must not downgrade automatically. Duplicate keys, mixed legacy/keyed forms,
unknown revisions and invalid sessions fail before mutation under M1's guards.
The current unkeyed worker form remains a bounded migration adapter: unchanged
current workers use it against a new queue. Remove that form once every fleet
host runs machine workers and the rollback window is closed. A keyed worker
requires the revision echo: an old queue's `request_invalid` rejection or missing
echo fails clearly with HTTP 426 `worker_protocol_invalid`, retaining custody
and requiring a queue upgrade. See [worker protocol](current-worker-protocol.md).

M3 targets **(machine, device)** for GPU work and (machine, no device) for CPU
work, with device count at most one. Reuse the existing per-target planner,
P1/P2 order and fairness, P3 preemption and R1's host-release feasibility check;
**vector fitting is deferred to count > 1** in
[#105](https://github.com/1kuna/mycelium/issues/105). Holds, maintenance and the
CPU/RAM ledger already use `hostID` and apply unchanged. Enumerate eligible
single-device targets, keeping the per-device batch slot; prefer compatible
free placement before stops. Use the existing policy/affinity/deadline/wait
ordering, then machine ID and device ID for deterministic ties. Evaluate
affinity/qualification against the selected device, never a neighboring card.

Keep R1's accepted single-sufficient-victim check, greedy selection and reverse
prune unchanged; do not revive the rejected exact subset search. The heuristic
is not always optimal. GPU/slot release contributes only on the selected
device; CPU/RAM victims may be elsewhere on that machine, using the existing
host-keyed ledger. Exclude already-refunded or zero-contribution victims,
include build core hand-back, and precompute host ledger facts once per planner
call. Each candidate must satisfy its one device's capacity and the shared
host allowance before any stop is committed. Pending stops are not free
capacity; replacement waits for exact acknowledgements. Status, simulation,
batch jobs, models and servers use this same placement path.
Fair usage remains allocated device-seconds or CPU core-seconds in the existing
resource families. Grouping devices under one machine never resets history or
turns several allocations into one for fairness or project concurrency caps.

Job-v1 gains only optional additive selection under `needs.gpu`:

```yaml
allowed_machines: [b70]       # optional machine restriction
needs:
  memory_gb: 8                # memory required on the one selected GPU
  host_memory_gb: 12          # additional host RAM, as before
  cpu_cores: 2
  platform: {required: [xpu]}
  gpu:
    count: 1                 # omitted defaults to 1 for GPU work
    devices: ["0"]           # optional allowed device IDs on each candidate host
```

`devices` is a nonempty set of candidate IDs, not a request to allocate every
listed device; omitted means any qualified matching device. IDs are host-local,
so use a machine restriction when a specific physical card matters. Platform,
project policy, job allow-list, extension floors and device selection intersect;
none overrides another. Unknown/offline IDs leave an otherwise valid job queued
with diagnostic code `no_matching_device`; discovery never broadens selection.
An extension caller may narrow this device set without overriding its owned
platform, count or resource floors. The resolved intersection is reserved.
Malformed/duplicate IDs, an empty explicit list, nonpositive/noninteger count or
a GPU selector on CPU-only work return HTTP 400 `gpu_selector_invalid`.

**Count greater than one is rejected in M**, at admission with HTTP 400
`gpu_count_unsupported`, including extension-resolved requests and model/server
configuration selecting multiple cards for one execution. Model/attempt masks
such as `0,1`, including `WithModelDevice` inputs, fail with typed
`gpu_count_unsupported` at config load, before any process starts. A worker's
`--device 0,1` instead offers two independently selectable components; it does
not request a multi-card execution. M4 must preserve that distinction. There is no
partial reservation
or silent reduction. Existing jobs with no `needs.gpu` retain one-device
behavior. One-host multi-GPU requests and vector fitting are deferred to
[#105](https://github.com/1kuna/mycelium/issues/105), which must define collective
launch, topology and multi-device memory semantics before enabling them.
Cross-machine execution remains outside M. CPU/RAM quantity selection already
uses R1's needs fields; NUMA/core pinning and other component selectors wait for their own requirements.

### Reliability, holds and compatibility

Keep the job-v1 endpoints, states, project/submission-key replay, cancellation
and optional field omission rules. Store the original client spec/fingerprint
unchanged; validate new placement separately. `existingSubmission` returns
before new-submission validation, so an existing key replay across migration
returns the same logical job even when its original spec names retired workers.
`machine` is the actual executing machine string (`b70` after migration); never synthesize a per-GPU worker identity in a new
assignment. Add diagnostic `allocated_devices: ["0"]` to the assignment/job and
record that selection durably on every copy/release. It is empty for CPU work.
A client must not derive device identity from the machine string. Regenerate
the job-v1 schema/examples in M3; existing clients need no new required fields.

Keep production takeover/hold keyed by physical host, optional and default off.
When enabled it fences lower-tier work across **all** devices and CPU/RAM on
that host, and waits for the same exact cleanup before production admission.
A free neighboring GPU does not bypass it. Without takeover, fit-based P3
preemption remains normal. Queue drain/disable outranks placement, preparation,
pins, holds and Started; a hold cannot resume a maintained host. No cooldown or
allowance rewrite is introduced.

Cancellation still wins over requeue; independently observed exit/OOM and
completion retain D5 precedence over mere stop intent. Queue-recorded
`planned_host_maintenance` takes precedence over priority preemption for the
same attempt; neither consumes infrastructure retries/losses. Preserve the
first bounded stop deadline, cause provenance and original start-window rules.
For migration/redeploy/reboot, record maintenance **before** stopping workers.
Ordinary leave/loss/restart without that queue-recorded cause retains the
existing infrastructure classification and production limit of **at most two**
requeues. Do not reset counters or conceal independent stage failure. Exact
release gates resource reuse; only the current accepted attempt publishes a
result. Previous-session cleanup can discharge only the exact custody it proves,
not all work ever associated with a machine.

Compatibility choices are deliberately confined to existing interfaces:

- **Retired device names fail clearly.** M4 rewrites queue-side project policy,
  pins, server restrictions and host policies to actual machine IDs plus any
  device restriction; there is no alias table or `aliases` config. Retired
  legacy machine records remain solely for exact late-result rejection and
  release, and name their replacement machine/device for diagnostics. A new
  submission or policy/pin/server update referencing `b70-gpu0` after retirement
  returns HTTP 400 `machine_renamed`, naming the replacement:
  `allowed_machines: [b70]` plus `needs.gpu.devices: ["0"]` for a job (or that
  interface's device selector). Config load reports the same typed reason.
  Do not parse arbitrary `-gpuN` suffixes or resolve retired names as aliases.
  Existing raw specs, history and submission fingerprints stay intact; keyed
  replay takes the early return described above. M4 requires Verso #298 merged
  and **no queued jobs referencing retired names** before conversion.
- **Old per-GPU workers can talk to a new queue.** Current unkeyed v3 workers
  keep one batch attempt each, restricted to their reported device and R1's
  common host ledger. M1 keyed workers also retain one-attempt admission until
  M3. Legacy sibling views may coexist during a queue-first upgrade; a machine
  worker and a legacy worker for the same physical host may not. M4 first
  reconciles and retires every legacy session under maintenance. An unretired
  conflict receives HTTP 409 `machine_worker_conflict`, with no new admission
  or implied release. Pre-v3 workers remain refused. Delete the legacy unkeyed
  worker form once **every fleet host runs machine workers and the rollback
  window has closed**; historical exact-attempt custody remains available.
- **Status has one machine row with a device vector**, host CPU/RAM/debt,
  per-device capacity/health/qualification, active keyed attempts, engine slots
  and outstanding releases. Legacy rows are marked transitional/historical,
  never counted again as capacity after retirement. Lost device evidence cannot
  erase an old charge. `eligible_hosts` remains physical host scope; unresolved
  names remain in `eligible_unknown_machines` as today; known retired names
  fail new intent with `machine_renamed`. Do not sum CPU/RAM once per device or sum unified memory into separate physical pools.
- **Qualification and servers stay device-specific.** Reuse extension digest,
  exact target and hardware/driver/baseline fingerprint, now keyed by machine
  and device. A pass on device 0 does not qualify device 1. Reuse an existing
  report only when its complete identity still matches; otherwise unqualified.
  Environment recipes remain OS/arch/platform keyed, not GPU-index keyed.
  Model caches remain disk artifacts; resident engines, calls, pins, readiness
  and release are keyed by machine/device/model/execution. Existing model
  `device` restrictions narrow the component choice and must agree with it.
  A machine pin selects one eligible device and retains it while healthy;
  a pin's optional device restriction selects the requested card. M4 rewrites
  legacy pins before retiring their machine names. Route to an exact ready slot.
  Retain shared model memory behavior, owner tiers, local-source approvals, secret
  handling, streaming interruption and no replay after output starts. Separate
  device engines may share verified weights on disk, not process custody.

### Installation, checkpoints and rollback (M4)

Fresh installs create one `myce-worker.service`; `myce join` enumerates the
machine in one foreground process. Explicit device flags restrict the offered
inventory rather than create a new hypha. The existing per-device memory,
reserve and selector settings become entries in the saved machine config;
host policy stays host-wide. No automatic GPU driver, service or host changes
are authorized by this spec.

**Deploy gate:** do not deploy the M2 binary to the current split per-GPU fleet
workers before this M4 conversion. The queue refuses another inventory-reporting
machine identity on the same physical host with HTTP 409
`machine_worker_conflict`: "run one machine worker per host; see M4 install
conversion". A queue-first upgrade does not authorize upgrading split workers;
convert their units/config/spools under the maintenance gates below.

Extend the current installer transaction rather than add a migration daemon.
For an existing split install, use a destination `--config MACHINE_CONFIG` and
repeatable `--from-worker-config SOURCE_CONFIG`, with `--dry-run` showing the
complete plan. Sources must describe every known per-GPU worker on that host,
including `myce-worker@gpuN` units. Explicit sources prevent guessing ownership
from a unit-name glob. Unselected known workers block with
`machine_migration_incomplete`; conflicting shared settings block with
`machine_migration_conflict` and named fields. An explicit existing installer
option can resolve a setting; the installer never silently chooses the first
value, adds host budgets, drops a secret reference or overwrites unknown state.

The queue itself runs on **b70**, so the window covers its upgrade and restart
recovery as well as both fleet workers. **Upgrade spark to a machine worker in
the same window**, retaining machine name `spark` and its single CUDA device
bound by UUID. This avoids a permanent legacy fleet dependency, exercises the
new queue's real reconnect path and gives the unkeyed adapter a deletion path.
The singular worker wire form remains bounded to the queue-first interval, but
the actual production binary must be tested separately: 08816cf retries a removed
host-control endpoint with HTTP 404 before check-in. Queue and **all B70 and Spark
workers must cut over in the same window**; old workers cannot provide service on
M. Spark is unavailable from the queue upgrade until its own conversion and new
machine check-in. Prestage both hosts' artifacts, mappings and restore manifests;
after queue migration and offline topology conversion, convert Spark first, then
B70, deferring lengthy qualification until both have reconnected under disabled
maintenance. A failed Spark conversion is an explicit abort point: while intake
is frozen and no new business intent has been accepted, settle custody and restore
the complete pre-window queue/worker snapshot. Unresolved custody blocks restore;
keep admission disabled. Do not start old Spark workers against M as a fallback.
Final acceptance requires Spark's upgraded machine worker. Include Spark work and
consumers in the agreed drain. No rolling service restart is implicitly safe.

1. Require Verso #298 and #106 merged into the staged commit (verify ancestry
   and maintainer-published binary SHA-256), plus a production-operator-agreed
   window. The starting point is **production 57ed4e6 queue/root and 08816cf
   workers**, not integ: it has local host intent/registry but no queue-owned
   maintenance API or host memory budget record. Freeze Verso/other intake.
   On that lineage use its root `host close --local --wait-released` bridge;
   exit 1 with `release_not_confirmed` means re-check status, never release.
   Require confirmed settlement, zero custody, then stop the exact units and
   snapshot binaries (root separately), configs, units, registries and DB.
   Upgrade the stopped queue first and let the M build open/migrate its state.
   Only now use M's queue-owned maintenance commands: disable **b70 and spark**
   with stop and wait for every
   batch/member, preparation, engine, buffered result and release to reconcile.
   Require no queued jobs whose job or resolved placement references a retiring
   name; let clients cancel/resubmit before the window, never rewrite keyed
   specs in place. A missing/stale worker without the exact production discharge
   receipt, or any residual obligation, fails the gate.
   Preserve queue/key/counter state and checkpoint generations. Record source
   IDs, sessions and device identities.
2. Snapshot the queue binary, SQLite state and queue-side config, both workers'
   binaries/config/secret references, environment files, spools and actual unit
   registration/activity through the existing rollback mechanism. Preserve NAS/
   checkpoint/model roots, cache paths/limits, Docker opt-in and endpoint,
   availability/reserve settings, cgroup limits and every XPU hotfix setting.
   Conflicting NAS roots require an explicit canonical root resolving the same
   relative checkpoint names; verify access before changing anything. Select
   **one machine cache root: the existing gpu0 root** and its reviewed limit.
   Leave the other roots untouched for separately authorized later cleanup;
   do not add per-device cache-root overrides or relocate absolute-path
   environments. Existing shared-root locks/publication remain intact.
3. Convert the **queue-side configuration** explicitly: project policy
   `allowed_machines: [b70-gpu0, b70-gpu1, spark]` becomes `[b70, spark]`;
   preserve card-specific intent using the appropriate device restriction.
   Rewrite pins and server `allowed_machines`/`device` restrictions and reconcile
   host policies onto the physical host IDs without summing duplicate CPU/RAM
   budgets. Preview each before/after setting, preserving intersections; any
   fence that cannot be represented equivalently blocks with
   `machine_migration_conflict`, never broadens placement. Already host-keyed
   policies remain unchanged. Reject existing multi-card masks as above.
4. The queue upgrade projects production's persisted closed `host_intents`
   into disabled queue-owned maintenance, preserving the equal per-worker host
   allowance once (disagreeing allowances refuse startup). Exact discharged
   source-session receipts authorize offline conversion of stopped/Gone sources
   without pretending they are fresh. No age-based release or fabricated session
   is allowed. This one-time projection adds no production host-control runtime
   or API to M. Reconnecting legacy workers remain held by M maintenance.
   Stop the M queue for
   offline conversion, then restart against the preserved DB and converted config.
   Verify queue restart recovery: jobs, keys, counters, maintenance, pins,
   checkpoints and all exact custody are preserved; no idle/loss inference
   releases resources or starts work. Verify Spark's discharged old session and
   refusal behavior, then its upgraded worker's reconnect. Failed queue recovery
   or upgraded reconnect is a failed gate, not permission to clear state.
5. Stop/disable the exact source worker units and verify process/daemon cleanup
   on both machines. For b70, **remove `myce-worker@gpu0` and
   `myce-worker@gpu1` from the host/installer's persistent enable set**, as well
   as disabling their systemd registrations; merely stopping/disabling them
   leaves redeploy able to recreate them. Archive source configs/spools
   recoverably; never merge spool files, invent a previous-session chain or
   rename attempt ownership. Retire fully settled legacy sessions in the
   maintained queue, retaining their exact historical records and replacement
   diagnostics. They may acknowledge old custody but never regain admission.
6. Install/enable one machine unit on each host with a fresh spool. Inspect
   effective manager settings; verify b70's full stable two-XPU inventory and
   fixed budgets, Spark's UUID-bound CUDA inventory, and one CPU/RAM allowance
   per host. Confirm Spark's new session reconnects and reconciles without
   duplicate capacity, lost history or counter resets. Keep maintenance active
   through conversion, reboot and redeploy; execution uses the qualification
   phases below. Re-running a completed plan is idempotent; interrupted steps
   resume the saved transaction or restore it, never enable both generations.
   Unknown restoration keeps admission off.
7. Return to ordinary workloads only after the qualification checklist below
   and explicit window acceptance. Implementation or installer success alone
   is not qualification.

Checkpoints do not contain worker topology. Keep logical job IDs, submission
keys, attempt numbers/counters and NAS-relative paths unchanged; resume the
last atomically complete monotonic generation with a new exact attempt. The
same applies to accepted file-store generations and immutable output refs.
No local-only or half-written progress is promoted by migration. A missing
previously accepted checkpoint/root leaves work waiting; it never silently
restarts saved resumable work from an empty directory. First attempts with no
saved progress and deliberately nonresumable jobs start normally.
Old per-GPU attempt identity remains available for late result rejection and
exact release after retirement; retirement never rewrites accepted intent.

Rollback is maintenance on both hosts followed by exact cleanup of **all**
new attempts, then restoration of the snapshotted queue-side config and source
worker files/units, persistent enable sets and verified prior activity,
including Spark's prior worker. Keep the upgraded queue for a topology rollback:
it supports the bounded singular worker wire form, but actual production binaries
may remain contained and unable to check in. Under maintenance, deactivate the machine sessions and reactivate
only recorded source identities, preserving their historical records, all jobs/
keys/counters and checkpoint progress. Record actual reconnect/refusal behavior.
Restoring working production requires the separately gated snapshot abort below.
Never run old and new workers together. A full queue downgrade may use its
pre-window database snapshot only if no subsequently accepted intent
would be lost; otherwise refuse downgrade and retain the compatible queue for
repair. Never open M-mutated state with an older binary or overwrite newer
checkpoints with a snapshot. A rollback that cannot prove cleanup/restoration
is incomplete, leaves admission disabled and preserves recovery artifacts.

### M4 implementation decisions

The conversion uses the installer's existing file transaction and service-manager
snapshot. Private mode-0600 `CONFIG.m4-rollback.json` journals retain original
files and unit registration/activity; they contain credential copies when those
are installer-owned and must stay outside public reports. Referenced secret
files are preserved by reference, never imported into the plan. Source spools,
binaries and all caches remain on disk; source unit/config definitions move to
recoverable transaction backups. In the current installer the persistent enable
set also includes the **production-lineage** `worker-*.json` records in
`~/.local/share/myce` (or the source config's explicit `host_state_dir`). Retire
only records whose config/unit/spool/host/name/queue match the selected sources;
refuse unknown or conflicting records. Journal and restore their exact bytes on
rollback. Remove these records and systemd registrations/config artifacts, and
refuse any unselected split unit. The production root copy's local open/close no
longer controls the converted worker: use M `host disable HOST --stop --wait`,
`host status HOST`, and `host resume HOST`. Retain the old root executable only
for the explicitly gated frozen-snapshot abort, never as M host control. Source units must also pass the existing Mycelium worker-unit
allow-list before any manager action. The gate applies to ordinary redeploys as well as fresh installs.

Queue conversion is an explicit offline `myce install queue --config ...
--machine-map ...` operation under the existing SQLite exclusive lock, using the
injected Clock for freshness. The map names every source and its physical host
and component, including Spark's same-name entry. A durable pending-publication
fence commits with retirement; queue startup refuses partial policy publication.
Retry the identical command to publish the remaining policy files, or restore
through the journal. Neither action restores a database snapshot. Policy files
must match the saved original or converted typed values; unknown edits block
with `machine_migration_conflict`. Projects lack a device restriction, so a
partial-card project allow-list blocks rather than widening to the entire host.
A pin or single-host server allow-list can retain its card through `device`.
Queued jobs resolved through project, pin or server fences also block retirement
with `QueuedRetiredMachinesError` (`machine_migration_queued_jobs`), including
logical job IDs and retiring machine names. Jobs and keyed specs are unchanged.

Rollback reverses the current representable pins, preserving additions, removals
and preemption counters. A new host-wide pin that cannot be expressed on split
workers blocks rollback until the operator explicitly selects a card or removes
it. Unknown policy edits similarly require reconciliation before restoration.
Rollback **refuses**, rather than rewriting keyed job intent, while any queued,
failed or cancelled job has a machine-era `allowed_machines` reference that
would disappear (including resumable/requeueable jobs). Succeeded historical
jobs retain their exact identity. Cancellation alone does not remove this gate;
retain the M topology until referencing intent is reconciled explicitly outside
the converter. This is independent of the much
stricter no-new-business-intent gate for restoring a production DB snapshot.
Queue-side rollback reactivates only recorded legacy identities and refuses the
machine generation until forward conversion. Retired records retain their old
sessions for exact result/release handling; a new check-in cannot revive them.
Worker-local catalogs use the queue's explicit retired records to rewrite only
placement fences, retaining local environments/secrets and exact original file
bytes in the private worker journal. Write-ahead SHA-256/mode/removal receipts
bind every publication, including intermediate writes before a crash. Rollback
checks all changed files before any manager mutation; unknown content/mode
returns typed `machine_migration_conflict` with sanitized file/difference names.
Unchanged files, especially a shared catalog already converted by the queue,
are not rewritten. Policy/catalog publication preserves original modes. SQLite
stores only model names, placement restrictions and original/converted hashes;
model environment values and secret references never enter conversion state.
Interrupted plans read source identity from the journal, even after source files
move to recoverable backups. Missing destination units are valid rollback input.
CLI SIGHUP/SIGINT/SIGTERM cancel setup; recovery ignores cancellation within its
bounded restoration context. Dry rollback performs maintenance, file ownership
and spool inspection and prints the file/unit plan without writes; after a real
stop it also acquires the exclusive spool lock before restoring source units.
The same machine spool survives forward -> rollback -> forward. Inspect both
source and reused destination spools: heartbeat and operator-stop state,
`attempt-*.json`, `*.result.json`, `*.docker.json`/ack and `engine-*.json`, using
the sparse formats actually written by production 08816cf as well as M. Retained
results require an exact terminal/released queue record; retained Docker custody
requires its release and acknowledgement. Process/cgroup custody must be absent.
No daemon calls, orphan cleanup or inferred result acknowledgement are made by
this read-only gate. This handles a separate local catalog on Spark as
well as a shared catalog already converted by the queue. During the queue-first
interval only the recorded same-name legacy session may report the saved old
catalog hash while its host is disabled with no obligations; its model endpoint
is withheld. Inventory workers, unknown sessions and resumed hosts still require
the current hash. This exception ends with conversion/rollback and cannot admit
work. Spark uses a fresh spool only after its same-name source completes the normal
leave handshake. No prior-session relationship is invented. The compatible
queue and all subsequently accepted jobs, keys, counters and checkpoints survive
worker rollback. A separate queue downgrade remains subject to the stricter
no-new-intent rule above. In the production-snapshot abort, stop all M writers,
archive post-upgrade synthetic probe evidence, restore the pre-window DB/configs,
local host intent/registries, exact units/enablement and each binary from its own
backup with `sha256sum -c`. Never open an M-mutated DB with 57ed4e6. Upgraded queue
startup failure, incompatible Verso client and failed verification all take this
same abort branch while intake remains stopped and no new Verso/other business
intent was accepted; after that cutoff, keep admission disabled and repair
forward. Restored 08816cf workers are not assumed runnable on the M queue: record
actual check-in refusals and exit behavior during the rollback interval. The
laptop historical-binary drill observes 08816cf retrying a removed host-control
route with HTTP 404 before check-in; direct retired check-ins return
`machine_renamed`. Their
`RestartPreventExitStatus=78` suppresses restart only when the process actually
exits 78; a retrying HTTP refusal is not an exited/safely restored worker.

### Native b70 maintenance-window qualification

Record binary/config digests, exact attempt IDs, masks, measured capacities and
results; keep private operational details in approved private evidence. Stop at
the first failed gate, restore the queue config and worker installations as
above when its gates permit; otherwise remain disabled and repair forward.
Verify restoration before ending the window. Conversion/inspection occurs disabled. During the
agreed window the production operator pauses ordinary submissions using its
existing controls and accounts for queued demand; explicitly resume the queue
only for test phases, then disable/stop/wait again before reboot, redeploy or
rollback. There is no test bypass of maintenance or Started. Final acceptance
reopens ordinary submissions and resumes both hosts. These checks are unrun by M0:

- Confirm Verso #298 merged, the agreed window, both durable host disables,
  fresh evidence, zero obligations and no queued retired-name references.
  Inspect converted queue-side project policies, pins, server restrictions
  and host policies. Verify typed `machine_renamed` guidance for a new job using
  `b70-gpu0`, while an existing keyed replay returns its original job.
- Upgrade/restart the **b70 queue** and verify persisted maintenance, jobs,
  submission keys, retry counters, pins, checkpoints and release custody.
  Observe Spark reconnect before conversion, then upgrade it and require a
  single machine worker/session with UUID-bound CUDA, preserved config and
  successful reconnect/recovery. Test a small Spark GPU job in the test phase.
- Inventory **two XPUs** with distinct PCI BDF/Level Zero UUID mappings,
  `ZE_ENABLE_PCI_ID_DEVICE_ORDER=1` in worker and members, unchanged fixed
  per-device budgets, one b70 worker process/unit and one CPU/RAM allowance.
  Exercise a mapping mismatch and require `device_identity_mismatch` with no
  start on that device; verify the mapping again after restart. Exercise a
  transient and an empty startup probe, then matching-identity recovery, plus a
  driver-version/name update that does not withhold the same hardware. A Level
  Zero probe failure at startup must report `device_probe_unavailable` and allow
  qualified CPU work while withholding accelerators.
- Run two small GPU jobs concurrently, one on each XPU, including a native
  sidecar in each. Inspect actual device visibility/allocation: main and helper
  see only their assigned card as in-job device 0. Test an optional card selector;
  it must not spill onto its neighbor.
- Run a **CPU attempt beside GPU work** and enumerate what its main, native
  sidecar and managed container can actually see through CUDA and XPU runtime
  APIs: require **zero accelerators**, an empty `CUDA_VISIBLE_DEVICES`, effective
  select-none `ZE_AFFINITY_MASK` and `ONEAPI_DEVICE_SELECTOR`, and forced PCI
  ordering. Empty/unset environment checks alone do not pass. Repeat on Spark
  for CUDA; failure withholds CPU work instead of claiming isolation. The
  visibility probes require `python3` and a loadable `libcudart` on CUDA hosts,
  and `sycl-ls` on PATH on XPU hosts. Qualify the installed `sycl-ls` behavior
  under `ONEAPI_DEVICE_SELECTOR=!*:*`, including its exit status and output;
  a nonzero exit is not automatically evidence of zero accelerators.
- Cancel/kill one exact attempt and separately force a member OOM. Require
  whole-group cleanup/release and immediate stage failure for OOM; the neighbor
  continues with unchanged process/device, heartbeat and correct accepted
  result. No host-wide signal, cgroup kill or GPU reset may be used as an
  attempt-local cleanup shortcut.
- Reject over-budget work, count > 1 and multi-card model/attempt masks with
  their typed errors. Verify worker `--device 1` reports/allocates only card 1,
  `--device 0,1` offers both, and an omitted flag reports all cards. Confirm fixed
  XPU accounting under concurrent load,
  per-attempt/member ceilings and no stale result publication.
- Exercise production hold across both devices and CPU work, priority planned
  stops more than twice without retry charges, exact-ack replacement and the
  two-infrastructure-requeue bound. Confirm a machine heartbeat stall uses the
  shortest active-work timeout and charges each affected production attempt.
  Drain/disable must still win, including preparation/Started races and
  independent failure precedence.
- Save a durable checkpoint, stop under maintenance, reboot and verify only
  machine units return. Reconcile queue/worker custody and resume the exact
  logical job from its accepted generation. Redeploy through `myce install`;
  verify queue config, the chosen gpu0 cache root, untouched other caches and
  hotfix budgets. Inspect the persistent enable set and systemd registration:
  neither `@gpu0` nor `@gpu1` may return. Exercise rollback to the two b70 source
  workers and Spark's source worker, then forward again while disabled,
  including queue recovery and Spark reconnect on both paths.
- Recheck production/consumer health and obtain window acceptance before the
  final return to ordinary workloads; record qualification separately for each
  device and the shared RAM envelope.

### Mergeable slices and estimates

M0 may merge alongside M1; implementation order is **M1 → M2 → M3 → M4** on the
isolated after-Verso track. Do not merge into the consumer's production build or
operate hardware until the existing GPU/cutover gates and maintenance window
permit it. The queue keeps M1's one-attempt admission rule through M2; M3
lifts that rule for machine workers and M4 migrates installations. Estimates are
changed/added hand-written production LOC and test/doc LOC, with one engineer's
days including focused validation/review; generated schemas and hardware wait
are excluded. They replace the previous package M estimates.

| Slice | Mergeable outcome | Production LOC; test/doc LOC; days | Acceptance |
| --- | --- | --- | --- |
| M1 (existing owner) | One keyed revision with an `attempts` collection; queue admits one attempt. | Existing M1 brief/estimate; excluded below. | Legacy/new queue compatibility, strict decode and multiple-assignment rejection, session-fenced stop, stale result rejection, exact release-only requeue and retained restart custody. |
| M2 | Additive machine inventory/config; replace current-job/process pointers, spool and recovery with keyed collections; per-attempt device env, stable identities and D5 subtrees. Reuse cache/detection/runner implementations. | 700–1,100; 900–1,400; 5–7 days | Fake two-device inventory plus real CPU helpers execute concurrent harness assignments; stop/member failure leaves neighbor running; select-none CPU masks, UUID/BDF mapping rechecks and mismatch refusal, group masks/token exclusion, parallel readiness/control, aggregate local refusal, every-attempt restart and buffered-result replay. M1 queue admission remains one attempt; compatibility guards reject unsupported inventory/replies clearly. |
| M3 | Enumerate (machine, device) targets using the existing per-target planner and host ledger; per-device engines/qualification, selectors, retired-name errors, job schema and machine status; enable queue concurrency. No vector-fitting planner. | 400–650; 650–1,000; 3–5 days | **`myce dev` multi-device scenario** through the real API/runner: concurrent GPU helpers, CPU backfill, independent cancellation, restart/replay and complete process/listener teardown. Fake-clock heterogeneous targets, same-card exclusion, shared RAM/unified debt, R1 victim usefulness/build hand-back and deterministic heuristic; new retired-name intent fails without changing keyed replay; count > 1 rejection; holds/maintenance, >2 planned stops, mixed-tier shortest loss timeout and bounded per-job infrastructure requeue, stale/missing reports and exact cleanup across restart. Check-in benchmark remains within R1's accepted 1.5× baseline gate on its scenario set. |
| M4 | Transactional queue/config and worker conversion on b70 and spark, enable-set removal, rollback and operator docs; laptop migration first, separately accepted native window. | 250–450; 450–700; 3–4 days plus native window | Fake service-manager/filesystem tests for queue upgrade/restart, split→machine, Spark reconnect/upgrade, conflict/missing-source/queued-retired-name refusal, preserved roots/secrets/hotfix flags, one gpu0 cache root with other roots untouched, enable-set removal, injected failure at each mutation, rollback and idempotent redeploy. Native checklist above before hardware acceptance. |

#### M2 implementation decisions

- The one locked machine spool indexes independent exact-attempt state files.
  Each uses the existing worker state and runner; reopen also scans those files
  so a crash between child persistence and index persistence retains custody.
  The old single-attempt state is adopted in place before new admission. Buffered
  results keep their session until every recovered attempt is acknowledged.
  Only singular legacy adoption may fill an absent assignment session from its
  saved spool session; nonempty mismatches and keyed-file mismatches fail closed.
- Canonical IDs follow saved hardware identities across enumeration order changes.
  A changed XPU ordinal or different hardware identity withholds that saved
  component with `device_identity_mismatch`; discovery never substitutes another
  card for it. Probe errors use `device_probe_unavailable`; both conditions are
  re-evaluated on every probe, and matching identity restores admission. Driver
  versions and names are informational. The native probe uses NVIDIA UUID inventory and the
  installed Level Zero API through Python's standard-library `ctypes`, with PCI
  ordering forced in the probe and all members. Native qualification remains an
  M4 gate; an unavailable or ambiguous runtime probe cannot authorize a launch.
- CPU and unselected XPU controls are `ZE_AFFINITY_MASK=2147483647` and
  `ONEAPI_DEVICE_SELECTOR=!*:*`; CUDA's select-none mask is the empty string.
  Native CPU admission requires actual installed-runtime enumeration with these
  masks. Metal has no qualified select-none control here, so native CPU work on
  a Metal host is withheld with `cpu_accelerator_isolation_unavailable`.
  `myce dev` and the protocol harness inject fake runtime visibility evidence,
  alongside fake inventory, and still run real CPU payloads.
- Until M3 enumerates placement targets, an inventory report projects one
  component into the existing planner. Scalar GPU fields add no capacity. The
  worker harness supplies explicit `allocated_devices` for concurrent jobs;
  the real queue continues to admit one attempt per worker.
- The queue refuses a second inventory-bearing machine identity on a live
  physical host. The existing `myce dev` clock retains its synthetic shared-host
  fixtures; this exception does not apply to an ordinary queue. Empty CPU
  inventories remain explicit through JSON cloning and reporting.
- The default single-card fake runtime treats its old empty selector and
  canonical inventory ID `"0"` identically for extension qualification. Native
  hardware identity checks remain required before launch.

#### M3 implementation decisions

- A check-in reconciles every exact attempt and offers at most one additional
  assignment. Subsequent check-ins fill other eligible components through the
  same planner; the collection and active-attempt count have no cardinality cap.
  CPU targets have no accelerator batch slot and share the host CPU/RAM ledger.
- Historical machine records carry explicit `replacement: {machine, device}`
  migration guidance. M4 creates that metadata transactionally. M3 refuses new
  intent naming those records and never infers retirement from a name suffix.
- Device entries carry their extension fingerprint and reports. The machine's
  `cpu_extension_fingerprint` identifies CPU qualification; the existing scalar
  reports retain the first-device compatibility view alongside CPU reports.
  Residency and pin assignment use the actual machine plus selected device.
- Unknown or unavailable explicit device selections expose the additive job
  diagnostic `diagnostic_code: no_matching_device`. The original submission
  fingerprint remains unchanged. Stop events return an `events` collection
  with an aggregate cursor, retaining the first event's fields for the bounded
  legacy adapter. Each exact event carries priority-stop provenance before
  signal delivery, so a checkpoint handler that exits zero after TERM remains
  an interrupted attempt even when the event precedes its heartbeat directive.
- Returning collection attempts recover independently against their selected
  slots and the existing retained CPU/RAM charge. A neighboring device cannot
  invoke the legacy scalar cap; conflicting slots or allowance debt still
  refuse recovery. Model copies use the shared verified cache, with eligibility
  checked against every component and one copy offer per model. A pending model
  load waits while its component is withheld by a probe, within the existing
  queue load deadline. A healthy offer snapshots that component before the
  asynchronous load; the normal fresh identity check still gates launch.
- A recovered legacy GPU copy without component evidence retains its original
  singular recovery guard and excludes all GPU targets until exact cleanup;
  no device identity is invented. Check-ins returning immediate cleanup for a
  reported attempt offer no new batch assignment, allowing startup supervisors
  to drain recovered custody and complete their existing fresh-session handoff.
- Startup check-ins explicitly set additive `recovering: true` until every
  saved attempt and buffered result is acknowledged and the existing fresh-session
  handoff completes (#106). Reports alone cannot distinguish startup recovery
  from ordinary concurrent work. Reconcile exact custody and stop/kill controls,
  including owned kill-phase attempts, but offer no batch assignments (including
  redelivery), preparation or model work. Recovery refuses admission **before**
  allocating an attempt: no attempt number, copy history, loss or infrastructure
  retry is consumed. Do not refund/reuse offered identities; stale execution
  fences remain monotonic. The worker rejects an assignment in a recovery reply
  as a protocol error, retaining custody, rather than reporting an interrupted
  execution. Actual launch/infrastructure failures keep their existing classification.
  The queue persists `recovering` and queue-observed `recovering_since` from
  each check-in; every device and CPU target of that machine is admission-blocked
  in placement, preemption planning and launch authorization. Other machines
  remain eligible, so production can preempt there. Refusing new admission must
  not hold existing supervisors' buffered-result replay. Status displays recovery
  and its start time, with `recovery_needs_attention` after **two minutes** of
  continuous recovery (the ordinary cleanup escalation interval). Heartbeats do
  not reset that timer. A check-in clearing recovery clears the timer and warning
  automatically after the worker finishes draining.
  **Deploy the #106 queue before workers.** A new worker receiving an assignment
  during recovery from a pre-#106 queue fails closed with HTTP 426
  `worker_protocol_invalid`, tells the operator to upgrade the queue first,
  and retains saved custody. A queue lacking keyed-attempt-v1 also fails closed
  with that upgrade instruction.
- For machine workers, the planner credits lower-tier stopping attempts in both
  grace and kill phases as soon-free capacity (#106), but only **before
  `StopAt + preemptionGrace()`** (ten seconds after `StopAt` by default).
  Within that window prefer the pending release over a new stop on another
  device. At the bound, stop crediting a wedged kill and preempt useful capacity
  elsewhere, on the same or another machine, within production's start window.
  A confirmed exit retains planning credit until fresh inventory arrives or the
  same absolute bound, whichever comes first. Neither completion nor heartbeats
  extend it. This is only a release projection: device/CPU/RAM charges
  and exact cleanup acknowledgements still gate replacement admission. Ordinary
  non-recovering workers may continue backfilling unaffected capacity. The bounded
  legacy adapter retains its existing fallback to another worker after stop
  grace, including expiry of acknowledgement credit.

M2–M4 total: **1,350–2,200 production LOC; 2,000–3,100 test/doc LOC;
11–16 engineer days**, excluding M1 and native access delays. M2 grows from
3–4 to 5–7 days because singular execution pointers, durable spool formats and
restart/result custody all become collections; changing only the launcher is
insufficient. M3 is 3–5 days: removing aliases and vector fitting reduces design
scope, but per-device engines/qualification, status and end-to-end concurrency
still require integration. M4 is 3–4 days because the queue and Spark conversion
are part of the transaction/window, not an assumed external upgrade.

Every slice keeps `myce dev` laptop-testable with the real API/command runner
and fake multi-GPU inventory;
run focused fake-clock/process tests, build/vet and serialized race checks.
Laptop success cannot establish actual XPU masks, device fault isolation,
cgroup/daemon behavior, reboot or the measured production memory envelope.

Delete the per-GPU **worker identity** and service-instance creation path after
migration: duplicate host participation/CPU/RAM reports, singular current-job
and process pointers, global device env, per-GPU worker model-slot keys and
per-GPU maintenance fan-out for new machines. Replace them with the inventory,
keyed supervisors and existing host policy. Keep only the bounded legacy wire
adapter until every fleet host runs machine workers and the rollback window
has closed, then delete the unkeyed worker form. Retain historical exact-attempt
custody. Add no new peer registry, lease, local host-control daemon, cache
installer, application-specific scheduler or second victim planner.

Open delivery risks are actual multi-XPU enumeration/stable mapping, device-local
native isolation and driver failures that can affect the whole host, stale
qualification/approval fingerprints, installer source-config conflicts, and
planner check-in cost. Keep pre-existing planner defects tracked in #104
separate; their feasibility/preparation repairs must land before M3 enables
concurrency. Enumerating device targets must not conceal them. CPU core
reservations do not enforce CPU time. Same-tier indefinite work still has no bounded-wait
promise, and multi-device attempts remain deferred to #105. Current state:
M0 specifies the target; M1 has its own implementation/review owner. Next step:
orchestrator review/merge of the revised M0 and M1, then M2's fake-device gate.
The M4 window remains blocked on Verso #298 and its explicit acceptance gates.

## CPU and shared host resources (R1, #98/#22)

A plain CPU job declares `needs: {cpu_cores: 2, memory_gb: 4}` without GPU
selectors. Here memory is host RAM in GiB. `platform: cpu` also selects CPU
execution, defaulting to one logical core when `cpu_cores` is omitted. Admission
normalizes CPU work to `platform: {required: [cpu]}`, zero GPU `memory_gb` and
positive `host_memory_gb`; stored/replayed jobs use that unambiguous vector.
GPU declarations retain GPU `memory_gb` and additional `host_memory_gb`, with
`cpu_cores` defaulting to one. Native sidecars share the attempt's declared
cores/RAM instead of adding another reservation. CPU commands receive
`MYCE_DEVICE=cpu`, `MYCE_PLATFORM=cpu`, an empty CUDA mask and no fleet token.
M2 must additionally apply explicit select-none XPU masks to every member as
specified above; R1's removal of inherited XPU selectors does not hide XPUs.
This is cooperative device selection.

Workers offer logical CPUs minus `--reserve-cpu-cores` (default one), capped
by `--cpu-budget-cores` when positive. Zero CPU budget uses detected capacity
minus reserve. CPU and RAM allowances are queue-owned physical-host policy;
`myce host budget HOST --cpu-budget-cores N` changes the CPU allowance while
that host is drained/disabled, beside `--memory-budget-gb`. Worker views must
acknowledge the same policy before admission. Host identity is shared across
CPU/GPU views, so capacity is never multiplied by device or worker count.
Running/stopping allocations, model residency and unconfirmed batch releases
retain their cores; pending release and its current attempt are charged once.
Background preparation uses one core per worker unless that execution already
reserves it. Preparation overhead RAM remains covered by the owner reserve,
following the existing host RAM budget contract.

A GPU-less Linux/macOS host can `myce join --gpu cpu`; automatic enrollment
chooses CPU when no supported GPU is visible. An explicit CPU target requires
no GPU probe. CPU extensions use exact Linux/macOS OS/arch/`cpu` variants and
the existing baseline check/qualification mechanism. Missing CPU counts, OS
support, RAM facts or live RAM readings withhold CPU work with a status reason.
GPU requests default to one core and require known CPU capacity as well;
missing telemetry never supplies invented cores. Upgrade queue and workers
together so every host reports complete capacity.

R1 alone keeps one batch attempt per worker. Package M above owns machine
inventory and concurrent attempts; Windows/WSL qualification remains W1/W2.
CPU counts are admission reservations, not CPU-time enforcement; macOS RAM remains admission-only and delegated Linux
RAM enforcement retains its existing qualification requirements. Real native
Linux/Windows acceptance and fleet cutover are separate from laptop proof.


## One-command enrollment: B0/B1/B6 (#108, 2026-10-21)

One mycelium is the cluster; a hypha is a machine and reports all its components.
The existing `myce invite` and `myce install worker --enroll` paths provide
bootstrap and enrollment. By default invite prints exactly one paste-ready line
to stdout; `--out FILE` also saves it privately. Foreground `join` remains the worker runtime.
NAS mode is required by #109. B2/B3/B4 own hardware preflight, NAS/tools and saved
service/policy fields; this slice passes their ordinary installer options through
unchanged. B5's automatic legacy conversion and B7/B8 qualification remain separate.

Releases contain linux/amd64, linux/arm64 and native darwin/arm64+cgo binaries.
The publisher signs an exact JSON manifest with Ed25519. The queue verifies it
against an independently provisioned public key, never a key from the manifest,
then supplies approved public release metadata to pinned authenticated inviters.
Explicit local manifest/key/origin overrides perform the same verification.
Release versions are ordered semver values (for example `v3.1.0`). Queue approval
is persisted in SQLite and must strictly increase unless `--allow-downgrade`
explicitly re-approves or downgrades it. Semver build metadata has equal precedence.
The verified manifest binds version, protocols, OS/arch, size, SHA-256 and native
Mac sensing. No release tooling existed to reuse. A small POSIX command is needed
because clean hosts have no myce, Go or Git. Its embedded verified hash matrix
selects the artifact and HTTPS origin; curl resumes a private partial download,
refuses redirects, and a platform SHA-256 tool verifies bytes before execution.
Publisher HTTPS plus the signed manifest are separate from the LAN queue's exact
certificate-DER pin. Never confuse that pin with curl's SPKI pinnedpubkey.

Authenticated inviters mint 15-minute tickets (maximum one hour) over pinned TLS.
The existing SQLite stores only ticket digests, expiry, queue pin and consumed
state. Used and expired rows are pruned. One atomic conditional update redeems
exactly once. Tickets authorize only redemption, never the jobs/status API; expiry uses the injected queue Clock. The
fleet credential travels only in the pinned TLS response and mode-0600 installed
joining file, never argv, URLs, history or diagnostics. If a redemption response
is lost before durable saving, mint a new ticket; do not make replay reusable.
Existing private file invitations remain supported with explicit
`--private-file --out FILE` for compatibility. Installed
reruns use their saved joining authority and do not redeem the spent ticket.
The limited ticket enters the installer on stdin, never in its argv or environment.
Bootstrap authority is retained at mode 0600 only until the installed joining file
is durable, then deleted, including on interrupted-install recovery.

Enrollment completion requires the installed spool's exact session, the current
host-launch and keyed-attempt protocols, every expected device identity, CPU/RAM
facts and live memory, policy acknowledgement, fresh queue observations, a small
recorded runner smoke with no fleet token, and stable service-manager health.
A machine's receipt binds its service PID, exact release/session, expected
inventory, CPU count and total RAM; queue-owned CPU/RAM allowances must be
acknowledged.
Saved per-device settings or explicit selector lists independently establish expected
devices. A failed probe retains `device_probe_unavailable`; a successful probe
with a configured but absent device reports `device_selector_unmatched` and
cannot be ready. launchd health reads the service block's direct keys, ignoring
nested resource states. Healthy service identity must remain stable for 30 seconds.
Unchanged receipts are not rewritten; receipt-write failures are logged without
terminating the worker, and retried after the next successful check-in. Registration
alone is incomplete. A healthy daily-use hypha may finish as `polite_waiting`;
an operator-maintained host finishes as `disabled` or `drained`, never `ready`.
Unknown devices, queue outage/426, recovery or a start-loop cannot complete ready.
The summary includes the hypha name, OS/arch, devices, CPU/RAM allowances, storage,
participation and enforcement. This smoke does not qualify project extensions.

Each printed line freezes the approved release and checksums for reproducibility.
Reuse that line for interrupted installation and same-version health checks;
use a line for a newer approved release to upgrade. Installed upgrades use saved
authority without redeeming or requiring another ticket.
Same-version reruns verify health without changing files or restarting services,
and explicitly explain that installer flags are not reapplied. Older releases are
refused unless the operator passes `--allow-downgrade` explicitly; comparison uses
the installed binary's version, not an invitation's age or a stale worker receipt.
Equal versions (including `+build` metadata) require identical bytes, otherwise
`--allow-downgrade` is required. Replacing an installed non-semver version such as
`development` also requires that explicit flag; the published target remains semver.
A changed release refuses busy local hosts before draining through existing queue
maintenance. A race with new custody is bounded: drain-wait failure conditionally
restores its own exact transition. Drain-wait and install/readiness have separate
budgets. The existing installer transaction and spool remain the install path.
Existing disabled/drained policy is preserved; an installer never resumes an
operator-disabled host. A private resume receipt fences a self-made drain by host,
name and policy timestamp. Its durable intent precedes the drain POST; an intent
plus an observed drain on rerun is promoted to the exact timestamp receipt.
Install/readiness failure restores the previous installation through the existing
transaction and verifies its enrolled readiness before resuming only the self-made drain, with a separate cleanup
budget. Errors state whether the previous release resumed or why recovery failed.
A crash loses the in-memory installer snapshot; if already-installed bytes never
become ready on rerun, report rollback unavailable and preserve maintenance.
Operator-created maintenance remains respected. No fleet
operation, reboot, driver install or privileged service is implied. Native fleet acceptance remains a separately authorized test window.
