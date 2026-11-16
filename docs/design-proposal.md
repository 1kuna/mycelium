# Mycelium v3: approved design

This is the approved design and the only product spec. Each topic states the
current design once. The maintainer's rulings are in [decisions](DECISIONS.md)
and are cited here by MY number where they govern. The text was consolidated on
2026-11-12 (MY-49) from its dated amendments without changing what it says.
Approval dates, issue receipts and replaced wording stay in git: see the
[text before consolidation](https://github.com/1kuna/mycelium/blob/e0488c0c5034b5a57827ceebfa69f770c060bb8f/docs/design-proposal.md) and the section map in
[#1433](https://github.com/1kuna/mycelium/pull/1433). Old heading anchors still
resolve here. Mechanism chapters under `reference/` linked below are part of
this approved design; their relocation changes no product contract.

Laptop tests and `myce dev` are implementation evidence. Native hardware
qualification and deployment are separate and are never inferred from them
(MY-10). A passage marked **Conflict** keeps two statements that disagree and
that this consolidation did not resolve.

## Why a redesign

v1 tried to do too much at once and never thought about how people would use or test it. It was too heavy to run in production and too hard to test. Mycelium keeps one rule above all others: **every feature must be easy to use, and easy to test on a laptop with no GPU.** If a feature can't meet that bar, it waits.

## What it is

One shared queue for production, research and future projects. The approved extension direction includes CPU and accelerator workloads through the same execution interface. CPU-only commands now declare logical cores and host RAM and may run on GPU-less Linux/macOS workers. Laptop tests use real CPU commands and optional fake GPU inventory. Projects submit jobs. Machines pull the most important job they can run. No machine sits idle because its "own" project is quiet.

## The parts

| Part | Where it runs | What it does |
|---|---|---|
| Queue service | One always-on machine, on its CPU | Holds the jobs in one local database file. Decides which job goes where. Serves a small HTTP API and one status page. |
| Worker | Every participating physical machine | One process reports its GPU devices, CPU/RAM and loaded models; runs exact attempts, sends logs and results. See [one machine, one worker](#one-machine-one-worker). |
| `myce` command | Anywhere | Submit, watch, cancel and read logs. `myce join` starts a worker on this machine. |

- **Dedicated machines** offer their GPUs whenever available. **Shared machines** can drain or disable queue admission when another owner needs them; that owner controls its own work and checks release before reopening Mycelium.
- **Machines that come and go:** running `myce join` adds a machine; stopping the worker removes it. Daily-use participation can wait for owner inactivity, safe heat and external power.
- All machines are on one local network. Workers use a configured queue address or authenticated joining; discovery finds addresses but does not establish trust. A privately provisioned join credential keeps strangers out.
- Single Go binary, as in v1. There is no peer network, no leader election and no replicated state. If the queue service restarts, queued and running batch jobs survive in its database file, and workers wait and reconnect. Model calls that were in flight are not replayed and fail; callers must retry them.

One mycelium is the cluster; a hypha is one machine, with its CPU, RAM and
accelerators as components (MY-06).

## Product decisions

1. **CPU and GPU compute.** CPU commands and accelerator workloads share the queue. Applications retain file watching, submission and publication.
2. **The queue owns GPU work.** Apps submit commands or model calls instead of keeping their own GPU processes between jobs. An unpinned idle model is unloaded after its configured interval (default 10 minutes), or when higher-priority work needs the memory. Explicit production pins are the reservation mechanism.
3. **Progress can move.** NAS checkpoints or verified file-store checkpoint generations allow a resumable job to continue on another compatible machine. Outputs must be safe to repeat; attempt-specific results reject stale executions.
4. **One local network, one queue.** No peer network, leases, leader election or replicated registry.
5. **One machine is one hypha.** A worker owns the machine and selects its components; CPU/RAM capacity is shared once.
6. **Plain commands.** Software must exist or be built on the worker. Native processes, environments and sidecars are the one runtime; managed Docker containment was retired on 2026-11-12 under [MY-49](DECISIONS.md#my-49), and a request that still asks for it is refused at submission.

## A job

```yaml
name: example-reindex
project: example
class: experiment           # arbitrary job class; legacy priority is accepted
deadline: null              # a time, required when priority is "deadline"
run:
  command: ["python", "reindex.py", "--batch", "7"]
  workdir: '{work}'         # worker-managed attempt directory; stage source as inputs
  env: {}
needs:
  memory_gb: 40
  platform: [cuda]          # any of: cuda, xpu, metal
  network: false            # payload offline by default; true requires protected outbound
  model: qwen3-32b          # optional; used to group jobs that share a model
expected_minutes: 90        # rough guess; used for planning, never enforced
resumable: true             # true = the job saves checkpoints and can be stopped and picked up later
```

The queue gives every job its job id, `MYCE_CHECKPOINT_DIR` and attempt number. With NAS configuration, assignments are NAS-relative and each worker resolves them under its own `--checkpoint-root`. Without a NAS, the file store transports verified checkpoint generations. A resumable job only has to (1) save to the checkpoint folder now and then, (2) save and exit when it gets a stop signal, and (3) pick up from the checkpoint folder when it starts. Only durable, complete progress can resume on another machine; see the resumable-jobs guide.

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
- **Model reservations are implemented now:** project-owned pins can hold a model continuously or within explicit weekday/time-zone windows. See [Models](#models).

## How the queue decides

When a worker asks for work, the queue picks, from the jobs that fit this machine's free memory and GPU type:

1. the highest owner-configured project/class tier first; without a configured policy, production, then deadline, then the catchall (experiment);
2. within that tier, the fitting project with the greatest weighted recent-usage deficit; equal usage prefers fewer active allocations per weight;
3. within that project, a model already loaded, then cached locally, then needing a NAS copy;
4. then the earliest deadline and the job that has waited longest.

**Owner priority tiers.** The durable SQLite policy contains ordered,
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
offers cannot bypass that stop fence. Admission fairness applies across all classes within a tier ([fair sharing](#fair-sharing-usage-history-and-status)). See
[the policy guide](scheduling.md) for format and API details.

**Making room.** The highest fitting owner tier is checked on submission and
check-in through the shared planner. Prefer compatible free capacity across all
eligible hosts; when one fits idle, preempt nothing elsewhere. Otherwise choose
one host and card set before stopping useful lower-tier work, and keep that
placement for the demand through exact release and admission, including a
fan-out parent's next wave. Admission waits for every selected release obligation;
unrelated capacity becoming free does not settle those obligations. A newly idle host
or sibling card does not redirect demand after it has caused a stop. Loss of the
selected worker/session clears the commitment durably and permits replanning
without releasing cleanup custody.
A demand with a selected release precedes uncommitted same-tier demand, so each
waiting batch plans its own target instead of borrowing another demand's release.
This includes non-resumable commands, native
sidecar groups, model calls/loads, pinned engines and environment builds. Idle
unpinned same-tier model cache may be evicted; active same-tier work is preserved except for the qualified [scarce-host yield](#scarce-host-yield) case. Production preempts lower-tier work. Accelerator work, including
non-resumable work, receives SIGTERM with ten seconds of grace by default,
capped at fifteen seconds, before SIGKILL if still running. Resumable CPU commands
receive ten seconds by default to save; non-resumable CPU work stops immediately. Deadline
demand preserves non-resumable commands and gives resumable CPU commands the
existing 120-second grace.
`--preemption-stop-grace` sets the other priority-stop
grace (`--production-stop-grace` is a compatibility alias). After two planned
preemptions, each resumed batch, build or desired engine gets at least that grace
of runtime before another priority stop. Strict higher tiers may still starve
lower tiers before admission.

Host RAM release planning considers signed reservation debt before deciding
that demand cannot fit (MY-56). Existing lower-tier releases may repay that
debt; no stopping attempt counts as free before exact release. Backfill cannot
deepen ready production's RAM/CPU deficit, including across independent cards
on one host. Compatible other hosts remain available.

Each planned stop carries `priority_preemption`, exact demand/victim identity
and policy match/revision. Stop intent never proves delivery or release.
Recorded member facts classify interruption; independent exit/OOM remains a
failure. Cancellation wins over completion, and completion observed before stop
delivery wins over preemption. A checkpoint handler exiting zero after a
delivered priority stop is an interruption, not a completed attempt. Confirmed
interruption requeues the same logical job and submission key without charging
infrastructure retries or losses, including production victims. Resume
cooperative jobs from their last durable checkpoint; restart other jobs in a
new attempt. Stop deadlines authorize kill, and exact cleanup still gates
replacement.

Pins remain durable desired state and return when capacity permits; higher tiers
can displace their engines. Streaming calls return a typed `priority_preemption`
interruption (OpenAI SSE error, or `X-Myce-Error-Code` trailer on generic streams)
and are never replayed after output starts. Model routing accepts `X-Myce-Class`
or legacy `X-Myce-Priority`, with conflicting declarations rejected.

**No machine is reserved (MY-50).** Production priority on a GPU host claims only the GPU: the devices and the memory production needs, through the existing tiers and preemption (MY-30). The host's CPU cores and memory keep serving other work. A host policy may opt into a device-scoped production hold, default off. It keeps exact release custody: production waits for each displaced attempt's cleanup acknowledgement, unknown cleanup survives loss and restart, and elapsed stop grace never proves release. Stopped experiments rejoin the shared queue and resume on any compatible machine, without an infrastructure retry. Pins in the same or a higher tier remain protected. The earlier whole-machine takeover was removed; a policy that still requests it is refused by the policy API, and saved state or a startup file from the previous release converts to device scope with a warning.

There is **no idle production cooldown** after the final demand and cleanup settle. Lower-priority work can run again; new production preempts through the queue. Production priority covers submitted GPU work, not CPU publication. Project machine allow-lists apply at every priority and to model calls; commands and servers receive their placement in worker-owned environment variables.

One measured physical-host memory allowance covers simultaneous production demand, other admitted work, sidecars, launch overhead and headroom. Its numeric value comes from component measurements, not this spec. Keep aggregate accounting and cgroup enforcement. Do not rewrite budgets on every ownership switch: queue maintenance excludes new work while another owner uses the host.

**Data stays local.** Every machine in the fleet is on-premises, so job data never leaves our machines. A job can also list the specific machines it is allowed to run on.

## Fair sharing, usage history and status
<a id="fair-sharing-usage-history-and-status-maintainer-2026-10-09"></a>

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
caps retain their scope. Status uses the dispatch planner.
This is admission fairness, with no same-tier interruption or bounded-wait
promise for indefinite allocations. Strict higher tiers may starve lower ones.

The same SQLite queue keeps minute-level device/project/job/model allocation
samples for seven days, then hourly totals through 90 days. Busy allocation,
idle time, memory, pins, preemptions and production start latency distinguish
active pool use from reservations and unknown downtime. `myce usage` reports
these totals. A self-contained responsive status page shows the fleet, queue,
seven-day SVG charts and problems. See [the guide](scheduling.md) for
configuration, accounting definitions and known measurement limits.

## Least valuable machine that fits
<a id="preserve-scarce-machines-slice-6-193-my-18"></a>

Placement uses the least valuable machine that fits (MY-18), without changing
adapter contracts or introducing an explicit speed/deadline goal. Eligibility,
accuracy/qualification and owner policy precede tiers and per-family project
deficit. Free capacity and existing victim stop-count/lost-work comparisons
precede host value. Value never selects another project, changes fair-share
charges or preempts equal-tier work. [Scarce-host yield](#scarce-host-yield) is
the narrow exception for moving already-running portable work.

Known measured inventory and owner allowances define distinct accelerator backend/
usable-memory bands and host-memory bands above the fleet minimum. Count the
physical hosts supplying each band, not devices or worker rows. Rank hosts
lexicographically by the count of bands supplied by one host, then two, and so
on, fewer first. Break ties on smaller usable host RAM, existing measured
completion/affinity, then stable target identity. Unified RAM remains one pool;
its host and device capacities are never summed. Unknown value gets no invented
ranking; prefer a known fitting alternative and explain an unknown-only choice.

The shared planner applies this ordering to currently admissible targets across
pulls and status. Owner activity, stale reports, maintenance and
other temporary occupancy do not redefine measured supply or value; only
immediate admission excludes those hosts. Compute scarcity once per read-only
planning pass and discard the projection on state changes. Never reserve or
wait for a cheaper host: an idle scarce host runs the work when cheap hosts are
busy, lost, owner-active, recovering or otherwise inadmissible. Attempt and
cleanup custody remain authoritative across restarts. Status exposes the chosen
host's scarcity vector and usable RAM, or why its value is unknown. Fake-clock
inventory evidence establishes laptop behavior, not native fleet
qualification. The explicit speed/deadline tradeoff is later work.

Ready queued accelerator commands also protect their resolved host CPU/RAM
vector against CPU backfill while a finite execution occupies their device.
The existing capacity-accumulation planner chooses one target, leaves excess
capacity usable and releases the fence when demand disappears. This predicts
known queued demand from real executions, with no idle GPU core reservation.
Running work keeps its stop contract and exact release still gates admission.

## Scarce-host yield
<a id="amendment-scarce-host-yield-327-328-my-26"></a>

Scarce machines yield portable work to demand that only they can serve (MY-26).

The shared batch planner may reclaim CPU, host RAM or accelerator capacity from
portable finite work when ready demand has a documented hard requirement and
exactly one structurally capable physical host. `requirement_reasons` records
hard `host`, `device`, `backend:NAME`, `os:NAME` or `arch:NAME` constraints in the
frozen submission. Occupancy, stale/maintained peers, missing recipes or unknown
qualification cannot manufacture uniqueness. Convenience pins and target labels
without a corresponding hard requirement do not prove it. Existing ordinary
higher-tier preemption runs first; this exception does not alter model calls,
services, engine state, streaming output, preparation or fan-out handling.

Victims must be at or below the demand's owner tier. This path never selects
production for non-production demand, including under custom tier ordering.
Only resumable work with a verified positive portable checkpoint generation, or
an explicitly disposable non-resumable experiment with `preemptible: true`, can
yield. No class silently opts in. Both need another qualified, currently
admissible physical host and frozen `resume_compatibility` evidence:

```yaml
resumable: true
resume_compatibility:
  checkpoint_format: counter-v1
  result_contract: counter-result-v1
  targets:
    - target: {os: linux, arch: amd64, platform: cpu}
      environment: none
      result_check: counter-resume-test
```

Exact target entries assert author-tested checkpoint, environment and equivalent
result support; the named result check identifies that evidence. Metadata is
not a substitute for tests or MY-19 parity. `environment` matches the frozen
resolved environment key, `none` for no environment, or `qualified` for native
extensions (which still require the exact frozen variant's ordinary checks).
Unknown, unqualified or incompatible alternatives hold. The schema rejects
incomplete/duplicate targets and invalid opt-ins. The queue never infers support
from CPU availability or labels. Source and destination workers must advertise
`scarce_host_yield_v1` so older runners cannot bypass authorization/final-save
handling.

The existing room planner chooses the minimum useful victims with its stop-count
and lost-work comparisons. Destination projections honor resources, readiness,
owner availability, freshness, maintenance and fair turns, and reserve nothing.
Persist `scarce_host_yield` with demand, victim, exact source attempt/session,
checkpoint generation and proposed destination. Revalidate immediately before
stop authorization, before dispatch and at the launch/Started fences. An
undelivered stop is withdrawn if demand or alternative admission disappears;
authorized stops retain cleanup custody. Completion observed before delivery
wins; stale reports cannot publish, qualify or release a newer attempt.

Reuse existing checkpoint verification/transport outside `q.mu`, stop grace,
process-group/sidecar cleanup and same-job requeue. Exact release is independent
of the final save. Resumable yield requires a verified final generation newer
than the authorization generation and a cooperative, unforced result. Failed
save/transport, forced exit or release alone produces an explicit hold, retaining
every committed generation. Transport retries retain local results; a failed
final save requires repair or cancellation by its owner. Only opted-in disposable
experiments may restart without saved progress. If destination capacity vanishes
after stop, retain verified progress queued with a reason. Exclude the scarce
host only while the recorded demand needs it; cancellation clears this routing
exclusion without cancelling already-authorized cleanup.

Queue and worker restart recover the same intent and exact custody. Keep logical
job ID, submission key, frozen choices and accepted outputs; use existing planned
displacement accounting and the runtime grace after two displacements, spending
no infrastructure or loss retries. There is no migration daemon, new cooldown,
capacity reservation or live process transfer. CPU dev proof uses simulated
inventory and real commands, file-store generations, native sidecars and one
verified accepted artifact; native hardware qualification remains separate.

## Machines leaving and loss

**Machines leaving.** Workers pull jobs, so a job is never tied to a machine until it starts. That means there is no list of waiting jobs per machine to clean up. Leaving works like this:

- **Planned leave** (`myce leave`, or Ctrl-C on the worker). The machine stops taking new jobs at once. Resumable jobs get the stop signal, save to the NAS, and go back to the front of their line. For jobs that can't be resumed, `myce leave` asks whether to wait for them to finish or to stop them now and put them back in the queue. `myce leave --now` skips the question and stops everything.
- **Sudden loss** (unplugged, asleep, network drop): without a check-in, the queue marks the machine gone after its measured check-in allowance has elapsed in control-service opportunity time (see [adaptive worker loss](#adaptive-worker-loss)). Queue delay alone never charges a loss or retry, for any class. Its running jobs go back in the queue. Resumable jobs continue from their last checkpoint. Others start over. Experiments and deadline jobs fail loudly after three counted infrastructure losses; their planned stops to make room or leave do not count. Production uses the bounded infrastructure retry budget below, including planned host release or leave. Model calls that were in flight are retried on another machine, and models loaded there are marked as gone.
- **Late return.** A network split can briefly leave the same job running on two machines. When contact returns, the queue keeps the resumable copy with the newest complete checkpoint, or the current attempt when there is no newer checkpoint, and tells the other copy to stop. Jobs must make output writes safe to repeat.
- **Jobs no remaining machine can run.** These are jobs pinned to the machine that left, or jobs that need a GPU type no current machine has (for example, Apple-only work when every Mac is away). They stay in the queue, and `myce status` says plainly why each one is waiting: "no machine in the fleet can run this right now (needs metal)". A job can set a give-up time, after which it is marked failed instead of waiting forever.

**Power and network loss are normal, not rare.** Every part must survive them without losing durable intent or treating uncertainty as release.

- **The queue machine loses power.** The database is written safely (every change is fully saved to disk before it's confirmed), so a power cut never corrupts it or loses a submitted job. The queue starts automatically when the machine boots. After a restart it does *not* immediately declare every job lost. It waits a few minutes for workers to reconnect and report what they're still running or have finished, and only then puts back in line the jobs nobody reported.
- **A worker machine loses power.** This is handled as a sudden loss: its jobs go back in line after the measured opportunity allowance above. On the always-on machines, the worker starts automatically at boot. On startup it cleans up any job processes left over from before the power cut, and then reports in fresh.
- **A worker loses network but keeps power.** It keeps running its current jobs, saves their logs and results locally, and keeps trying to reconnect. It does not start new jobs. When it reconnects it sends everything it saved. If the queue already gave a job to another machine, the queue reconciles the overlapping copies: the resumable copy with the newest complete checkpoint keeps running, otherwise the current attempt does, and the other stops. Output writes must be safe to repeat.
- **The local network drops, or the queue machine loses network.** If a strict majority of workers (including all of them) go quiet at once, the problem is almost certainly the network, not every machine at the same time. So the queue does not put everything back in line. It waits until machines reconnect, or until it can confirm specific machines are really gone.
- **Configured storage is unreachable.** NAS-backed resumable jobs wait for their checkpoint folder; file-store-backed jobs require their declared inputs and durable checkpoint transfers. Non-resumable production with local inputs starts with local scratch and does not require a NAS. NAS probes and checkpoint reads/preparation have short bounded waits outside the queue lock. A displaced experiment that cannot save within its grace is force-stopped and reported interrupted; production proceeds after memory release, and the experiment later resumes its last durable save. Running jobs otherwise keep going and retry their saves. Status explains the storage failure; local-only progress never masquerades as portable progress.
- **Saved progress is never half-written.** The resumable-jobs guide requires writing each checkpoint to a temporary file and then renaming it, so a power cut mid-save leaves the previous checkpoint intact. When resuming or reconciling copies, a job uses the greatest monotonic generation recorded atomically in its checkpoint; file times and clocks on other machines do not determine progress. Current execution/session/attempt identity and a complete checkpoint generation are required. Historical unsequenced saves need offline migration or remain held; the runtime does not guess their ownership or rank them as a fallback.
- **Tests:** each of these cases has a test using fake time and pretend machines: queue restart, worker restart, worker cut off from the network, the whole network dropping, and the NAS going away.

### Adaptive worker loss
<a id="adaptive-worker-loss-661-my-31my-34"></a>

See [adaptive worker loss](reference/scheduling-mechanisms.md#adaptive-worker-loss) for the
approved mechanism and its constraints.

### Production infrastructure retries and start windows

**Production infrastructure retries.** An interrupted production batch job requeues at the front of its priority line under the same job ID and `submission_key`, at most **2** times (`queue --production-infrastructure-retries`, a nonnegative limit clamped to two; zero disables requeue). Worker restart or loss, an unreported attempt after queue restart, undelivered/lost results, worker interruption/release, planned leave and reboot all use the existing requeue path. A queue restart that reconnects a still-current running command or its buffered result does not interrupt it. The third interruption fails with `infrastructure_retries_exhausted` and a terminal timestamp. The durable job counter `infrastructure_retries` records requeues used and is an additive optional integer in status and job-v1 (omitted at zero). It survives restart; release evidence cannot refund or revive an exhausted production budget.

Classification uses Mycelium-recorded facts for native and native-group attempts. Infrastructure evidence is a durably recorded stop or kill delivered to that member before its exit was observed; loss of observation; a Mycelium-side pre-execution or launch refusal/failure (including authorization, exec/spawn or environment preparation infrastructure failure); or Mycelium's own probe proving that an allocated readiness port belongs to a process outside this group when readiness fails. Exit codes, payload stderr, stop intent and signals recorded after exit observation are never infrastructure evidence. Independent member exits, OOM (including an extension job), and readiness timeouts without such evidence fail production immediately without an infrastructure retry. A successful terminal outcome is preserved. Cancellation takes precedence: once `will_not_run: true` is acknowledged, no interruption can requeue the job. Superseded production attempts cannot recover ownership or publish a late result; exact release evidence can still discharge their custody. At most one attempt's result is published. Commands and output writes must still be safe to repeat; external side effects are not exactly-once. Experiments retain their existing retry, checkpoint recovery and planned-stop behavior.

A production attempt that had started gets its original-length start window from interruption detection, including a worker interruption/release. This also applies when the original length came from an explicit first-start `start_by`; the renewed cutoff retains its absolute semantics during capacity or device-recovery waiting. Planned leave keeps its original start deadline. A requeued job may wait for its maintained host to resume within that window; a new production submission to only disabled eligible hosts still fails immediately. Unstarted attempts do not renew their start deadline.

Production batch jobs and adapter entries may declare `start_window: 600s`, a
positive Go duration no greater than `1h`; omission retains the `150s` default.
This bounds availability waiting while allowing contended startup preparation;
longer windows spend more caller latency before failure. Generated deadlines
start at submission, dependency readiness, or physical fan-out child assignment,
as applicable. Children inherit the declared length. Explicit absolute `start_by`
takes precedence and passes unchanged through fan-out; its original window is
the deadline minus original submission time. Job GET exposes the declaration in
`spec.start_window` and the effective original length in top-level `start_window`
(nanoseconds), including before startup. Experiments omit the fields and have no
start deadline. Loss recovery retains that original effective length.

A typed `device_fault_last_healthy` hold pauses the remaining relative startup
or recovery budget. Recovery and subsequent capacity waits retain that saved
amount, including across restart; caller-owned absolute
`start_by` and queued-only `give_up_at` still bind. A hold with no eligible
recovery terminates at the existing one-hour maximum start window measured from
the latest queue-recorded device-fault loss, with typed reason
`device_fault_recovery_timeout`. Restart and heartbeats never renew that bound.
An active hold checks expiry before resuming, including when a late report clears
quarantine after the bound.
This limits an unrecoverable wait without shortening the worker's measured
health-proof window or releasing exact-attempt custody.

## Submission identity and cancellation
<a id="submission-identity-and-cancellation-80"></a>

`POST /api/jobs` and submit clients accept optional `submission_key`, unique within `project`. When supplied it must be 1–128 ASCII characters from `[A-Za-z0-9._:-]`; submit and cancel-by-key reject an invalid key with HTTP 400 `submission_key_invalid` before lookup or mutation. Omitting the key on submit preserves unkeyed behavior; an explicitly empty or null key is invalid. Without it, each submission is a new job. Atomically persist key, canonical client spec and job ID before replying. Identical repeated/concurrent submissions return the existing ID, even for terminal jobs; they never resurrect work. Compare client values ignoring object-key order and excluding the key and server-generated IDs, timestamps and default start deadline. An explicit client deadline remains part of the spec. A different spec returns HTTP 409 `submission_key_conflict` without mutation.

**Cancelled reservations take precedence.** The first later valid POST against a spec-less cancelled reservation binds a minimal terminal `cancelled` job with an ID and returns the normal Job shape, whatever spec was submitted: no comparison, no 409, no assignment and no execution. Preserve cancellation time. Later submissions return that cancelled job regardless of spec. Submit and cancel-by-key for the same `(project, submission_key)` each execute in one SQLite transaction under the queue's single update lock, serializing with each other. If submit commits first, cancel acts on its job; if cancel commits first, submit gets the reserved cancelled job.

| Retained identity | Lifetime and answer |
| --- | --- |
| Job key, spec fingerprint and ID | Entire job lifetime plus 30 days after removal; return the original answer within retention. |
| Cancelled reservation with no job | 30 days from creation; later submit can bind only a cancelled job. |
| SHA-256 digest marker of `(project, key)` | Retained indefinitely after full-record expiry. Submit returns HTTP 410 `submission_key_expired`, never creating a job; cancel-by-key returns `unknown`. |
| No record, with intact durable marker store | Genuinely never seen; cancel-by-key records an `absent` reservation. A cache miss alone never proves absence. |

**Queue history retention.** Finished job records leave job listing and ID
lookup **14 days after `finished_at`** (MY-49). Result files use
`--file-retention`, `72h` by default (MY-47). Missing terminal timestamps start
their window at the first maintenance observation. A record stays while it is
running, queued or held, owes delivery, release or custody, still holds files,
logs or a checkpoint folder, is the `after:` parent of a retained job, or is an
extension check that native qualification reads. Pending blob cleanup transfers
atomically to a deduplicated set of expired hashes; shared blobs remain
protected by their other owners. Before a record goes, a one-line summary (ID,
project, name, final state, finish time, host, duration, peak memory) and every
learning input it holds (retained memory evidence and the work observations its
ledger learned) are written to the separate `job_summaries` table; ID lookup
then answers HTTP 410 `job_expired` with the summary. A keyed answer keeps the
job's identity and outcome for replay. Learned work-curve observations are not
pruned. Removal is automatic, at most 64 records per maintenance pass: summaries
are written outside the queue mutex, and each record is rechecked and removed in
the existing crash-consistent SQLite transaction. Its logs are deleted after
commit, outside the mutex.

**Retained execution ledger.** The per-job append-only ledger shares the
existing job/replay record and transaction, not a separate service or retention
policy. Assignment, Started authorization, each native custody reservation/start,
queue outcome and exact release are durable entries. New native generations are
recorded before execution permission; port-collision replacements preserve their
own identity and release evidence within one queue attempt. Read-only API/CLI and
Python pages use immutable sequences and a fixed high-water mark, with no total
row cap. Keep all entries while the job or keyed replay answer is retained,
including the extra 30-day post-removal window below. Unresolved custody is never
pruned. An expired or unknown ledger returns explicit HTTP 410
`job_ledger_unavailable`, never an empty complete history. Pre-ledger history and
older workers without native custody receipts report incomplete coverage; missing
historical custody cannot be reconstructed. See the
[ledger contract](jobs.md#retained-per-job-ledger) for fields and commands.

The existing key contract remains: after job removal the full answer, spec
fingerprint and cancellation/release evidence remain available by submission key
for another **30 days**, then become permanent digest-only markers. Thus ordinary
full history is limited to the retention windows plus cleanup backlog; unresolved
custody and permanent markers intentionally have no age limit. This is not a
fixed byte quota. SQLite reuses deleted pages; physical file shrinkage is an
offline compaction operation, never an automatic `VACUUM` on the live queue.

A key's acknowledged `will_not_run: true` remains true permanently, including after expiry. While a consumer operation is open, recover a lost response by retrying its stored identical spec/key within retention. Once closed, cancel by key only, never resubmit. An expired/unknown answer leaves consumer coverage unresolved and blocks its reopen. Consumers own key generation, stored specs, operation intent and acknowledgement persistence. This promises no exactly-once external effects; keep output writes safe to repeat.

### Job reply contract

The generated [Job reply schema](schemas/job-v1.schema.json) describes actual `Job`/`Spec` JSON output, including the minimal reserved terminal job. Identity echo is **`spec.project` and `spec.submission_key`**, never top-level fields. Stable contract fields are annotated `x-contract: "stable"`: job ID/state, project/key echo, execution identity (`machine`, `session`, `attempt`), recorded release confirmation, recorded terminal time (`finished_at`), durable cancellation time (`cancelled_at`), and optional production requeues used (`infrastructure_retries`). The state enum is `queued`, `running`, `stopping`, `succeeded`, `failed`, `cancelled`; preparation is an assigned `running` job with no `started_at` yet. Optional identity/time/release fields follow the Go JSON omission rules: absent `release_confirmed` is not confirmation; queued/reserved jobs omit machine/session, and a reserved job has attempt 0. A reservation's minimal spec preserves only project/key identity: zero/empty command, priority, needs and environment fields are valid **reply** data, not a valid new runnable submission. `reason`, progress, placement, accounting and other projections are diagnostic (`x-contract: "diagnostic"`); clients must not parse reason text or infer release from it. Diagnostics may grow. Terminal state/results stay intact when cancellation adds durable exclusion metadata.

Schemas describe wire shape, not proof of physical release or permission to consume results. Go tests regenerate the Job schema from the actual types and validate real marshalled jobs, cancellation acknowledgements and HTTP error responses. Examples cover queued, running, succeeded and reserved cancelled replies.

### Cancellation acknowledgement

Cancel by ID with `POST /api/jobs/{id}/cancel`, or by key with `POST /api/jobs/cancel` and `{project, submission_key}`. Both are idempotent and return `{job_id, project, submission_key, state, will_not_run, release, acknowledged_at}`. `job_id` and `submission_key` may be null where no binding exists. `acknowledged_at` echoes the stored first `cancelled_at` (or reservation cancellation time) in UTC RFC3339; retries, reopen and reservation binding preserve it. It is null for an expired digest marker with no retained timestamp. `will_not_run: true` durably means no **new** attempt is created or assigned after the acknowledgement. An attempt already assigned before the acknowledgement may still begin briefly; the worker stops it at its next check, and `release` stays `pending` until release is proven. Consumers that need execution exclusion wait for `release: "confirmed"` (or `not_applicable` when never attempted). Every batch command requires the current attempt-fenced Started acknowledgement before its payload executes; engines retain exact execution custody and queue-authorized plans. `release` is `not_applicable`, `pending` or `confirmed`, or null when no release fact can be established. For a retained job, release always covers **every known attempt**, regardless of whether the job is queued, active, terminal or removed: `not_applicable` only when it never had an attempt; `pending` while any known attempt has unproven release (its charge remains); `confirmed` once all known attempts are proven released. In particular, a requeued job can still have a lost, unreleased attempt; cancelling it returns `pending`, then a repeated cancellation returns `confirmed` after that exact release is proven. Cancellation never creates release custody or placement blockers; runtime tracking owns charges. Fresh rejoin records release only for copies from the immediately previous session (`PreviousSession`, matching the recorded old session), even without a retained release record; older sessions remain unresolved. Pre-v3 persisted queue state is refused and never contributes inferred release evidence.

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

### Fan-out abort scope and read-only release
<a id="fan-out-abort-scope-and-read-only-release-205"></a>

Cancelling a declared parent by ID or project/key aborts every declared piece,
materialized wave and registered dependent. Undispatched pieces stay excluded;
valid future dependencies against a retained cancelled parent bind only cancelled
jobs. Terminal state/results remain intact with `cancelled_at` exclusion metadata.
Callers cancel the parent once; they need not cancel children individually.

`GET /api/jobs/{id}` and status expose `cancellation` after cancellation, with
the same acknowledgement fields and release value as repeated cancel. A logical
parent's aggregate is `pending` for any unreleased current or old descendant
copy/host release, `confirmed` once every attempted descendant is proven released,
and `not_applicable` when neither it nor any descendant was attempted. Remaining
declared work is durably excluded. Never-assigned cancelled jobs explicitly expose
`cancellation.release: not_applicable`; this is settled exclusion, without setting
`release_confirmed` or inventing physical cleanup evidence. The current execution's
`release_confirmed` alone is never an aggregate acknowledgement.

Every retained declared parent additionally exposes read-only `cancellation_scope`,
even before cancellation. `pieces` enumerates all declared keys in declaration
order, including unaccepted/undispatched keys. Each entry has `materialized`,
`accepted`, optional `wave_id`, `will_not_run`, `release` and sorted `dependents`
(registered dependent job IDs for that piece). Piece release aggregates its wave
and dependents. `members` is a sorted flat list of all retained wave/dependent
jobs with `job_id`, `state`, `will_not_run` and aggregate `release`. Before abort,
`not_applicable` describes no attempt so far; only `will_not_run: true` establishes
durable exclusion. No job or attempt is created for an undispatched piece.

This is a snapshot of registered work, not a closed submission transaction:
future valid dependencies appear on subsequent reads and remain fenced after
abort. Finished job records expire 14 days after they finish (MY-49) unless a
retained job names them in `after:`; see [queue history retention](#submission-identity-and-cancellation).
Retained keyed answers follow the existing 30-day-after-removal limit, then
digest-only expiry. Once a
source is no longer retained, new dependencies are rejected rather than inferred;
GET cannot enumerate an expired/unknown source. Artifact expiry is independent
and does not erase retained keys, scope or release custody. This projection adds
no dependency types or graph scheduler.

## Queue-owned host maintenance

Maintenance is a durable queue record keyed by the physical `host_id`, shared
by every device and worker view of that machine. It survives checkin, queue
restart and worker rejoin. It is independent of the worker's `disabled` report
and project/job allowlists.

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
infrastructure losses, including when cleanup, loss detection or restart
happens later. The cause never transfers to the next attempt. Cancellation
still wins; observed independent command failure, OOM or sidecar/readiness
failure still fails production immediately. Started attempts get their
original-length start window on requeue; an unstarted attempt keeps its
deadline. Retries can wait on the maintained host or use another eligible host,
subject to ordinary deadlines. The [two-infrastructure-retry
contract](#production-infrastructure-retries-and-start-windows) still applies
to other causes.

The queue bootstraps the memory allowance once from the first worker report,
including zero for an uncapped laptop. Subsequent reports cannot change it.
`host budget` or `drain|disable --memory-budget-gb GiB` sets a positive
allowance only while drained/disabled. Every worker view receives the policy
and must report it consistently before new work starts. Lowering it keeps
existing attempt charges and exposes aggregate RAM debt; it does not resize a
running attempt's ceiling or release resources. Qualification remains
`UNQUALIFIED` until separately authorized native measurement. Sidecar
ceilings and total per-attempt charges are unchanged.

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

### Admission and release
<a id="host-admission-and-release-80"></a>

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
The same cleanup handles native groups and their sidecars.

Drain before an upgrade; use `disable --stop --wait` when active work must stop.
Resume explicitly when ready. There is no offline handoff certification: during
queue outage use separately authorized service/container tools after inspecting
owned work, preserve pending results, then reconnect for exact reconciliation.

## One machine, one worker

The physical machine is the hypha: one durable host ID, one live worker session,
one spool/lock and one CPU/RAM ledger. A second inventory-bearing worker on the
same physical host is refused with `machine_worker_conflict`. CPU-only inventory
is empty, never a fake accelerator. A check-in reconciles every exact attempt
and offers at most one additional assignment; later check-ins fill free capacity.

### Inventory and resource ownership

See [inventory and resource ownership](reference/worker-mechanisms.md#inventory-and-resource-ownership) for the
approved mechanism and its constraints.

### Attempt fencing and containment

See [attempt fencing and containment](reference/worker-mechanisms.md#attempt-fencing-and-containment) for the
approved mechanism and its constraints.

### Placement, protocol and compatibility

See [placement, protocol and compatibility](reference/worker-mechanisms.md#placement-protocol-and-compatibility) for the
approved mechanism and its constraints.

## CPU and shared host resources
<a id="cpu-and-shared-host-resources-r1-9822"></a>

A plain CPU job declares `needs: {cpu_cores: 2, memory_gb: 4}` without GPU
selectors. Here memory is host RAM in GiB. `platform: cpu` also selects CPU
execution, defaulting to one logical core when `cpu_cores` is omitted.
Admission normalizes CPU work to `platform: {required: [cpu]}`, zero GPU
`memory_gb` and positive `host_memory_gb`; stored/replayed jobs use that
unambiguous vector. GPU declarations retain GPU `memory_gb` and additional
`host_memory_gb`, with `cpu_cores` defaulting to one. Native sidecars share the
attempt's declared cores/RAM instead of adding another reservation. CPU
commands receive `MYCE_DEVICE=cpu`, `MYCE_PLATFORM=cpu`, an empty CUDA mask and
no fleet token. Every member additionally gets the explicit select-none XPU
masks specified under [attempt fencing](#attempt-fencing-and-containment);
removing inherited XPU selectors alone does not hide XPUs. This is cooperative
device selection.

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

[One machine, one worker](#one-machine-one-worker) owns machine inventory and
concurrent attempts; Windows/WSL qualification remains separate work. CPU
counts are admission reservations, not CPU-time enforcement; macOS RAM remains
admission-only and delegated Linux RAM enforcement retains its existing
qualification requirements. Real native Linux/Windows acceptance and fleet
cutover are separate from laptop proof.

## Memory

Agents state needs, and Mycelium learns memory from its own measurements
(MY-31). Its limits are measured or physical (MY-34).

### Per-attempt RAM ceilings

Declared batch/model host RAM is enforced on Linux cgroup v2 workers with
systemd memory delegation. Each execution remains inside its worker service's
cgroup, so ordinary service stop still contains every native descendant.
Workers report unsupported enforcement plainly and keep laptop admission-only
execution. An optional physical-host budget caps the sum of declared ceilings,
including configurable slack, and retains unconfirmed allocations after loss.
OOM is a
normal failure rather than infrastructure loss. The configuration, unsupported
cases and qualification limits are in [operations](reference/host-capacity.md#host-ram-admission-and-ceilings).

### Host roles and measured capacity
<a id="amendment-host-roles-and-measured-capacity-2026-11-11"></a>

MY-45 and MY-34. Each host carries an explicit queue-owned role, set through
the host command and defaulting to `node`. On a node with a measured allowance,
capacity is live availability minus one margin: the larger of the explicit host
reserve and the observed idle burst above the idle p50. The allowance is
physical RAM minus the idle p50 minus that margin, never less than the observed
idle maximum. The `services` role (the host running every project's always-on
services, accelerators still schedulable) keeps the idle p95 as persistent
external demand, with the same single margin. The `interactive` role is exactly
the hosts with the shared-interactive profile and keeps the existing rule and
limits, as do workers declaring owner use or running on a laptop. The explicit
reserve remains the operator's named override for overshoot not yet measured;
no new fixed reserve is introduced.

### Unified CUDA free-RAM floor
<a id="amendment-count-the-idle-footprint-once-2026-11-11"></a>

See [unified cuda free-ram floor](reference/memory-mechanisms.md#unified-cuda-free-ram-floor) for the
approved mechanism and its constraints.

### Charging running attempts by use

See [charging running attempts by use](reference/memory-mechanisms.md#charging-running-attempts-by-use) for the
approved mechanism and its constraints.

### Job memory envelopes
<a id="amendment-job-memory-envelopes-146"></a>

See [job memory envelopes](reference/memory-mechanisms.md#job-memory-envelopes) for the
approved mechanism and its constraints.

### Learned memory by default and bounded first runs
<a id="amendment-learned-memory-by-default-and-bounded-first-runs-585-559"></a>

See [learned memory by default and bounded first runs](reference/memory-mechanisms.md#learned-memory-by-default-and-bounded-first-runs) for the
approved mechanism and its constraints.

## Native job isolation
<a id="amendment-minimum-native-job-isolation-177-219-248"></a>

The minimum (MY-23) is a v1 requirement under [#248's clarification](https://github.com/1kuna/mycelium/issues/248#issuecomment-6447873336):
managed job directory and temp, no ambient credentials, and no worker control
access for sandboxed payloads. New extension registrations default to a **kernel-enforced boundary**; `0700` alone
cannot separate processes sharing a UID. This covers native batch payloads and their sidecars, including queued extension run/check payloads. It
does not establish isolation for local setup/qualification hooks, arbitrary
preparation code or standalone model servers.
This text records the contract and its limits, not qualification or rollout.

**Declared stage exception and legacy registration policy (MY-24).** An
extension's top level or an adapter entry may declare `isolation: unsandboxed`
and a required nonempty `reason`. Invalid modes and blank reasons are rejected.
`isolation: sandboxed` explicitly selects the kernel boundary. The choice applies to
main, queued qualification/check payloads and every sidecar of that stage;
there is no global switch or per-sidecar escape. Extension jobs cannot override
the frozen registration's policy at submission.

The queue stamps newly created registrations with server-owned
`sandbox_default: true`, outside the immutable bundle/digest. Persisted
registrations and frozen job copies without that marker retain unsandboxed
execution when no authored mode is present. Identical registration replay
preserves the original marker. Opt in a legacy registration by authoring a new
version/digest with `isolation: sandboxed`; existing queued/frozen jobs retain
their original policy across restart. No date, source revision or content
identity rewrite determines this choice.

**Plain native compatibility policy (MY-25).** Plain `run.command`
jobs with no frozen extension registration remain unsandboxed by default in v1.
Live consumer commands prepare dependencies and write shared home caches that
break under the new boundary. Opt in with `isolation: sandboxed`; then the
sandbox and offline default apply, including network capability admission.
Status/job GET expose `unsandboxed: plain native job defaults to unsandboxed in v1; set isolation: sandboxed to opt in`, and workers warn once per attempt.
This does not change new extension defaults or frozen registration policy.

An unsandboxed stage omits kernel sandbox setup and its kernel-capability
admission requirement, including protected native-network capability. It has
ordinary worker-user filesystem, process, IPC and network access; offline
defaults and cache/control/sibling content denials below do **not** apply.
Managed work/temp, private source/input staging, environment/token scrubbing,
CPU/GPU selection and device binding, cgroup limits, authorization/Started,
attempt fencing and release custody still apply. A managed cwd does not confine
an unsandboxed payload's other file accesses. Status and job GET retain
`isolation: "unsandboxed: <reason>"` through queuing, execution and terminal
outcomes; legacy mode uses a fixed public explanation. Worker logs emit one
warning per attempt, shared by main/sidecars and launch retries. Reasons are
public diagnostics: authors must keep secrets and private paths out of them.
The queue and workers must run the policy-capable release together; an older
worker cannot implement a declared exception.

The kernel guarantees in the following paragraphs apply to sandboxed stages.
Every native attempt gets a managed `{work}`, `.tmp` and `.scratch` beneath it.
The worker overrides `TMPDIR`, `TMP`, `TEMP` and `MYCE_SCRATCH_DIR` after
authored environment/activation; main and its own sidecars share these exact
paths. Native workdirs must resolve inside `{work}`; stage source and data
through inputs or the frozen extension, rather than executing from an arbitrary
host checkout. Extension source is copied privately under
`{work}/.myce-extension`. Prepared environments and shared extension caches
permit payload reads and execution, not content writes. Operator-configured
model/cache roots permit reads without content writes or direct execution.
Narrow operator declarations can add read-only model roots or read/execute
runtime roots, refusing filesystem, home, NAS and installed Mycelium roots and
protected-content overlaps. Linux exposes only the current and trusted
attempt-member cgroup memory metrics, without ancestor/sibling cgroup contents
or cgroup control writes. NAS inputs and outputs retain their declared worker
staging/publication contract; only the attempt's exact checkpoint directory is
writable in place. Neither a mounted NAS nor an authored environment path
grants the payload the whole shared filesystem. Keep the environment allowlist,
stripping agent/session variables and queue credentials; `MYCE_TOKEN` never
reaches a payload. Explicit job inputs and `run.env` are intentional
disclosures to that job. Linux metadata limitations below also apply to cache
permissions.

Linux prefers rootless **bubblewrap** (`bwrap`) with user, mount, PID, IPC and
network namespaces, a read-only host-root view, private `/tmp` and `/dev/shm`,
and narrowly writable attempt/checkpoint paths. Landlock plus seccomp further
restrict file contents, devices, sockets and process access. A per-attempt
namespace anchor is shared by main and sidecars using `nsenter`; independently
isolating each member would break sidecar loopback. The installed worker's
service context must pass the capability probe. The current implementation
requires Landlock ABI >= 6 and working `bwrap`/`nsenter` for this stack.

Only namespace **policy refusal** permits the narrower Landlock ABI >= 6 plus
seccomp fallback. Missing tools, unsupported ABI, broken setup or timeout fail
closed. Landlock allows TCP connect/bind only on the exact worker-reserved
sidecar ports, with no IP endpoints allowed for a job without ports; seccomp
denies UDP, raw/packet, SCTP, MPTCP and new AF_UNIX stream/datagram endpoints.
The [v1 exception](https://github.com/1kuna/mycelium/issues/177#issuecomment-6453575275)
permits fallback-only AF_UNIX `SOCK_SEQPACKET` creation, protocol zero and normal
CLOEXEC/NONBLOCK flags, retaining Landlock abstract/signal scoping and every
other denial. **same-UID pathname SEQPACKET endpoints reachable (partial)**:
this cannot enforce abstract-only access. Namespace mode retains its Unix
endpoint denial. Private Unix stream socketpairs remain available for process
wakeups; datagram and seqpacket pairs remain denied. A cheap startup inventory
warns about unexpected non-root/non-CUDA seqpacket listeners, unknown ownership
or inventory failure. This is advisory; future endpoints remain a residual.
**This fallback provides partial network isolation:** a reserved port
also permits off-host TCP on that destination port and binding that local port
on any address. Readiness verifies actual sidecar ownership and a
`127.0.0.1` listener; it is not kernel enforcement of the bind address. No other
TCP connects/binds are permitted. Landlock does not isolate metadata: `stat`,
`chmod`, `chown`, timestamps and other unmediated operations remain subject to
ordinary host permissions. Do not describe it as a private filesystem or as
protection against every form of sibling interference.

Both Linux modes are currently **offline-only** capabilities: namespace egress
transport is unqualified, while the fallback's reserved-port exception does not
qualify general outbound access. Neither advertises `native_network`.
`needs.network: true` is the explicit declaration for payload-time downloads or
API calls. Placement requires an online-capable host; submission returns
`no_qualified_target` immediately if none qualifies, rather than accepting an
impossible job indefinitely. Worker-owned environment/input preparation keeps
its network access. A declaration never waives credential/control protection.

macOS uses inherited `sandbox-exec`/Seatbelt rules to deny same-spool sibling
contents and writes, protected worker/operator credential roots, Unix control
sockets, cross-job process access and unwanted Mach services, while retaining
Metal services. This is targeted protection with metadata and unrelated host
files still visible, not a private mount/PID namespace or a full home sandbox.
Default outbound access is limited to the attempt's reserved TCP loopback
sidecars. `needs.network: true` permits general outbound access while denying
other loopback endpoints and the queue control TCP port at **every** address;
this also denies unrelated services on that port. Seatbelt is deprecated:
missing launcher, rejected profile or failed enforcement must fail the attempt,
never run it unconfined.

Retain assigned GPU access and existing CPU/GPU qualification, cgroup placement,
per-member/aggregate memory limits, authorization/Started gates and exact cleanup
custody. The unsandboxed worker/supervisor performs control and observation;
payload confinement is not a release proof. Same-user **unsandboxed host
processes**, root, kernel/driver exploits and malicious operator runtime files
are outside this boundary's threat model. It is not hostile multitenancy.
See [native isolation](native-isolation.md) for the platform guarantee table,
limitations and a separately authorized per-host AppArmor option. Broader #177
hardening remains separate from this minimum.

## Containers
<a id="docker-command-containment-maintainer-2026-10-11"></a>

Mycelium does not run containers. Managed Docker command containment, added
on 2026-10-11 as an optional path (MY-13), was retired on 2026-11-12 under
[MY-49](DECISIONS.md#my-49) once its last consumer ran its model jobs natively.
A job spec, profile or adapter entry that still sets a `run.docker*` field is
refused at submission with `job_docker_retired`, pointing to native
environments and sidecars ([services](services.md#a-model-server-in-a-native-job-instead-of-a-container-image)).
Historical Docker job records still load and display (MY-42); their retained
specs do not authorize execution. Docker CLI flags and installer settings are
removed. Old config keys are discarded when read and omitted on reinstall.

## Models
<a id="models-nas-masters-local-cache-and-production-pins-maintainer-2026-10-09"></a>

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

### Pre-copying
<a id="pre-copying-maintainer-2026-10-09"></a>

See [pre-copying](reference/model-mechanisms.md#pre-copying) for the
approved mechanism and its constraints.

### Shared native server entries
<a id="shared-native-server-entries-68"></a>

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
See [operator configuration](services.md#shared-entries-local-secrets-and-approved-model-folders).

### Log opt-out
<a id="log-opt-out-rollout-68-review-decision"></a>

Workers advertise `logs_none` in `capabilities`; the queue assigns
`logs: none` batches and server entries only to those workers. Waiting status
names the missing capability. Environment build output remains in environment
records, without per-job copies or job-log fallback text for opted-out jobs.
Roll queue and workers together; do not downgrade with opted-out work live.
A queue-first upgrade drains live opted-out servers until their workers upgrade.
See [log policy and downgrade behavior](jobs.md#scheduling-and-results)
and the [native proof checklist](services.md).

## Files with or without a NAS

The queue can store SHA-256-identified files and
serve resumable HTTP transfers, with bounded caches on execution hyphae. Inputs
and models are explicit; outputs and checkpoint generations are published only
after verified durable transfer. Checkpoint commands still use
`MYCE_CHECKPOINT_DIR`; local-only progress cannot resume elsewhere. The single
SQLite queue, plain commands, injected scheduling clock and attempt ownership
remain. Explicit NAS configuration remains supported. One store is not a backup.
See [files without a NAS](files.md) for configuration and retention limits.

### Explicit local-first checkpoint generations (#1600)

`advanced: {resumable: true, checkpoint: local_first}` opts an individual plain
command into `checkpoint-local-first-v1`. Existing configured NAS checkpoints
remain the default, unchanged. This platform-independent opt-in gives the payload
a managed local `MYCE_CHECKPOINT_DIR`; it never silently substitutes local storage
for a NAS checkpoint. Compatible workers advertise the versioned capability.

V1 supports cooperative self-contained JSON saves up to the existing 1 MiB
metadata limit. Python `Checkpoint.save()` syncs a new immutable generation file,
renames it, then atomically publishes a synced `generation.json` manifest last.
The worker captures only that manifest's verified files, outside queue locks.
Save waits for the exact generation/manifest's durable current-attempt queue
acceptance before returning or publishing another generation. Local-only work
is not portable. Unsupported policies/forms refuse with
`checkpoint_policy_unsupported` or `checkpoint_protocol_unsupported`.

The existing file-store transport restores the greatest accepted generation on
another compatible target. With configured NAS checkpoints, acceptance also
records immutable mirror custody in the same SQLite transaction. The existing
queue-owned output publisher and identity/content receipts mirror generations
asynchronously under the configured checkpoint root. A stalled syscall keeps
its single slot; no overlapping retry or payload NAS write is introduced.
Pending mirrors pin their queue bytes across restart and retention; settled
checkpoints retain the existing 72-hour grace. Queue capacity and measured local
headroom cause cooperative backpressure; arbitrary command writes have no hard
filesystem bound. NAS mirror lag is unbounded during outage. See the
[publication, acknowledgement and recovery limits](files.md#local-first-checkpoints-1600).
Native sustained SMB/restart/migration acceptance is release work, not a laptop
or merge claim.

### Ephemeral execution-worker data (#1585)

`advanced: {worker_data: ephemeral}` opts a non-resumable plain command with
managed inputs and declared outputs into synchronous local removal. The worker
bypasses persistent input caches, confines payload writes to private work,
temp, home and checkpoint paths, stops and reaps its processes, and preserves
successful result bytes until exact durable queue acceptance. It then unlinks
all local payload and delivered log copies, syncs the parent directories and
verifies absence before acknowledging release. Each attempt retains a cleanup
receipt (relative roots, logical bytes, timestamp and success/failure); failed
removal stays visible and holds release. Retry attempts restage authoritative
inputs. Startup recovery reconciles process custody and repeats cleanup;
offline/crashed workers remain pending, never presumed clean.

Initial support requires installed tools, offline execution and Linux PID/mount
namespaces with a qualified file core route or Mycelium core router. Other
combinations receive
`job_worker_data_unsupported`; older workers cannot receive these jobs. The
worker sets soft and hard `RLIMIT_CORE` to zero in the disposable exec helper.
Linux piped core handlers ignore that limit. The qualified Mycelium router
matches the dumping PID's cgroup to exact attempt custody and closes ephemeral
core streams unread, recording only signal/time/attempt metadata. Other piped
handlers and legacy routers are refused, without changing global configuration.
Failed/interrupted attempts discard private payload after process cleanup and
retain logs until queue delivery.
This is an explicit local-data exception to MY-47's finished-file retention;
queue/NAS retention is unchanged. Unlink is **not secure erasure on SSDs**;
full-disk encryption is the at-rest control. RAM, VRAM, swap, snapshots, prior
jobs' copies and privileged/operator processes are outside this guarantee.
See [the file contract](files.md#ephemeral-worker-data).

### Shared read-only inputs
<a id="shared-read-only-inputs-221"></a>

Profiles and plain native jobs may declare `shared_inputs`, independently of
private `files`. A declaration identifies a stored blob by SHA-256 and size, or
a NAS-relative path plus that identity. A NAS directory uses an explicit file
manifest, inline or referenced by a NAS-relative `manifest_file` and its hash/size
so large runtime manifests do not travel in every submission. NAS references resolve under each worker's `--checkpoint-root`
and never upload through the queue. Profiles may import a `local` blob once;
the submitted specification contains only its immutable identity. No environment
catalog entry or queue restart is required.

Workers publish verified cache entries atomically and expose absolute paths in
`MYCE_INPUT_<UPPERCASE_NAME>`. Files/trees have read-only mode bits; these are
cooperative controls, not a hostile-code sandbox. `archive: tar|tar.gz` unpacks
once, bounded by declared `unpacked_bytes`; archives contain regular files and
directories only. NAS trees copy only manifest members and preserve
executability. The cache records per-file hashes, verifies them on first use
after restart, and checks inode/change-time/size/mode metadata on reuse to
detect tampering. Shared use locks span preparation and process cleanup; LRU
eviction skips active entries. Admission enforces the physical disk guard
before staging. Failed staging is disposable; authoritative NAS/store data
remains untouched. Stored references use ordinary live-job and terminal
retention. All bulk I/O runs in worker or unlocked submission verification
paths. Transfers use size-aware, renewed socket deadlines, with no fixed
whole-input timeout.

### Durable asynchronous NAS output delivery
<a id="opt-in-durable-asynchronous-nas-output-delivery-349-2026-10-29"></a>

See [durable asynchronous nas output delivery](reference/file-mechanisms.md#durable-asynchronous-nas-output-delivery) for the
approved mechanism and its constraints.

## Extension composition
<a id="extension-composition-e1-98"></a>

Extensions wrap existing software; their author owns packages and correctness (MY-12).

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
Variant environments use the existing uv requirements or uv project lock
snapshots, with `backend: uv` (also the default), Python version and bundled
relative lock path. A uv project includes both uv.lock and pyproject.toml.
Immutable recipe keys include lock bytes, Python, backend and exact
OS/arch/platform, excluding private mount paths. The existing environment
cache, exclusive build locks, atomic publication and shared execution
references remain authoritative. `backend` selects the builder: uv, or Pixi or
npm through the same recipe contract ([environments](environments.md));
unsupported backends are rejected and never executed as uv. Composition adds no
solver, driver installer or package catalog. For variant setup/check, pass
`--environment-cache` (and optionally `--environment-cache-bytes`) to the
existing extension CLI. Both qualification and jobs acquire the same immutable
recipe through the existing cache builder. The existing worker environment
preparation may populate its configured cache; automatic extension
distribution, setup and qualification are specified under [the project
instruction file](#one-authored-contract), not by composition.

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
optional environment/env, optional finite member host-RAM cap, optional
port and HTTP readiness path/timeout. An absent port means a plain helper;
zero allocates a loopback port and a positive port reserves that explicit port.
Members inherit main's workdir and environment when no environment is named.
All environments are acquired before payload launch and retained through cleanup.
Explicit member RAM caps must fit `needs.host_memory_gb`, the whole simultaneous
host reservation; they are not added as separate reservations. Delegated Linux
cgroup v2 enforces the aggregate parent and each member leaf before exec.
Main RAM is optional (`execution.main.memory_gb` or `run.memory_gb`); when
omitted its leaf uses the parent cap. A separate control leaf contains the
supervisor. Member OOM fails the stage and identifies that member; cleanup
verifies the whole subtree and removes leaves before releasing the parent.
macOS and nondelegated foreground workers remain admission-only; requiring
memory ceilings refuses hosts without delegation.

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

Ports bind `127.0.0.1`; readiness accepts HTTP 2xx, without proxies or
redirects, and verifies listening-process group ownership (Linux procfs, macOS
libproc). An occupied port or unverifiable listener fails preparation. Loopback
is not a private-network sandbox. Endpoint env and literal substitutions,
examples and platform limits are described in [Native sidecars](services.md).
All members share worker-selected GPU masks, receive no `MYCE_TOKEN`, and honor
`logs: none`. Concurrent attempts and cards per job are defined under [one
machine, one worker](#one-machine-one-worker); assets/archive expansion and
native Windows execution remain separate slices.

## One project instruction file
<a id="amendment-one-project-instruction-file-192"></a>

Profiles and extensions are one kind of project instruction file (MY-22),
which delivers the one-file outcome (MY-17). The agent is its author (MY-20),
standalone proof comes first (MY-09), and packages and correctness stay with
their author (MY-12). Placement and measured planning keep their own sections.

### One authored contract

Use `.mycelium/adapter.yaml`: strict YAML, never prose interpreted by a model
at execution. One file holds named command/stage entries. `myce run` chooses
`default`; `myce run --entry analyze` chooses an entry; `--adapter FILE`
selects another file.

```yaml
schema: 1
project: example
default: analyze
entries:
  analyze:
    source: {git: HEAD, paths: [scripts/]}
    class: experiment
    run: {command: [python, scripts/analyze.py]}
    needs: {platform: cpu, cpu_cores: 1, memory: auto}
    targets: [{os: linux, arch: amd64, platform: cpu}]
    # Project environment; see project-supplied environments below:
    environment: {backend: uv, python: '3.12', lock_file: uv.lock}
    inputs: {item.json: data/item.json}
    outputs: {result.json: results}
    checks:
      result: [python, scripts/analyze.py, --check]
```

The header requires `schema: 1`, `project`, nonempty `entries` and a `default`
that names an entry. Entries declare a command, target requirements and
resource needs. Class defaults to `experiment`, source to committed Git `HEAD`,
and resumability to false. `environment: {}` explicitly uses installed tools;
named catalog recipes remain usable. A lock reference uses [project-supplied
environments](#project-supplied-environments). Application scripts, manifests,
locks and data are existing software assets, not additional Mycelium
instruction files.

Reuse current field types and validation:

| Field | Meaning and lowering |
| --- | --- |
| `run` or `execution` | Exactly one. Existing `run.command` argv or composed `execution.main` plus optional `sidecars`, lowering to the same `RunSpec` and runner. No implicit shell. |
| `needs`, `targets` | Existing cores, RAM, auto memory, accelerator/device constraints and OS/arch/backend alternatives. Job targets require OS; omitting arch requires the author's `portable: true` declaration covering the complete payload, including NAS inputs, archives, environments and runtime downloads. Without it, name the arch or declare portability. No payload inspection or build inference establishes portability. Backend may be omitted when needs already constrains it. Extension variants remain exact. Select one target, not a test matrix. Owner policy, allowances, maintenance, qualification and MY-19 parity intersect. |
| `environment` | Catalog name, installed tools, or locked recipe. Composed execution can instead use existing named `environments` and member references; single environment and environment map cannot coexist in a recipe. |
| `source`, `inputs`, `outputs` | Existing Git/directory/snapshot selection, client-local input mappings and required relative output-name/prefix mappings, with verified file transport. Paths resolve from the project root (invocation root for an explicitly located adapter). |
| `fan_out`, `workload`, `after` | Existing declared pieces, measurements and accepted-output dependency fields, with their current admission restrictions, keyed gather and cancellation scope. No workflow language. |
| `checks.result` | Per-attempt argv in the prepared environment and attempt workdir after main succeeds, before output publication. Failure fails the attempt. Omission means command exit plus required-output verification; converted profiles retain their authored check. |
| `checks.qualification` | Native replay argv. Existing `baselines`, `references` and `sweeps` retain their comparison/measurement contracts and exact qualification identity. They never become per-run success checks. |

An entry requiring different commands, environments or floors uses existing
complete `variants`, each with one `target`, `needs`, `environments`, `execution`
and `checks`. Flat execution/needs/targets/environment fields cannot accompany
variants. Source, I/O, class and pieces belong to the entry; variants do not
inherit or merge execution recipes. Existing setup, assets, work-type declarations,
notes, explicit version labels and resumability remain optional advanced fields.
Extension output-name lists convert into ordinary output maps without changing
caller-visible filenames. Keep diagnostics and `memory_class: per_item`.

Freeze the selected entry and its reachable recipe/source/input bytes. Its
digest binds commands, targets, checks and environment bytes; unrelated entries'
edits do not invalidate it. Reusable kind/program selection uses the existing
registry/file store as a client-managed immutable upload, not another authored
profile. Explicit extension selectors and kind/program APIs remain low-level
interfaces to the same persisted plans. No second executor or registry is needed.

Finite commands do not acquire native baseline requirements by being renamed.
Native variants keep theirs: environment preparation is not accuracy proof.
First-demand qualification extends the preparation lifecycle: after
preparation, the worker creates check scratch on its own host and requests an
exact-host/device internal check job for each required work type. Reuse the
existing check runner/comparison and normal resource, launch and owner
availability fences; only this exact internal check can run before
qualification. Persist its check identity so reconnect/repeated demand adopts
the same in-flight check instead of creating duplicates. The queue keeps useful
work waiting until the worker publishes a successful baseline comparison tied
to the accepted check invocation, frozen entry, target, environments and
host/component fingerprint. Stale/cancelled checks or changed identities cannot
qualify. Check failure stays visible and unqualified without an idle retry
loop; new identity or an explicit qualification retry can recheck. No
submitter-local scratch path, operator SSH or manual worker setup/check command
is part of this journey. References must already exist. A cold fake worker must
prepare, qualify and run useful work from one invocation, and a warm repeat
must reuse matching evidence. MY-19 still requires cross-platform parity before
splitting. `fan_out.pieces: [{key: item-a, args: [...]}]` keeps current
maximums, wave membership, affected-wave retries and exact-attempt publication.
Result checks run on each ordinary attempt/wave under its execution contract,
not once on the logical parent in place of members. Repeated `--input` still
supports independent items. Arguments after `--` append literally as today,
never replace qualified commands. [Whole-parent dependent
fan-out](#whole-parent-dependent-fan-out) owns the result-check/dependent-job
validation boundary; unsupported combinations fail.

Shared servers/models/pins retain their existing endpoint/catalog contracts.
A native sidecar already fits this file; long-lived server authoring is later
work. This merge does not require rewriting serving consumers. There remains
one binary, SQLite queue and scheduler.

### Project-supplied environments
<a id="proposed-option-project-supplied-environments"></a>

A project's uv or Pixi lock travels with the adapter, without operator
registration or queue restart ([project lock files](environments.md#project-lock-files)).

Project locks shipped as inline uv and Pixi locks (#1163) and relative lock
files uploaded by `myce run --entry`/`--profile` (#1297); the named catalog
remains. [MY-44](DECISIONS.md#my-44) covers project-carried locks
(agent reading, 2026-11-12, #1434). Items 5 and 6 (worker-specific build
failure) shipped in #1438 for r6.16: failed worker/target/recipe identities stay
excluded across queue restarts while compatible workers can prepare and run
the same queued job.

Reuse `EnvironmentRecipe`, composed extension bundles, the file store and the
uv/Pixi builders. This adds client-carried finite-command recipes and
worker-specific failure propagation, not another package manager.

1. Client snapshots use `uv.lock` plus `pyproject.toml` and declared Python,
   existing pinned `requirements.txt`, or `pixi.lock` plus adjacent `pixi.toml`
   and optional Pixi environment name. Include referenced activation/build
   inputs. Local workspace dependencies require explicit installable snapshots;
   reject unavailable paths instead of using another checkout. Keep existing
   small-description limits; large immutable inputs use file references.
2. Existing authenticated preflight/upload verifies sizes/hashes. Queue validation
   checks backend, companions, bytes and targets, then persists immutable recipe
   references with accepted intent. This new form never reads the submitter's
   `lock_file` as a queue-local path. Lock/manifest bytes, Python/backend, selected
   Pixi environment and build inputs bind both intent and cache identity. Changing
   bytes at an unchanged path changes intent; explicit-key reuse conflicts.
   Identical bytes at different author paths retain identity. Accepted-key replay
   uses its original snapshot before re-reading source/catalogs.
3. Compatibility requires declared exact OS/architecture/backend targets, lock
   platform coverage and successful preparation on that worker. Explicitly map
   Pixi platform names to Go OS/arch targets. `cpu` alone does not promise binary
   portability. uv uses frozen installation with the target interpreter; Pixi
   uses `--locked` and rejects manifest disagreement. No re-solving, CPU fallback,
   alternative package/backend, driver install or host-service changes.
4. Reuse stable cache prefixes, single-flight locks, exact preparation launch
   authorization, cancellation/maintenance custody and atomic ready publication.
   The content-derived key includes the full locked recipe, target and relevant
   backend/toolchain identity, not just a filename or lock hash. Successful-build
   evidence is worker/key/fingerprint-specific; another worker must prepare or
   verify its own usable prefix. Cached installation is not native qualification.
5. A real build failure excludes only that worker/target for that recipe identity.
   Status shows environment hash, worker, target, preparation failure and log
   reference; another compatible worker can build/run. The queue retains the
   failed build evidence and tries other compatible builders. If no known host
   can perform the preparation, or every compatible builder has deterministically
   failed, fail accepted demand with one finding and one action. Repair or changed
   inputs require a new submission; terminal jobs do not reopen.
   Unknown/offline capacity is neither success nor a proven failure.
6. Persist failure evidence for the same key/fingerprint across restart. New
   recipe/relevant host identity or an explicit retry of that worker's failed
   preparation permits another build; idle polling does not. Planned stops and
   temporary disk-headroom refusals are visible holds that resume when their
   admission condition changes, not permanent package failures. Retrying a
   build never clears a native qualification failure.
7. Bound storage with existing environment-cache allowance, free-space floor,
   download budget, LRU eviction and use/build locks. Published, partial, retired,
   package/download and asset bytes stay in their existing guarded budgets;
   active prefixes cannot be evicted. No extra quota or fixed build-time cap.
   Builders and payloads never receive `MYCE_TOKEN`. Builds remain trusted code
   under the worker user, not a sandbox.

Prepared environments are shared immutable inputs by contract. Model downloads
and mutable caches belong in attempt scratch or explicit hashed assets, never
the package prefix. Retain bytecode suppression and document cache paths; do
not claim filesystem isolation of arbitrary native code. Hashing, transfer,
installation and baseline comparison stay outside `q.mu`, with
identity/authorization/cancellation rechecks before the small commit.

### One command, verified completion

`myce run --entry NAME --wait --out DIR` (or `--profile NAME`) freezes,
submits, waits in the foreground and downloads declared accepted outputs into
`DIR` with size/SHA-256 verification against `output_refs` (#1343; see the
[adapter guide](adapter.md#run-wait-and-fetch-in-one-command)). `--out`
implies `--wait`. Without either flag, an adapter or profile submission returns
a receipt, and `myce wait` plus `myce result` finish the trip; plain
`myce run -- CMD` waits and streams logs by default, and `--detach` returns the
ID. Success requires command/check success, accepted-attempt publication
and requested local verification. Explicit large-output remote references
remain an option and are reported as remote. Dirty Git source refuses with an
actionable message. The instruction file and explicitly referenced locks may be
frozen uncommitted, separately from the clean selected source. Explicit
directory/snapshot source keeps its existing safety rules and no secret-scan
promise.

Persist keys, frozen intent and receipts before network I/O. A fresh invocation
creates a new operation; resume uses that operation's saved key and explicit
repeat intentionally creates another. Low-level keyed API replay, cancellation
and expiry do not change. Receipts are written privately under the project's `.mycelium/receipts/`
before any upload, and projects ignore that folder. Optional
async delivery retains the completion observer/notifier and private harness
binding; without a working binding, wait in the foreground. Submission is never
evidence of an armed wake or verified result. Plain `myce run -- CMD` without
an adapter keeps its current installed-tool behavior/defaults; it does not
promise portable source/dependencies or checked artifacts. Queue failures never
fall back locally.

### Migration: one-shot converter, fix forward

See [migration: one-shot converter, fix forward](reference/adapter-mechanisms.md#migration-one-shot-converter-fix-forward) for the
approved mechanism and its constraints.

### Remaining work
<a id="implementation-plan-and-acceptance"></a>

See [remaining work](reference/adapter-mechanisms.md#remaining-work) for the
approved mechanism and its constraints.

### Per-item profile runs
<a id="per-item-profile-runs-222"></a>

The profile contract covers per-item runs: one committed profile plus a caller-owned loop submits one ordinary job per item.
Repeatable `myce run --profile P --input DEST=PATH -- ARGS` adds frozen,
content-addressed files alongside the profile's common inputs. Destinations must
be clean relative filenames, distinct from common inputs and each other, with
no file/directory overlap. Relative local paths resolve from the project root.
Source, common inputs and item inputs use the same verified queue file store;
existing blobs are checked and reused rather than uploaded again.

Default job keys include item bytes and appended arguments. A profile's memory
identity instead binds its base command, check, environment, resource
configuration and common input bytes (including shared read-only inputs), excluding appended arguments, item input
names/bytes and the source archive. `memory_class: per_item` opts the appended
arguments and item input identities back into memory comparability. This is
memory estimation only; all existing acceptance, OOM, constraint, hardware and
allowance fences remain. The CLI supplies a frozen `profile_memory` digest in
the job spec; it does not replace the job's full execution fingerprint.

`source: {git: HEAD, paths: [scripts/, pyproject.toml]}` selects literal,
repository-relative committed files/directories. Every selection must exist and
contain regular files; links, submodules, escaping paths and Git pathspec magic
are refused. The archive retains repository-relative paths, executable modes
and raw Git blob bytes, without archive-attribute substitution. Identity covers
only selected names, modes and blob IDs. Literal HEAD's dirty check covers only
those selections; `allow_dirty` still explicitly means committed bytes only.
Omitting `paths` preserves whole-tree source behavior. `paths` is unavailable
for directory/snapshot sources. Unrelated commits cannot change the scoped
archive, generated key or queue spec, so replay from a fresh checkout adopts the
retained job. Changed item bytes or selected source bytes generate a new job.
No generated per-item profile, separate manifest, planner or transport is needed.

## Portability advice and variant eligibility

Agents can run any software on any cluster machine except under hard
requirements, and Mycelium derives its portability guidance from live inventory
(MY-26, MY-27).

### Live portability advice
<a id="amendment-live-portability-advice-325-my-26"></a>

`myce portability` and authenticated `GET/POST /api/portability` provide a
read-only projection of live inventory and selected adapter/extension metadata.
Exact observed and declared-but-absent targets, runtime support, recipe/lock
coverage, preparation/build state, qualification identity and current admission
remain distinct. Unknown or stale facts never establish support, and missing
variants never establish hard software requirements. Advice follows the user's
cluster, includes explicit CPU alternatives and backend-specific device/lock
steps, and preserves MY-19 output parity before splitting. Optional authored
`requirement_reasons` explain constraints without changing admission or
identity of existing registrations. No payload execution, registration or
placement occurs on an advice read. All file hydration, hashing and lock
analysis happens outside `q.mu`. See [portability](scheduling.md) for
interfaces and evidence limits. Registration warnings and variant eligibility
are described below. [Scarce-host yield](#scarce-host-yield) implements
relocation through the existing checkpoint/requeue lifecycle.

### Variant eligibility and production approval
<a id="variant-eligibility-and-production-approval-499-my-27my-31"></a>

The existing exact extension variants, native qualification and
least-valuable-fit placement remain the only implementation model (MY-27,
MY-31). Status and job GET project each declared variant against observed
host/device targets for nonterminal native jobs: target match, exact
qualification identity, freshness, production parity and live
admission/resource blockers. Eligible means admissible now, not reserved or
chosen. Assigned attempts retain their execution and display alternatives as
requiring requeue. Reads never execute, qualify or move work and run outside
the queue lock.

Successful registration returns non-blocking warnings from the existing live
portability projection. Missing recipes, declared-but-unqualified coverage and
unknown inventory remain distinct; genuine authored requirements are accepted.
Queued unrestricted work may receive concrete missing-variant advice when its
qualified implementation fits a scarce host but lacks current capacity, and a
fresh, less-valued host has no recipe. Existing resource floors screen impossible
destinations; an unimplemented backend's actual resources and parity remain
unknown. Policy, preparation, caps and parity holds have their own remedies.
Advice neither changes accepted jobs nor authorizes relocation.

New multi-variant registrations carry server-owned registration metadata
requiring measured MY-19 parity or explicit approval before production
placement. Native checks and experiments remain admissible to obtain evidence.
After comparing outputs to the reference within the project's accuracy
criteria on a machine of the candidate's kind, the project approves that kind
of machine (OS, architecture, accelerator platform; never a host, MY-51) for
its own project/work with that comparison as evidence. A native
baseline proves execution, not cross-variant parity. Historical registrations,
identical replays and their frozen accepted jobs retain their admission
contract. Each variant may publish `quality[WORK][METRIC]` with a frozen
`reference` identity, `value`, `reference_value` and `margin_of_error`
(uncertainty of their difference). Missing/null numbers are unknown. A job
declares `quality_tolerance` as metric to maximum absolute difference; projects
can reuse an adapter entry. An omitted metric tolerance defaults to the
measured margin; explicit zero means exact agreement. All metrics reported for
this work across variants and requested by the job must pass. Complete measured
parity satisfies MY-19; unknown evidence retains the approval fallback for
production and is allowed for experiments. Known failures exclude a variant
regardless of class or approval; native checks remain exempt. The same
eligibility applies to production spillover and storage recovery. Retargeting
persists a positive-parity requirement across later candidate changes and
restart, so one measured digest cannot authorize an unmeasured alternative.
Evidence is immutable recipe metadata, not an evaluation runner or native
qualification. The hold and owner action appear in eligibility/status. Approval
is eligibility, not a job host pin. When the project's approvals contain an
exact work entry, its approved machine kinds constrain every production
assignment and retarget, including historical registrations and measured
variants; every host of an approved kind is admitted. Measurements cannot
authorize an unapproved kind; the project must approve it with passing
comparison evidence. An empty list admits no machine for that production work. Checks and experiments remain available for
gathering evidence.

Host-pin adoption is measured read-only over retained jobs submitted in an
explicit time window, grouped by project and production/non-production: an
explicit `allowed_machines` or authored host-constraint pin is pinned.
Scheduler choices, production bindings and backend constraints are excluded.
This reproduces the host-pin audit's denominator; it does not edit consumers'
jobs. See [the audit command](status.md#read-only-host-pin-audit-499) and its
retention limits.

### Authored requirement explanations
<a id="amendment-retain-authored-requirement-explanations-748"></a>

Adapter/profile submission carries the existing `requirement_reasons` map in
frozen job intent and its private receipt. Native extension snapshots keep their
existing copy. Status and job GET project sanitized constraint names, whether an
explanation was authored or is missing, and the source field in the frozen spec
or digest-identified extension. Raw maps and arbitrary keys/prose retain the
existing observation redaction policy. This does not change scheduling, pins,
qualification, parity approval or historical jobs. Missing metadata does not
establish that a hard requirement is unjustified. See
[status](status.md#authored-requirement-explanations-748).

## Measured work and completion-aware placement

Mycelium learns from its own accepted attempts and uses those measurements to
place work (MY-07, MY-31).

> **Cut down by [MY-49](DECISIONS.md#my-49) (2026-11-12).** Each kind of work keeps one record: its last accepted duration and accepted peak. Throughput curves, sweeps, the onboarding benchmark, calibrated forecasts and batch/wave planning are removed; placement prefers the shorter last accepted duration on each eligible target (MY-31), and declared pieces run as ordinary serial jobs. The subsections below record the earlier design. See [status](status.md#execution-elapsed-time-last-accepted-duration-and-application-progress).

### Comparable observations
<a id="comparable-observations-slice-1-107-approved-2026-10-22"></a>

See [comparable observations](reference/measurement-mechanisms.md#comparable-observations) for the
approved mechanism and its constraints.

### Safe sparse curves
<a id="safe-sparse-curves-slice-2-107-approved-2026-10-24"></a>

See [safe sparse curves](reference/measurement-mechanisms.md#safe-sparse-curves) for the
approved mechanism and its constraints.

### Completion-aware placement
<a id="completion-aware-placement-slice-3-107-approved-2026-10-24"></a>

See [completion-aware placement](reference/measurement-mechanisms.md#completion-aware-placement) for the
approved mechanism and its constraints.

### Repeated plain-command measurements
<a id="repeated-plain-command-measurements-878"></a>

See [repeated plain-command measurements](reference/measurement-mechanisms.md#repeated-plain-command-measurements) for the
approved mechanism and its constraints.

## Declared fan-out and batch planning
<a id="declared-fan-out-and-batch-planning-slice-4-107-approved-2026-10-24"></a>

> **Batch planning removed by [MY-49](DECISIONS.md#my-49).** Declared pieces remain; each runs as an ordinary serial job.

Splitting follows measured capability and output parity (MY-07, MY-19). This
covers explicit homogeneous independent pieces, measured discrete horizon
planning and the model-measurement binding. Generic stage graphs, dynamic
enumeration and timed downstream batching are not part of it.

An extension job may declare `fan_out: {pieces: [{key: ..., args: [...]}],
max_concurrency: N}` with at most 256 unique, stable keys. Its `workload` describes
one piece. The application supplies the pieces; Mycelium never splits a program.
Inputs and output names use the existing artifact contract. A variant's reference
sweep may additionally declare `run`, a command implementing the same measured
internal concurrency mode on a keyed input manifest. Without this explicit
execution contract or safe matching measurements, execution stays serial. One
wave is one ordinary attempt on one component; independent GPU attempts remain
one per device. A failed/lost wave retries only its own fixed membership, with
existing cleanup and stale-attempt fences. Accepted outputs gather by piece key
on the parent; the application owns semantic reduction of the gathered results.

**Explicit discrete-XPU sharing.**
`fan_out.share_xpu: true` opts a declaration and its materialized/retried waves
into measured same-card sharing only with other opted-in waves. Default omission
or `false` retains the existing admission and frozen bytes; ordinary/default-off
work is not enlisted. Single-card discrete XPU, equal tiers, fresh card activity
and memory, measured host/attempt headroom, declared or accepted learned vectors,
growth guard and exact release all remain required. Supporting queue/worker/CLI
code can ship with this off. Production opt-in requires owner-accepted matched
replay benefit and unchanged output parity; baseline and candidate use the same
release, workload and extension digest, changing only this flag. This permission
does not change host allowances, qualification or worker configuration.

Only declared groups use the new planner. Enumerate safe measured counts and
integer waves over a completion horizon, including measured smaller tails.
Filter through existing qualification, owner availability, placement, tier,
fairness, caps, memory and release fences before considering capacity. Charge
shared CPU/RAM once per host, conservatively scale declared per-piece floors,
and never lower a floor below measured high-water memory. Forecasts grant no
reservations: recompute on ordinary queue transitions and pull only currently
admissible work. Busy daily hosts contribute zero. Jobs and calls without a
fan-out declaration keep their existing placement and concurrency behavior.

A model can bind a reference work type and exact frozen extension digest to its
server configuration digest. The sweep explicitly declares that same server
digest; extension-command concurrency alone is insufficient. Shape, model bytes,
server command/arguments, environment, backend, component, allowances, freshness,
qualification and OOM checks must all match. Only explicitly declared, compatible
model-call groups may use these points, bounded by configured `max_in_flight`.
Unbound, changed or unqualified evidence falls back to serial for declared groups;
legacy undeclared calls retain configured admission, including zero-as-unlimited.
No automatic quality-tolerance policy is added; #145 remains eligibility first.

Laptop acceptance includes the research arithmetic, short tails, daily-owner
exclusion, exact affected-piece retries/gather and unchanged undeclared decisions.
The `myce sim` comparison of declared synthetic curves against unsplit and
naive-split arrivals was removed with the simulator (MY-49); Git history keeps it.

### Per-piece accepted-output dependencies
<a id="per-piece-accepted-output-dependencies-175-approved-2026-10-25"></a>

An ordinary batch or extension job may declare one `after: {job: FULL_ID,
piece: KEY, outputs: {LOCAL_NAME: PRODUCER_OUTPUT}}` edge to an already
submitted fan-out parent in the same project. The piece and outputs must be
declared; local input/output names cannot overlap. Fan-out consumers, forward
references, program/check jobs and broader graphs are excluded. Existing
project-scoped submission keys preserve identical replay and reject changed
intent.

The job remains `queued` with a structured `dependency` diagnostic while blocked.
Acceptance of a current producer attempt binds selected verified file references
or NAS-relative paths once, separately from the submitted spec and fingerprint.
Acceptance, binding and readiness commit together before planning. Measured waves
accept all their pieces jointly after complete publication and verification; this
adds no partial-wave streaming. Retry and restart keep the same binding. Missing
inputs cannot authorize command execution: the existing input staging and Started
fences still apply. No network or bulk I/O runs inside the queue lock.

Blocked jobs create no placement, preparation, fairness, preemption or production
hold demand. Runnable wait age and a generated production start window begin at
readiness, using the injected Clock. Explicit client deadlines remain absolute.
Unaccepted pieces fail dependents with `diagnostic_code: upstream_failed` when
the producer fails. Accepted independent pieces and their consumers remain valid
when a sibling fails, including late submissions while the source is retained.
Explicit parent cancellation instead aborts all registered nonterminal consumers,
even after parent success/failure, and fences future dependencies. Terminal
results remain; cancellation release aggregates exact current and old downstream
attempt custody. Cancelling a consumer does not cancel its producer or siblings.

Producer gather is unchanged: every producer piece must succeed; downstream
outcomes neither delay nor fail it. Partial accepted results retain artifacts
through the parent's existing live/terminal window, and bound inputs through each
consumer's window. New bindings to expired results return `result_expired`;
existing keyed replay still returns its original answer. The existing collector
and publication fencing own reclamation. There is no additional store, event
scheduler, fallback, automatic second gather or atomic pipeline publication.

Laptop proof covers CPU commands over stored files and local-directory NAS,
acceptance before gather, restart/replay, retries, failure, cancellation and
retention. Paired synthetic simulations use identical work, policy and seeds,
separate publication/verification and input preparation costs, and complete keyed
output coverage. Earlier downstream execution and lower pipeline makespan are
required in the overlap-capable case; mixed-load production p95 must be exactly
unchanged with zero extra failures or unfinished work. Report contention
regressions individually. Native throughput and consumer integration remain
separate work.

### Whole-parent dependent fan-out
<a id="whole-parent-dependent-fan-out-180-approved-scope-2026-10-26"></a>

Omit `after.piece` to depend on an already submitted fan-out parent **as a
whole**, in the same project. `after.outputs` explicitly maps local input names
to gathered `KEY/OUTPUT_NAME` names. The consumer may be an ordinary job or
another declared fan-out. For a fan-out consumer, explicitly map keyed local
inputs (for example `first/input: first/out` and `second/input: second/out`);
its existing piece args and wave manifest select those inputs. All mapped
inputs are immutable common inputs staged through the existing transport for
each wave. There is no implicit key substitution, input discovery or per-wave
input filtering.

Readiness requires parent success, including every declared child, even when
only a subset of its outputs is mapped. It binds the gathered references and
accepted producing executions by key in `dependency.executions`; the existing
single `dependency.execution` applies only to a selected-piece edge. The queue
persists bindings and fixed child membership in its existing SQLite state. An
upstream failure fails a blocked consumer with `upstream_failed` and a clear
reason, without a runtime attempt. Upstream cancellation aborts nonterminal
dependents and their children transitively, fences future consumers, and
includes their exact attempts in release acknowledgement. Terminal results
remain intact. Readiness timing, retention, transport verification and
stale-attempt fencing retain the per-piece contract; no I/O is added under the
queue lock.

`after.piece` keeps early per-piece readiness for ordinary consumers. A fan-out
consumer uses the whole-parent form only. There is still one existing-parent
edge per submission: no general DAG language, multiple-parent joins,
forward references, dynamic enumeration, timed
accumulation, atomic graph submission, or automatic semantic reduction. Broader
stage planning is later work.

The #1650 implementation task extends this same edge to ordinary producers:
omit `piece`, map declared output names directly, and bind only after success
and verified acceptance. `dependency.execution` records that producer's exact
attempt. Existing readiness, transport, retention, replay and cancellation
custody apply; no additional workflow engine is introduced. Internal pieces,
check jobs and program jobs remain excluded as sources. Read-only resolve
checks the same source/output rules, and rejected `myce run` intents preserve
the typed queue reason in stderr and the receipt.

## Enrolling a machine

Adding a supported machine type takes one command (MY-08).

### One-command enrollment
<a id="one-command-enrollment-b0b1b6-108-2026-10-21"></a>

See [one-command enrollment](reference/enrollment-mechanisms.md#one-command-enrollment) for the
approved mechanism and its constraints.

### Onboarding preflight
<a id="one-command-onboarding-preflight-b2b4-108109"></a>

See [onboarding preflight](reference/enrollment-mechanisms.md#onboarding-preflight) for the
approved mechanism and its constraints.

## Upgrade and state formats
<a id="upgrade-and-migration-80"></a>

There is one runtime, and only the adjacent release interoperates (MY-03, MY-42).

v3 is the only runtime; no pre-v3 worker-report, capability/profile, empty-attempt or broad host-cleared fallback. Old or missing worker/report protocol headers, including checkpoint reports and worker events, receive HTTP 426. Within host-launch-v3, current one-attempt workers remain compatible with the [keyed wire extension](#placement-protocol-and-compatibility); upgrade the queue before keyed workers. Exact execution ownership and release evidence remain mandatory across restart/loss; a keyed report never silently clears omitted obligations.

**Pre-v3 formats are refused; operators archive and install fresh.** Pre-marker queue state is not imported. Stop the old services and prevent their restart, archive their queue/worker data directories and shared host-control directory, preserve configuration and secret references, then install matching current binaries and units. All current queue, worker and cache state roots carry `.myce-format` with `v3` and a newline. One startup gate checks every root before any filesystem write: absent or empty roots receive a fsynced marker first; non-empty roots without that exact marker return `legacy_state_present` naming the root. No content-specific legacy detection or migration of pre-marker development state is supported. There is no pre-v3 converter or dual runtime decoder. Split-worker conversion is retired under MY-42. Preserve archives, checkpoints and unique evidence; this rule does not authorize deleting them or changing production outside the queue.

Installation overlays explicit options on saved current configuration, preserving
queue address, NAS, names, GPU environment and secret references.
Matching binaries and service definitions replace each selected installation.
There is no local start condition, stage-only host gate or timer installation.
Linux workers start as ordinary-account services with `Restart=on-failure`,
`Delegate=memory`, `KillMode=mixed` and bounded stop. TERM goes through the worker's
recorded attempt-stop path; the manager retains the final group-wide KILL. Installer rollback
restores files, modes and actual manager registration/activity, and reports any
incomplete restoration. Use queue maintenance to keep admission disabled across
an upgrade. Saved configurations containing removed local-control fields must
be rendered in the current schema; preserve the old config and secret references.

## Fleet upgrade
<a id="amendment-one-command-seamless-fleet-upgrade-244"></a>

Updates across all hyphae are fast, easy and seamless (MY-21). An explicit
authenticated `myce fleet upgrade [--release VERSION]` may replace only
Mycelium's own installed binary and restart only its existing service unit on
enrolled hosts. Only a signed release already approved with `myce release
approve` is eligible. Never silently update, downgrade, change GPU drivers, OS
or packages, or use SSH. The downgrade credential scope remains separate and
unchanged.

It is the one rollout path (MY-35, MY-49); the procedure is in
[operations](operations.md#rolling-fleet-upgrades) and the failure codes in
[fleet upgrade internals](reference/fleet-upgrades.md). The operation lives in
the existing SQLite queue, continues after the CLI exits and survives the
queue's own restart. One host is active at a time: idle hosts first, then busy
hosts, then hosts running production, and the queue's host last, its worker
before its queue. For each host the queue:

1. drains it, as the drain owner; a host an operator drained or disabled keeps
   the operator's policy and is never resumed by the rollout;
2. waits for its running work (MY-35). Draining withdraws unstarted work and
   blocks admission. Normal urgency never stops production or non-resumable
   work. Once only started resumable non-production attempts with a checkpoint
   contract remain, each gets a fixed natural-completion window based on its
   last measured planned stop-to-release duration, capped at the existing
   two-minute checkpoint grace. Without completed evidence, the window is one
   checkpoint grace: it prevents indefinite admission downtime, at the cost of
   up to one grace before signalling (MY-34). Waiting longer would cost more
   downtime than the stop budget; this is no completion forecast (MY-49).
   At expiry, planned-leave SIGTERM gives two minutes to checkpoint before
   SIGKILL, including GPU work. The job requeues with its completed attempt and
   progress preserved, no loss/retry charge, and `maintenance_resume`. Status
   lists each exact attempt, deadline and basis in `planned_stops`, then signal
   intent in `stop_requests`. Plans survive restart without extending the wait;
   natural completion removes them. Preparations, model calls/streams, engines
   and cleanup still finish naturally, and exact release custody gates every
   service update even after grace expiry. A normal update leaves a host enabled
   while draining it would strand open production work (#1387);
3. has each service fetch the release through the queue's authenticated
   endpoint and verify it against its pinned publisher key, the signed size and
   SHA-256, version and protocols, and on macOS its code-signing identity; a
   finite helper of the same binary replaces the binary atomically and restarts
   the existing unit;
4. requires a fresh runtime proof from a new process with the release's exact
   bytes, a healthy service manager and runner smoke, stable for the
   enrollment window;
5. resumes only the drain it took, then advances.

`--urgency critical` (MY-39) uses the planned-maintenance stop on the selected
host instead of waiting: resumable work checkpoints and is requeued without a
retry charge, and status lists each stop request. An unhealthy result stops
the operation with the host's drain kept and later hosts untouched; nothing
rolls back (MY-03), and a rerun is a new operation that adopts the left drains.
Decisions use the injected Clock; network, hashing and service probes run
outside the queue lock. Fake-clock tests cover ordering, drains waiting on
running work, critical stops, failure-stop and rerun; a real-process
end-to-end rolls two workers and the queue between locally signed releases
while each worker's production command, and a model stream on a drained
worker, finish untouched. Tested releases roll out without separate authorization (MY-28);
hardware, OS, driver and package changes stay outside that authority, and
native manager/signature acceptance remains separate (MY-21).

### Open production work
<a id="amendment-to-the-fleet-upgrade-open-production-work-1009-my-35"></a>

A project may declare open production work as an expiring lease (project, key,
optional stages with job placement fields, TTL). Under MY-35, planned drains,
operator or fleet-owned, keep at least one enabled host able to start each open
lease stage and each queued production job; the operator drain refuses with
`production_work_open`, and the fleet upgrade leaves the host pending with
admission open. `disable` stays the explicit override. A running upgrade's own
drain, an operator drain and a stopped upgrade's retained drain all count as
temporary capacity waits: production keeps its relative start window paused
until capacity returns. Explicit `start_by` and `give_up_at` still bind;
deliberately disabled hosts still fail new production promptly (#1573).
Leases expire on their own, carry no project names
in code or configuration, and grant no priority or capacity.

## Shared interactive host profile
<a id="shared-interactive-host-profile-389"></a>

The opt-in [shared-host profile](operations.md) keeps orchestration and
interactive machines usable for fitting native CPU commands at lower priority,
with positive existing CPU/RAM allowances, an explicitly measured interactive
reserve, historical/live RAM admission and existing last-resort placement.
Agent default under MY-45: the shared interactive profile excludes `batch`,
`test`, `ci`, `rollout` and `eval` classes and same-named owner tiers at any
fleet pressure. Light work still fits within the measured host allowances. GPU
admission uses the existing component exclusion path. Drains, exact launch
fences and cleanup remain authoritative. First phase uses prepared
environments; extensions/new preparations remain withheld.
Shared-profile commands and sidecars use background taskpolicy on macOS; common
runtime concurrency is bounded by declared cores. Linux's delegated execution
subtree enforces aggregate CPU weight/quota outside worker control threads.
Fresh OS memory-pressure/PSI and foreground-class scheduling-latency
measurements with amplitude hysteresis stop work through exact-attempt planned
preemption and the existing checkpoint grace, then requeue the same logical job
without infrastructure loss. Unknown/stale samples withhold admission. Status
names the pressure reason and enforcement mode. macOS CPU time and RAM remain
unenforced; QoS/concurrency hints are cooperative, and native
Linux/representative owner-pressure qualification remains separate. See the
shared-host guide for thresholds, evidence and costs.

## Health alerts
<a id="minimal-health-alerts-v1-amendment-200--248"></a>

The queue records health events from its existing check-ins, maintenance clock and
attempt outcomes; no agent or user polls for alerts. V1 detects only measured disk
free space below the existing guard (1%, minimum 256 MiB), an unhealthy or stale
host, a production attempt lost to machine silence or a start/observation/cleanup
timeout, and
checkpoint/NAS/file storage unavailable continuously for a configurable
duration (default five minutes). Planned leave and host maintenance suppress host
alerts; planned and operator stops do not count as lost production attempts.
A wholly quiet fleet still emits stale-host alerts even while loss/retry decisions
wait for connectivity. Startup's existing restart grace applies to stale alerts.

Events commit with queue state and appear in authenticated status as
`health_alerts`. A condition emits once per episode, with a 15-minute cooldown
across flaps; production losses emit once per exact job attempt. The last alerted attempt is
persisted on the job independently of the bounded event window. The queue retains
the latest 256 delivered events (or 256 total when no notifier is configured).
Pending configured deliveries survive restart and remain until acknowledged;
operators must repair a broken receiver to drain that backlog.

An optional queue-local executable uses the existing completion notifier's
`--check` and JSON-stdin/exit-status contract. Each delivery contains only a stable
`delivery_id`, fixed `kind`, host/job/attempt identities and queue timestamp, plus
`delivered: false`; no raw diagnostics, paths, job argv or fleet token. The
operator's hook routes to their own wake or notification channel. File-store availability is sampled by the existing maintenance tick; it adds
no probe or snapshot lock to ordinary control transactions. Execution is
outside queue locks and never blocks worker control. Delivery is at least once:
receivers deduplicate `delivery_id`. A successful hook acknowledgement is persisted;
a failed/ambiguous delivery retries the same identity after one minute. Hooks are
limited to one invocation per second and 30 seconds per invocation/check, preventing
a burst or wedged hook from overwhelming delivery or stranding queue shutdown.
These transport limits cost at most one second per queued notification and require
a hook to deliver within 30 seconds. Health thresholds use the injected queue clock.

Not implemented: #200's extension-configuration or architecture-caused
workload-failure detectors, trend analysis or automatic issue filing. Those
broader acceptance criteria remain open.

A pending cleanup
result can alert before release; neither alert path changes execution state or
releases resources. Stale attempt diagnostics cannot emit a production-loss alert.

A suspect worker ([adaptive worker loss](#adaptive-worker-loss)) also raises an
owner-attention health alert.

## Testing

Keep laptop tests for queue policy across both device workers, Started/cancel
races, queue and worker restart, stale/missing reports, unknown release custody,
RAM/debt and sidecar limits, exact stop/recovery/result, files and privacy.
Use injected clocks for queue decisions. `myce dev` runs the real API and command
runner with fake GPU inventory; standalone e2e checks a command effect, exact
release, queue maintenance and complete process/listener teardown.

Linux daemon, GPU, memory-enforcement, manager and reboot checks are separately
authorized native acceptance; laptop fakes do not qualify hardware. See
[component probes](development.md) and [native checks](development.md).

## Scope and later work

Supported v3 features include plain batch commands, queue-managed model calls and HTTP servers, software environments, production pins, fair sharing, enforced memory ceilings and native sidecars. Capability profiles, authenticated discovery/joining, qualified extensions, polite daily-use participation, optional measured placement and verified file transport are also implemented. An extension describes a kind of work or program; qualification is tied to its exact version and a matching native baseline. Hardware facts or benchmark timing alone do not qualify it. Unknown required participation signals withhold work; unsupported hardware is reported rather than silently using the CPU.

Explicit NAS roots remain supported for models, shared files and checkpoints. The queue file store also supports jobs without a NAS. Ordinary commands and servers do not need to become extensions. These are supported v3 paths, not a second runtime or a reason to restore older worker/report protocols.

CPU-only scheduling is implemented. [One machine, one worker](#one-machine-one-worker)
defines the machine worker, concurrent attempts and cards per job (MY-30);
multi-machine/gang work and vision expansion remain deferred under #17, #22 and
#69. Automatic dependency detection for arbitrary projects and a backup queue
service also wait; a shared queue is a direction, not yet a design (MY-40). Jobs
name their software recipe or lock file and state their needs; memory may be
left to Mycelium ([learned memory](#learned-memory-by-default-and-bounded-first-runs), MY-31).
