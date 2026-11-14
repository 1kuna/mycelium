# Mycelium v3: approved design

This is the approved design and the only product spec. Each topic states the
current design once. The maintainer's rulings are in [decisions](DECISIONS.md)
and are cited here by MY number where they govern. The text was consolidated on
2026-11-12 (MY-49) from its dated amendments without changing what it says.
Approval dates, issue receipts and replaced wording stay in git: see the
[text before consolidation](https://github.com/1kuna/mycelium/blob/e0488c0c5034b5a57827ceebfa69f770c060bb8f/docs/design-proposal.md) and the section map in
[#1433](https://github.com/1kuna/mycelium/pull/1433). Old heading anchors still
resolve here.

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

Loss detection is measured, not a fixed timeout (MY-31, MY-34).

Loss uses the current session's send interval and observed arrival/path variation,
measured with the injected queue Clock. Workers advertise their actual interval
through an optional header compatible with older strict JSON decoders. An older
worker without that header first establishes cadence from successive accepted
wall-time arrivals; queue occupancy cannot compress its expected send period.
The second accepted arrival learns cadence before admission, so a headerless
worker waits at most one observed send interval plus actual control-path delay
for its first offer after a queue cutover. Startup recovery reports can supply
these samples before ordinary admission. An empty keyed reply is idle, with no
assignment to launch; existing attempts continue reporting and retain custody.
Retained engines receive retention-only plans and their existing cleanup
deadlines during cold qualification; those plans cannot start a missing engine.
Until then automatic loss is unknown and capacity is suspect, even with a saved
operator minimum, so no new attempt can acquire an unbounded unknown loss
allowance. The allowance is at least two send intervals (the
expected arrival and one missed-arrival confirmation), extended by the shared
RFC 6298 mean/variation estimator. It has no class-specific default duration.
Explicit old timeout settings remain minimums, never permission to count queue
delay as silence; zero selects automatic timing. Existing saved settings survive.
Each accepted transaction also saves its session's frozen contact allowance for
offline conversion. That stopped-queue observation expires against the original
accepted wall timestamp; opening a queue does not renew it. An older database
without this evidence needs the existing verified-empty drain path. Running
queues still require live contact and startup reconciliation.

The queue accounts the union of all queue-mutex occupancy, including readers,
with constant-size local counters. This time is unavailable control service and
is excluded from silence. A newly slow transaction is protected immediately,
without first training a latency percentile. A current-session check-in protects
that worker while the server processes or writes its response, including work
outside the mutex; it cannot protect a dead peer. Rejection removes that credit.
Cancellation retains a reconnect opportunity following the worker's existing
retry schedule, without changing accepted LastSeen. Unsettled synthetic arrivals
expire after the measured allowance. Stale sessions cannot renew current contact.

For a normally responding queue and an ordinary one-second worker, detection
takes roughly two seconds of service opportunity, plus learned variation and the
next maintenance turn. An observed first one-second gap initially gives three
seconds under the estimator; further steady samples converge to the two-interval
floor. Under overload the wall duration additionally includes actual queue
occupancy. For example, a 31-second blocked transaction adds 31 seconds, not a
charged infrastructure failure. Suspicion uses wall time since the last current-session arrival, with an
allowance of at least two observed wall arrival intervals plus measured variation.
That wall cadence includes delivered queue service cost even for a worker that
advertises a shorter send interval. It remains separate from the opportunity
allowance for confirmed loss; otherwise a healthy slow control path could
withhold every new offer. Before the second arrival, the completed response's
measured wall cost extends the advertised interval. Recovery inference excludes
late pending arrivals but retains this completed delivery evidence, so normal
slow delivery can establish cadence instead of repeatedly resetting it.
Confirmed loss requires both a missed wall arrival and the opportunity
allowance: available service alone cannot make an expected arrival overdue.
Pending NAS inspection delivery uses the same wall
allowance, including in detached status/resolve views. Even during continuous mutex saturation, a dead sender
becomes **suspect** after that allowance plus the next maintenance/observation
turn. An old in-flight request cannot suppress suspicion. Suspect workers are
never shown as healthy, receive no new attempts or preparation launches, and
raise an owner-attention health alert. New arrival evidence clears suspicion.
Suspicion never interrupts an existing attempt or charges loss/retry.

A worker's check-in loop also does local durable work between check-ins, and
under host load that work can delay its next check-in past the allowance while
the process is alive (#1450). The worker's stop-event long poll runs beside
that loop, so a current-session poll also counts as contact for confirmed loss:
while it is open, and for the allowance after it ends normally. A dropped
connection gives no credit after its start, so a dead process is fenced at once
and a silently dead host after at most the server's existing poll window. The
poll proves the process, not its loop, so its credit is bounded: check-in
silence beyond the measured filesystem-operation stall backstop (30 s, #289),
or twice the session's longest completed check-in gap if that is longer, is
still confirmed loss, so a wedged loop's attempts are retried. Suspicion and
cadence learning still use check-ins only, so a stalled loop is still suspect
and receives no new work.

Continuous saturation or a wholly unreachable fleet has no finite trustworthy
**confirmed-loss** bound: retain custody until control service or connectivity
supplies evidence. If even maintenance cannot execute, neither diagnostic
publication nor a useful relaunch can have a finite execution bound. Suspicion
is computed at every subsequent observation; its elapsed-time threshold does
not stop with the control clock. The explicit startup recovery grace also excludes blocked queue service so
workers can reconcile before it expires. Genuine loss preserves exact
attempt fencing and the existing retry budgets, and never proves release.

Fleet, maintenance, placement, inventory and stale-host diagnostics use the same
confirmed-contact and suspicion calculations, frozen at status capture time. LastSeen remains an actual
accepted queue timestamp. Physical memory/disk/pressure/probe sample validity
still ages independently; liveness never manufactures fresh sensor evidence.

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

Device IDs are unique canonical nonnegative decimal strings, stable against
hardware identity (CUDA UUID, PCI BDF or Level Zero UUID), not discovery order,
driver version or display name. Recheck runtime identity at startup and before
every member launch. XPU discovery and every member force
`ZE_ENABLE_PCI_ID_DEVICE_ORDER=1`. A mismatch withholds that device with
`device_identity_mismatch`; unavailable probes use `device_probe_unavailable`;
missing budget uses `device_budget_unavailable`. Re-evaluate these conditions;
matching identity restores eligibility. Accepting replacement hardware requires
settling the old custody, preserving its spool, a fresh `--state-dir` and native
requalification. Other healthy components remain usable.

Worker `--device 0,1` offers two independently selectable cards; omitted means
all. Unmatched allowances expose `device_selector_unmatched`; no matched card
refuses startup. Fixed XPU budgets/reserves stay per device and never substitute
host RAM or unreliable telemetry. Unified capacity uses measured memory when
`--memory-gb 0`; a nonzero allowance caps it. Live headroom and reserves still apply.

Assignment atomically reserves host cores/RAM and zero, one or several GPUs of
one host with their VRAM in SQLite before delivery. Preparation, stopping, engines and unconfirmed release remain
charged exactly once. Discrete VRAM and additional host RAM fit separately;
unified allocations share one physical pool. Host reserves, launch slack and
preparation/core hand-back are machine-wide. Lowered allowances expose debt and
withhold new work; they never resize running ceilings. Omission, elapsed grace,
service exit or missing inventory never refunds custody.

**Shared GPUs (MY-30, MY-31).**
A GPU holds several single-card batch attempts when their memory fits: the
newcomer's declared or learned device vector and its host vector must fit the
card's and host's existing headroom beside every co-tenant's charge, unified
hosts counting the one pool. A finished co-tenant stays charged until exact
release. A learned host
peak without a device measurement retains a recipe-default device figure;
attempts with that figure keep the card to themselves until
an accepted device measurement supplies their learned vector. The
envelope's `source` identifies `GPU recipe default` in this case. Model-server,
extension-check, fan-out, exploratory first-run
and multi-card attempts keep the card to themselves, as does a hint that names
no device memory (its device figure is only a default), and a newcomer never
joins a higher tier's attempt. For placement, equal tiers never preempt; a higher tier stops the
fewest co-tenants whose release frees enough on its selected host/cards and
starts there only after that exact release; a multi-card demand stops every
co-tenant on its selected cards. On XPU and CUDA cards (including unified-memory
CUDA), a higher tier instead stops every lower-tier co-tenant and waits for each
exact release even when memory would fit beside them. XPU teardown can reset the
card's engines; CUDA
co-tenants can grow past their reservations and starve a higher-tier model load
without reliable fresh per-process growth evidence. The cost is lost mixed-tier
co-residency on XPU and CUDA. Equal-tier packing, including experiments packing
among themselves, and other platforms are unchanged. Waiting reasons name
`device N shared with K attempts`. CPU attempts can run alongside GPU work and one another within host resources and caps;
model engines retain their usual shared-memory admission. `needs.gpu.devices` narrows candidate IDs, not the
number allocated. CPU work cannot have a GPU selector. Malformed/duplicate IDs,
empty explicit sets and invalid counts use `gpu_selector_invalid`. An explicit
count above one and multi-card model masks use `gpu_count_unsupported`, never
partial reservation or reduction. Unknown/offline selections remain queued with
`no_matching_device`.

Shared-card memory growth uses fresh per-process device samples from check-ins
(younger than ten seconds, using the injected clock). If the sum of each
tenant's larger measured use or reservation, plus resident model charges,
exceeds the card budget, stop the lowest tier; among equals choose the tenant
most over its reservation. This safety stop may displace an equal tier. It
retains normal checkpoint grace and exact-release custody and requeues without
spending a failure budget. Each grower's measured peak × 1.25 becomes a durable
device-memory floor for its next placement; running reservations stay fixed.
Single tenants are unaffected. Every tenant needs fresh per-process evidence
before the guard acts. Missing or stale samples establish no growth, and the
running job's reason reports unavailable per-process evidence. Host memory-drop
estimates cannot attribute co-tenants and do not trigger this guard. At
check-in send time, workers read the sampler's latest per-attempt device
counter independently of cached actor reports. Slow actor log/result I/O must
not hide fresh device evidence. This refresh retains the sensor's original
timestamp; it cannot make a stalled sensor fresh or renew old host-memory
credit.

**Cards per job (MY-30).**
`memory_gb` (declared or learned) decides the cards. Work one card of its host
can hold gets one card. Work no single card holds, but N same-backend discrete
cards of one host hold in equal shares, gets the fewest such cards: the N with
the most free memory, reserved together with one release record, each charged
`memory_gb / N`, host RAM and cores once. All are visible to every member
(`MYCE_GPU_COUNT`, `MYCE_DEVICES`); preemption reclaims them as one attempt, and
a card stopped for a waiting multi-card demand is withheld from equal and lower
tiers until it starts, while the host's usable cards can still hold it. Model-server and extension-check jobs keep one
card; unmeasured `memory: auto` first runs explore one card, and multi-card
attempts do not feed per-card memory learning. Extension work needs every card
qualified; no separate multi-card parity check exists yet. Using more cards for
speed when one fits is not planned (MY-49 removed work curves). Cross-machine gang execution
remains deferred.

### Attempt fencing and containment

Every supervisor, result, log, checkpoint, launch, Started, stop and release is
bound to `(machine, session, job, attempt)` and its selected device. Private
per-attempt environments never mutate the worker environment. CUDA uses the
assigned UUID; XPU uses the verified physical affinity mask and renumbered
selector (one assigned card is in-job device 0); Metal uses device 0. Hide other
families. CPU uses empty `CUDA_VISIBLE_DEVICES`,
`ZE_AFFINITY_MASK=2147483647` and `ONEAPI_DEVICE_SELECTOR=!*:*`, qualified by actual
runtime enumeration. Unavailable CUDA/XPU select-none evidence withholds CPU
with `cpu_accelerator_isolation_unavailable`; loss before launch requeues without
an infrastructure loss. macOS CPU work remains cooperative, with Metal isolation
explicitly `not-enforced`. No child inherits `MYCE_TOKEN`; authored environment
cannot widen device visibility. These masks are not a hostile-code sandbox.

Each native attempt has its own control/main/sidecar subtree and verified
aggregate/member ceilings before exec where supported. Engines have independent
subtrees. Attempt stop/OOM/cleanup never targets siblings or the worker service.
Service stop intentionally contains all descendants (`Delegate=memory`,
`KillMode=mixed`). Unsupported platforms remain admission-only and
required ceilings refuse them. Core reservations do not enforce CPU time.

One machine heartbeat uses its session's adaptive contact evidence for every
active class. A genuine worker loss affects every owned attempt independently,
including each production retry counter (at most two infrastructure requeues).
Unknown release stays charged; a timeout cannot authorize overlap. A single
attempt's failure does not mark its neighbors lost. Control/heartbeats and bulk
preparation/result I/O remain independent; bulk I/O never holds the queue lock.

Spool reopen reconciles every saved attempt and buffered result before new
admission. `recovering: true` blocks batch, model and preparation admission at
planning and launch without allocating an attempt or consuming a retry. Exact
cleanup/replay continues; all machine targets are blocked while other machines
remain eligible. Persist `recovering_since`; continuous recovery reports
`recovery_needs_attention` after two minutes, with no heartbeat reset. Clearing
recovery clears the warning. Legacy GPU custody lacking device evidence blocks
all GPU targets until exact cleanup; do not invent a component.

### Placement, protocol and compatibility

Use one shared planner for jobs, models and status. Enumerate
(machine, device) GPU targets and (machine, no device) CPU targets. Qualification,
engines, pins and readiness are exact-device identities; a neighbor's check never
qualifies the selected card. Disk weights may share caches, not engine custody.
Policy/allowlists/platform/device restrictions intersect. Prefer free capacity
before stops, retain single-sufficient-victim/greedy/reverse-prune selection and
its nonoptimality, and count useful host resources only once. Fairness remains
allocated device-seconds or CPU core-seconds; grouping cards resets no usage/caps.

Pending stops are planning projections only. For machine workers, lower-tier
grace/kill releases have credit only before `StopAt + preemptionGrace()` (ten
seconds by default). Confirmed exits retain credit until fresh inventory or that
same bound. Neither heartbeats nor completion extend it. Then seek useful
capacity elsewhere; admission still requires exact cleanup acknowledgements.
Physical-host production holds fence every device and CPU resources; an opt-in
device scope keeps their release custody and leaves reclamation to this planner. Maintenance
outranks holds, preparation and Started. Planned maintenance/preemption preserves
infrastructure counters; independent stage failure remains failure, completion
wins over stop intent, and cancellation wins over both.

`host-launch-v3` plus echoed `attempt_protocol: keyed-attempt-v1` carries a
collection of exact keyed reports/controls without a wire cardinality cap.
Strict decoding refuses mixed forms, duplicates, invalid sessions or revision
mismatch before mutation. Keyed workers require the echo and fail closed with
HTTP 426 `worker_protocol_invalid` against older queues; upgrade the queue first,
preserving saved custody. The bounded current unkeyed v3 adapter remains for
supported old workers, restricted to one attempt/device and the common host
ledger. Pre-v3 is refused. No automatic downgrade. See [wire details](development.md#current-worker-protocol).

New assignments use physical `machine` plus additive `allocated_devices` (empty
for CPU). Retired identities exist only for exact historical result/release
handling and explicit replacement diagnostics. New intent naming them fails
`machine_renamed`; never infer aliases from `-gpuN`. Existing keyed replay returns
the original logical job before new-placement validation and preserves its
fingerprint, counters and checkpoint generations. Legacy sessions and a machine
worker cannot coexist on the same host. The unkeyed adapter can retire only after
all hosts convert and the rollback window closes; historical custody remains.

Split-worker conversion and rollback are below the [MY-42 release floor](DECISIONS.md#my-42)
and remain only in source history. Retained historical identities and exact custody
still bind. Keep native device isolation, reboot/manager, driver fault and
simultaneous memory qualification separate from laptop proof in
[development](development.md#native-acceptance-checklist).

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

Hosts running only Mycelium jobs give their memory to jobs (maintainer,
2026-11-11; MY-34). The unified CUDA physical free-RAM floor is compared with
live used RAM, which already contains the OS, driver and idle-service footprint.
A measured allowance already excludes that footprint, so physical RAM minus a
measured allowance would subtract it twice. With a measured allowance and no
shared-interactive profile, the floor is the existing OS/driver reserve alone,
and the allowance still caps reservations. The same holds for an authored
allowance: an allowance of any origin caps charges and is never a floor, so the
idle footprint is not withheld twice (orchestrator decision, 2026-11-11, under
MY-34). Only shared-interactive hosts keep the larger of the reserve and
physical RAM minus the allowance, as their stricter owner protection under
MY-45. The reserve is the configured value, with no built-in minimum (MY-34,
MY-48).

### Charging running attempts by use

A running attempt is charged the memory it uses, not its declared envelope,
only where a guard contains its growth: started native CPU-only work with a
declared size, on a dedicated host with Linux cgroup v2 ceilings or the Darwin
RSS guard, with learned evidence for that work on that target and a fresh
exact-attempt sample. The charge is the larger of the learned need, the
attempt's high water and its live forecast, plus the measured overshoot before
containment acts (on Darwin, growth rate times the guard's reaction window),
bounded by the enforced ceiling. On a unified CUDA host with cgroup v2
ceilings, GPU work is charged by use too: measured on a GB10, the cgroup
contains host, managed and pinned allocations, while device allocations
(`cudaMalloc`) escape it and RSS but appear in the per-process NVML sample and
in host availability. Each share is charged the larger of its accepted peak on
that target (without the envelope's uncertainty), the attempt's high water and
its forecast, from a fresh per-process device sample, bounded by the envelope.
Metal work with a declared size is guarded and charged on `phys_footprint`:
measured on M4 Macs, Metal buffers raise it 1:1 while RSS sees none of them.
The Darwin worker sums it over the attempt's owned tree at the RSS guard's
cadence and stops the attempt past its unified envelope (device plus host plus
slack) after the same consecutive samples. The attempt's peak records RSS as
the host share and the footprint beyond RSS as the device share, so unified
sizing can learn Metal work. Its charge is the larger of those accepted peaks,
its high water and its forecast, plus drift and the guard's measured overshoot,
bounded by the envelope plus that overshoot. First runs, auto-sized Metal work,
non-cgo Darwin builds, discrete accelerators, multi-card attempts,
interactive hosts and missing, stale or estimated samples keep the envelope.
When by-use jobs grow back into memory lent to their neighbours and live free
memory can no longer absorb the growth expected before a stop completes, one
job yields through the planned-stop path: the lowest tier first, then by-use
work, then the most recently started. It checkpoints if resumable, then
requeues with no retry charge (`memory_overcommit_yield`). Production yields
only to protect other running production work on the host, and only if
resumable.

### Job memory envelopes
<a id="amendment-job-memory-envelopes-146"></a>

**Local host calibration (#1044).** Each comparable observation retains its
own physical-demand floor and measured range. With complete non-reclaimable
CPU, xe or unified CUDA host evidence, its forecast is range maximum plus the
larger of that range's spread, measured sampling growth and launch drift. Keep the
cumulative resident/driver peak plus launch drift as another floor, including
when the range restarted after incomplete coverage. Take the largest of these
per-observation bounds. Do not add a newer range's variation to an older,
already larger cumulative peak, or treat differences between allowance-key
maxima as another measured range. Incomplete observations independently retain
their total-cgroup peak plus sampling growth, with launch slack added separately;
a complete neighbour never discounts them. These margins cover measured between-attempt variation,
growth before the next sample and snapshot-to-launch drift, respectively.
They are empirical bounds, not guarantees about an unseen tail; pressure,
OOM recovery, physical admission and release custody still bind.

Unified CUDA complete evidence includes the cgroup non-reclaimable counters,
exact-attempt proc pin/lock readings and a conservative absolute whole-host
physical residual for uncharged driver allocations. Missing or interrupted
coverage retains total-cgroup sizing. The residual keeps unrelated occupancy
and possible device overlap; it is an upper bound, not a per-client NVIDIA
host-allocation counter. Device sample coverage, extrema and step variation are
retained per attempt for native qualification. Device peaks and their conservative
growth margins remain separate and are added once to the host charge. Retained independent maxima
cannot establish a physical union or justify subtracting cache or device
overlap. Native coexistence and output proof remain separate from calibration.

Explicit `needs.host_memory_gb` and GPU `needs.memory_gb` retain their current
admission meanings and defaults, including production. Optional `needs.memory:
auto` first selects fresh accepted whole-attempt peaks ([comparable
observations](#comparable-observations)) for the same work, recipe, input shape
and machine/component, times 1.25. Host RAM uses the largest comparable
accepted peak across sibling devices on the same host, target and memory layout
when the environment matches and the source device's hardware/backend
fingerprint still matches its current inventory (including the Linux kernel).
Device-memory evidence retains its per-device identity and transfer margins.
For local Linux CPU work, at least two complete, unpressured non-reclaimable
attempt peaks per contributing observation enable peak plus the largest of the
observed range (maximum minus minimum), 25% of the retained component peak, and
the existing snapshot drift floor (at least 128 MiB). Two attempts establish a
range, not confidence in an unseen tail; retain the existing 25% allowance for
unobserved growth between 200 ms samples. Keep all comparable resident peaks as
a floor. Missing/incomplete local observations keep total-peak sizing. GPU work
uses the same conservative total-peak fallback unless discrete Linux xe/XPU
attempts also retain complete per-client driver system/GTT allocations,
independent runtime xe debugfs GTT-manager usage, and whole-host
MemAvailable-drop evidence from before payload launch. For that case, use the
non-reclaimable peak plus the peak of max(driver allocations, whole-host drop
minus synchronized current components, zero), also floored by whole-host drop.
Retain all resident/driver floors, then the same range/25%/launch-drift margin.
Two complete attempts per contributing local observation remain necessary. This
is an observed envelope; native pressure-free concurrency and output parity
require separate proof. Without local history, use the same recipe/work/ shape
from another machine: prefer the same OS/architecture/accelerator/memory-layout
family, times 1.25 × 1.5; otherwise times 1.25 × 2. Label both as a
cross-machine estimate. For CPU work, accepted same-OS evidence takes
precedence over the conservative foreign-OS prior. Constrained same-OS
observations are lower bounds only and cannot replace that prior. If the prior
exceeds every qualified target-OS allowance, fresh comparable same-OS
lower-bound evidence (with the existing 25% margin) and any submitter hint must
fit before permitting one bounded exploration on the largest qualified
allowance. Keep normal launch slack, drift margin and live admission guards.
Assignment durably consumes this exception per frozen work/configuration and
target OS, including across resubmission, history collection and restart. A
clean result learns through the single eligibility gate; OOM gets no retry, and
constrained or otherwise ineligible results cannot reopen the exception.
Successful constrained attempts retain their ordinary job outcome. The foreign
prior remains available with the capacity-remedy diagnostic. Claiming at
assignment may spend a probe before launch, preventing duplicate launches after
a lost offer. Qualification and output parity remain unchanged. For a
discrete-GPU target with only one accepted sample from another target/layout
family, the same durable probe applies when the transferred host estimate
exceeds every eligible allowance in the target family. The measured peak plus
25% and any host hint must fit the largest allowance after launch slack and
drift margin. Keep total-cgroup sizing when complete evidence cannot
distinguish reclaimable cache; a smaller resident figure alone is not enough.
Assignment consumes one probe per frozen work/configuration and target
OS/architecture/backend/layout. Accepted target-family evidence replaces the
prior; OOM gets no retry and ineligible results cannot reopen it. Unified
targets, multi-card work and fan-out retain their existing rules. Next use an
optional submitter hint; a bootstrap declaration is not required and recipe
defaults do not count as submitter hints. Without either, allow an exploratory
first run only with measured memory, a known allowance (physical RAM if
uncapped), at least half that allowance available after existing reserves and
charges, and no protected attempt or model residency charging a quarter of the
allowance. Use that available room, less existing per-attempt slack, as the
envelope. Lower-priority work remains reclaimable through the existing release
planner; release acknowledgement still precedes admission. Accepted exploratory
peaks become measurements. Status shows the source, envelope and enforcement
mode. Memory learning is independent of throughput occupancy comparability:
clean, attempt-specific single-piece serial fan-out children teach the ordinary
recipe's memory class, including piece arguments, while their throughput stays
unknown. Operator resource allowances and power mode remain throughput fences,
but do not identify memory need. Existing allowance-stale memory evidence is
revalidated through the normal curve-recovery path; hardware, backend,
environment, recipe and reference changes still invalidate it. No stored key
rewrite or database edit is needed. Input-size/duration scaling per recipe is
later work. Reservations are frozen per attempt; stale, failed, OOM,
constrained and resumed observations cannot shrink an envelope. Resumed jobs
can use prior accepted estimates. An auto OOM or constrained attempt
invalidates comparable learned memory points across machines and families,
without recording a bound or setting the performance Forbidden flag. To measure
the true need after a ceiling-bound run, submit the same comparable work with
`memory: auto` and no submitter hint: that next run explores the available host
headroom. When a constrained peak invalidated the measurement, its source reads
`exploratory (previous peak constrained)`. A successful unconstrained
exploration re-teaches the peak. Ceiling pressure alone does not retry or
change the job's outcome. After OOM, one retry keeps a job-local host floor
with 25% more room, on the same host if it fits, without raising its GPU
reservation from a host OOM. If that floor exceeds every eligible host
allowance or card, fail explicitly with the required GiB. Device reservations
never exceed the card total and never stand in for measured device peaks.
Member caps are optional; explicit caps remain supported and fit the job
envelope.

Linux native attempts use their existing shared parent cgroup for main and all
sidecars, bounded/zero swap and a job OOM cause. Sample whole-parent
anon + kernel + shmem from `memory.stat` every 200 ms and retain the kernel's
total high-water mark from `memory.peak`. Complete new measurements also retain
anon + kernel + shmem + unevictable + file_dirty + file_writeback as a
conservative non-reclaimable peak for CPU work. All six counters must be present.
Retain all kernel/shmem memory and conservatively double-count overlapping
locked/dirty/writeback pages rather than omit pages requiring I/O before reclaim.
Clean file cache is excluded only with the complete local evidence above.
The unevictable LRU counter is
not a DMA pin counter: XPU pinned host buffers, shared USM CPU storage, CUDA
registered/mapped host memory and driver allocations are not bounded by VRAM
measurements. Discrete xe/XPU attempts additionally retain all DRM system/GTT
host-object allocations, deliberately double-counting any overlap with cgroup
charges. Add absolute xe debugfs GTT-manager use on every participating card,
conservatively double-counting its overlap with client GTT objects. This runtime
source is required: missing, unreadable or malformed manager counters invalidate
GPU completeness. Direct reads use the worker's existing permissions. An optional
operator-owned `--gpu-telemetry-helper` executable can supply the same raw manager
text keyed by PCI, with a timestamp within its current invocation; stale, failed,
oversized or missing evidence rejects completeness. It receives no arguments,
job environment or fleet credentials and runs outside the resident sampler with
the same coverage limits. The installer preserves this opt-in setting but
installs no helper, privilege grant or permission change; service privilege
policy is unchanged. The provided unprivileged `myce gpu-telemetry-client` uses
`/run/mycelium/gpu-telemetry.sock`: no request payload, a fresh per-connection
JSON snapshot, and EOF. A separately installed root-owned broker supplies the
raw counters; no sudo/setuid is used by the worker or client. See the [operations contract](reference/host-capacity.md#optional-privileged-gpu-telemetry-source).
Retain a whole-host availability-drop floor from an empty-cgroup baseline;
unrelated RAM release can mask that net delta, so it never replaces absolute
driver accounting. Missing DRM client identity invalidates the entire attempt.
Keep resident/component/host-pressure sampling independent of driver I/O. One
driver collection has a 200 ms coverage window, matching resident cadence; a gap
or final age beyond two periods rejects completeness. Attempt completion waits
at most one period for a cancelled collector; late results cannot change its
published peak. This bounds observation latency, never job execution time.
Every host/driver sample must succeed; existing nonzero host swap is allowed.
Host OOM or memory PSI `some avg10 >= 1%` rejects learning. Additionally,
baseline the host and attempt-cgroup cumulative `some total` counters before
payload/sidecar launch, while the attempt cgroup is empty. Reject learning if
either counter's increase in microseconds is at least 1% of the elapsed attempt
window measured with the injected Clock. `some` includes `full` stalls, so do
not add those totals again. The total-delta check catches short stalls before
Linux refreshes avg10 (roughly every two seconds); avg10 is an extra signal,
never the only check. Exclude baseline/final sensor-read time from the elapsed
denominator, conservatively counting any stalls during those reads. The same
1% band means 2 ms stalled in a 200 ms attempt, or 100 ms in ten seconds.
Hierarchical parent PSI covers descendants; capped members retain their own
pressure/event checks.

Missing/unreadable/malformed host or cgroup PSI, counter resets, or a nonpositive
observation window makes the Linux sample ineligible for **all** memory-envelope
learning, including ordinary RSS/total fallback. Preserve raw diagnostic peaks
with `pressure_unavailable: true` on the result and attempt facts, across the
worker/queue JSON boundary and SQLite restart. Unknown pressure cannot refresh
or replace prior qualified memory evidence and does not invalidate it. Real
pressure still invalidates comparable peaks under the existing policy. Missing
coverage also prevents complete component/GPU evidence. No swap allowance can
override either stall check or missing coverage. Queue support for this additive
result field must precede workers sending it.

Host swap-used growth and cumulative swap-out bytes are compared independently
with a budget of 1% of the smaller of the attempt's resident peak and the
minimum host MemAvailable. Reject if either exceeds that budget. Compare
whole-attempt maxima against the prelaunch baseline at finish, rather than
against startup's near-zero RSS; historical swap is never new pressure. One
percent bounds the potentially hidden resident error far below the existing 25%
growth margin, while the availability bound prevents large jobs excusing swap
churn as headroom disappears. Also reject if minimum MemAvailable falls below
max(operator host reserve, 25% of resident peak, 128 MiB): preserve the
existing growth and minimum launch-drift headroom even without a PSI spike.
These thresholds bound measurement error and available growth room; they are
not a claim of native two-card qualification. Other GPU work retains total-peak
sizing. Native GPU concurrency acceptance is separate from estimator
implementation. Missing counters, older workers, macOS RSS evidence and
cross-machine transfers retain conservative sizing. This does not relax
pressure rejection. Device peaks remain separate, with the existing
conservative host-plus-device accounting on unified devices.

**Ceiling-bound envelopes.** There is no low-resident file-cache exception. OOM, swap-out,
anonymous workingset refault, or any `memory.events` `max > 0` / `high > 0`
constrains the measurement, including pressure in capped members. A pressured
sample cannot establish true need even when its resident-only peak stays low.
The queue applies this rule to older-worker results and excludes already-stored
pressure evidence from estimates. Successful unconstrained exploration can
re-teach the peak. File-heavy jobs can therefore reserve more memory or explore
again; pressure invalidates the measured range too, so the next comparable
unhinted attempt explores. A smaller resident-based ceiling is not a promise
that a file-heavy workload will avoid reclaim pressure. Ceiling-bound explicit-memory runs cannot seed an
auto estimate. Explicit admission and job outcomes retain their existing behavior.
The attempt's `memory.events` max/high/OOM counts are kept beside its peak;
absent counts mean they were not read, not zero.
Workers without cgroups (macOS) learn the summed RSS of the attempt's process
groups, sampled every 500 ms outside the queue lock; single-command jobs are
also raised to the main process's exit max RSS (native groups are not). The
sum counts shared pages once per process but omits compressed pages, members
that left the group and spikes between samples, so it can read high or low;
rusage alone reports only the largest waited process. These peaks are
measured but unenforced. Host swap and pressure are machine-wide and cannot
attribute the cause to this attempt. Darwin warning/critical memory-pressure
levels (2/4), swap-out bytes beyond the same 1% budget, or available RAM below
the same headroom floor reject that attempt's learning. Normal level 1 and
small swap-out remain eligible; missing pressure/swap-out readings reject
learning. These host signals never invalidate other hosts' learned peaks; a
real OOM still does. Pressure reads share a 200 ms observation budget, bounding
extra sampling/teardown work; failure costs only learning eligibility. The next
run retains its existing ×1.25 or larger headroom.
Darwin Metal/unified-memory jobs still explore, since no device peak is
measured.
Fully capped native groups preserve member-level OOM attribution and kill mode.
GPU samples belong to the attempt's process inventory where supported, only for
auto or estimate-feeding attempts, every five seconds and until unsupported.
Independent host and GPU sampler goroutines max-track under one small mutex;
cancellation joins both before the final read, so a slow GPU query cannot
delay host sampling. The GPU query keeps its existing two-second timeout.
Without an in-flight host tick, mark the result constrained and do not learn
from start or post-exit readings alone.
Empty process rows, uninitialised counters and transient query timeouts retry;
missing tools or unparseable counters stop GPU sampling. A retained recipe GPU
default is named as such beside a measured host source.
Missing counters stay unknown. Native measurement on a GB10 (#1044) found
`cudaMalloc` allocations outside memcg charges, so unified hosts add measured
host and GPU peaks. An unknown device peak cannot supply an exact unified estimate.
Keep the existing operator/driver reserve and
per-attempt slack; do not invent another fixed reserve. Live available RAM and
the shared machine allowance both bound admission, with a visible refusal reason.
For unified CUDA, retain the physical [free-RAM floor](#unified-cuda-free-ram-floor) equal to the existing OS/driver reserve. Fresh raw host observations and outstanding growth bound queue
admission; workers recheck raw RAM at launch. Cgroup credit never reduces the
device vector. Lower tiers must also leave room for a sole qualified host's
queued or forecast production envelope with existing estimator margins. Existing
production holds, preemption and work-scoped parity approvals remain authoritative.

### Learned memory by default and bounded first runs
<a id="amendment-learned-memory-by-default-and-bounded-first-runs-585-559"></a>

Under MY-31, agents state needs, not memory numbers they would have to guess.
A plain job with no usable memory figure is accepted as `memory: auto`;
partial figures stay submitter hints. Explicit figures keep their meaning.
Plain auto commands get a comparable memory identity from project, name, run,
environment and inputs, so their accepted peaks teach later runs through the
existing single eligibility gate.

A cold CPU job with no comparable accepted peak and no hint does not reserve
most of the host, so a small job cannot block unrelated work while it is first
measured. The first run starts from the largest fresh accepted unconstrained
CPU peak of similar work on the same OS (the same project and name, or the same
extension and work type, under any configuration) times 2.5. That is the 1.25
headroom times 2 used for cross-machine estimates. Without such peaks, it
starts from a quarter of the host allowance. Either figure is then raised to
twice any retained constrained peak of the same work; constrained peaks only
ever raise this bound. An OOM on a bounded first run doubles its job-local
floor without spending the one OOM retry. When the bound reaches half the
allowance, the host-wide exploration above applies unchanged, with its custody
fences and its one retry. The cost is one ceiling hit or OOM per doubling for
work larger than the bound. GPU work on a discrete card keeps its existing
exploration, since the card is already exclusive.

On a unified-memory host, a plain `memory: auto` GPU job with no accepted estimate owns the
card exclusively and reserves the host's whole shared physical headroom
(`unifiedGPUFirstRun`, since e0202e34b): without a device sample, the shared
pool cannot be split into two separate size guesses. With neither figure
stated, the device side is a one-page marker of exclusivity, not a device-use
estimate, and the enforced host member gets the rest of the pool. A hint
figure keeps its value. GPU allocation failures are not classified, so the
device side never doubles on failure. Learning needs a complete sample: where
the driver reports per-process device memory (measured on a GB10, #1044; see
[charging by use](#charging-running-attempts-by-use)), an accepted run's host
and device peaks become the learned estimate, and later runs are sized and
charged from it. A host-only sample never turns unknown device use into
accepted sharing evidence, so such work keeps exploring. The cost is one
exclusive host per cold run; a declared `--mem` is not `memory: auto` and skips
it. Extension jobs keep their exploration.

Job GET `memory_envelope.basis` is `declared`, `learned`, `hint` or
`exploratory`. `evidence` counts the accepted attempts behind a learned figure
or a similar-work bound. Status and waiting reasons show both. The local queue
policy field `learned_memory: false` turns all of this off: omitted figures are
rejected again, and cold runs explore host-wide. Unset means on. An omitted
field in a policy update keeps the saved switch.

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
The previous release's Docker wire fields and install keys are accepted and
ignored for one release (MY-42).

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

Tasks may explicitly select `output_delivery: async_nas` with declared outputs
and a positive `disk.output_bytes` estimate. Existing tasks retain their result
semantics. The queue's `--output-delivery-root` selects the NAS destination
independently of checkpoint mode; SQLite and the queue file store stay on durable
local storage. This authorizes output delivery only, not local-first checkpoints.

After payload exit, the worker retains an immutable synced local output spool
until exact durable queue acceptance. Queue admission reserves estimated output
bytes, exchanges them for the current attempt's exact missing manifest, and
preserves physical-space/quota backpressure. Queue success atomically records
verified file-store outputs and a pending delivery. `output_refs` retrieval and
compute completion never wait for NAS publication. Consumers needing NAS files
explicitly wait for verified delivery and the accepted execution identity.

One queue-owned background publisher stages complete immutable job/attempt
paths, verifies bytes and an identity/policy/manifest receipt, syncs and renames.
A pending NAS syscall retains its slot. Returned failures retry durably using the
injected Clock. Restart/lost-ack replay compares receipts and content; conflicts
and stale executions cannot claim verified delivery. Pending/failed delivery pins
blobs beyond ordinary expiry, and status exposes state, bytes, age, attempts and
cause. Verified delivery preserves ordinary retrieval retention; no early deletion,
rollback, new storage service or publisher election is introduced. Details and
supported forms: [file guide](files.md#complete-from-queue-storage-deliver-to-nas-later-349).

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
   failed build evidence without terminally failing waiting jobs. If all known
   compatible workers fail, keep accepted demand
   queued with an explicit no-buildable-worker reason until repair, cancellation
   or its existing deadline. Unknown/offline capacity is neither success nor a
   proven failure.
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

Choose a **one-shot source converter**, not runtime compatibility readers for a
deprecation window. MY-03 favors one current runtime; two permanent readers
retain competing concepts and validation drift. Cost: one reviewed source/caller
change per maintained consumer. Queued jobs, keys, frozen plans, output refs,
checkpoints and exact custody remain unchanged. Retire old source-authoring
entry points, not the internal decoder needed by saved bundles. Today preparation
calls `extensionFromFiles` and execution calls `ReadExtension` on frozen
`extension.yaml`; keep those paths (including the exact legacy/composed schemas,
bytes and digest verification) for persisted job/registry snapshots. New adapter
entries lower into the same execution representation. Internal snapshot decoding
cannot accept a mutable author path or register a new old-format file. It remains
while any retained snapshot needs it; consumer conversion alone cannot justify
its deletion. This is decoding durable v3 intent, not an old runtime fallback.
No accepted identity or digest is rewritten and no rollback layer is added.

`myce adapter convert` reads explicitly selected profiles/extensions and emits
one adapter plus field/caller-change report without overwriting its sources. An
explicit apply publishes the reviewed output. Original sources stay in Git
history. Refuse conflicting names, missing bytes and unrepresentable fields;
never guess targets, references, outputs or locks. Preserve catalog names;
copying their exact recipe bytes into a project requires the project-supplied
environment option. Never invent a lock for installed tools.

Profiles retain source, argv appending, env, project/class, targets/allow-lists,
resources, memory comparability, common/item input identity, outputs/diagnostics
and notifier recovery. Their `check` becomes `checks.result`. Extensions retain
complete variants, setup/assets, named environments, sidecar caps/readiness,
work types, baselines, references/sweeps and resumability; their `check` becomes
`checks.qualification`. Changed frozen digests require fresh native qualification,
not a relabelled old pass. Observations remain historical and reusable only when
their complete comparability identity still matches.

Stage converter/new reader first, then convert maintained callers and remove old
YAML authoring readers/init paths in the cutover batch. This is implementation
staging, not a supported dual-format window or automatic fallback. Retirement
requires verified consumer conversion, not an arbitrary date. Old new-submission
commands return typed conversion guidance; accepted-key replay, cancellation,
receipt recovery and serialized job/registry snapshots keep working through
the retained internal decoder. Retirement additionally requires a laptop restart
proof: accept a native job before cutover, restart queue/worker with empty local
extension/environment caches, fetch/decode its original snapshot, qualify that
exact identity and execute the job, then verify accepted output and keyed replay.
The original bundle, digest and submission fingerprint must remain unchanged.
Incomplete conversion or failed cold-recovery proof leaves the retirement issue
open and source-authoring entry points intact until cutover.

Consumer migration preserves each existing seam: exact source/runtime and frozen
inputs, target/resource/placement restrictions, sidecars, checks and artifacts,
submission/key/cancellation behavior, endpoints and existing holds. A conversion
must not substitute an environment, infer native qualification or launch held
work. Runtime or platform changes require separate parity and native proof.
Keep private paths, host IDs, data and receipts in consumer repositories and
use sanitized fixtures in Mycelium.

| Work kind | Preserve and prove at its existing seam |
| --- | --- |
| Offline suites and native naming | Two offline-suite entries retain Pixi CPU tools, wrapper, exact target and JUnit check. Its native naming extension retains CUDA variant, sidecar, baseline/result names and production submit/key/cancel seam. Model endpoints remain unchanged. |
| Application backend/frontend/tooling | Backend/frontend/tooling entries retain targeted/full distinctions, macOS-only suites, placement restrictions, Python/Node prerequisites and markers/archive. Copy exact temporary queue-side lock bytes only under the approved environment option; do not infer Linux qualification from a list. |
| Python/browser tests | Pytest/browser-e2e entries retain explicit Git bundle input and stale-bundle check (source omits `.git`), resources/thread limits, installed Python/Node/browser prerequisites and success markers. Conversion does not substitute a new environment. |
| Rendering | Render/canary and per-item catalog entries retain frozen scene/binary inputs, snapshot identities, large frame archives, semantic checks, private placement and existing holds. Conversion must not launch another render. |
| Generation/validation/evaluation | Generation/validation/evaluation entries retain the exact native CPU runtime/source closure, checksum checks, seed/candidate coverage, artifacts and holds. Historical loader alternatives stay historical; new uv/Pixi runtimes or GPU ports need separate parity/native proof. |

Convert Mycelium's gate/go-test profiles and starter extensions first. Prove a
small CPU fixture and fake native variant in `myce dev`, then compare sanitized
consumer conversions through the same lowering. Consumer owners apply changes,
update harness/CI callers and record one small authorized accepted-result proof
per active entry type, requalifying changed native digests. A held consumer stays
held with owner/next action and blocks removal of any reader it still uses.
Retire redundant docs/examples and update the offload skill with cutover.

### Remaining work
<a id="implementation-plan-and-acceptance"></a>

The unified file and lowering (#250) and the converter with internal dogfood
(#253) are complete; their acceptance lists are in the
[text before consolidation](https://github.com/1kuna/mycelium/blob/e0488c0c5034b5a57827ceebfa69f770c060bb8f/docs/design-proposal.md#L2352-L2384). One slice remains open.

**[Consumer cutover and reader retirement (#254)](https://github.com/1kuna/mycelium/issues/254).**
Acceptance: reviewed maintained-consumer files/callers with evidence or explicit
holds; no active caller requires a removed reader; changed native digests
requalify; old new-submission calls give conversion guidance while keyed
recovery works. Require the pre-cutover native job's cold-cache queue/worker
restart proof described under migration; retain internal snapshot decoders while
referenced, even after source-authoring readers retire. Update
guides/examples/skill without weakening checks, widening targets or consuming
unaccepted results.

#192 and #248 remain open until live acceptance; design text alone removes
no consumer support.

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

Observations change no admission, qualification or concurrency limit.

A variant may declare `references`, keyed by its existing work type. Each entry
has `input_shape` (an opaque compatibility bucket), `unit` (a generic work
unit), and positive `units` (the amount in that work type's bundled native
baseline). It requires that variant's check command and baseline. This is a
serial reference contract: one independent item, concurrency 1, vector batch
size 1. Threads, backend/cache settings and correctness semantics belong in the
frozen recipe; changing the reference or recipe changes the extension digest
and requires qualification again. Sweeps run only as [safe sparse
curves](#safe-sparse-curves) allow.

An ordinary extension job opts into comparable timing with
`workload: {input_shape: small-v1, units: 100}`. The author promises the same
serial execution mode and work unit as the matching reference. Absent metadata,
legacy profiles, checks (which have not yet qualified), resumptions and unmatched
shapes remain unknown. Successful accepted attempts alone update a per-key mean;
failed, interrupted, stopped, stale and duplicate results cannot contribute.

Keys bind work type, extension digest, exact target, reference, input shape,
physical host/component, hardware/backend/environment fingerprints, host/device
allowances, and the attempt's declared resources. Queue Clock timestamps measure
Started acknowledgement through accepted completion: preparation is excluded,
while cleanup/result handoff is included. These are observed elapsed times, not
isolated kernel timings; contention is reported and mixed/unknown occupancy is
excluded from comparable estimates. No rates are inferred from old scalar
onboarding benchmarks. Evidence persists as per-record SQLite rows alongside jobs.

Status exposes the frozen prediction, actual accepted elapsed time, sample
count and unknown reason. Predictions use only matching prior evidence, and
placement uses them only through [completion-aware
placement](#completion-aware-placement). Status remains a read-only projection:
no mutation, persistence or history-wide reserialization is added to status
reads.

### Safe sparse curves
<a id="safe-sparse-curves-slice-2-107-approved-2026-10-24"></a>

> **Removed by [MY-49](DECISIONS.md#my-49).**

Sweeps change no placement or ordinary admission/concurrency limit.

A variant may declare `sweeps`, keyed by a declared reference work type, with a
`command`, sorted supported power-of-two `counts` starting at 1, and positive
`max_concurrency` (at most 64). One command implements concurrency internally;
Mycelium does not launch concurrent GPU attempts. `--curve-max-concurrency N`
on `join`, together with `--onboarding-benchmark`, explicitly enables sweeps up
to the smaller operator/recipe bound (default 0, disabled). There is no periodic
load generation. Prepared exact variants and native baselines must qualify first.

Reuse host-wide benchmark preparation custody, command supervision and cleanup.
Busy/recovering hosts cannot start; components run serially. Daily-use hosts need
fresh locally measured idle recovery, external-power and safe-heat evidence. Owner return or
unsafe/unknown signals cancels the command, retaining custody until cleanup.
A sweep has a 60-second Clock budget, and each command a ten-second bound.
Stop on non-increasing aggregate throughput, memory exhaustion or failure.
The declared per-item CPU/RAM/device floors bound counts conservatively; unified
memory is charged once against the one host allowance. Unknown memory permits
no point. No resources are granted by learned measurements.

The command gets `MYCE_BENCHMARK_CONCURRENCY`, normal check input/output/work,
and `MYCE_BENCHMARK_OUTPUT`. It replays the bundled reference with unchanged
quality, writes each item’s native output to `MYCE_CHECK_OUTPUT_DIR/0`, `/1`,
and so on (all are compared), and reports aggregate `units`, measured service `seconds`, mean
`latency_seconds`, baseline and peak host/device GiB, plus `out_of_memory`.
Correct output is required for successful samples. Incremental memory is peak
minus baseline; high-water memory never decreases. OOM permanently forbids that
exact point, even if a later successful sample arrives.

Store points as separate rows in the existing observation store. Clean accepted
ordinary attempts refine their exact point using `0.8 * old + 0.2 * sample`;
serial-only ordinary timing stays serial. Failed, mixed, interrupted, duplicate
and stale attempts never refine rates. Fingerprint, hardware, recipe/reference,
environment or allowance changes retain evidence with a stale reason and prevent
rate reuse; 30-day-old points also become stale. JSON, CLI and HTML status expose
rates, latency, incremental/high-water memory, samples and stale/forbidden reasons.
Native mixed-load curves and fleet acceptance require separate authorization.

### Completion-aware placement
<a id="completion-aware-placement-slice-3-107-approved-2026-10-24"></a>

> **Reduced by [MY-49](DECISIONS.md#my-49)** to the last accepted duration on each target.

Rank qualified extension commands using a common completion estimate: declared
workload units divided by the exact comparable serial measured rate, plus cold
environment setup, model copy and input-file transfer costs. Reuse the existing
setup/copy estimates; input transfers require measured network throughput.
Missing, stale, forbidden, resumed or incompatible evidence remains unknown.
Unknown estimates retain the existing placement decision exactly, including
legacy scalar benchmark ordering. Comparable measured candidates precede
unknown ones; distinct reference groups and equal estimates have deterministic
ordering.

Eligibility precedes ranking. Existing resource and learned memory floors,
platform preferences, owner policy, tiers, fairness, project caps, holds and
checkpoint/cleanup fences remain authoritative. Reuse the existing victim
selection and minimize stop count/lost-work cost before completion ranking.
Only currently admissible alternatives can defer dispatch; estimates create no
reservations and never wait for a busy fast host. Ranking changes no
concurrency: attempts share a card only under the [shared GPU
rules](#inventory-and-resource-ownership) (MY-30). Freeze the chosen measured
estimate's reason for JSON/CLI/HTML status. All time comparisons use the
injected Clock; ranking does no I/O.

Model calls use the shared estimate seam but remain unknown without the
explicit model/reference binding defined under [declared fan-out](#declared-fan-out-and-batch-planning).
Their routing, configured limits and victim selection remain as configured.
Extension command sweep concurrency does not qualify a model server for
concurrent calls. The quality-tolerance filter belongs at the existing
qualified-variant eligibility boundary ([variant eligibility](#variant-eligibility-and-production-approval)),
before completion ranking; ranking adds no quality policy.

### Repeated plain-command measurements
<a id="repeated-plain-command-measurements-878"></a>

Under MY-31/MY-34, native plain commands without workload units can teach seconds
per whole job through the same accepted-attempt timing path and observation store.
The speed key adds a digest of the exact run, frozen source archive and input
manifests, output contract, environment recipes and isolation. It retains the
existing target, physical host/component, runtime fingerprint, resources,
allowance and power-mode identity. This exact-byte identity is deliberately
narrower than the memory work identity; changing source/input size cannot reuse
a prior speed, even when memory learning remains comparable. Unversioned legacy
inputs, dynamic upstream inputs, external work directories/runtimes and missing
runtime fingerprints remain unknown. Undeclared external state is outside this
comparison contract; these observations do not qualify software or prove parity.

Only successful accepted serial attempts with exact custody and complete clean
contention evidence contribute. Resumptions, failed/interrupted/stale/duplicate
results and intervals with another host obligation are excluded. Existing memory
pressure classification must be eligible, and complete CPU scheduling-stall,
quota-throttling and I/O-stall counters must measure zero. Linux `cpu.pressure`
is read in the existing resource sampler; quota throttling alone cannot detect
an unrelated process delaying execution. An independently negotiated optional
CPU-pressure field preserves the prior telemetry contract with older queues.
This has no new tuned pressure threshold: its cost is that internal parallelism,
intrinsic I/O stalls and platforms without complete counters cannot teach a clean
comparison. No second collector or benchmark runs.

Each comparable key retains the incremental mean seconds/job, a clean sample
count and observed minimum/maximum. One sample explicitly leaves variability
unknown; the observed range is not a confidence interval or a deadline. Existing
staleness/invalidation rules apply. Each speed mutation records its exact
before/after digest in the job's accepted-sample ledger transaction, preserving
release-verification evidence even when the usable timing window restarts.
Status and inspection expose this evidence
through work curves and attempt timing. Completion ranking adds existing setup
and measured transfer costs and remains after admission, tiers, fairness and
least-valuable fit. Estimates neither reserve capacity nor wait for a busy faster
machine. Prediction-error calibration is separate work (#879).

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
ordinary-job sources, forward references, dynamic enumeration, timed
accumulation, atomic graph submission, or automatic semantic reduction. Broader
stage planning is later work.

## Enrolling a machine

Adding a supported machine type takes one command (MY-08).

### One-command enrollment
<a id="one-command-enrollment-b0b1b6-108-2026-10-21"></a>

A hypha reports all its components. The existing `myce invite` and `myce
install worker --enroll` paths provide bootstrap and enrollment. By default
invite prints exactly one paste-ready line to stdout; `--out FILE` also saves
it privately. Foreground `join` remains the worker runtime. NAS mode is
required. The [onboarding preflight](#onboarding-preflight) owns hardware
preflight, NAS/tools and saved service/policy fields; enrollment passes its
ordinary installer options through unchanged. Legacy conversion is retired
(MY-42); qualification remains separate.

Releases contain linux/amd64, linux/arm64 and native darwin/arm64+cgo binaries.
The publisher signs an exact JSON manifest with Ed25519. The queue verifies it
against an independently provisioned public key, never a key from the manifest,
then supplies approved public release metadata to pinned authenticated
inviters. Explicit local manifest/key/origin overrides perform the same
verification. Release versions are ordered semver values (for example
`v3.1.0`). Queue approval is persisted in SQLite and must strictly increase
unless `--allow-downgrade` explicitly re-approves or downgrades it. Repeating
an identical verified approval is idempotent. `myce release approve` advances
the running queue's default after a signed deployment without changing catalogs
or service settings. The queue persists the publisher key provisioned only on
its host; API approval requires an exact key match and verifies against that
stored key. Startup verifies then skips/logs older or equal flag manifests
unless explicitly forced, preserving the newer default across restart. Missing
stored trust refuses API approval until local provisioning; key rotation
remains local. Semver build metadata has equal precedence. The verified
manifest binds version, protocols, OS/arch, size, SHA-256 and native Mac
sensing. A small POSIX command bootstraps clean hosts, which have no myce, Go
or Git. Its embedded verified hash matrix selects the artifact and HTTPS
origin; curl resumes a private partial download, refuses redirects, and a
platform SHA-256 tool verifies bytes before execution. Publisher HTTPS plus the
signed manifest are separate from the LAN queue's exact certificate-DER pin.
Never confuse that pin with curl's SPKI pinnedpubkey.

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

Each printed line freezes the approved release and checksums for
reproducibility. Reuse that line for interrupted installation and same-version
health checks; use a line for a newer approved release to upgrade. Installed
upgrades use saved authority without redeeming or requiring another ticket.
Same-version reruns verify health without changing files or restarting
services, and explicitly explain that installer flags are not reapplied. Older
releases are refused unless the operator passes `--allow-downgrade` explicitly;
comparison uses the installed binary's version, not an invitation's age or a
stale worker receipt. Equal versions (including `+build` metadata) require
identical bytes, otherwise `--allow-downgrade` is required. Replacing an
installed non-semver version such as `development` also requires that explicit
flag; the published target remains semver. A changed release refuses busy local
hosts before draining through existing queue maintenance. A race with new
custody is bounded: drain-wait failure conditionally restores its own exact
transition. Drain-wait and install/readiness have separate budgets. The
existing installer transaction and spool remain the install path. Existing
disabled/drained policy is preserved; an installer never resumes an
operator-disabled host. A private resume receipt fences a self-made drain by
host, name and policy timestamp. Its durable intent precedes the drain POST; an
intent plus an observed drain on rerun is promoted to the exact timestamp
receipt. Install/readiness failure restores the previous installation through
the existing transaction and verifies its enrolled readiness before resuming
only the self-made drain, with a separate cleanup budget. Errors state whether
the previous release resumed or why recovery failed. A crash loses the
in-memory installer snapshot; if already-installed bytes never become ready on
rerun, report rollback unavailable and preserve maintenance. Operator-created
maintenance remains respected. No fleet operation, reboot, driver install or
privileged service is implied.

### Onboarding preflight
<a id="one-command-onboarding-preflight-b2b4-108109"></a>

Onboarding reuses the machine inventory, installer overlay and service
transaction. It enumerates all components, preserves explicit selector lists
and per-device fixed budgets, and refuses typed driver, inventory or memory
failures instead of saving CPU fallback. Enumerate PCI display and
processing-accelerator classes, classify each vendor and integrated/discrete
topology from sysfs facts (local device memory or PCIe link width), then
correlate every runtime device with its PCI address. A CUDA device can expose a
nonzero PCI transport width while sharing host RAM. When its memory total is
unavailable, the selected PCI device's CUDA driver integrated attribute can
establish unified topology; an unavailable total alone cannot. Product names
never determine memory mode. Missing facts stay unknown; ambiguous multi-device
correlation refuses instead of guessing. NVIDIA devices and discrete Intel
endpoints require their runtime independently of any other working component. A
failed `nvidia-smi` is informational when PCI inventory confirms no NVIDIA
display device. Working integrated Intel graphics enrolls as `xpu` with unified
memory; a missing integrated runtime permits CPU, while a broken runtime still
refuses. Other display controllers are informational and do not block CPU
enrollment. Explicit CPU selection remains supported; re-detection is an
explicit install operation so upgrades do not silently broaden an owner's
component allowance. Inventory errors retain the `device_probe_unavailable`
runtime rejection while also carrying the more specific typed onboarding
diagnosis.

Enrollment completes hardware, storage, policy, state-root and service-session
preflights before acquiring a drain. The candidate binary and a private staged
config run the existing service-environment hardware probe before the live
installation changes. Post-publication validation and exact-session readiness
still verify the installed worker. Environment backend acquisition stays lazy.

NAS mode is the only supported onboarding storage mode. Checkpoint, model and
environment roots must use an existing reachable network filesystem. Use the
saved NAS root, an explicit `--nas PATH`, or the documented default (`/mnt/nas`
on Linux, `/Volumes/NAS` on macOS). Never adopt another discovered share;
missing selection refuses `nas_not_configured` with candidate mount paths and a
`--nas` hint before printing the plan. Preserve saved roots. Never mount a
share, modify fstab or substitute local directories. The legacy deviceless test
flag cannot bypass this requirement for CPU/GPU workers. The file store remains
the job input/output transport, not a second storage mode. Resolve uv/pixi on
the service PATH first and preserve that order on upgrades. Only a native
environment build that needs a backend may acquire its pinned vendor binary in
the ordinary user's installation; invoke it by absolute path. Include uvx when
shipped with uv. An offline machine with no recipes needs no backend download.
Do not execute remote installer scripts or print credentials.

NAS validation uses the existing bounded checkpoint I/O pattern with one 250 ms
network-filesystem budget per metadata/create/write/sync/close/rename/ cleanup
operation (following the worker CIFS allowance), with a 5 s cap on the whole
check, including mount inventory. Fresh/default NAS selection for both real and
dry-run plans uses that same bounded, cancellable mount helper. Slow healthy
sequences may exceed 250 ms in total. Timeout returns `nas_unresponsive`;
caller cancellation remains `context.Canceled`. Cancelled/timed-out results are
discarded before retrying the same key. An uninterruptible syscall can finish
afterward, with no second sequence launched while it is pending. Known NFS/SMB
mount types, autofs-managed mounts and explicitly network FUSE subtypes
(`fuse.sshfs`, `fuse.rclone`, `fuse.smbnetfs`) are eligible. Generic or local
FUSE is not. Autofs is triggered only inside bounded validation.

Save daily-use, laptop and desktop policy through the same flags/overlay/Args
as budgets. False and zero are values, including on rerun. Linux onboarding
enables and verifies user lingering without sudo, or reports the administrator
prerequisite. macOS installs a LaunchAgent in the logged-in gui domain and
validates its environment there. The injected Clock owns the service-probe
deadline; a real ticker paces local output polling without advancing scheduling
time. Bounded service stderr retains only allowlisted typed reasons. It starts at GUI login, not before login, and
does not promise survival after logout. CPU/RAM admission on macOS is not an OS
ceiling; required unsupported memory ceilings refuse explicitly. Hardware,
service-session and native participation qualification remain separate gates.

The enrollment command calls `PreflightHardware` and `ApplyHardwarePreflight`
while planning, then `OverlayHardwareSettings` after explicit memory overrides.
Call `PrepareOnboardingStorage` before publishing a service (discovery only for
a fresh default mount), `BootstrapEnvironmentTools`, and
`EnsureOnboardingServiceSession`. The existing `Install` transaction invokes
`ValidateWorkerServiceEnvironment`; `detect --config` returns the whole
selected hardware plan without reading credentials. These functions accept fake
probes; service-probe deadlines use the injected Clock. Ordinary install
upgrades keep saved component choices; `install worker --redetect` explicitly
refreshes them.

Tool versions are uv **0.8.22** and pixi **0.81.0**, with vendor GitHub release
archive SHA-256 pins for Linux/macOS amd64/arm64. Both use versioned user-local
directories; no existing system/user executable is replaced. The saved PATH
prepends those directories and remains stable on rerun. Unsupported releases,
downloads, checksums, archive shapes and executable versions refuse with typed
reasons. These pins are onboarding prerequisites, not extension qualification.

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
reserve, historical/live RAM admission and existing last-resort placement. GPU
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
