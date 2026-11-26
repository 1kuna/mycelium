# Warm pieces in an ordinary job

The maintainer approved this design on 2026-11-25. [#2095](https://github.com/1kuna/mycelium/issues/2095)
asks for an ordinary submission that loads a model once, processes several
items, and releases working memory between items. Start with one native
command group for a finite list of independent pieces. Qualify elastic
memory separately from keeping the group alive.

## Submission

Add an opt-in `advanced.warm_pieces: fixed` to plain-command `pieces:`.
A later stage adds `elastic` as a value of the same field. The raw job
form uses the existing plain-command `fan_out.pieces` declaration and
per-piece file prefix. For example, in an adapter:

```yaml
entries:
  score:
    run: {command: [python, score_batch.py]}
    needs: {cpu_cores: 2, memory: auto}
    resumable: true
    advanced: {warm_pieces: fixed}
    inputs: {model.bin: weights/model.bin}
    outputs: {score.json: results}
    pieces:
      - {key: a, args: [--mode, fast], inputs: {input.bin: data/a.bin}}
      - {key: b, args: [--mode, fast], inputs: {input.bin: data/b.bin}}
```

The submitter still runs `myce run --entry score`, records the parent ID
and fetches `KEY/OUTPUT_NAME` with the existing result command. Ordinary
pieces keep their independent placement and child IDs. In warm mode a
single placed attempt runs the command and any native sidecars. The queue
keeps piece states and results on the parent without creating child
executions. The parent exposes the same keyed `fan_out.results` view.
Order, arguments, inputs, mode, runtime and model identity
freeze at submission. Existing submission-key replay returns the same
parent and refuses changed intent.

The command initializes its model once per attempt that runs work. A
worker-supplied helper exposes a small loop: get the next item's key,
arguments and paths, process it, and report success or item failure.
Common inputs and model snapshots use ordinary staging. Piece inputs
are staged under their own key and outputs have a private per-delivery
directory. The helper uses one inherited attempt-local duplex stream.
Possession of its payload endpoint grants the job's helper authority.
The worker passes it only to main, which may share it with descendants
that act for the same job. Other jobs, other attempts and sidecars
receive no endpoint. The worker stamps each operation with its own
machine, session and execution identity. Every payload member stays
free of the fleet token.
Sidecars can serve main locally for the lifetime of this group, within its
resource limits.

The first slice uses the existing plain-piece count and input bounds,
serial delivery, one device or CPU, and verified file-store outputs.
Verified NAS delivery, additional target shapes and extension support can
follow the same protocol when their contracts are exercised. Refuse
unsupported combinations explicitly, including per-piece DAG inputs,
member-specific placement, and output/checkpoint modes without proven
custody. Existing downstream `after.piece` bindings may read accepted
file-store results through the usual same-project acceptance fence.
Production adoption requires the target and output parity of MY-19 and
MY-51. The implementation updates the consumer skill with the submission,
loop and recovery recipe in the same change. [Declared pieces](jobs.md#plain-command-pieces)
and [native sidecars](services.md#native-job-sidecars) supply the baseline.

## Item delivery and recovery

1. After ordinary launch and Started authorization, offer the first
   unsettled key in declaration order, with one outstanding delivery per
   attempt. A delivery carries job ID, worker/session, attempt, piece
   key and a delivery sequence. Retransmission returns the same delivery.
   Main waits for the worker's fenced start signal before executing it.
2. Main writes outputs to its delivery directory. The worker freezes a
   manifest and uploads immutable blobs through ordinary verification.
   The queue snapshots identity, verifies artifacts outside `q.mu`,
   rechecks the delivery and stop/cancel fences, and commits the item
   receipt, exact producer and dependency readiness together in SQLite.
   A lost reply replays that receipt for the same delivery and manifest. Changed
   manifests and stale attempts are refused. Cancellation fences all
   unfinished keys. An independent success observed before stop delivery
   can win the existing stop race when backed by its exact receipt evidence.
3. A reported item error settles that key as failed and the loop can
   continue when the runtime is healthy, as with plain pieces. An
   unexpected process exit, sidecar failure or OOM follows existing batch
   classification and retry policy. There is no per-item retry budget.
   A retry resumes with the first unsettled key, including an interrupted
   delivery. Settled keys never run again. Exhaustion records unfinished
   keys as failed and preserves prior receipts. The parent succeeds if all
   keys succeed and ordinary attempt finalization succeeds. A late result
   cannot change an accepted key or resurrect the parent.
4. Committed receipts are durable item progress. Optional mid-item
   progress uses ordinary portable checkpoint generations, scoped to
   the piece and frozen resume compatibility. A checkpoint may resume
   the same unaccepted item and cannot mark it accepted. On priority
   stop or leave, stop offering items, save if possible and use existing
   signal and exact group cleanup rules. On loss, use only the last
   verified compatible generation or restart that item. Requeue and loss
   budgets are the existing ones. Old attempts retain release custody
   until exact evidence, including across queue restart. A poll or
   timeout never releases a process group.

An item may execute again after a loss before its receipt commits. The
contract gives one accepted result per key. Commands must make effects
repeatable, use declared outputs for publication, and avoid unkeyed
external writes. Checkpoint handlers exiting successfully after a delivered
stop remain interruptions. If the last receipt committed but finalization
was interrupted, retry finalization with an empty remaining list before
starting main or its sidecars. [Checkpoint contract](files.md#resumable-jobs) and
[accepted-output dependencies](design-proposal.md#per-piece-accepted-output-dependencies).

## Memory contract

### Size from observed phases

Measure three envelopes per physical pool: cold load and preparation,
resident between items, and the total peak during an item. The resident
floor includes weights, allocator caches that stayed resident, helpers,
and long-lived runtime state. Working set is the increment over the
resident floor from activations, KV cache, buffers and staging. Measure
the total too, so startup spikes, overlapping copies and concurrent members
are accounted for. A runtime that retains its item buffers keeps their
charge until it actually returns them.

Freeze the workload signature by source, runtime/model revision, topology,
phase, and comparable item shape such as tensor shape or frame count.
Use complete, attributable measurements. Apply the existing calibrated
upper-quantile rule only with enough comparable clean repeat errors
(`k >= c/(1-c)` at the pool's configured confidence). Correlated items in a
single attempt do not prove independent repeat coverage. Missing driver
attribution, capped samples, swap and OOM never train a lower forecast.
Gather sparse evidence with a fully charged fixed run or an isolated probe
on the affected pool. Include budgeted staging and measured overshoot in
the appropriate phase. [MY-34](DECISIONS.md#my-34) and
[pool evidence](design-memory-admission.md#23-estimating).

For each pool, every cold admission, grow, shrink, requeue and recovery
uses the same calculation:

```text
minimum = max(retained exact peaks for the authorized phase,
              explicit applicable limit, retained shortage/OOM floor)
charge  = max(minimum, measured forecast including margin)
held    = max(charge, conservative still-resident footprint)
```

A completed item's peak remains a floor for the next comparable active
phase. The idle phase may shed it only after the worker proves that the
item is over, buffers were returned, and the phase cannot allocate item
memory. An unresolved shortage floor whose phase is unknown applies to
every phase. Untagged exact history remains a bound for startup and active
items until phase attribution is qualified. An explicit host-memory limit,
including an owner resize, remains a reservation floor across phases.
If a proven requirement exceeds that user's hard ceiling, report the
conflict with its measured value. Typed needs remain starting estimates.
The fixed mode reserves the full envelope throughout.

Check the final charge against total allowance before searching for room
on any path. A forecast above allowance can try another qualified target.
If no target fits, report the capacity reason, distinguishing a proven
floor from forecast uncertainty. An isolated measurement run can resolve
an uncertain forecast under qualified containment, with exclusive custody
of that pool and the full usable allowance reserved. If a proven floor
exceeds the allowance, even a probe cannot use that target. The normal
admission and grow paths never trim a forecast to available room or to an
unrelated prior.

### Change the reservation through the worker

Install and verify the initial cold-load controls before any member starts.
In elastic mode main calls the helper at an allocation-safe boundary before
an item. The request identifies the piece and its work shape or a qualified
working-set bound. It cannot revise the limits, select a host or authorize
allocation. The grow target is the new *total* envelope in every affected
pool. At the boundary all group members can run within the old grant
indefinitely for correctness. An item that needs more than its budget must
refuse the allocation and report the limit hit as a lower bound. The first
slice covers the item's entire peak up front. Following an overrun it can
retry from the last verified progress under a larger grant in a new attempt.

- **Grow.** The queue serializes grants with ordinary admission and
  durably reserves the positive delta in all pools against a grant revision.
  The worker rechecks fresh physical room and identity, installs and reads
  back any limits, then rechecks the execution and stop fences at the
  item-start gate before authorizing the matching delivery. A lost reply is
  reconciled by revision. The queue retains the larger pending charge
  until it has an exact acknowledgement or proof that the old limit stayed
  in force. While waiting, main and its helpers remain barred from using
  the proposed increase.
- **Shrink.** Once cold load is complete or after an item, the group
  quiesces, synchronizes device work and frees its transient buffers.
  The worker verifies the remaining
  footprint and installs the lower guard first. When the exact fenced
  acknowledgement commits, the ledger releases the delta immediately.
  Until then it holds the old charge. Inconsistent readings, pending
  allocator frees and failed limit writes keep the old charge. A subsequent
  increase requires a fresh authorization, even when no other work arrived.
- **Recovery.** Persist request ID, old and new revisions, direction and
  phase with the current attempt and worker custody. Process one change
  at a time. A stop, cancellation, loss or session replacement fences
  unapplied grants and all future items. Unknown application retains the
  larger charge until worker reconciliation or exact group teardown.
  A restart may reconcile an authenticated worker's records or stop and
  recover the attempt. Old or reordered messages can settle historical
  custody but cannot authorize work or free a current charge.

### Enforce and account on the actual machine

The pool ledger is admission control. An isolated limit or qualified
budgeted allocator enforces what the group can use. A prediction by itself
does not become a kill limit. Eligibility for elastic sharing requires
proof of containment for every affected pool and evidence that the whole
group respects the boundary, including helpers.

| Pool | Enforcement and accounting |
| --- | --- |
| Linux host RAM | Keep the existing worker slice and explicit attempt/member cgroup limits. An elastic command can explicitly opt into negotiated cgroup caps for controllable RAM. Set the aggregate attempt cap before authorizing allocation, within the slice and any authored ceiling. Shrink only after quiescence with verified usage safely below the cap, so applying it does not induce reclaim or OOM. Memory outside that cgroup needs its own qualified enforcement and accounting. |
| Discrete accelerator | Use one RAM pool and a separate device pool for the assigned card. Bound device allocations with a qualified runtime allocator or enforceable device partition. A card's physical capacity alone bounds aggregate allocation, so it cannot protect co-tenants. Without per-attempt enforcement, keep the affected device pool exclusive and retain the full fixed charge. Apply existing device sharing eligibility as well as memory fit. |
| Unified CUDA | One RAM pool. CUDA allocations can escape the cgroup. Qualify a runtime limit covering device, managed and shared allocations from the whole group, including growth and synchronization, plus cgroup/host guard for the rest. Use synchronized host availability and uniquely attributed, disjoint occupancy. Deduplicate shared backing across PIDs and cgroup/driver counters before giving room credit. A PID sum, an inferred residual or stale measurements cannot earn credit. If uniqueness is unknown, keep the uncertain charge and give it zero live credit. If the peak or enforcement cannot be bounded, use fixed or exclusive qualification. |
| Other unified systems | One RAM pool, with the existing platform availability and physical-footprint signals where available. A platform without a verified runtime bound stays fixed. Sampling and pressure stops are backstops, not hard limits. |
| Fixed-budget hosts | Debit the same final charge from the configured usable allowance, including a high-water hold for unresolved growth and cleanup. No live-memory credits. The worker still verifies quiescence and the applicable cgroup or runtime limits before releasing charge. External consumers are outside the fixed ledger. A supported allowance must cover them and the owner's reserve, otherwise elastic sharing stays off. |

On live pools, require both aggregate ledger fit under usable allowance
and fresh physical room for the additional charge. Use the existing
allowance net of the owner's reserve in the ledger check. For the
physical check, subtract the owner's reserve and each reservation's
unreflected held charge from raw availability. Credit at most
`min(held, proven reflected bytes)` in the same snapshot, once across the
whole host. For example, a 4 GiB shared CUDA
buffer visible in two processes can earn at most 4 GiB of total credit,
including any cgroup overlap. Each live shrink or release refreshes the
room snapshot and rechecks revisions before a new grant. Sampling detects
unexpected pressure and triggers existing containment and priority handling.
When hard bounds or attributable measurements are unavailable, the runtime
does not qualify for elastic co-tenancy. The planner can place an elastic
submission on another qualified target or report the missing capability.
A submitter can choose fixed mode for that target.

Treat [#2086](https://github.com/1kuna/mycelium/issues/2086)'s round-two
findings as implementation prerequisites. Test above-allowance forecasts,
larger retained exact peaks, cold production explicit limits, missing
CUDA attribution and shared-buffer double credit on both cold and running
paths. Also preserve a genuine OOM independently of ordinary result
ordering: cgroup `oom_kill` or qualified allocator OOM establishes the
fact even without a ceiling diagnostic or when pressure-stop evidence
arrives first. Persist the maximum shortage evidence for the exact
execution and phase atomically with release/requeue. If cleanup arrives
before the diagnostic and the worker cannot supply a complete final
memory record, keep the conservative retry floor and withhold lower
admission on that work until reconciled. Late authenticated historical OOM
facts can raise future floors without mutating a newer result or granting
release on its behalf. Queued [resize](https://github.com/1kuna/mycelium/pull/2027)
continues to refuse unreleased attempts.

## Scheduling and fairness

A warm job pays for all allocated time, including load, safe waits and
model residency, using existing device/core usage and project caps. Device
and CPU reservations stay with the group until release. Ready continuation
competes at an item boundary using current tier, recent project usage and
the same ordering as new work. Warm affinity applies within the selected
project. If the planner would give contested resources to another job, the
warm job cooperatively ends its segment and requeues unfinished pieces
with receipts intact. Record a boundary yield as a planned requeue with
no failure or loss penalty. An active same-tier item keeps its usual
protection. A same-tier boundary turn also waits until the attempt has
done at least its measured load time of item work, so recent-usage
ordering cannot reload the model for every item.
There is no periodic time slice. Finite pieces and boundary turns prevent
one long warm batch from buying indefinite priority with a single admission.

For a local grow request, use free room first. A wait joins the planner
as one ranked demand for the delta on that attempt's assigned pools. It
shares the existing head/backfill protection with queued jobs. There is
one protected demand per pool decision, so two waiters cannot claim each
other's pending releases. If a ranked competing demand can only fit after
a warm waiter gives up its idle floor, that waiter gives up the group at
the boundary. If another qualified target can take the work now, end the
segment and requeue there through normal placement ahead of stopping a
co-tenant. Requeue carries the next item's admission need alongside cold
load sizing, so a job cannot repeatedly reload where its next item is
known to wait. A capacity hold ends on a relevant room, custody, policy
or qualification change.

When existing priority policy permits a running grow to make room, plan
useful lower-tier resumable victims with the current comparator and exact
release commitments. Reuse stop grace, lost-work comparison and the
post-preemption grace guarantee. An accelerator stop normally sends TERM
with ten seconds of grace, capped at fifteen seconds, then KILL if needed.
Admission still needs verified release and a fresh room check. Production
retains its existing priority and may stop other eligible lower-tier
victims where MY-56 already allows it. Unknown cleanup, production peer
occupancy and external pressure can delay admission beyond stop grace.
Expose the wait reason and preserve existing start-window semantics.
A higher-tier production submission can reclaim an active lower-tier
warm attempt via ordinary preemption or reclaim an idle floor at a boundary.

End and unload immediately when the finite list settles or the next
execution loses its turn. With no ready item, retain the model only while
a concrete pending operation, such as acceptance verification or a
selected release, still makes progress under its existing bounds. If
there is no viable progress or independent ready work needs that floor,
end the segment and wait in the queue. There is no idle session awaiting
future submissions. The catalog's `idle_timeout` still governs catalog
servers. Report current item, accepted/failed counts, grants and evidence,
wait/victim/release reason, and measured load, item and idle durations in
ordinary job status. [Approved scheduling](design-proposal.md#how-the-queue-decides)
and [pool backfill](design-memory-admission.md#211-waiting-fairly-one-reservation-rule).

## Expected gains

The paying consumer measured naming sidecar readiness at 25.6–51.5 s per
launch, roughly 31–63 accelerator-minutes over 73 launches. If all 73 items
fit one uninterrupted warm attempt, 72 avoided loads represent about
30.7–61.8 accelerator-minutes of gross startup savings, or about 26–52 s
per additional item. Separation sidecar startup measured 9–37 s per
launch. Its additional in-process model load is inside the 133–203 s
runtime and has no separate measurement. Speech-to-text reloads in
process, with 55–82 s total per job. Measure its model load separately.
Rendering has no model in this workload.

Compare matched one-shot and warm runs with identical inputs and output
checks. Measure loads per accepted item, cold load versus compute,
end-to-end and per-item tail latency, throughput including parallel
one-shot placement, reclaimed GiB-seconds, and useful throughput from
backfill. Include reloading after yields, waits, pressure and release
latency. A warm batch may save accelerator time while serializing work
that formerly ran on separate machines. Choose batch membership based on
the measured tradeoff. [Consumer measurements](https://github.com/1kuna/mycelium/issues/2095).

## Stages and decisions

| Stage | Laptop acceptance | Fleet acceptance | Rollback |
| --- | --- | --- | --- |
| 1. Finite pieces with fixed reservation | `myce dev` runs real CPU main and loopback sidecar over several keys, observes one load, and verifies parity and exact accepted files. Fake-clock tests cover item failure, lost receipt, duplicate delivery, dependency readiness, preemption mid-item, worker loss, restart, cancellation during verification, and stale results/cleanup. | On a qualified native canary, compare output parity and loads per accepted item against one-shot runs. Exercise a restart from portable progress on a compatible target. Record production start and per-item latency before any adoption. | Stop new warm submissions and drain or cancel with exact release. Retain committed results. Resubmit only unfinished keys as ordinary pieces under new submission keys and preserve their mapping for consumers. |
| 2. Elastic grow and shrink from free room | Same API/runner using fake GPU inventory and injected clock. Exercise the entire revision handshake in both fixed and live pool simulations. Cover concurrent grants, lost/ahead/reordered acks, stop during limit installation, shrink with retained buffers, over-allowance demand, above-limit conflict, incomplete telemetry and all #2086 replays. Use a delegated Linux fixture to verify cgroup behavior independently. | Qualify each platform/allocator combination. Show whole-group residency and device quiescence, safe reallocation beside a co-tenant, conserved unified-memory credit, explicit-limit behavior, and equivalent fixed-budget debits. Compare measured cold/item/idle peaks and pressure to the fixed baseline. | Disable new elastic grants. Retain outstanding charges until exact settlement, then drain. Use fixed mode for new runs, and resume unfinished work with the stage 1 recovery procedure when needed. |
| 3. Contended continuation and reclaim | Fake-clock planner tests cover head protection, ranked wait, yield, victim selection, stop grace, exact reclaim, production arrival mid-item, two conflicting growers, worker loss with outstanding release, and no reload loop. End-to-end `myce dev` lets a resumable borrower use freed memory and verifies it checkpointing and returning that capacity. | Run a native canary where one model load serves several items and a lower-tier resumable batch performs useful work in idle memory, yields on demand and resumes. Report item wait and release latency, borrower throughput, production start latency and memory pressure against baseline. | Disable elastic reclaim, preserve issued stop/release obligations, and reconcile them. Fall back to free-room grants or fixed mode after any unknown custody clears. |

Use injected `Clock` and the existing API, plain-command worker, SQLite
queue and artifact store throughout. Workers advertise the supported
warm mode and enforcement capabilities. Older workers keep handling ordinary
jobs. A warm attempt without proven takeover drains naturally during normal
worker upgrade. Before any queue rollback, settle live warm custody and
retain receipt history in an adjacent-readable state or drain with a
verified export of those receipts. Unknown protocol state holds admission.
This proposal amends the job execution and memory-grant contracts after
approval and leaves the MY-40 study on its own approval path.
[Approved design](design-proposal.md) and [MY-40](DECISIONS.md#my-40).

1. **Approve finite, declared warm pieces as an ordinary submitted job
   mode?** Yes. This reuses submission, results and native sidecars,
   gives the first slice a closed work list, and makes item replay testable.
2. **Approve elastic grants at item boundaries after #2086's accounting
   prerequisites and platform enforcement qualify?** Yes. Keep fixed
   reservations available on paths that lack qualified evidence.
3. **Approve cooperative turns at boundaries and existing priority
   reclaim for contended grants after free-room qualification?** Yes.
   This keeps existing project fairness and allows production to reclaim
   capacity while preserving accepted items.

## Stage 1 implementation choices

- The worker passes one stream descriptor to main. Holders use it serially,
  sending one length-prefixed operation byte and reading one length-prefixed
  JSON reply before sending another request. The worker reads only stream
  bytes and accepts no ancillary descriptors. It closes the channel on a
  malformed frame, a stalled partial request or an unread reply. Time
  spent loading or processing between calls does not consume the frame
  timeout. Attempt cancellation closes the channel even when it is idle.
- The worker stages piece inputs under their declared key at attempt start.
  Each delivery receives a fresh output directory. File manifests use
  the existing verified publication pipeline and are downloadable by key
  as soon as they are accepted, including when another item fails.
  Memory learning keys a warm attempt by the frozen piece declaration,
  source and input references, model snapshots and ordinary runtime identity.
  Status measures the time from Started to the first offer, item start
  through receipt acceptance, and the interval between accepted items.
  Startup includes main initialization and items include verification.
- With `resumable: true`, the helper exposes a directory for the active
  item's ordinary file-store checkpoint generations and can publish one
  during execution. The worker can publish a final generation on orderly
  stop and retries transient publication failures before acknowledging
  release. A permanently refused final save records a cleanup diagnostic
  and releases the attempt with the last accepted generation intact.
  A settled receipt clears that item's resume data in the same queue
  transaction. File-store warm checkpoints can run with the NAS absent.
  Explicit `advanced.checkpoint` policies are refused for this mode.
- A reported failure records the failed key and exact attempt. Application
  diagnostics belong in job logs. Exhaustion records unfinished keys on
  the failed parent. Adapter result-check wrappers are refused. Main can
  run its item checks before reporting success.
- Fixed-mode boundary continuation keeps affinity within the selected
  project. A competing ready job must need this attempt's resources and
  outrank continuation before the worker yields. Host RAM and CPU contention
  can come from work assigned to another device on the host. Its admission still
  waits for exact release. A new attempt gets its first item under its
  ordinary placement turn. An empty remaining list runs the ordinary
  start, cleanup and final-result path without launching main or sidecars.

## Alternatives

- **Restore resident bindings or pins.** The retirement reduced unused
  model-serving machinery and kept demand-driven catalog serving and
  per-job model snapshots. Bindings would couple independent job attempts
  to another engine's placement, lifecycle and reservation. Pins would
  create desired residency without a running job. A warm group has one
  attempt, ledger and cleanup owner.
  [Resident retirement](https://github.com/1kuna/mycelium/commit/d682aa590)
  and [pins retirement](https://github.com/1kuna/mycelium/pull/2008).
- **Use an on-demand catalog model server.** This works for HTTP inference
  clients. It requires separate setup and puts calls on the service
  contract, with its own identity, retry and `idle_timeout`. It cannot
  provide a plain job's item receipts, checkpoint progress and elastic
  envelope by itself. [Catalog scheduling](services.md#scheduling-draining-and-recovery).
- **Append to a live job or pull from a second work queue.** Mutable
  membership complicates keyed replay, cancellation, final result
  completeness and idle custody. A finite list covers the observed batch
  load cost and lets the existing queue own delivery.
- **Put an item loop inside an opaque job.** This works today for load
  amortization under one peak reservation. It leaves the queue unable to
  accept individual items or lend and reclaim memory through the planner.

## Addendum: extension stages, per-item dependencies and in-engine slots

Proposed 2026-11-26 for [#2187](https://github.com/1kuna/mycelium/issues/2187).
Slice A is built under the approval above, which anticipated extension
support "when their contracts are exercised". The orchestrator approved
slices B and C as written on 2026-11-26 ([#2196](https://github.com/1kuna/mycelium/pull/2196)).
B is built, and C stays designed only. The trust model, settle semantics, recovery rules and
fixed reservation above apply unchanged to all three.

**Evidence.** A production consumer's 73-item, four-stage batch submits
each item as its own extension job, gated by `after` on its upstream stage.
Per item, the model stages pay 15.6–40.2 s from assignment to command
(12–18 s of it queue control calls), 9–14 s of sidecar start, and up to about
30 s of fixed in-command time. The replay in #2187 puts warm execution of
those stages at 40–55 min of batch wall time (316 → 262–274 min). It also
cuts 63–99 min of lane time on the discrete-XPU host and 27 min on the
unified-memory CUDA host, whose single LLM lane is the critical path.
Those stages are extensions, and their items become ready one at a time,
so they need both slice A and slice B.

### A. Extension stages (built)

- `advanced.warm_pieces: fixed` is accepted on `extension:` and `kind:`
  jobs with a `workload` describing one item and `fan_out.pieces` without
  `max_concurrency` or `share_xpu`. Programs and checks stay refused.
  Per-item stored inputs use `.myce-pieces/KEY/DEST`, as for plain pieces.
- The parent is selected, qualified and placed as one ordinary extension
  job. The worker lowers the variant as usual, then main and its sidecars
  start once. Variant parity holds (MY-19, MY-51) and the automatic
  qualification check are unchanged. The check runs the variant's one-shot
  `check` command, so a variant's main uses the helper only when
  `MYCE_WARM_PIECES_FD` is set.
- Items read inputs and write outputs in the attempt's staged directory,
  even when main runs in `{extension}`.
- Memory and duration evidence uses the work name `warm:<work type>`. A
  whole warm attempt is not comparable with one-shot runs, so its first run
  is sized like any new work: typed needs, with exclusive card use until
  comparable device evidence exists (MY-34). Qualification keeps the
  ordinary work type.
- Workers advertise `warm_pieces_extension_v1`. Older workers hold the job
  with a `capability` reason.

### B. Per-item dependencies (built)

Each piece may declare its own `after` with the existing grammar. Its local
names land in that item's input folder:

```yaml
extension: naming@1
workload: {input_shape: item-v1, units: 1}
advanced: {warm_pieces: fixed}
outputs: {labels.json: results}
fan_out:
  pieces:
    - {key: rec-001, after: {job: UPSTREAM_JOB_ID, piece: rec-001, outputs: {segments.json: segments.json}}}
    - {key: rec-002, after: {job: OTHER_UPSTREAM_ID, outputs: {segments.json: whole/segments.json}}}
```

1. **Membership stays frozen.** The approved design rejected live append.
   Producers must already exist, in the same project, with stored-file
   results. Each piece may name a different producer, and job-level
   `after` stays refused.
2. **Bindings.** The queue keeps one binding per key in `fan_out.warm`,
   with the same record and metadata-only binding as job-level
   `dependency`. A producer that ends without accepting that output settles
   the key as failed with `upstream_failed` (or `result_expired`). The key is
   never delivered, and the other keys continue. A cancelled producer cancels
   only that key. Cancelling the parent fences every unsettled key. The
   producer's `cancellation_scope` lists the warm parent as a dependent.
3. **Placement.** The parent becomes eligible when at least one unsettled key
   is ready. A production start window starts at the first ready key, and
   starts again at the first ready key after each requeue.
4. **Delivery.** `next` offers the first unsettled ready key in declaration
   order. The sequence stays the declaration index. A retry resumes with the
   ready unsettled keys, and settled keys never run again.
5. **Staging.** A delivery names the binding's immutable references. The
   worker downloads and verifies them into the item's folder before `start`,
   through ordinary transport and outside `q.mu`. Retention and claims
   count them as dependency inputs. An attempt receives only the files its
   own pieces declare, never a producer's other outputs. The worker removes
   an item's staged inputs when its receipt is acknowledged, and removes all
   attempt scratch at attempt end under ordinary cleanup.
6. **Idle bound.** When no key is ready, `next` returns `wait`. The queue
   decides this on the injected Clock. The attempt keeps its reservation
   while two things hold: the idle time since the last settle is under the
   attempt's measured load time, and `warmPieceYields` finds no contender.
   Otherwise it ends the segment as a planned requeue, with no failure or
   loss charge, and is placed again when a key becomes ready.
   Idling for at most one reload's worth wastes no more than the reload it
   avoids, so the total cost stays within twice the best choice in hindsight.
   The bound comes from each attempt's own measurement, not a fixed timer
   (MY-34).

Implementation notes:

- `next` answers `wait` to the worker, which asks again at its ordinary
  poll interval. Main's `next()` simply blocks.
- On acceptance the worker removes the whole item folder, which holds the
  item's own stored inputs as well as its staged dependency inputs.
- Workers advertise `warm_pieces_dependencies_v1`. Older workers hold the
  job with a `capability` reason.
- A ready binding is final, like a job-level one. A key bound before its
  producer was cancelled keeps its accepted inputs and still runs.
- If every remaining key fails upstream while the parent is queued, it is
  placed once to finalize with an empty list, as after an interrupted
  finalization.

**Consumer pattern.** Submit one warm parent per stage per group of items.
The consumer confirmed this fits all four of its stages, with groups of 8–10
recordings, and gates adoption on byte-identical output against one-shot runs
of public parity fixtures.
A first stage's inputs must be uploaded before submission, so it submits a
group as each set of uploads completes, and each group pays one load.
A downstream parent names each item's upstream piece or job, and is
submitted once those producers exist. Matching the group boundaries
across stages keeps that order free.

### C. In-engine parallel slots (designed only, not planned)

Two LLM-stage processes on one GPU measured 302 s each when overlapped,
against 139 s solo, so separate processes add nothing. A second lane there
has to come from one engine serving concurrent requests. The consumer then
measured two slots in its naming engine on that GPU. Throughput fell 2.45%
and label agreement got worse: that stage is GPU-bound. Slice C stays
designed only, for an engine that shows a measured gain.

- An opt-in `advanced.warm_slots: N` (2–8) lets the helper keep up to `k`
  deliveries outstanding. Each has its own sequence, output folder and
  receipt, and a stop fences all of them.
- `k` starts at 1. It rises to `k+1` only after measured aggregate item
  throughput at `k+1` beats `k` on comparable items, using the overlap
  learning of #2055 keyed by work and concurrency. It falls back when it
  doesn't. Two-process measurements never count as slot evidence.
- The fixed charge is the resident floor plus `k` times the measured per-item
  increment, taken from peaks observed at that `k`. Without evidence for
  `k+1`, the queue doesn't offer it. In fixed mode the charge changes only
  at a new attempt. Growing within an attempt waits for stage 2 (#2086).
- Build it only for an engine whose canary shows higher items per hour at
  `k` = 2 than at 1, with output parity.

### Slices and expected gain

| Slice | Content | Adopters | Expected saving |
| --- | --- | --- | --- |
| A, built | Extension stages with sidecars, no dependencies | A first stage, or any stage whose inputs exist at submission | Up to about 45 s per additional item on that stage's lane (15.6 s assignment to command, at most 29 s fixed time). Small makespan effect while the CUDA lane bounds the batch. |
| B, built | Per-item `after`, ready-order delivery, idle bound | Every model stage | 40–55 min of wall time in the #2187 replay |
| C | In-engine slots | Engines with a measured slot gain | None for the consumer's naming stage: two slots measured 2.45% lower throughput |

The smallest change that delivers most of the 40–55 min is A and B
together.

### Decisions

The orchestrator approved each recommendation below as written.

1. Approve B: frozen membership, per-piece `after`, delivery in ready order,
   and the measured idle bound? Recommended. It reuses the dependency and
   receipt records and adds no live append.
2. Keep C designed only? Recommended. The one measured engine lost
   throughput with two slots, and no other consumer has asked for it.
3. Should a variant declare warm support, so the queue can refuse a warm
   submission at submit time? Not yet. The submitter opts in, as for plain
   commands. A variant without the helper fails its first attempt with "main
   exited before all items settled".
