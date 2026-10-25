# Per-piece accepted-output readiness

Design proposal for [#175](https://github.com/1kuna/mycelium/issues/175),
2026-10-25. Reviewed source: `origin/main` at
`a3333dd15cb7904bfc7133a7187ad3e6ef9d80fd` (slice 4).
This note recommends a narrow addition; the [approved design](../design-proposal.md)
remains the only product spec. Implementation requires its amendment, not an
inference that slice 4 already supports stage graphs.

## Recommendation and existing coverage

Choose **(a), one explicit per-piece dependency held by the queue**. Submit the
producer, then submit ordinary next-stage jobs against its returned ID and stable
piece keys. The submitting application can exit; queue transitions make those jobs
runnable. This removes consumer scheduling without introducing a workflow service,
dynamic enumeration, timed downstream batching, or gang execution (MY-07/MY-09).
Ready means eligible for normal placement, not an immediate capacity reservation.

Existing features cover substantial parts, but not the readiness edge:

- [`Spec`](../../internal/myce/types.go) has no job dependency/`after` field.
  [`SubmitContext`](../../internal/myce/queue.go) admits ordinary queued work;
  supplying an upstream path/hash alone does not wait for upstream acceptance.
- [`submission.go`](../../internal/myce/submission.go) already provides atomic
  project/key binding, identical-spec replay, conflict detection, cancelled
  reservations and permanent expired-key markers. Reuse it for both jobs;
  submission identity does not express ordering or available output.
- [`maintainFanOut`](../../internal/myce/fan_out.go) already persists
  `FanOut.Results[piece] = {execution, output_refs, output_files}` before gather.
  `CompleteContext` verifies file-store output outside `q.mu`, then rechecks
  current machine/session/attempt and cancellation before accepting success.
  The result is the natural durable readiness fact; raw worker exit/upload is not.
- [Completion receipts](../agent-offload.md#completion-notification), implemented
  by [`profile_wait.go`](../../cmd/myce/profile_wait.go), observe terminal jobs
  with one detached caller-side waiter and a notifier. They provide adoption and
  delivery identities, not per-piece queue dependencies or a boot-surviving
  event stream. [#157](https://github.com/1kuna/mycelium/issues/157) explicitly
  prefers this observer before adding an outbox. Extend neither into a scheduler
  merely to solve #175.

**Acceptance granularity:** a measured wave is one attempt. Today all its pieces
become accepted together only after that command succeeds and its complete output
manifest is accepted. Serial fallback accepts one piece at a time. This proposal
starts downstream work before *parent gather*, not during a still-running wave.
Partial-wave publication would require another execution contract and is deferred.

## Comparing the options

| Concern | (a) Queue-held dependency | (b) Completion event or receipt |
| --- | --- | --- |
| Owner of readiness | Existing SQLite queue checks durable piece results. | Receiver observes acceptance and decides/submits the next job. |
| Caller disappearance | Predeclared jobs survive with the queue. | Observer/receiver must recover missed notifications and pending submissions. |
| Retry and cancellation | Serialize acceptance, binding and cancellation in existing queue transactions. | Receiver reconciles duplicate/out-of-order success, failure and cancel notices against queue truth. |
| Idempotency | Existing submission keys plus one persisted output binding. | Keys help deduplicate submission, but not missed events or late submission after cancellation. |
| Added machinery | Spec edge, readiness filter, output binding and retention/cancel integration. | Simple receipts are best-effort observation; reliable dispatch needs replay/cursors or an outbox, acknowledgements, deduplication and recovery ownership. |
| Fit for #175 | Declarative submit/wait seam; no consumer scheduler. | Useful for notifications; as the dispatch mechanism it recreates consumer orchestration. |

If Mycelium owns an event reaction that already knows the next job spec, it has
effectively implemented (a) with an extra delivery hop. Keep receipts as optional
observability. A future external event API can expose the same accepted facts;
it must not become correctness-critical for dispatch. No broker or third-party
workflow platform is needed for this one local queue and known finite piece set.

## Proposed minimum contract

Illustrative new syntax, **not supported at the reviewed commit**:

```yaml
name: summarize-first
project: research
submission_key: operation-42.summarize.first
extension: summarizer@1
after:
  job: PRODUCER_JOB_ID
  piece: first
  outputs: {input.json: metrics.json} # downstream local name: upstream output name
outputs: {summary.json: results}
```

Initially allow one edge on an ordinary batch/extension job, referencing an
already-submitted fan-out parent in the same project. Validate the full ID,
declared piece and nonempty mapping of declared output names; reject input-name
collisions and unsafe paths using existing artifact validation. Reject `after`
combined with `fan_out` initially. Existing-parent-only references and this narrow
shape avoid forward references/cycles without a graph engine. Multiple consumers
may reference a piece. Do not reference internal wave IDs: placement determines
wave membership later. Broader graphs and fan-in wait for demonstrated needs.

Persist the original client spec and fingerprint unchanged. Separately store the
resolved input binding: parent ID, piece key, producing child execution/attempt,
and selected refs or NAS-relative paths. Bind once from `PieceResult`, never from
the latest attempt or a guessed output location. Stage these as ordinary inputs
through existing file-store/NAS preparation; paths remain relative to each worker's
configured root. Missing or unavailable artifacts must never authorize execution.

Keep the wire state `queued` while blocked, with a structured readiness diagnostic
and source identity. Blocked jobs consume no execution resources, concurrency
slots, fair-use charges, model/build preparations, production holds or preemption
demand. One shared readiness predicate must gate planner demand as well as
assignment and Started. Acceptance and dependent binding/readiness commit in the
same queue update after `maintainFanOut`, before planning. Submission after
acceptance takes the same path. A restart recomputes unresolved edges from durable
facts; no polling process or timer is needed.

Start the server-generated production start window when the dependency becomes
ready; otherwise a predeclared job could expire before its input exists. Persist
that readiness time once. Explicit `start_by`, `give_up_at` and scheduling
deadlines stay absolute client intent and may expire while blocked. Dependency
waiting earns no runnable wait age; initialize `WaitingSince` at readiness.
Existing post-interruption deadline and retry rules then apply unchanged.

## Failure, retry and cancellation

| Upstream/downstream transition | Required outcome |
| --- | --- |
| Producing wave loses a worker or is interrupted and requeued | Dependents on its unaccepted pieces remain blocked. Reuse fixed wave membership/count, retry budgets, checkpoint and exact-release fences. Accepted other waves are untouched. |
| Worker sends stale/duplicate success or only uploads a file | No new acceptance or binding. Current-attempt success and complete durable outputs are required; replays cannot change an accepted binding. |
| A wave fails terminally, including its existing measured-wave OOM outcome | Existing parent failure and cancellation of unfinished producer children remain. Dependents without an accepted source fail with a typed upstream-failure reason, not endless waiting. |
| Another piece fails after this piece was accepted | This piece's consumers remain valid and may run/finish. Parent failure does not retract accepted independent outputs or roll back downstream effects. A late submission can use that accepted piece while artifacts remain available. |
| A downstream attempt fails or loses its worker | Use that job's existing failure/retry rules and the same bound input. No upstream retry or sibling cancellation. A deliberate rerun uses a new submission key. |
| Explicit parent cancellation, even after success/failure | Apply cancellation to producer children and all registered dependents, stopping nonterminal consumers, including ready/running ones. Future dependencies on that cancelled parent bind only cancelled jobs. Preserve terminal states/results and add exclusion metadata as today. |
| Cancel only a downstream job/key | Reuse current cancellation/reservation behavior; no reverse cancellation of producer or sibling consumers. |

Explicit parent cancellation is a pipeline abort for its registered consumers;
ordinary parent failure is not. Distinguish the existing automatic cancellation
of unfinished producer waves from that explicit abort. Serialize cancellation
with acceptance and downstream submission: a committed cancel prevents later
binding/assignment; an already-assigned consumer uses the normal stop path.
Expand the parent's cancellation release aggregation to cover downstream attempts
and all their old copies, not just producer waves. `will_not_run` is durable
exclusion; `release: pending` remains until exact cleanup is proven. Cancellation
cannot undo external effects or erase completed accepted outputs. Applications
still own final publication and must keep repeat execution safe.

## Gather, retention and reuse

Keep gather unchanged: every producer piece must succeed before the parent
succeeds and exposes final `KEY/OUTPUT_NAME` outputs. Downstream outcomes neither
delay nor fail producer gather. Applications still own semantic reduction; if
they need all next-stage results, they retain those returned job IDs and wait for
them. This slice adds no automatic second gather or all-or-nothing publication.

Artifact lifetime needs explicit integration. [`file_retention.go`](../../internal/myce/file_retention.go)
currently walks `Spec.Files` and `OutputRefs`, not partial `FanOut.Results` or
new bound inputs. Include partial accepted results in the parent's retention
window and bound inputs in each dependent's live/terminal reference window.
This prevents a completed wave's files expiring while gather or a queued consumer
still needs them. Reuse the existing collector and `fileLifecycle` fencing; do
not add another blob store or indefinite retention. Reject a new binding whose
source result has expired with the existing `result_expired` contract. Replay of
an existing submission still returns its original job, even after source expiry.
NAS bindings retain the existing attempt-specific publication/storage contract.

Reuse `Spec.Validate`, `Queue.update`/SQLite persistence, `PieceResult`, the
completion verifier, ordinary input staging/runner, planner and stop/requeue
paths. The new work is primarily the admission edge and persisted binding, shared
readiness gate, cancellation enumeration, retention references and diagnostics.
Any blob verification or NAS I/O stays outside `q.mu`; snapshot first, perform I/O,
then recheck source, dependent and cancellation fences before committing.

Use stable keys for both submissions and persist the producer's returned ID.
Each consumer key represents operation/stage/piece, not attempt number. The
`after` selector and output mapping participate in the existing canonical client
fingerprint; resolved hashes/times do not. Same-key concurrent/retried submits
return one job; changed edge/mapping returns 409. Preserve early replay before
new validation, cancelled-reservation precedence and 410 expired-key behavior.
No exactly-once external-side-effect claim is added. Separate submissions are not
an atomic pipeline: an interrupted submitter resumes its saved finite submission
list using keys, while already-submitted jobs progress without it.

## Laptop proof and slices

1. **Approve and implement the narrow contract.** Amend the sole spec, add the
   edge/binding, readiness gate, deadlines, retention and cancellation behavior as
   one coherent queue change. Fake-clock regressions must show a consumer becomes
   ready on accepted output while another producer wave remains unfinished;
   cover both arrival orders, duplicate/key-conflicting submission, current-vs-stale
   retry results, upstream terminal failure, cancellation/acceptance races, reopen,
   and retention while a consumer waits. Exercise a measured wave's joint
   acceptance rather than implying partial-wave streaming.
2. **Prove the plain-command seam and document it.** Extend `myce dev` with tiny
   CPU producer/consumer extensions and fake inventory using the real API/runner.
   Verify consumed bytes through file-store transfer and a local-directory NAS
   fixture, one early consumer before producer gather, restart/replay, parent abort
   with exact downstream cleanup, and absence of `MYCE_TOKEN` in payloads. Update
   fan-out/job/client docs and generated reply schemas. No GPU or fleet needed.
3. **Compare against gather-then-stage before consumer integration.** Extend
   [`sim.go`](../../internal/myce/sim.go) and [`sim_batch.go`](../../internal/myce/sim_batch.go),
   which already drive the real queue planner with synthetic execution timers.
   Predeclare the same next-stage jobs in both modes: baseline eligibility waits
   for successful producer gather; candidate uses real per-piece dependencies.
   The baseline barrier is a simulator control, not a new production API. Preserve
   identical arrivals, piece work, downstream work, resource floors, curves, policy,
   owner availability and seeds; no consumer event handler chooses placement.

Compare a homogeneous control, heterogeneous wave completion/short tails, separate
stage capacity, shared-stage capacity, and the existing mixed-production workload.
Model producer publication/verification and downstream input transfer/preparation
separately in both modes; acceptance follows producer publication, not compute
exit. Record first downstream start, per-piece
acceptance-to-ready and acceptance-to-start, full pipeline makespan, producer
gather time, production start p95/unstarted count, unfinished/failed/cancelled
counts, allocation use and peak intermediate bytes. Verify complete keyed output
coverage; a smaller makespan from skipped/failed work is not a gain. Include a
controlled loss/retry and cancel run for propagation, separate from speed claims.

Require an earlier downstream start and lower end-to-end makespan in the
overlap-capable case, exactly unchanged production p95 in the paired mixed-load
qualification (matching slice 4), and no new unfinished work. Report per-scenario
regressions, including producer slowdown from downstream contention; do not hide
them in averages or promise universal speedup. Synthetic timings establish queue
behavior, not native throughput. Use focused tests/quick gate per change and the
full gate once for the implementation batch; native acceptance stays separate.

**Current state:** design only; source inspection found the reusable acceptance,
keyed submission and notification seams. No dependency implementation or simulator
comparison has run. **Next owner/step:** the orchestrator reviews this recommendation
on #175 and scopes the spec amendment and first implementation slice. Existing
#175 tracks the remaining implementation, simulator evidence and documentation;
no new issue or fleet action is needed for this design handoff.
