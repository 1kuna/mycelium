# Learned storage admission

Proposal for [#504](https://github.com/1kuna/mycelium/issues/504), 2026-10-31.
The [approved design](design-proposal.md) remains the only product spec; this
proposal needs acceptance before implementation. No deployment is implied.

## Predict work, rather than prescribe capacity

Reuse memory envelopes' accepted-attempt evidence, identity checks, exploratory
bootstrap and SQLite observations; extend that path with storage facts, not a
second learner. Record complete verified output-manifest bytes per program,
recipe/source digest, variant, output-affecting arguments and input shape
(known bytes, item count or declared work units). For plain commands use their
frozen command/environment/input identity. Keep logical output size separate
from newly allocated, deduplicated store bytes. Cache hits teach output size,
not a misleading zero. Track worker scratch/spool peaks separately.

Initially use the largest comparable complete output plus the larger of its
observed range and largest positive prediction error, rounded to allocation
units. Only interpolate across measured input shapes; an unseen size or variant
uses `spec.disk.output_bytes` as an optional first-run prior, then exploration.
With neither evidence nor prior, serialize exploration on the affected volume,
using remaining uncommitted room as its provisional envelope. Unknown is not
zero. Final manifests replace estimates with exact missing bytes. Neither the
memory multipliers nor pressure-tainted peaks become storage sizing constants.

Failed, truncated, stale or disk-constrained attempts cannot teach a smaller
need or erase accepted neighbours (#303's lesson). Keep their lower bounds and
rejection reasons for the next exploration. Persist sample count, shape, source,
prediction/error, logical bytes and physical increment; show these per program
in status. Reuse evidence across machines only for equivalent qualified output
variants. MY-27/MY-26 placement still considers every compatible host, choosing
MY-18's least valuable fit; storage evidence never qualifies a variant.

## One admission and reclamation path

On submission/dispatch and each new allocation:

1. Probe physical free space and snapshot references/commitments. Run existing
   retention and #479's collector before a capacity refusal. Continue its bounded
   batches while they make useful progress; a busy publication fence means a
   visible wait and retry, not zero reclaimable space.
2. Expedite eligible delivery and expiry. Among legally disposable bytes prefer
   already delivered copies, then expired terminal data and rebuildable inactive
   caches, using tier and measured recomputation/transfer cost to break ties.
   This applies MY-18's opportunity-cost principle; class alone never permits
   deletion. Stop after enough measured space is reclaimed.
3. Admit in existing owner-tier/fairness order. Persist the selected attempt's
   predicted output commitment before Started, extending today's async-NAS
   reservations to every managed file-output path. Queued production demand is
   considered before new experiment commitments. Revoke lower-tier unstarted
   commitments or use supported checkpoint/stop/requeue; keep already-written
   bytes and uncertain custody charged until verified release/collection.
4. Exchange predictions for exact manifest/tus commitments without double
   charging shared digests. Inputs and imports use their known lengths. Apply
   the same accounting to preflight, tus creation, native/Docker/async results,
   checkpoints and retries. Preflight remains advisory; allocation is serialized.

Production gets a demand-sized commitment, not a standing 16 GiB pool. Accepted
commitments survive restart and cannot be consumed by later Mycelium writes,
including unidentified clients. With no production demand, no idle production
reserve remains. A prediction miss first reclaims and yields lower-tier work,
then acquires the delta; an immutable worker spool survives until acceptance.
If it still cannot fit, hold publication/start with the exact deficit and owner
of protected bytes. Stopping compute does not free retained results.

**Guarantee:** admitted production has priority-backed space for its committed
bytes. Predictions cannot guarantee unlimited future output, arbitrary future
arrivals or writes by unrelated OS users. If all bytes are uniquely protected,
production must wait for delivery/expiry or more storage; deleting them would
violate the spec. Keep start deadlines and infrastructure retry budgets intact.

## Follow the disk; deliver, then discard

For each physical volume, `admissible = max(0, free - safety_floor - unwritten
commitments)`. Existing bytes are already absent from `free`; count tus remaining
lengths and predicted/exact missing outputs once. Merge aliased roots, including
queue and worker roles sharing a volume. Project attribution remains visible
(shared physical digests counted once), but #464 gets no project quota or daily
write budget. Fairness and tiers reuse the scheduler.

Measure the floor from control-state/WAL/log/sync working space plus the largest
observed unaccounted free-space fall over a reaction interval: sample age plus
measured stop/reclaim latency. Retain burst maxima, not average retained growth;
include filesystem allocation overhead. This protects control transactions while
writes are stopped, at the cost of that measured headroom. Missing/stale probes
withhold new allocations; unknown cold-start measurements retain today's
`max(1% of volume, 256 MiB)` bootstrap floor until replay validates its measured
replacement. Sample/measure with the existing disk probes and injected Clock.
Monitor growing scratch between admission and completion; request more room or
stop through the existing contract before the floor, without claiming a hard
filesystem quota against arbitrary external writers.

Tasks still select a destination and preservation semantics: retained recovery
or proposed `until_delivered`. These express ownership, not guessed sizes. NAS
uses the existing single publisher; submitter delivery uses a resumable client
pull, checksum verification, sync and exact execution/manifest acknowledgement
recorded in the job ledger. Never connect a queue publisher to arbitrary client
paths. For `until_delivered`, verified delivery plus terminal/released custody
authorizes #479's existing expiry transaction and collector automatically.
Preserve downstream/shared references, checkpoints, registry assets, publication
grace (six hours) and unknown imports. The policy explicitly gives up queue
recovery of expired job files. Expired retrieval remains typed; keep receipts and
keyed answers. Legacy retention remains 168 hours. Operators set storage roots,
priority policy and genuine external capacity constraints, not admission sizes.

## Evidence, failures and cutover

The 2026-10-31 incident was a configured-ceiling exhaustion: maintenance freed
zero because retained/protected references held the bytes. A roughly 35-hour
inventory measured 1.55 GB/hour net growth, but only 0.28 GB conditional expiry
in the following day. A one-off large registry import distorted that average;
short bursts were much faster. These sanitized observations motivate demand
accounting and delivery, not another rate-derived quota. Do not delete private
evidence or unrelated filesystem data.

| Retire | Replacement evidence / condition |
| --- | --- |
| 100 GiB default; historical 150 GiB and live s061 185 GiB caps | Fresh physical free space, remaining commitments and measured floor; replay the ceiling incident. |
| Flat 16 GiB production reserve, manually sized as two batches plus retries | Actual queued/admitted tier demand; each retry reacquires its own commitment. |
| Message-agreed daily budgets, including 1 and 2 GB/day; proposed #464 quotas | Known upload lengths, learned output demand, tier/fairness ordering and verified reclamation. |
| Mandatory output-size guesses | Optional `disk.output_bytes` cold prior; complete manifest history thereafter. RAM hints are outside this change. |
| Fixed 1% / 256 MiB floor after bootstrap qualification | Measured control footprint and worst observed consumption during reaction latency. |

Burst requests serialize reservations; sustained retention exceeding disk becomes
an explicit hold. NAS outage pins undelivered outputs, reduces admission and
leaves local-input non-NAS work eligible; it never invents a delivered receipt or
changes checkpoint mode. Restart reconstructs commitments and rechecks exact
attempts. All probes, hashing, delivery and deletion run outside `q.mu`, followed
by reference/attempt fence checks. One binary, one SQLite queue, existing workers
and collector suffice; no peer storage service or replicated registry.

Migrate in stages: add observation/status in shadow mode under s061's 185/16 GiB
settings; finish s061h1's supported installer migration (#477/#485), saving and
verifying those same values before the release owner retires the exact ExecStart
drop-in. That removes an override, not the capacity policy. Subsequently introduce
explicit automatic admission, persist/reconstruct existing tus and output
commitments, upgrade input clients (including Python), and compare shadow demand,
floor and collector progress. Enable only after incident/miss/burst/outage/restart
replays pass; atomically switch off the cap/reserve checks and report legacy
values as inactive. Preserve 168-hour legacy data and never rewrite keyed specs.
Retire manual budgets after verified delivery/reclaim progress. Any rollout is
separately authorized; this design performs none.

Open question: For newly authored tasks with a destination, should
`until_delivered` become the default? Recommend yes, with explicit retained
recovery available; until decided, require opt-in and preserve all old policies.

## Implementation sub-issues

Each phase uses byte-sized temp volumes, real HTTP/SQLite/CPU commands and fake
Clock/probes; quick per change and full once before a separately authorized
deployment. Hardware qualification is separate.

- **[Learning #505](https://github.com/1kuna/mycelium/issues/505).** Outcome: per-program output envelopes/status.
  Current state: manifests and memory observations exist. Next: extend them.
  Acceptance: shape/variant fences, optional prior/exploration, constrained-sample
  preservation, dedup distinction and restart visibility.
- **[Physical accounting #506](https://github.com/1kuna/mycelium/issues/506).** Outcome: free-disk admission/floor.
  Current state: probes and tus lengths exist. Next: unify volume commitments.
  Acceptance: aliased roles, measured floor/bootstrap, stale probes and external
  growth leave control space without double charges.
- **[Tier admission #507](https://github.com/1kuna/mycelium/issues/507).** Outcome: production demand displaces lower
  tiers. Current state: reserve and async commitments exist. Next: extend the
  planner. Acceptance: concurrent bursts, misses, retries, stale attempts and
  restart preserve commitments; protected bytes cause truthful holds.
- **[Delivery/discard #508](https://github.com/1kuna/mycelium/issues/508).** Outcome: #201 completion lifecycle.
  Current state: NAS publisher and #479 explicit release exist. Next: add task
  policy and submitter receipts. Acceptance: no deletion before verified delivery,
  custody release and reference checks; NAS/client outage/replay remain safe.
- **[Migration/proof #509](https://github.com/1kuna/mycelium/issues/509).** Outcome: retire stopgaps. Current state:
  s061h1 migrates settings only. Next: shadow, replay, then explicit mode cutover.
  Acceptance: 185/16 GiB/drop-in migration, old-client visibility and the incident
  reproduce on a laptop; no legacy retention or keyed-answer changes.
