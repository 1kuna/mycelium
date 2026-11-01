# Learned storage admission

> Pending storage review under [#564](https://github.com/1kuna/mycelium/issues/564), item 3.
> This note is unchanged pending the parallel storage decision; it does not activate protection.

Revision 3 for [#504](https://github.com/1kuna/mycelium/issues/504), 2026-10-31.
The [approved design](design-proposal.md) remains the only product spec. This
proposal needs acceptance, implementation and native qualification. Inspection
was read-only; no quota, fleet or runtime behavior changes are claimed.

## Separate experiment custody from production capacity

Unique experiment outputs stay on the producing worker, including while their
destination is healthy. Extend the existing immutable, synced attempt spool's
custody through verified destination delivery, rather than queue acceptance.
A health probe never authorizes uploading unique experiment bytes into the shared
store: the destination can fail during transfer. Shared storage holds production,
inputs and legally reclaimable experiment output copies. A delivered copy still
pinned by recovery policy, publication grace or downstream references remains on
isolated worker capacity until those protections permit shared-copy reclamation.
Attaching a new protected reference requires fresh capacity admission.

**Isolation is physical, not a directory name.** Worker backlog uses independent
disks or fully backed bounded filesystems/reserved volumes excluded from advertised
shared capacity before admission. On a co-located queue/worker, a project quota
alone bounds growth but does not reserve production availability. If independent
backing cannot be proved, route/hold output-writing experiments; never spill their
unique data into shared storage. The cost is stranded local capacity and reduced
experiment placement, rather than production blocked by experiment custody.

Each worker charges retained spools plus active attempts' remaining hard limits
against its local budget, reconstructing them after restart. When that budget
fills, stop new output-writing experiment admissions there; other hosts remain
eligible. Destination outages retain bytes and retry delivery within those bounds.
Production on other hosts and shared production capacity are unaffected by that
backlog. Production can still exhaust its own capacity or be blocked by protected
inputs/legacy data: isolation is not unlimited disk.

Reuse the queue-owned publisher's staged copy, verification and exact
execution/manifest receipts with a worker-spool source. Run source-local NAS copy
through the existing worker command/control path, qualifying its destination root
by stable destination ID; submitter pull can relay through queue HTTP with bounded
memory and no shared-disk staging. SQLite remains the sole delivery ledger; no
peer storage network, new daemon or replicated registry. A synced spool receipt
can release compute resources while recording delivery pending; it does not claim
ordinary file-store `output_refs` availability or NAS success. Clients explicitly
see local custody/pending delivery. Adopt this changed result contract only for
new/versioned policies and upgraded clients. Worker loss retains the custody
record and alerts its owner; it cannot manufacture durability, authorize deletion
or silently overwrite unique results on retry.

## Learn per-job demand; admit by tier

Extend memory-envelope observations, identity fences and SQLite persistence to
logical output bytes, allocated scratch/spool peaks and file counts, keyed by
program, frozen recipe/source, variant, output-affecting arguments and input shape.
Deduplication changes physical allocation, not learned output size. Complete
verified manifests teach output need; incomplete, stale or constrained attempts
cannot shrink accepted need or erase clean neighbours (#303). Preserve failed
lower bounds and provenance separately.

Use the largest comparable complete peak plus the larger of observed range and
positive prediction error: `limit = peak × (1 + margin/peak)`, rounded to filesystem
units. Include simultaneous spool, metadata and stop/checkpoint writes; stage
inputs additionally. Optional `spec.disk.output_bytes` seeds output-only cold
starts; unknown scratch needs bounded exploration. Status shows evidence, shape,
error, bound and enforcement. Remove next-batch/cadence/burst forecasting: custody
isolation protects production even when predictions miss.

For each independently backed capacity pool, admit against fresh physical free
space minus measured control space and unwritten accepted hard limits. Count
aliased roots, missing digests, remaining tus lengths and native backing/reserves
once; existing bytes already reduce free space. Persist commitments before Started
and retain them through exact cleanup/restart. Extend existing reservations and
tier planning across input, native/Docker results, checkpoints and retries.
Production takes priority; MY-27/MY-26 consider every qualified compatible variant
and host, with MY-18 least-valuable fit.

Before refusal, run retention and #479's collector in bounded batches while they
make progress. A publication fence means wait, not zero reclaimable bytes. Prefer
authorized delivered copies, expired terminal files and inactive rebuildable
caches by tier/replacement cost; count observed freed bytes. Yield lower-tier
unstarted commitments or use supported stop/checkpoint/requeue; stopping compute
does not free retained blobs. If production still cannot fit, hold with exact
deficit and existing deadline/retry semantics. Send a deduplicated alert to the
queue operator and affected production project's agent/owner through configured
routes, naming protected holders, destination health and next action. Never delete
unique bytes.

## Enforce scratch with standard filesystem limits

Read-only filesystem inspection found ext4 on both inspected Linux roles, with
project quotas inactive; one superblock lacks project/quota features and the
other was inaccessible without more privilege. Two reachable Macs use APFS with
Data quota/reserve zero; another Mac was unreachable directly and through a
working relay. No backend is qualified by this inspection.

Prefer inherited per-attempt hard block/inode project quotas on enabled
[ext4/XFS](https://man7.org/linux/man-pages/man5/ext4.5.html). On macOS use
[APFS volume quotas/reserves](https://support.apple.com/guide/disk-utility/add-delete-or-erase-apfs-volumes-dskua9e6a110/mac),
not directory project quotas. Linux fallback is a fixed-capacity, fully allocated
loop filesystem. Charge complete backing/reserves and overhead before launch;
provision through separately authorized installer setup, never runtime root
remounts. Qualify actual allocation/denial behavior before advertising support.

Confine main/sidecar temp, output, spool and local checkpoint writes using existing
sandbox grants; shared inputs/caches are read-only, worker publication separately
charged. Frozen unsandboxed policies require equivalent confinement or route/hold.
A quota/full failure becomes `disk_limit_exceeded`, failing that attempt like OOM;
retain unique progress, diagnostics and lower-bound evidence. Larger re-admission
must acquire space. An unsupported backend gets zero protected-volume write
allowance; only physically independent writable storage permits conservative
placement. Serial execution, sampled kills, sparse maxima, per-file limits and I/O
throttles do not bound aggregate writes.

Measure SQLite/WAL/log/sync/checkpoint working space and backend metadata overhead
for the control floor. Enforce it outside all possible managed expansion through
native reserves or fully allocated backing, counting each once. Measurements and
enforcement are both prerequisites; the historical 1%/256 MiB floor is insufficient.
Unrelated unbounded OS writers require separate isolation. Probes guide placement
and learning, never inter-probe write safety.

## Deliver, discard and migrate

New destination tasks default to `until_delivered` (#201); explicit retained
recovery and frozen/keyed legacy policies remain. Only verified, synced delivery,
released custody and authorized policy permit #479 expiry/collection. Preserve
shared/downstream references, checkpoints, registry assets, unknown imports,
six-hour publication grace, 168-hour legacy retention and typed expired retrieval.
Operators set storage/destination roots, preservation semantics and owner tiers
because they express ownership; agents choose software/variants, not byte budgets.

| Retire | Replacement evidence |
| --- | --- |
| Default 100, historical 150 and live 185 GiB caps | Physical free capacity, admitted enforced bounds and measured control footprint. |
| Flat 16 GiB production reserve | Structural experiment-custody isolation plus per-job production admission. |
| 1/2 GB daily agreements; proposed #464 quotas | Tier placement, bounded local custody and enforced attempt limits; keep attribution. |
| Mandatory output guesses; 1%/256 MiB floor | Optional cold prior, accepted peaks/error and measured enforced control space. |

Shadow under s061's 185/16 GiB settings. s061h1/#477/#485 saves/verifies equivalent
settings before retiring the exact ExecStart drop-in; this does not activate auto
admission. Drain legacy shared experiment backlog only after verified preservation
and authorized release; otherwise report migration incomplete and retain existing
guards. After #506 isolation/denial qualification, #508 custody/client upgrades
(including Python) and restart reconstruction, explicitly disable legacy cap/reserve
checks together, showing saved values inactive. Retire manual budgets after actual
delivery/collection progress. No legacy policy rewrite or fleet change here.

## Replay the reviews and incident

These are implementation acceptance scenarios, not claims of implemented behavior.
Units below exclude already funded control/backing overhead.

| Scenario | Required result |
| --- | --- |
| Round 1: 100 shared units, experiment produces 95, production arrives needing 10 | 95 stays on independently backed worker storage (or experiment holds there); shared 100 remains available for production's 10. No forecast premise. |
| Round 2: complete 4-unit history, zero range/error; experiment produces 96 while NAS is healthy, then outage; production needs 6 | Keep prediction/bound 4: first attempt fails. Re-admit 6 from shared 100; worker's protected 96 never entered it. No larger forecast margin or NAS recovery required. |
| Scratch: 12 free = 8 production + 2 control + 2 experiment; experiment writes 12 between probes | Its 2-unit hard boundary denies growth; production publication and WAL sync succeed. |
| Local backlog fills; outage/restart/lost acknowledgement or worker loss | Hold that worker's new output-writing experiments, retain exact custody, reconcile receipts, alert on loss; other hosts continue. |
| 2026-10-31: configured ceiling reached, zero eligible reclaim, physical room beyond cap | Collector truthfully frees zero; legacy references remain protected. Qualified auto mode can use measured physical room. Retained growth eventually holds/alerts if production/inputs fill it; cap removal cannot create reclaimability. |

Use injected Clock/probes, small files, real HTTP/SQLite/CPU commands and local
bounded-volume tests. Cover mid-transfer outage, spool duplication and protected
reference acquisition. Fake quotas prove scheduling only; native denial/isolation
is a separate qualification gate. One binary/SQLite queue suffices; I/O remains
outside `q.mu` with fence rechecks.

## Implementation sub-issues

Filed issues each retain Outcome, Current state, Next step and Acceptance criteria:
learning [#505](https://github.com/1kuna/mycelium/issues/505), enforced accounting
and isolation [#506](https://github.com/1kuna/mycelium/issues/506), tier admission
[#507](https://github.com/1kuna/mycelium/issues/507), worker custody/delivery/discard
[#508](https://github.com/1kuna/mycelium/issues/508), migration and incident proof
[#509](https://github.com/1kuna/mycelium/issues/509). #505/#506/#508 gate #507 activation;
#509 integrates all. Quick per change, full once per integrated implementation
batch before separately authorized deployment. Open product questions: none.
