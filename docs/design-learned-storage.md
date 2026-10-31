# Learned storage admission

Revision 2 for [#504](https://github.com/1kuna/mycelium/issues/504), 2026-10-31.
The [approved design](design-proposal.md) remains the only product spec. This
proposal requires acceptance, implementation and native qualification; the
filesystem inspection below was read-only, with no quota or fleet changes.

## Learn demand; protect the next production batch

Extend memory envelopes' accepted-attempt observations, identity fences and
SQLite persistence. Learn logical output bytes, allocated scratch/spool peak and
file count by program, frozen recipe/source, variant, output-affecting arguments
and input shape. Deduplication reduces physical allocation, not learned logical
size. Complete verified manifests teach output size; incomplete, stale or
quota-constrained attempts cannot lower accepted need or erase clean neighbours
(#303). Preserve rejected lower bounds and provenance independently.

Use the largest comparable complete peak, with margin equal to the larger of
observed range and positive prediction error: `limit = peak × (1 + margin/peak)`,
rounded to filesystem units. Size scratch, simultaneous immutable spool,
checkpoint-on-stop writes and metadata together; input staging is additional.
Unknown shapes use optional `spec.disk.output_bytes` as a first-run output prior,
then serialized exploration inside an enforceable allocation. Unknown scratch
never inherits an output-only estimate. Status shows source, shape, samples,
limit, error and enforcement. MY-27/MY-26 consider every qualified compatible
variant/host; MY-18 still selects the least valuable fit.

Before experiment admission, protect **queued production plus the next expected
batch**, not just already-running jobs. Learn next-batch program mix, item sizes,
overlapping count and cadence from accepted production/fan-out history; size its
outputs and scratch using those envelopes, including measured error margins.
Use the largest comparable observed burst, not average arrival bytes; expose the
forecast and its evidence. Count queued/running members once, and keep the next
batch's uncommitted allowance separate from accepted commitments. Unknown next
batch evidence withholds experiment growth on the protected volume until a
production exploration/prior supplies it; other eligible storage/hosts remain
usable. Recompute on submissions, retries, forecast changes and delivery progress.
Do not decay the forecast during an outage.

For each physical volume, experiments fit only inside:
`free - measured_control_floor - unwritten_hard_limits - uncommitted_production_forecast`.
Merge aliased roots, including queue and worker roles. Existing bytes already
reduce `free`; charge missing digests/tus remaining lengths and scratch expansion
once. Persist commitments before Started; retain them through restart and exact
cleanup. Extend existing async-NAS reservations and owner-tier/fairness planning
across input, native/Docker results, checkpoints and retries, rather than creating
another scheduler. Input/import lengths are known; preflight remains advisory.

A destination outage immediately pauses **new experiment admissions** producing
outputs for that destination, including local scratch/spool allocations; running
attempts finish only within their fixed limits. Production takes precedence.
Undelivered unique data remains protected and charged. Resume experiments only
after destination health, backlog and forecast fit are verified. A prediction
miss within the production hard limit is already funded; exceeding that limit
fails the attempt like OOM. Larger future envelopes require fresh admission.
If even production cannot fit after collection/yield, hold with exact deficit
and preserve existing start deadlines/retry rules. Emit a deduplicated health
alert to the queue operator and affected production project's agent/owner through
existing configured notification routes; status names protected backlog, failed
destination, forecast and next action. Never delete unique data to make the
promise true. Forecasts cover the next expected batch, not arbitrary unbounded
future arrivals; a prolonged outage eventually needs restored delivery or capacity.

## Enforce writes with standard filesystem boundaries

Read-only `findmnt`/superblock and `diskutil` inspection found:

| Inspected deployment role | Finding and implication |
| --- | --- |
| Linux queue/worker and accelerator worker | ext4; project quotas not active. The inspected queue-host superblock lacks project/quota features; the other superblock was unreadable without additional privilege. No current project-quota capability is claimed. |
| Two reachable macOS workers | APFS; their Data volumes have no quota/reserve. A third worker was unreachable directly and via a verified SSH relay; its filesystem/enforcement remains unverified. |

Prefer [ext4/XFS project quotas](https://man7.org/linux/man-pages/man5/ext4.5.html)
when genuinely enabled: an inherited project ID per attempt, hard block/inode
limits, and no writable uncharged paths. Do not enable features or remount root
as a runtime fallback. On APFS use standard
[volume quota/reserve](https://support.apple.com/guide/disk-utility/add-delete-or-erase-apfs-volumes-dskua9e6a110/mac)
for bounded writable volumes; APFS is not a directory project-quota filesystem.
A quota bounds growth; a reserve backs availability. Provision/control them
through a narrowly scoped, owner-authorized installer path, not payload authority.
Neither filesystem identification nor the manuals qualify enforcement: #506 must
prove actual allocation, metadata and denial behavior before advertising support.

Where directory quotas are unavailable, prefer a fixed-capacity, fully allocated
loop filesystem (Linux) or bounded APFS volume (macOS). Count its complete backing
allocation/reserve and filesystem overhead up front; no sparse-image maximum,
per-file `RLIMIT_FSIZE`, cgroup I/O throttle or sampled kill substitutes for a
total bound. This standard fallback costs allocation/mount time and temporarily
unused disk, but prevents an inter-probe write from consuming another job's space.
Without a qualified bounded backend, allow only work whose writable storage is
physically separate from protected volumes; otherwise its protected-volume write
allowance is zero and placement waits/routes elsewhere. Merely serializing an
unbounded writer is insufficient. This is a capability hold, not a host pin.

Bind all main/sidecar work, temp, output, spool and local checkpoint paths to the
attempt boundary using existing sandbox write grants; stage shared inputs/caches
read-only and charge worker-mediated publication separately. Legacy unsandboxed
specs are not rewritten: they cannot claim protected shared-volume admission
without equivalent write confinement. Retain unique progress and pending spools
within their charged boundary until authorized collection. Quota/contained-full
write failures fail the attempt as `disk_limit_exceeded`, retain diagnostic/lower
bound evidence and require larger exploration next time, not unlimited retries.
A payload cannot raise its limit. Stop/checkpoint allowance is funded before launch.

Derive the control floor from measured SQLite/WAL/checkpoint/log/sync working
space and backend allocation/metadata overhead, not historical payload burst rate
or a new chosen percentage. Enforce it by leaving that capacity outside the sum
of all possible managed hard-limit expansions; use native control-volume reserves
or physically allocated backing where supported. Count already allocated backing
once; availability probes and reservations must not deduct native reserves twice.
Missing measurements/backend enforcement withhold shared-volume admission;
the old 1%/256 MiB bootstrap is not production proof. Probes guide placement and
learning, not write safety. Protection against unrelated unbounded OS writers
requires separate capacity isolation; never advertise a root-volume guarantee.

## Reclaim legally; deliver and discard

Before refusing capacity, reuse retention and #479's collector, advancing bounded
batches while they make useful progress. A busy publication fence means wait,
not zero reclaimable bytes. Prefer authorized delivered copies, expired terminal
files and inactive rebuildable caches, ordered by tier and measured replacement
cost. Count only observed freed space. Yield lower-tier unstarted allocations or
use existing stop/checkpoint/requeue; stopping compute cannot free retained blobs.

New destination tasks default to `until_delivered`; explicit retained recovery
and frozen/keyed legacy policies remain. NAS uses the existing publisher;
submitter delivery uses verified/synced client pull and an exact execution/manifest
receipt in the job ledger. After terminal/released custody and verified delivery,
reuse #479's expiry transaction/collector. Preserve downstream/shared references,
checkpoints, registry assets, unknown imports, six-hour publication grace,
168-hour legacy retention and typed expired retrieval. Roots, destination,
preservation semantics and owner tiers remain settings because they express
ownership; agents do not choose byte budgets.

## Retire stopgaps and prove the change

The 2026-10-31 incident had zero eligible reclaim despite retained growth and
physical room beyond its configured ceiling. Removing that ceiling alone simply
moves the failure to the physical disk; forecast protection and enforced writes
are prerequisites, not optional refinements.

| Retire | Replacement evidence |
| --- | --- |
| Default 100, historical 150 and s061 185 GiB caps | Fresh physical capacity minus enforceable allocations, forecast and measured control space. |
| Flat 16 GiB production reserve | Learned queued demand plus the next batch, including measured miss margin. |
| 1/2 GB daily write agreements; proposed #464 quotas | Tier admission, destination outage holds and per-attempt filesystem limits; retain project attribution. |
| Mandatory output guesses; 1%/256 MiB safety floor | Optional cold prior, accepted peaks/error and measured, enforced control footprint. |

Shadow under s061's 185/16 GiB settings first. s061h1/#477/#485 saves/verifies
those values before retiring the exact ExecStart drop-in; this preserves policy.
After bounded-backend qualification, client upgrades (including Python), forecast
coverage and restart reconstruction, explicitly switch off legacy cap/reserve
checks together; show saved legacy values as inactive. Retire manual budgets only
after delivery/collection progress. No legacy spec rewrite or fleet change here.

Laptop proof uses injected Clock/probes, small files, real HTTP/SQLite/CPU commands
and local bounded-volume write tests. Replay 100 usable units with 10 forecast
production units: a 95-unit experiment cannot start. Replay a 4-unit production
prediction whose measured margin funds 6: 96 experiment units cannot be committed;
the 6-unit result fits without NAS recovery. Inject a 12-unit managed
write between probes into a 2-unit boundary beside 8 production/2 control units:
the write fails inside its boundary; production publication and WAL sync succeed.
Test outage, restart, spool duplication, shared refs and exhausted production
alerts. Fake quota tests prove scheduling only; native backend denial is a separate
qualification gate. One binary/SQLite queue and existing workers/collector suffice;
all I/O stays outside `q.mu`, followed by fence checks.

## Implementation sub-issues

Each filed issue retains Outcome, Current state, Next step and Acceptance criteria.
Land learning [#505](https://github.com/1kuna/mycelium/issues/505) and enforceable
accounting [#506](https://github.com/1kuna/mycelium/issues/506) before tier integration
[#507](https://github.com/1kuna/mycelium/issues/507). Delivery/discard
[#508](https://github.com/1kuna/mycelium/issues/508) proceeds independently on the
existing publisher/collector. Migration/proof [#509](https://github.com/1kuna/mycelium/issues/509)
integrates all four, including both bounded starvation scenarios and inter-probe
write denial. Quick per change, full once per integrated batch before separately
authorized deployment. Open product questions: none.
