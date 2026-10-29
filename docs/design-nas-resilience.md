# Design proposal: keep shared NAS contention off the compute path

Status: proposed, 2026-10-29. Source baseline: `c39d996e` from `origin/main`.
This document proposes changes; [design-proposal.md](design-proposal.md) remains
the approved product spec. No fleet configuration, backup, workload or NAS setting
was changed during this investigation. Implementation requires an approved spec
amendment; issue creation does not approve a new storage mode or completion contract.

## Recommendation and ownership

1. **Mycelium storage maintainer:** use existing immutable local caches for repeat
   inputs, and make verified queue-store outputs the completion boundary for an
   explicitly selected asynchronous NAS-delivery policy. Budget queue/worker disk
   first. This removes repeated input metadata and NAS publication from compute
   latency; it does not make cold data available during an outage.
2. **Zach, as NAS/backup owner:** confirm the appliance model and share-to-volume-to-
   disk mapping, then prefer a backup destination on separate physical storage.
   A backup window is an interim owner choice, with a longer recovery-point gap.
   Do not apply a global SMB cap expecting it to isolate incoming Time Machine I/O.
3. **Mycelium scheduling/checkpoint maintainers:** report dependency-specific NAS
   health, route cache-ready work independently, then add explicitly configured
   local-first checkpoints backed by verified queue generations. Keep local,
   portable and NAS-mirrored progress distinct. Never promise a bounded NAS sync
   age through an outage without stopping progress or using another durable store.

Health/phase telemetry can proceed alongside cache adoption. Checkpoint changes
follow output delivery because they alter failure recovery and need a spec amendment.

## Evidence: what is known and what is not

The exact **hardware model, installed OS firmware version, pool/RAID layout and
shared physical disks are unverified**. The current credentials do not authorize
a clearly identified NAS login: `dcli` credential metadata had no entry matching
the observed appliance address, hostname, UGREEN or UGNAS. An entry for a different
address/port was not assumed to be this device. No credential was printed or used.

Read-only observations at approximately 20:16–20:20 UTC on 2026-10-29:

| Question | Observed fact | Limit of inference |
| --- | --- | --- |
| Vendor / OS family | HTTP redirects reach a UGREEN NAS desktop; HTML identifies `com.ugreen.pro.desktop`, UGOS Pro, web version `1.20.0.78578`, build date 2026-09-16 | A desktop bundle version is not independent confirmation of installed OS firmware or hardware model |
| SMB identity | Mycelium's mounted share negotiates SMB 3.1.1; Bonjour `_device-info` advertises `model=MacSamba` | `MacSamba` and the SMB `OS_X_SERVER` flag are protocol compatibility labels, not an Apple appliance or a hardware model |
| Network path | The Time Machine mount's server address equals the address resolved for the Mycelium share's hostname | Both reach the same NAS endpoint; this does not prove the same volume or disks |
| Share separation | Time Machine and Mycelium have different SMB share names | Share boundaries isolate namespace/quota, not physical IOPS |
| Multichannel | `smbutil multichannel -m` reports one active connection and `Multichannel ON: no` for the Mycelium share | Does not establish whether the NAS setting is disabled or a second usable path exists |
| Public device API | Heartbeat succeeds but returns empty data, including with `product_model=true`; the desktop's common-system-info GET requires authentication | Model and pool membership cannot be established from these responses |

Evidence is a light client-side mount/SMB/DNS inventory and unauthenticated HTTP
GET/HEAD responses, not a benchmark. No recursive NAS listing, bulk read/hash,
write probe, SSH login to the NAS, remount or service operation was performed.
Private addresses, share names and raw mount paths remain outside this public doc.

**Resolve the disk question:** Zach or an operator with a clearly matched NAS
credential reads Control Panel → About, Files → Shared folder management (the
location of both shares), and Storage → Storage Pool & Volume (each volume's pool
and member disks). Record model, actual firmware, volume and pool identity, media
type, SSD/cache role and health. Compare member disk sets. Two volumes on one
pool still contend; separate disk pools still share NAS CPU, memory and network.
Do not move, reformat or delete anything to answer this question.

### Incident distinctions

- [#234](https://github.com/1kuna/mycelium/issues/234): successful create/unlink
  operations took 17.734 s / 23.223 s; one probe sequence took 40.957 s. Read-only
  filesystem capacity calls took up to 8.233 s. Previous availability/freshness
  policy expired successful-but-pending observations. Progress-aware probe fixes
  and later qualification are recorded; this is not a proposal to raise every timeout.
- [#286](https://github.com/1kuna/mycelium/issues/286): NAS path traversal repeats
  metadata for private inputs. Earlier output tails were 625–666 s; later s054
  evidence records 143.944 s to staged inputs and 386.548 s from payload exit to
  queue finish. These phase measurements do not identify every blocking syscall.
  Two-copy overlap, grouped output directories and publication overlap already
  landed. More parallelism is not established as the remaining solution.
- [#289](https://github.com/1kuna/mycelium/issues/289): installer preflight now
  accepts progressing operations using its measured 30 s stall backstop. Native
  rename/delete took 26.0 s / 13.5 s. Preserve mount and durability qualification.
- [#339](https://github.com/1kuna/mycelium/issues/339): a qualification attempt was
  still preparing NAS checkpoints before cancellation; the later `launch_refused`
  record was withdrawal, not proof of an earlier isolation rejection.
- [#342](https://github.com/1kuna/mycelium/issues/342): a 509,313,092-byte result
  could not fit the queue store's configured 100 GiB ceiling despite physical disk
  space. This is a separate capacity barrier. Async NAS work cannot bypass it or
  call a locally held result complete.

The owner reports concurrent Time Machine contention for today's incident. No
controlled backup-on/off comparison was performed here. An older incident's
backup attribution was explicitly superseded by a stray recursive traversal
(#234, 2026-10-26); do not reuse that event as proof against Time Machine.

## NAS capabilities: supported controls versus verified configuration

These are UGOS product/documentation capabilities, not claims that this appliance
has spare slots, enabled features or particular disk hardware. Source retrieval:
2026-10-29. UGREEN's knowledge-center pages are stronger evidence than assumptions
from another vendor's UI.

| Option | Support / applicability | Benefit and cost |
| --- | --- | --- |
| Time Machine capacity quota | UGREEN documents a dedicated SMB Time Machine folder and shared-folder capacity quota [N1] | Protects capacity; does not limit bandwidth, IOPS or metadata latency. Reserve enough history for the owner's recovery needs |
| Time Machine bandwidth / IOPS | No native Time Machine-specific, per-user or per-share IOPS/rate limiter was established in the examined official docs; live settings unverified | Do not prescribe an invented knob or unsupported `smb.conf` edit |
| QoS / Data Control | UGOS supports minimum/maximum **outbound** service/port bandwidth [N2]. Its broader network guide mentions inbound/outbound, but the dedicated article and performance guide explicitly limit this control's claim [N3] | Can constrain NAS downloads; cannot be relied on to limit the Mac's incoming backup writes. A shared TCP 445 rule also affects Mycelium. Router QoS helps only if that traffic traverses the enforcing device, and does not reserve disk IOPS |
| Share / volume / pool separation | Dedicated shares and quotas [N1]; pools consist of drives and contain logical volumes [N4] | New shares or volumes on the same pool do not isolate spindle contention. Separate physical pools, a direct-attached backup disk or another backup appliance provide stronger isolation, with capacity, purchase and migration costs |
| Backup scheduling | Apple exposes Time Machine backup-frequency selection on macOS 13+ [N5]. No exact UGOS Time Machine service-window feature was verified | An owner-selected quiet window is a workaround, not a cluster guarantee. Frequency is not a strict start/end window. Exact windows need an owner-approved Mac-side scheduler; any detached scheduling belongs in T3. Less frequent backups increase potentially lost work; missed windows need visible overdue-backup status |
| SSD cache | UGOS documents per-volume SSD cache [N4]. UGREEN specifies one M.2 SSD for read cache or two for RAID1 read/write cache, and an HDD pool [N6] | May accelerate repeated random/small-file I/O, not reliably one-time streaming backups [N7]. Costs slots, endurance and capacity; write-back needs power-loss qualification and power protection. Installed availability is unknown; no purchase is recommended before identifying the model |
| Dedicated SSD pool | UGREEN documents an independent SSD pool for latency-sensitive files [N6] | More predictable separation than competing for a cache. Costs dedicated SSD capacity and a verified migration; same NAS/network remains shared |
| SMB multichannel | UGOS documents an “Enable SMB3 multichannel” control [N8]; macOS requires server support and usable paths [N9] | May improve network throughput/redundancy; no disk-latency isolation. Could feed backup writes faster into an overloaded pool. Current client session is single-channel; do not change it as a presumed fix |

**Owner sequence:** identify/map storage first; retain existing backup protection;
if disks overlap, prefer a separate physical backup destination. If no separate
destination is available, Zach chooses an interim backup window/frequency and its
recovery-point cost. A dedicated backup share/quota is worthwhile capacity hygiene
but is not sold as the latency fix. Review SSD pool/cache only after measuring
whether disk IOPS, NAS CPU or the client network path is limiting. No firmware,
pool, cache, quota, scheduler or multichannel changes are authorized by this doc.

## Existing Mycelium building blocks and binding constraints

Use the existing queue file API, content identities, cache locks, atomic file and
directory publication, worker result spool and SQLite transaction machinery.
Standard filesystem copy/rsync plus checksums is adequate for a supervised one-off
migration, but does not itself implement attempt fencing, a queue-pinned pending
delivery, or a crash-recoverable delivery receipt; those are the named reasons
for the small persistent publication state proposed below. No new storage service,
broker, peer network, leases or replicated registry is needed.

Source anchors at the baseline:

- [file_cache.go](../internal/myce/file_cache.go): verified hash-keyed private-file
  cache; attempts receive copies. [shared_input_cache.go](../internal/myce/shared_input_cache.go)
  and [shared_inputs.go](../internal/myce/shared_inputs.go): read-only cached files,
  archives and manifest trees, use locks, local metadata verification, eviction.
  `shared_inputs` currently supports plain native jobs, not extension jobs or
  Docker/workdir forms. Existing plain commands may adopt it now; extension parity
  is an implementation gap, not an available flag.
- [extension_files.go](../internal/myce/extension_files.go) and
  [worker.go](../internal/myce/worker.go): NAS outputs are synchronously published
  before the successful local result is persisted.
- [worker_attempts.go](../internal/myce/worker_attempts.go) and
  [file_checkpoint.go](../internal/myce/file_checkpoint.go): process release is
  separate from file-result upload; queue-backed results require verified blobs;
  checkpoints already have complete generation manifests and attempt fencing.
- [guide-files-v3.md](guide-files-v3.md): default 100 GiB queue limit, separate
  private/shared 10 GiB worker-cache allowances, seven-day terminal result
  retention, six-hour publication grace and verified transfers. Queue store on
  durable local disk is a prerequisite; putting it on this NAS restores contention.

[DECISIONS.md](DECISIONS.md) MY-01 requires generic public presentation, MY-02
requires shared ordinary build caches, MY-03 requires one v3 runtime and preserves
checkpoint/retry contracts, and MY-18/MY-26 require fit/qualification-aware placement
and safe relocation. MY-02 is **not** a new checkpoint policy. Preserve the
[resumable-job contract](guide-resumable-jobs.md): `MYCE_CHECKPOINT_DIR`, atomic
fsync/rename, monotonic generations, complete immutable payload manifests,
attempt/session identity and last complete portable progress. No network, hashing
or bulk I/O inside `q.mu`; work outside and recheck identity inside the commit.
The approved spec's NAS-only onboarding/refusal rule (#109) remains binding;
local-first NAS checkpoints require an explicit amendment, not automatic fallback.

## Option A: local-first inputs and cache-ready placement

For immutable data, prefer `shared_inputs` with declared SHA-256/size (or an
immutable tree manifest) and the existing environment path. A warm hit consults
only local cache metadata/content. A mutable NAS path alone is not a cache identity:
the producer freezes/version-identifies bytes before submission. A cold miss copies
and verifies from the declared source, atomically publishes locally, and holds
the task while incomplete. A source change produces a new identity or a visible
hash failure. Private mutable inputs keep attempt copies; do not hard-link them
into a writable job. Existing read-only permissions are cooperative, not a new
security boundary against the owning account.

First adopt the existing API where supported; then extend the same cache seam to
extension inputs. Snapshot cache readiness into ordinary worker reports for
placement; scheduling never scans or stats the NAS. Apply this only among hosts
that fit hard requirements, qualifications and project policy, preserving priority
and scarce-machine opportunity cost. Prefetch is explicit desired work on existing
workers; it must not compete without limit with active copies or backup traffic.

**Buys:** repeat launches avoid NAS lookup/copy/unpack, and warm data survives NAS
slowness. **Costs:** sum of distinct pinned working sets plus unpacked trees and
temporary cold-build copies; private writable materialization still costs a full
copy. Under current defaults, private/shared caches can together consume 20 GiB,
plus model/environment caches and attempt scratch. Cold misses still wait; a
destroyed cache needs its authoritative source. Corruption refuses use, not stale
bytes; unknown source reachability cannot justify deleting the last useful copy.

Acceptance: two laptop jobs reuse one digest with zero NAS calls/download/unpack
on the warm job, including after restart with local re-verification; changed bytes
are refused; an active use lock prevents eviction; a cold NAS miss holds only the
dependent job while a cached job starts. Measure bytes and assigned → staged →
Started phases on the next separately authorized native workload (#221/#286).

## Option B: complete from durable files, deliver to NAS asynchronously

Make this an explicit per-task delivery policy under [#201](https://github.com/1kuna/mycelium/issues/201).
Existing jobs keep their completion semantics until migrated. For opted-in jobs:

1. After payload exit, capture required outputs into immutable local spool files;
   hash, sync and publish the spool manifest before discarding work files. Missing
   outputs fail. Release physical custody through the existing separate handshake.
2. Upload through the existing file API. The queue verifies/syncs every blob, then
   atomically commits the accepted current-attempt result **and pending delivery**
   with file-store references in SQLite. Only that durable commit reports success.
   An acknowledgement lost in transit is safely replayed using exact identity.
3. Expose `compute/result complete` independently from `delivery pending/verified/
   blocked`, with age, bytes, last progress and a sanitized cause. A caller needing
   the NAS path explicitly waits for delivery; normal result retrieval uses
   `output_refs`. Consumers must update the corresponding wait, not silently assume
   success means a NAS path already exists. Downstream jobs use file references.
4. The queue's existing process owns a bounded background publisher using its
   configured NAS destination root and local store. Persist publication intents
   in the same SQLite database. One task is not kept assigned to a GPU worker
   merely to copy files. No new service or distributed publisher election.
5. Stage a complete job/attempt directory on the destination volume; validate
   confinement, size and hashes, sync according to the filesystem contract, then
   atomically expose it and a receipt keyed by accepted job/attempt, destination
   policy revision and manifest digest. After crash, compare an existing receipt
   and bytes before accepting it; matching replay is idempotent and conflicting
   bytes refuse. Never overwrite a different job's directory. An optional “latest”
   alias is deferred; immutable paths avoid stale-writer races. Do not credit a
   superseded attempt even when its uncancellable NAS call finishes late.
6. Pending delivery pins authoritative queue blobs across ordinary result expiry
   and restart. Worker spool can be reclaimed after durable queue acceptance;
   it need not wait for NAS delivery. Queue collection requires a verified
   destination receipt plus the task's agreed retention/retrieval policy. Early
   deletion changes the seven-day retrieval contract and is explicitly selected,
   never inferred. Failed/unknown delivery is retained and alerted, not aged out.

**Buys:** NAS publication no longer adds 6–12 minutes to result completion or keeps
compute custody occupied; consumers can retrieve durable queue bytes immediately.
**Costs:** new persistent delivery state, receiver-side hashing and temporary
destination space up to one output set per active publisher. Worker spool and
queue copies coexist until acceptance. Queue local storage remains an authority
and single failure domain; two acknowledged disks are not a backup guarantee.
Queue loss after worker cleanup can lose undelivered bytes. Keep independent
backups/retention according to the owner's recovery policy.

Capacity is part of this option, not follow-up polish. Account for used blobs,
existing reservations, pending delivery and expected result bursts before launch.
Use declared disk needs/preflight first, with explicit extra reservation if an
output exceeds estimate; never pretend a reservation makes physical space.
Refuse/hold new storage-consuming work before consuming all spool capacity; retain
finished results and continue heartbeats/control and independent jobs when full.
No finite disk permits indefinite computation during an indefinitely unavailable
destination. #342 needs its own authorized operational recovery; this design does
not resize the live quota, shorten retention or recover that result.

Acceptance: block every NAS operation in a laptop fixture; the result succeeds
only after verified queue commit, is retrievable, and an independent command and
check-in continue. Restart at pending/receipt boundaries; exactly the accepted
attempt publishes one complete directory with matching receipt. A hash mismatch
or stale result never becomes verified delivery. Exhaust quota: no false success,
no lost spool, no control stall; admission explains capacity. Collection preserves
pending references beyond ordinary retention and releases only under chosen policy.

## Option C: local checkpoints, portable queue generations, asynchronous NAS mirror

Do not silently turn an existing NAS-root assignment into a local directory.
Add an explicitly configured policy after amending the approved spec and guide.
`MYCE_CHECKPOINT_DIR` points to the worker's local attempt folder in that policy;
the program's atomic save and monotonic-generation format stays unchanged.
Reuse consistent capture and verified file-store generation publication. Queue
commits are the portable recovery authority; NAS is an asynchronous mirror.
NAS-only jobs retain their current availability gates and semantics.

Report three facts: local generation, queue-verified portable generation, and
NAS-verified mirrored generation, each with observation age and lag. Reconciliation
selects the greatest complete portable generation using current identity fences;
it never compares timestamps or calls a local-only save portable. Restart on the
same worker may reuse verified local progress only through existing exact-identity
reconciliation, not by discovering arbitrary old directories. Planned relocation
prepares a complete accepted generation on the destination before release/yield
is declared safe. Sudden worker loss resumes only queue-accepted progress.

**Bounded staleness must name its failure domain.** Set an owner/workload recovery
budget `R` from acceptable lost compute, not a universal arbitrary timer. Measure
save interval `I`, bounded work-unit duration `W`, and queue capture/upload/commit
tail `U`; choose `I + W + U <= R` where measurement supports it. Save age is
reported separately from monotonic generation order. If the command does not save
at the declared cadence, the recovery guarantee fails visibly.

An unavailable NAS cannot have a guaranteed mirror age. While queue commits
continue, keep compute running and mark the NAS mirror overdue; portable recovery
can still meet `R`. If the owner explicitly requires NAS to be current within
`R`, or queue publication also stalls, warn on the first missed budget and hold
new dependent admission. Enforcing a bound on **further unreplicated work** needs
cooperative save-boundary backpressure in the checkpoint helper/command contract;
do not claim arbitrary plain commands are paused safely by a background copier.
A hard NAS-age requirement sacrifices compute liveness during NAS loss. Select
queue-backed recovery for the “backup cannot stall compute” goal. Forced stop
still uses the previous complete portable save and existing retry/loss rules.

**Buys:** local save latency no longer inherits NAS flush latency; portable recovery
survives NAS slowness when the queue is available. **Costs:** local snapshot plus
queue and NAS generation bytes, capture/hash work and progress/lag state; host
loss can discard the newest uncommitted save. Coalesce pending mirrors to the
newest complete generation, protecting the current portable and in-flight
generation. Keep at most one active copy plus the newest pending snapshot per job;
do not delete retained checkpoint history without its separately agreed policy.
Size cadence/bytes so checkpoint traffic does not saturate the queue store.

Acceptance: fake-clock outage/resume/leave/loss tests prove no manifest precedes
payloads, no stale generation wins, and no NAS mirror status increments on a
partial copy. With NAS blocked, local save and queue transfer complete and a
different fake worker resumes that generation. With queue blocked too, status
exposes lost-work risk and any advertised cooperative budget enforces its boundary;
uncooperative commands get no hard bound. Preserve Linux CIFS directory-fsync
exception only on that actual filesystem; local files keep full barriers.

## Option D: dependency-aware NAS health and admission

Reuse existing operation-progress probes and bounded observation patterns; never
add a probe per scheduling pass or per job. Report separately: root reachability,
metadata completion latency, last successful sample, pending progress/stall,
capacity observation freshness, cold-copy progress and delivery backlog. Correlate
workers by configured storage identity without publishing credentials or raw paths.
“Slow”, “unreachable”, “cold/unknown” and “capacity unknown” are distinct states.

Rank a healthy storage path above a slow one only when the job actually needs a
cold NAS read or NAS-only checkpoints and the candidate otherwise fits. Warm local
inputs and queue-backed checkpoints do not inherit a global NAS gate. For one
shared overloaded pool, moving to another host may buy nothing; no retry storm or
blind migration of running work. When all required paths are unhealthy, hold only
dependent jobs with a clear cause. Missing required bytes and incomplete preparation
never permit launch. Keep #109 install/mount qualification; this is runtime routing,
not a preflight bypass. A NAS-independent install mode would be another spec change.

Extend [#200](https://github.com/1kuna/mycelium/issues/200) with a single deduplicated
cluster alert for a correlated slow root, stalled publication or exceeded recovery
budget, plus recovery notification over the user's existing wake channel. Use
recorded healthy/incident traces and workload start/recovery budgets to qualify
thresholds; no universal new 30 s health limit. Startup observations are cold until
first success; preserve the operator's cold-start ruling in #341. Scheduling,
freshness and retry decisions use injected `Clock`.

**Buys:** independent work keeps running and operators see the shared cause.
**Costs:** a few fields per worker/root, cache-readiness summaries and status/alert
logic. Delayed samples can misroute work; cache misses and shared-server contention
remain. A single stuck kernel SMB syscall may outlive its observer timeout.

Bound NAS work independently of control-plane goroutines. Start with one active
publication operation per configured destination root; reuse the existing two-copy
per-attempt staging bound. The first prevents a blocked root from accumulating
publishers (cost: serialized cold delivery); the second is already based on the
two-input incident (#286). Track outstanding calls until they return; a timer must
not spawn another call, erase ownership or delete in-use scratch. Tune concurrency
only with observed throughput/latency and disk headroom. Pending intents live in
SQLite rather than unbounded goroutines. Watch progress, not a whole-job wall cap.

Acceptance: replay the #234 slow-successful, stalled and recovered operations with
fake time; the queue control path and cache-ready work remain responsive. A cold
NAS-dependent job selects a healthy compatible path or shows a dependency hold.
One cluster alert fires for the incident, none on healthy/cold-start history, and
one recovery event clears it. Repeated observer expiry creates no extra active I/O.

## Phases, measurable exit criteria and handoff

| Phase | Owner / existing track | Small deliverable and acceptance |
| --- | --- | --- |
| 0: resolve storage facts | Zach / authorized NAS operator | Read About and both share/pool mappings. Record whether disks overlap, actual model/firmware and available SSD roles. Choose separation or an interim backup policy; no destructive migration |
| 1: immediate cache adoption + measurement | Storage maintainer, consumer adapters; #221/#286 | Adopt `shared_inputs` on supported jobs; document unsupported extension forms. Warm-hit zero-NAS-call fixture and measured stage/exit/accept/delivery phases. Independent work must start with NAS blocked |
| 2: durable async delivery + disk admission | Storage/API maintainer; #201/#342 | Opt-in destination policy, atomic accepted-result/pending-delivery transaction, pinned blobs, replayable verified receipt and quota-pressure behavior described in B. Update consumers that wait on NAS paths |
| 3: health routing and alerts | Scheduling maintainer; #200/#234/#341 | Existing-probe telemetry, dependency-specific gates and healthy-host preference. Fake-clock incident/healthy-history and nonblocking control proof described in D. Can begin with phase 1 |
| 4: local-first checkpoint policy | Checkpoint maintainer; #109 and resumable guide | Explicit approved spec amendment; local/portable/mirror progress, measured recovery budget and outage/relocation proof described in C. Preserve NAS-only configuration |
| 5: native acceptance | Release owner with separate authorization | Observe a representative backup concurrently with a cold input, a warm repeat, output delivery and checkpoint transfer. Collect all phase timings, queue latency, sample ages, bytes, disk headroom and correctness. Do not replay protected customer payloads for this task |

Size retained disk from measurements: worker pinned inputs + private work +
unaccepted outputs + local checkpoint snapshots + existing shared-host reserve;
queue retained blobs + pending-delivery bytes + checkpoint generations + upload
reservations + incoming result burst + reserve. For output rate `b` bytes/second
and tolerated destination outage `T`, pending delivery alone needs at least
`b*T` bytes (plus burst/temporary copies). If measured admission space cannot cover
that horizon, reduce admission or change owner-selected capacity/retention; do not
silently delete undelivered data. Recheck actual free space, not just quota.

No latency improvement or outage-proof operation is claimed by this documentation
PR. Each implementation slice gets a focused laptop regression and the repository
quick gate; full gate once per deployment batch. Native filesystem durability,
power-loss behavior and concurrent backup acceptance are distinct from laptop
proof. Read-only fleet authorization here excludes submissions, probes that write,
maintenance changes and deployment.

## Sources

- N1: [UGREEN: Time Machine setup and shared-folder quota](https://support.ugnas.com/detail/article/en-US/350).
- N2: [UGREEN: outbound port traffic control](https://support.ugnas.com/detail/article/en-US/660).
- N3: [UGREEN: network settings](https://support.ugnas.com/detail/article/en-US/100) and [multi-user bottlenecks / outbound-only limitation](https://ai.ugreen.com/blogs/how-to/fix-slow-nas-multiple-users).
- N4: [UGREEN: storage pools, volumes and SSD cache](https://support.ugnas.com/detail/article/en-US/313).
- N5: [Apple: Time Machine backup-frequency controls](https://support.apple.com/104984).
- N6: [UGREEN: SSD/HDD roles, cache prerequisites and separate SSD pools](https://ai.ugreen.com/blogs/how-to/mix-ssds-hdds-nas-setup).
- N7: [UGREEN: limits of SSD-cache improvement](https://support.ugnas.com/detail/article/en-US/807).
- N8: [UGREEN: SMB3 multichannel setting and routing limitation](https://support.ugnas.com/detail/article/en-US/757).
- N9: [Apple: SMB multichannel behavior](https://support.apple.com/102010).
- [UGREEN: where actual model/firmware information is displayed](https://support.ugnas.com/detail/article/en-US/109).

## Tracking and current state

Parent: [#346](https://github.com/1kuna/mycelium/issues/346).

- [#347](https://github.com/1kuna/mycelium/issues/347): Zach / authorized NAS operator — model, firmware, disk mapping and owner-selected backup isolation.
- [#348](https://github.com/1kuna/mycelium/issues/348): storage/cache maintainer — extend existing cache reuse to NAS-backed extension inputs; #221 keeps its native pool acceptance.
- [#349](https://github.com/1kuna/mycelium/issues/349): storage/API maintainer — durable asynchronous delivery and capacity handling under #201.
- [#350](https://github.com/1kuna/mycelium/issues/350): scheduling/health maintainer — dependency-aware routing and one correlated incident alert under #200.
- [#351](https://github.com/1kuna/mycelium/issues/351): checkpoint maintainer — propose and qualify an explicit checkpoint policy amendment.

Current state: research/design only; hardware identity and disk overlap still need
the owner/operator read above. Next step: independent Sol high review, then the
orchestrator approves/scopes implementation slices. No product ruling, runtime
implementation, native speedup, backup intervention or deployment is claimed.
