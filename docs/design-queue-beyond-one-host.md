# A shared queue on ordinary fleet machines

Design for [MY-40](DECISIONS.md#my-40) and
[#1176](https://github.com/1kuna/mycelium/issues/1176). **Draft. Do not merge
until the maintainer approves.** The [single-queue design](design-proposal.md)
is still in force.

## Recommendation

Use the NAS to keep one authoritative record of queue changes. Run a small
PostgreSQL database **on the NAS itself**, using its local storage, with a
separate network connection from the SMB file share. Every enabled fleet
machine runs the queue beside its worker, keeps a local SQLite copy of the
committed records, and can serve reads or take over coordination. One machine
at a time orders changes. A single surviving fleet machine can run the queue
as long as it can reach the database. Sleeping or removing a machine never
changes how many machines must be online.

Keep inputs, outputs and durable checkpoints on the NAS. A worker stages
verified bytes to its own disk as needed and sends its own new artifacts to
the NAS. Caches are disposable after custody and delivery obligations settle.
The queue copies records such as job specs, grants, usage and artifact
references. Keep the current [retention and checkpoint rules](files.md)
and remove bulk transfer, hashing and file-share operations from queue
transactions.

This removes the dependency on any **fleet machine** and makes the NAS
database a shared dependency even for cached work. A coordinator crash can
briefly delay new decisions while another machine takes over. Already-started
work on the other machines keeps running as far as its own dependencies allow.
Jobs tied to hardware that is offline wait for compatible capacity.

## What we measured

| Measurement | Result |
| --- | --- |
| Live SQLite file sizes, 2026-11-25, from a read-only directory listing | Database **8,157,110,272 bytes (7.60 GiB)**, WAL **7,329,512 bytes (6.99 MiB)**, shared-memory file **32 KiB**. File size includes retained history, indexes and any reusable pages. The listing cannot show the size of active records or table contents. |
| Job payload, earlier file-store snapshot | About **88 GiB** in the old store at that instant, subject to custody and retention. [File-store correction](https://github.com/1kuna/mycelium/pull/2040#issuecomment-7169120452) |
| Stage 0–1 laptop fixture, 11,500 retained jobs | WAL **598,362 → 494,469 B/s**, or **51.7 → 42.7 GB/day** if sustained. After stage 1, bound row values included about **585 kB/s** of wait diagnostics, **50 kB/s** of control state, **2.7 kB/s** of usage and **1.3 kB/s** of job rows. Those are separate measures from WAL bytes and actual replication traffic. [Stage 0 report](https://github.com/1kuna/mycelium/blob/a41607ba78670d5ab96d4acc072f34129e849b34/docs/queue-write-baseline.md) |

A full cold copy is already multi-GiB. Keep a returning machine's local
copy and catch up by record number. Measure live change traffic, catch-up
time, replica disk and memory use, and NAS commit latency before cutover.
The passive fleet sample had shared-disk flush timings and interface
totals. Live queue write rates and inter-machine latency are still unknown.
Stage 1 shortened the local lock hold, and check-in p95 still reached 325 ms
in one fixture window with longer commits. [Stage 0 report](https://github.com/1kuna/mycelium/blob/a41607ba78670d5ab96d4acc072f34129e849b34/docs/queue-write-baseline.md)

## Four ways to do it

An atomic database applies a change all at once. For example, one transaction
can check that a job is still waiting, reserve the worker's capacity and
record the assignment. Two callers cannot both claim the same waiting job
if they check and change the same locked record. SQLite already does this
locally. Network access and survival of the computer holding the database
are separate properties. SQLite WAL requires clients on the same host.
[SQLite WAL](https://sqlite.org/wal.html) ·
[PostgreSQL row locks](https://www.postgresql.org/docs/current/explicit-locking.html)

| Option | What it still depends on |
| --- | --- |
| **A. Let the NAS decide who coordinates. Recommended.** The NAS database gives one coordinator a renewable turn and durably saves each accepted change. Every fleet copy can catch up from it. | A NAS that can run and recover the database independently of the file-share clients. New writes can pause during handover or database loss. The file share remains a separate dependency for job bytes. |
| **B. Let every online machine vote.** A safe majority uses the last agreed list. Two enrolled voting machines need both, so either sudden loss blocks new decisions. Five need three, so three Macs going offline together leave two Linux machines unable to decide. A planned shrink works if the old group can still agree to it. | An available majority of the last agreed group, through membership changes as well. If each side of a split counts only machines it can see, both can decide independently. In a two-machine split each would count itself as a majority of one. [Raft membership changes](https://raft.github.io/raft.pdf) |
| **C. Let each machine schedule while disconnected.** Machines can continue eligible cached work through a split and reconcile later. | An explicit contract allowing conflicting starts, cancellations, production effects and quotas. It needs durable event exchange after recovery. To survive loss of the machine that accepted an offline write, that write must also reach independent durable storage before acknowledgement. |
| **D. Use a managed shared database elsewhere.** A database on a different ordinary host just moves the dependency. A managed high-availability service can survive a database-machine loss with durable replication and automatic failover. | A separate service and its network, credentials and operator or vendor. An off-site service adds WAN availability, cost and a metadata custody decision, while job bytes still need the NAS. It could meet the fleet-host requirement, with more infrastructure than A. [PostgreSQL replication trade-offs](https://www.postgresql.org/docs/current/high-availability.html) · [Managed failover example](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZSingleStandby.html) |

**Why a database connection instead of a file lock?** Both Macs have
repeatedly lost their SMB mounts. An upgrade preflight rename hung on both
before the worker restart, and a separate Mac SMB stall was reported the
same day. Linux CIFS can recover through an existing automount when the
network returns. Failed retries can exhaust systemd's start limit without
the documented operator setting, and a pending kernel request can outlive
the application timeout. SMB byte-range locks belong to an open handle,
which the server may preserve for reconnect. A client timeout leaves
ownership uncertain. A file lock has no way to reject the old owner's
local SQLite writes or API replies. [SMB incident](https://github.com/1kuna/mycelium/issues/2058) ·
[Same-day stall](https://github.com/1kuna/mycelium/issues/2058#issuecomment-7170699337) ·
[Linux CIFS recovery](reference/nas-mounts.md#linux-cifs-boot-recovery) ·
[SMB locking](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/6178b960-48b6-4999-b589-669f88e9017d) ·
[SMB reconnect](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/eb5bfe99-47fe-4e87-8e87-08a084dcefb6)

NFSv4.1 has server-managed leases and recovery rules, including a reclaim
period after restart that can delay new locks. It would still need the
queue's commit and stale-writer protections. A NAS-native transactional
service keeps those checks together and avoids making the file mount a
dependency for queue decisions. NAS support, persistence across appliance
updates, independent recovery and latency under file traffic are unverified.
Qualify those before choosing a production deployment. [NFSv4.1 recovery](https://www.rfc-editor.org/rfc/rfc8881.html#section-8.4)

**What C would take.** Merge immutable events with globally unique event and
attempt IDs. Match submissions by project and key, surface mismatched specs,
preserve cancellation as an intent, choose one winner for each conflicting
result and retain exact release records for both attempts. Result fencing
today relies on one ordered view of the current attempt. Two disconnected
queues can each accept and expose a result before either learns about the
other. A later winner cannot undo an external side effect or a cancellation
promise that the other side already broke. Production would have to wait
for shared authority or explicitly allow duplicate execution and delayed
publication. Each worker could still enforce its own CPU, memory and device
limits, but fleet-wide project caps, priority and fairness would be
approximate. Merge actual usage by attempt, including wasted work, and keep
unreleased charges until that worker confirms cleanup. Duplicate runs are
possible throughout a split, and their rarity is unmeasured. [Current
submission and cancellation contract](design-proposal.md#submission-identity-and-cancellation)

## How A works

1. **One durable decision.** In a short database transaction, lock the
   coordinator record, check its owner and handover number, check expiry
   against database time after taking the lock, and append the next
   ordered change. Taking over increments the handover number under that
   same lock. Use durable commits.
   Acknowledge assignments, starts, cancellations and results only after the
   transaction commits. For a lost reply, look up the stable operation or
   submission key before retrying. A failed renewal or uncertain write
   stops new grants until authority and the committed outcome are known.
   No file I/O or worker RPC runs inside this transaction or the queue mutex.
   [PostgreSQL commit durability](https://www.postgresql.org/docs/current/runtime-config-wal.html)
2. **Take over from the committed record.** Reuse stage 1's changed-row
   batches. Define a versioned form with stable IDs, assigned sequences and
   explicit deletes. Stream changes to each machine's local SQLite copy and
   keep a verified snapshot plus the subsequent changes on the NAS
   ([retention](https://github.com/1kuna/mycelium/issues/2157): the
   coordinator snapshots its copy once the changes since the last snapshot
   reach that snapshot's size, and prunes below the previous one). The NAS
   commit need not wait for another machine's copy. Before serving decisions,
   the new coordinator applies every committed change through the handover
   point. Local status/history reads can share load. Freshness-sensitive
   reads, waits and negative lookups catch up or go to the authority. Workers
   use the local queue endpoint. External clients retry known authenticated
   endpoints under the same fleet identity, without the old host or its keys
   being needed at runtime.
3. **Keep attempt checks.** A worker handles launch and stop instructions
   against the current committed handover number and exact job, attempt and
   worker session. A stale coordinator cannot commit new authority. After
   handover, reconcile outstanding grants, live sessions and physical release
   before making replacements. Reports from an already-running attempt can
   still be accepted if that exact attempt remains current. Keep the existing
   startup and worker-loss rules so handover does not itself count as worker
   loss. An isolated old attempt can still run briefly after reassignment as
   today. Publish only the result the queue accepts.
4. **Keep file custody separate.** Stage or upload immutable bytes in
   attempt-specific storage, verify the durable NAS copy, then commit the
   accepted artifact or checkpoint reference under the attempt check.
   Reclaim unreferenced bytes through retention. Publish managed destinations
   from the accepted reference only. A mutable destination must serialize
   replacement with its own current-version check to keep delayed delivery
   from overwriting a newer result. Keep pending bytes through replay.
   Local-first checkpoints still write locally first, but in shared mode
   durable acceptance needs the NAS copy. A share outage can therefore
   delay a save that today can queue an asynchronous mirror. A mount
   fault with the database reachable holds only work that needs that
   backing. External writes still need repeat-safe outputs. [File contract](files.md)

Each addition addresses one failure. The renewable turn lets a survivor
take over a vanished coordinator. The handover number prevents a former
coordinator from writing after it returns. Saving the change on the NAS
before replying prevents an acknowledged write from dying with one
machine, even if all the other copies were asleep. Pick renewal and
handover timing from measured commit, network and start latency.

## What keeps working

| Failure | What the fleet does |
| --- | --- |
| **Coordinator goes down** | Already-started work elsewhere continues. New decisions resume after a reachable machine takes the expired turn and catches up. Accepted queue records and durable NAS files survive. |
| **Any other fleet machine goes down** | No handover and no change to write availability. Work on that machine follows existing loss, release and checkpoint rules. Other machines continue. |
| **NAS or its database goes down** | No new starts, authoritative writes or result acknowledgements. Local copies can serve clearly marked last-known status. Already-started jobs can continue where local dependencies permit and buffer pending reports. Recover from the NAS database when it returns. Permanent NAS loss can lose writes newer than the surviving copies or backups, as well as job files without a storage backup. |
| **Network splits** | The coordinator and workers that can reach it keep using the database. If the coordinator loses database access, a reachable machine can take over after expiry. If both sides can reach the database but cannot reach each other, the side cut off from the current coordinator waits while it holds the turn. Existing attempts can keep running locally. |
| **Two fleet machines go down at once** | Any remaining machine with database access can coordinate, even if it was asleep and must catch up. If none remain online, work resumes when one returns. If the NAS is one of the losses, the NAS-outage row applies. |

## Shortest path from today

1. **Stages 0–1, underway.** Finish [#2099](https://github.com/1kuna/mycelium/pull/2099)
   and its fix round under [#2092](https://github.com/1kuna/mycelium/issues/2092).
   Keep its single write path, post-commit publication and injected clock.
   Measure which records are needed for recovery and keep replaceable
   diagnostics out of the shared commit path where their contracts allow.
2. **Stage 2, prove the new storage and routing.** On a laptop, run several
   queue processes, a local PostgreSQL instance, disposable file storage and
   real CPU jobs. Exercise catch-up, a missing commit reply, a suspended old
   coordinator returning, each failure in the table and a running job across
   handover. Include a single survivor starting work without help from any
   other fleet machine. Prepare NAS file custody and verify every retained
   manifest and pending delivery needed at cutover. Seed queue copies and test auth,
   status and worker/client reconnect using more than one endpoint. On the
   real NAS, qualify service support, restart and flush durability, and
   measure latency during file I/O. Verify a broken Mac mount does not block
   queue writes or other machines ([qualification plan](shared-queue-nas-qualification.md)).
   No new shared authority serves clients yet.
3. **Stage 3, switch authority once and prove the upgrade path.** Briefly
   hold old queue writes, settle committed work and import the final
   consistent state. Fence the old writer before enabling the shared mode.
   Keep exact running attempts and custody, then prove normal submit,
   assignment, wait, cancel, checkpoint resume and verified result fetch
   with the original queue host offline. Include a planned handover and a
   coordinator process crash. **Only after this proof can the original
   host go down for the upgrade without serving the queue.** Check any
   unrelated services and jobs that need its hardware separately. For a
   return to single-queue mode, first fence shared writers, then restore
   from the latest shared commit with verified file references. An older
   pre-cutover backup would lose acknowledged work.
4. **Share more planning if it limits starts.** Move candidate placement
   calculation to local queues only if measured coordinator load warrants
   it. Confirm the proposal against current priority, capacity, custody and
   attempt records in the same ordered write. Reads, local staging and
   transfer already spread out in stage 3.

## Questions for the maintainer

1. Can new starts wait briefly when the machine coordinating the queue
   disappears, while jobs already running on other machines keep going?
   **Yes.** Let the next machine catch up before it gives out work.
2. Can new starts and queue changes wait when the NAS database is down,
   even for fully cached jobs? **Yes.** Use the NAS we already depend on
   for file custody and keep one authoritative queue.
3. Should the first shared setup require a NAS, with fleets without
   one continuing to use the existing single queue? **Yes.** Prove the
   shared setup with the infrastructure it needs.
