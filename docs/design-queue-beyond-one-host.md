# Queue beyond one host: every machine runs the queue

Design for the maintainer's decision, 2026-11-24. Tracking issue #1176. Refs
MY-40, #2034, #2017, #2021, #2031, #1066, #1179. It supersedes draft #1183
(`docs/research/queue-collective-design.md`) for that decision; section 4.4 says
what it keeps from that study and what it changes.

Nothing here is implemented. The [approved design](design-proposal.md) and
`AGENTS.md` still say one queue on one machine, and they stay in force until the
maintainer approves a design (MY-40). Section 9 lists the lines an approval
would change.

How to read the claims:

- **[S n]** a primary source, listed in section 11, fetched 2026-11-24.
- **[C]** the code at release tag `s094r6.42`, named by file and function.
- **[M]** a measurement recorded on the linked issue, or one read-only status
  reading taken 2026-11-24 22:23 UTC with a CLI built from that tag.
- **[I]** this design's proposal or inference. Everything in sections 5 to 7 is
  [I] unless tagged otherwise.

## 0. What was asked

The maintainer, 2026-11-09 (MY-40):

> "i want the queue to be something the machines share rather than just running
> on one machine. just like we load balance the jobs, i think we should load
> balance the queue as well. ... where information is part of the collective in
> a way. that way if a machine goes down, nothing is truly lost."

The maintainer, 2026-11-09, rejecting dedicated queue machines (#1176):

> "nope. the design is already a fail. i'm not doing "always-on machines that
> don't run gpu jobs" they need to be the same machines that the fleet is
> comprised of. no exceptions. people don't have extra machines they can just as
> specific queue devices. i'm trying to make mycelium as universal and adaptive
> as possible."

The maintainer, 2026-11-24:

> "we should also have fable think through how to get the queue running on more
> than only [the first Linux worker]"

> "i just wanna clarify the queue should run on multiple machines like i said a
> long time ago. it should run on every machine basically so the queue load is
> shared and not based on a single machine."

> "mycelium should keep trying to maximize speed and concurrency. we have tons
> of machines this stuff can run on." (#2034)

What the design has to do, taken from those words:

1. The queue runs on essentially every fleet machine.
2. Its load is shared across them.
3. No single machine is needed for it to work; any one can be lost or taken
   down for maintenance.
4. Nothing is lost when a machine goes down.
5. No dedicated queue machines. Every member also runs GPU or CPU jobs.
6. Custody stays exact: no job handed out twice, no stale result accepted.
7. One binary, testable on a laptop with no GPU.
8. What it costs a machine that runs GPU jobs is measured and bounded.

## 1. Summary

**The shape.** Every fleet machine runs a queue **member** beside its worker,
from the same binary. Each member keeps a full copy of the queue's records on
its own disk, answers any request, and stores and serves job files. A machine's
worker talks only to the member on its own machine.

**Three kinds of work, spread differently.**

| Work | Who does it | Moves when |
|---|---|---|
| Keeping a copy, answering requests, serving files | every member | never; a member under memory pressure hands requests to another |
| **Record keeping**: a change counts only once enough machines have saved it | three members (five in larger fleets), the **voters** | a voter leaves, sleeps or dies; the steadiest other member takes its place |
| **Coordinating**: putting changes in one order and having the final say on which job starts where | one member at a time, the **coordinator** | automatically, in seconds; it sits on whichever voter has the most spare memory and the steadiest disk |

**Why one machine has the final say at any instant.** "This job goes to that
machine" has to be decided once. If two machines could each decide it, the same
job would run twice and two results would be published; that is what broke v1.
The coordinator's job is kept small so that this costs little: it puts changes
in order and checks them. The planning that leads up to a decision is spread
out in the last stage (section 5.10), where each machine works out what it
should run next and the coordinator only confirms it.

**What changes for the first Linux worker.** Today it carries the whole queue:
about 4 GiB of memory [M, #2021], every request, every file and every decision.
After stage 4 it is an ordinary member. Its queue footprint becomes a member's
(a copy on disk and a small process), the coordinator normally sits on a
machine with memory to spare, and taking it down for maintenance no longer
stops the fleet.

**What each machine pays.** A copy of the queue database on disk, a bounded
record log, one disk flush for each batch of changes, a small process, and
whatever file copies it has room for. Members that are not coordinating decode
nothing and get a memory ceiling of the kind the installer already gives the
worker service [C]. The numbers are measured in stage 0 and stage 3, before any
machine is given a vote (section 5.9).

**How it gets there.** Eight stages, 0 to 7, each of which runs on the real
fleet, can be tested on a laptop and can be rolled back (section 6). Stages 0 to 2 change
nothing about where the queue lives and help with the lock contention seen on
2026-11-24 on their own. Stage 3 puts a copy on every machine. Stage 4 removes
the single machine. Stage 5 does the same for files.

**What it does not do.** It cannot land before the memory install planned for
2026-11-25; that window is a fleet stop, as today. It does not by itself make
planning cheaper: that is stage 1 for the disk write and stage 7 for the rest.

## 2. Today, and the evidence from 2026-11-24

- One queue process, one SQLite database and one file store, on the first Linux
  worker, which also runs the paying production consumer's GPU stages.
  "Mycelium has one SQLite writer. Queue loss stops submissions, new starts,
  acknowledged cancellation and queue-mediated routing"
  ([operations](operations.md#queue-backup-restore-and-relocation)).
- The database opens with a per-database file lock, one writer connection, and
  `journal_mode=WAL` with `synchronous=FULL` [C: `queue.go`, `Open`].
- Every change runs in one function: take the queue lock, change the in-memory
  state, write the changed records to SQLite and commit, publish the read view,
  release the lock [C: `queue.go`, `updateTransactionMode`; `usage.go`,
  `persistUsage`]. The disk commit happens while the lock is held.
- Only records whose bytes changed are written, as key and value rows
  [C: `queue_persistence.go`, `persistState`]. That set of changed rows is the
  thing this design replicates.
- **Memory.** The queue process held 3.9 GiB on a 31 GiB host where jobs get
  about 16 GiB, and two production stages of about 16.4 and 7.4 GiB could not
  run side by side [M, #2021]. A laptop fixture with a 7.8 GB database measured
  2.19 GiB before and 1.37 GiB after the #2031 change, which has not yet been
  validated on the fleet [M, #2031]. The installer's default gives the queue
  service no memory ceiling (`MemoryMax=infinity`), where the worker service's
  default is `512M` [C: `install.go`].
- **Lock contention.** In 43 sampled check-ins the lock was held for a median
  1.105 s: 545 ms maintenance, 157 ms placement, 4.9 ms SQLite commit. Log
  reads waited a median 3.73 s for the lock and then held it 0.1 ms. Uploads of
  333 bytes timed out behind it [M, #2017]. Four fixes followed that night. On
  the benchmark fixture used for them, a check-in holds the lock about 87 ms
  [M, #2031]; the live figure after the fixes is not yet recorded.
- **One point of failure.** Any maintenance on that machine stops the fleet. A
  queue release updates that machine last and restarts control for everyone
  ([fleet upgrades](reference/fleet-upgrades.md): "the queue's own host only
  once every other host is done").
- **Live size** [M, status reading]. Five enrolled machines, four present: one
  Linux x86 machine with two XPU cards, one Linux arm64 machine with unified
  CUDA memory, two Apple Silicon machines, and a third Apple Silicon machine
  away. 99 active jobs (75 queued, 17 paused, 7 running). File store 87.9 GiB
  used of a 233 GiB allowance, 77.3 GiB of it held by published files. One
  status reply with machine and storage sections was 23 MB.
- **What already survives loss.** A queue restart keeps accepted jobs, and a
  three-minute grace lets workers report before anything is declared lost
  [C: `queue.go`, `RestartGrace`]. A worker cut off from the queue keeps running,
  saves results and logs locally and replays them [C: `worker.go`, `replay`].
  Results are tied to one attempt of one job in one worker session, and a
  superseded attempt cannot publish [C: `attempt_protocol.go`]. These are the
  fences the new design leans on.

## 3. What "queue load" is, and where each part goes

"Load balance the queue" only means something once the load is split into its
parts. Measured or read from the code today, all on one machine:

| Part of the load | Evidence today | Where it goes |
|---|---|---|
| Holding the records in memory | 3.9 GiB live [M, #2021] | Only the coordinator decodes records. Other members keep them on disk. History is served by any member, so the coordinator keeps live work only. |
| Answering reads: status, job lookups, logs | 23 MB status reply [M]; log reads waiting 3.73 s for the lock [M, #2017] | Any member, from its own copy, never through the coordinator's lock. |
| Receiving and serving files | 87.9 GiB on one disk [M]; small uploads timing out behind the lock [M, #2017] | Each machine keeps the files it produces and uses. Outputs are handed to the local member without crossing the network. Any holder serves. |
| Saving changes to disk | commit under the lock [C]; 4.9 ms median, 487 ms in the longest sampled hold [M, #2017] | Outside the lock. Three machines save each batch; the slowest of the three is not waited for. |
| Planning: which job fits which machine | 157 ms placement and more in qualification per check-in [M, #2017] | Stays with the coordinator through stage 6. In stage 7 each machine plans for itself and the coordinator checks. |
| Housekeeping: retention, evidence compaction | 545 ms per check-in at the worst [M, #2017] | Any member with headroom, as background work that proposes changes. |
| Being reachable | one address; one machine's uptime | Every member answers. Workers use the member on their own machine. |

So most of the load is not deciding at all. It is memory, reads, files and
disk writes that happen to share a process and a lock with the deciding.

## 4. Options considered

### 4.1 Comparison

The earlier brief asked for the full range. The maintainer's clarification
rules out the first two as the answer; they are kept here because both show up
as waypoints in the migration.

| | A. Move the queue to a better machine | B. Warm standby with log shipping | C. External replicated SQLite (rqlite, dqlite) | D. Split the queue into shards | **F. A member on every machine, one record log (recommended)** |
|---|---|---|---|---|---|
| Runs on every machine, load shared | No. Still one machine. | No. One active, one idle copy. | Records yes; load no. | Load yes; each shard is still one machine. | Yes (section 3). |
| Custody and fencing | Unchanged. Safe only if the operator proves the old queue is stopped: "Never serve two queues with the same identity". | Promotion needs a fence that the tools do not provide. Replication is asynchronous, so the newest accepted results can vanish on failover. | Exact (consensus). | Exact inside a shard; across shards nothing decides who may use a machine's memory. | Exact (section 5.3). |
| File store, about 88 GiB | Moves with the database. | Needs its own copy job. | Not covered. | Splits by shard. | Two copies on different machines; any holder serves (section 5.6). |
| How workers find it | Every worker and client changes address. | Address switches on failover. | Any node. | A lookup per job. | The member on the worker's own machine (section 5.5). |
| Upgrades and drains | The queue's machine is still last and still a fleet stop. | Same, plus the standby. | Rolling. | Per shard. | Rolling; no machine is special (section 5.8). |
| Partition | Only the queue's side works. | Two actives are possible unless fenced by hand. | Majority side works. | Per shard. | Majority side works; the rest keeps running what it has (section 5.7). |
| Laptop test | Yes. | Partly; failover is a procedure. | rqlite: a second program. dqlite: no, Linux only. | Yes. | Yes: several members in one process (section 5.11). |
| Migration with running work | A stopped copy and a restart. | Low risk. | Persistence rewritten. | Jobs re-homed. | Staged; the database file stays a normal queue database throughout (section 6). |
| Cost on GPU hosts | Moves the 4 GiB elsewhere. | Adds a second copy; saves nothing on the active host. | A second service on every machine. | Smaller per machine. | A copy and a small process on each; the heavy role sits where memory is spare (section 5.9). |

### 4.2 Why not A to E

**A. Relocation.** The runbook is explicit that it "does not provide automatic
failover or replicated availability"
([operations](operations.md#queue-backup-restore-and-relocation)). It would
free the production host's memory and nothing else.

**B. Warm standby.** Litestream "continuously copies write-ahead log pages from
disk to a replica", with "asynchronous replication", and "Each database
replicates to a single replica destination" [S7]. It "trades off durability of
recent writes", and "Synchronous replication is on the Litestream roadmap but
has not yet been implemented" [S8]. Its page describes no failover. LiteFS
"uses asynchronous replication which means that it does not confirm that writes
are committed to other nodes", chooses its primary with "leases via Consul",
intercepts writes "over FUSE", and is "outside our scope of support" [S9]. A
standby that can lose the last accepted result lets a job that was reported
done run again. A standby is also an idle copy, which is the opposite of
shared load.

**C. rqlite and dqlite.** The consensus is right; the packaging is not. rqlite
"is a standalone application", "you must only write to the database via the
HTTP API", and "It does not provide any scaling for writes, since all writes
must go through the Leader" [S10]. That is a second service on every machine
and a rewrite of persistence into SQL over HTTP. dqlite is a C library that
"runs on Linux and requires a kernel with support for native async I/O" [S11],
and half this fleet is macOS. Both prove that SQLite behind a consensus log
works, and rqlite's handling of large snapshots is a reference for section 5.2.

**D. Shards.** Splitting jobs across several queues spreads work, but each
shard still lives on one machine, so it does nothing for loss. It also breaks
what the queue is for: one machine's memory is shared by every project, and
fair sharing, priority and keeping a machine busy when its usual project is
quiet all need one view of the fleet. Shards would need something to arbitrate
between them, which is the consensus problem again. This design shards the
labour (reads, files, planning) and keeps one authority.

**E. Every machine decides, state merges later (gossip, CRDT).** This is v1.
Merging can converge on a set of jobs but cannot decide that exactly one attempt
is current or exactly one result is published. The earlier study covers it in
detail and nothing since changes that conclusion.

### 4.3 The safety rule behind F

Raft gives exactly the three properties custody needs [S1]:

- "Election Safety: at most one leader can be elected in a given term."
- "Leader Completeness: if a log entry is committed in a given term, then that
  entry will be present in the logs of the leaders for all higher-numbered
  terms."
- "State Machine Safety: if a server has applied a log entry at a given index
  to its state machine, no other server will ever apply a different log entry
  for the same index."

And the availability the maintainer asked for: the group is "fully functional
(available) as long as any majority of the servers are operational and can
communicate with each other and with clients", and "a single slow follower
will not impact performance" [S1].

### 4.4 What this keeps from draft #1183 and what it changes

Kept: an embedded consensus library in the one binary, replicating the changed
records rather than re-running queue logic on every machine, and choosing
voters from the ordinary fleet machines.

Changed:

- The organising requirement is now "every machine, load shared", so the roles
  are split and each has a stated cost and a rule for shedding it.
- A worker talks to the member on its own machine. Workers no longer find a
  queue at all.
- Output files are handed to the local member, and the member's file store and
  the worker's file cache become one store.
- Planning is spread in a final stage, with a measured gate, instead of staying
  on one member for good.
- The membership rule starts simple (three voters, steadiest machines) and
  only grows the features a measurement asks for.
- Every stage runs on the real fleet and leaves a normal queue database that
  the single-queue mode can open.

## 5. The design

### 5.1 Members and roles

A **member** is the queue part of `myce` on a fleet machine. It runs as its own
service beside the worker service, as the queue and worker services already do
[C: `install.go`], so a member restart never touches running jobs and the member
can have its own memory ceiling.

Every member:

- holds every committed record in its own local SQLite file;
- accepts any API request, answering reads itself and passing writes to the
  coordinator;
- holds and serves job files, within the disk allowance it has;
- is the only thing its own machine's worker talks to.

Three **voters** (five in larger fleets, section 5.4) each save a change before
it counts. One voter is the **coordinator**. It runs today's scheduler, turns
each change into a log entry, and releases a reply or a worker instruction only
after the entry is saved by a majority of voters.

A member that is neither is a **copy**: it receives every entry, a moment
later, and can become a voter in one step because it is already up to date.

### 5.2 The record log

**What an entry is.** Today `persistState` produces the set of rows that
changed and writes them inside one SQL transaction, together with usage rows and
model intervals [C: `usage.go`, `persistUsage`]. That set of row changes becomes
one log entry. Applying an entry on any member is writing those bytes into the
same tables. Only the coordinator decodes records and runs queue logic. Every
member's database holds the same committed rows, and it stays a normal queue
database that today's code can open.

The library requires this: "Apply must be deterministic and produce the same
result on all peers in the cluster" [S3]. Row images satisfy it; re-running
today's transaction on every machine would not, because the transaction reads
the clock, the NAS and the filesystem.

**Every durable write goes through it.** Six files write to the database
outside the main transaction today: enrollment tickets, environments, history
retention and persistence, log retention, and job log chunks
[C: `enrollment_ticket.go`, `environments.go`, `history_retention.go`,
`history_persistence.go`, `log_retention.go`, `queue.go`]. Each becomes an entry
of a named kind.

**Exactly once.** Usage rows are additive (`busy=busy+excluded.busy`) [C], so
applying an entry twice would double them. Each member stores the number of the
last entry it applied in the same SQLite transaction as the entry, and skips
any entry at or below it.

**Commit outside the lock.** The order becomes:

1. Take the queue lock, run today's change, produce the entry, release the lock.
2. Hand the entry to the log. The coordinator saves it and sends it on; it is
   committed when a majority of voters have saved it. Entries from one
   coordinator keep the order the lock gave them.
3. On commit, apply it, publish the read view, and only then release the reply
   or the worker instruction.
4. If a commit fails, the member is no longer coordinator. It discards its
   in-memory state, fails the waiting requests with a retryable 503, and reloads
   from its committed records the way a restart does today.

Step 1 keeps the rule that no network or bulk I/O happens under the queue lock,
and removes the disk flush that is under it today. Several entries share one
flush: the library batches up to 64 entries per round by default [S4].

**What is replicated, and what is not.** Replicating today's check-in writes as
they are would send them to every disk. On a laptop fixture one check-in wrote
20,765 bytes of SQLite log in the foreground and 97,067 bytes with the
background writers [M, #1066]. At five workers checking in once a second
[C: `worker.go`] that is about 0.5 MB/s, roughly 42 GB a day, per copy
[calculation]. So the classes are:

| Class | Examples | Treatment |
|---|---|---|
| Intent | submit, cancel, pause, resume, policy, pins, maintenance, release approval, enrollment | Committed before the reply. |
| Custody and outcome | attempt assigned, started, completed, result accepted, release confirmed, retry and loss counts | Committed before the instruction or reply. |
| Learned | curves, memory evidence, efficiency baselines | Committed in batches; a few seconds may be lost and are re-learned. |
| Liveness and telemetry | last contact, device readings, tool inventory, free memory | Not replicated. The coordinator holds them in memory and rebuilds them from the next check-in, as a restart already does for inventories [C: `Open`]. |
| Wait diagnostics | why a queued job is waiting, transition history | Not replicated per check-in; recomputed, with a summary committed when it changes. |
| Job logs | log chunks | Committed in batches to start with. Moved to file-style storage (section 5.6) only if stage 0 shows they dominate. |
| Files | inputs, outputs, bundles, checkpoints | Not in the log. Holder records are (section 5.6). |

**Falling behind and catching up.** The log is trimmed once its entries are
applied. A member that is a little behind gets the entries it missed; the
library keeps a tail for this ("so that we can quickly replay logs on a follower
instead of being forced to send an entire snapshot"), 10,240 entries by default
[S4]. A member that is far behind gets a consistent copy of the database and
then the tail. SQLite's `VACUUM INTO` produces "a consistent snapshot of the
original database" from a live one [S13], and because the copy carries its own
last-applied number, entries it already contains are skipped. The library allows
a state machine that recovers by itself: `NoSnapshotRestoreOnStart` is "useful
if your FSM recovers from other mechanisms than raft snapshotting" [S3].
Copying several GB to a returning machine is a cost to measure (section 8).

**Library.** `hashicorp/raft` v1.8.0, MPL-2.0 [S3]: voters and non-voters,
leadership transfer, an in-memory transport for tests, and manual recovery. It
is the library under Consul and Nomad. `etcd-io/raft` is the alternative:
"both network and disk IO are left to the user", and it is driven by an
explicit `Tick()`, which suits fake-clock tests better [S6]. Start with the
first because it is complete; keep it behind one small interface so the choice
can change.

### 5.3 Custody and fencing

The chain, in order. Each link is either already in the code or is a property
of the log.

1. **One process per database.** The file lock in `Open` stays, per member [C].
2. **One coordinator per term** [S1, Election Safety].
3. **Nothing leaves before it is committed.** No reply, no worker instruction
   and no file publication is released until its entry is saved by a majority.
   A coordinator that has lost its majority cannot commit, so it cannot assign
   or stop anything. It steps down by itself: "If we reach this interval
   without contact, we will step down as leader" [S3], 500 ms by default [S4].
4. **Committed entries survive every change of coordinator** [S1, Leader
   Completeness]. A new coordinator starts from everything any earlier one
   committed.
5. **A fencing number on every instruction.** Each instruction carries the term
   and entry number it came from. The worker's own member is part of the log,
   knows the current term, and refuses anything older than the newest it has
   acted on. This is the fencing-token pattern: "a number that increases ...
   every time a client acquires the lock", checked by the receiver, which
   rejects "any writes on which the token has gone backwards" [S14].
6. **Attempt identity is unchanged.** Instructions and reports are keyed by
   job, attempt number and worker session [C: `attempt_protocol.go`]. A result
   from a superseded attempt is refused against committed state.
7. **One accepted result.** Accepting a result is one committed entry, and the
   log has one order, so two results for one job cannot both be accepted
   [S1, State Machine Safety].
8. **Retries do not duplicate.** A submission key is what the Raft paper calls a
   client serial number: "If it receives a command whose serial number has
   already been executed, it responds immediately without re-executing the
   request" [S1]. A client whose reply was lost in a coordinator change replays
   its key and gets the original answer, as it does after a restart today.
9. **A new coordinator waits before declaring loss.** It opens the existing
   restart grace and marks every machine as not yet reported [C: `Open`].
   A change of coordinator is a queue restart as far as workers can tell, and
   the restart tests become the coordinator-change tests.
10. **A worker that cannot reach a majority starts nothing new**, keeps running
    what it has, and saves results until they are acknowledged, as today
    [C: `worker.go`].

What this does not change: a machine cut off from the fleet keeps running its
job, and after the measured allowance the queue hands that job to another
machine. For a while both run. That is the approved design's "late return"
rule, and the queue keeps the copy with the newest complete checkpoint. The
log removes the other way to get two copies, two authorities each assigning
the job, and leaves this one as it is.

**Reading your own write.** A client that submits through one member and reads
through another could miss its own job for a moment. Every reply carries the
entry number it reflects; a member that has not yet applied up to the number a
client last saw waits for it, and a job lookup that misses locally asks the
coordinator before answering "not found".

### 5.4 Membership: joining, leaving, sleeping, maintenance and loss

**How many voters.** Three for any fleet of three or more, because "a majority
of the servers" must stay up [S1] and three is the smallest group that survives
one loss. Five once at least five members are measurably steady: "a typical
cluster of five servers can tolerate the failure of any two servers" [S1].
More voters do not add safety beyond that and slow every commit. Every other
member is a copy, and there can be any number of copies.

**Which machines vote.** The coordinator ranks members from what the queue
already records: how steadily each has been reachable, whether it is a laptop
or on battery, whether it is about to drain or restart, and how fast its disk
flushes. The three steadiest vote. Changes happen one at a time and each is
itself a committed entry. A machine has to be caught up and steady for a while
before it can vote, the rule Consul uses: "Time duration that a new server must
remain healthy before it can become a voting member" [S5]. A margin keeps the
set from flapping on momentary load.

**Which voter coordinates.** The one with the most memory to spare after the
jobs queued for it, and a steady disk. A machine whose job memory is the
fleet's bottleneck ranks last. The move uses `LeadershipTransfer`: the
coordinator "will stop accepting client requests, make sure the target server
is up to date" and hand over [S3].

| Event | What happens |
|---|---|
| **A machine joins** | `myce install` puts a member on it. It joins as a copy ("The server will receive log entries, but it won't participate in elections or log entry commitment" [S3]), catches up, and starts answering requests. It can vote once it has been steady. |
| **Planned restart or upgrade** | If it coordinates, it hands that over first. It keeps its vote: it is back in seconds and three voters tolerate one being away. |
| **Planned power-off or maintenance** | It hands over coordinating, then its vote, then goes. Nothing pauses. |
| **Sleep** | The same hand-over, triggered by the system's sleep notice (stage 6). A machine that sleeps often ranks low and is rarely a voter. |
| **Sudden loss of a copy** | Nothing changes. |
| **Sudden loss of a voter** | The other two carry on. After the measured contact allowance the coordinator gives the vote to the steadiest copy. Consul does this as "periodic dead server removal from the Raft peer set" [S5]. |
| **Sudden loss of the coordinator** | The other two voters elect one of themselves (the default timeout is one second [S4]), it loads state as a restart does (about 3 to 4 s on the 7.8 GB fixture [M, #2031]), opens the restart grace and carries on. Requests in flight get a retryable error. |
| **Two of three voters lost at once** | The survivor keeps every record and answers reads, and the queue pauses new starts until one of the two is back. Running jobs keep running. It never acts alone, because the missing two may have committed something it has not seen. |
| **A machine returns after days** | It rejoins as a copy and catches up from the log tail or a database copy. It cannot coordinate with stale records. |

**Fleet sizes.**

| Fleet | Arrangement | Honest limit |
|---|---|---|
| 1 machine | One voter. Today's queue. | The queue stops with the machine. |
| 2 machines | One voter and one copy. A planned leave hands the vote to the other. | If the voter dies without warning, the copy has everything except the last moment and pauses until the voter returns or someone runs one recovery command. Two voters would pause on either machine's loss, which is worse. |
| 3 or 4 | Three voters, the rest copies. | Any one machine can be lost. Two voters lost at the same instant pause the queue until one returns. |
| 5 or more | Five voters once five are steady. | Any two can be lost at once. |

**Manual recovery.** For the case where a majority of voters is gone for good,
the library has `RecoverCluster`, and its own warning is: "in general this is
an extremely unsafe operation" [S3]. It is exposed as one explicit operator
command that names the survivors and prints that warning. It is the only path
in the design that can lose an acknowledged change, and it never runs by itself.

### 5.5 Finding the queue

- **Workers** use the member on their own machine, over loopback. There is no
  address to configure and nothing to discover. A worker whose local member is
  down behaves as it does today when the queue is unreachable.
- **Members** find each other from the committed member list each keeps on
  disk, from the existing `_mycelium._tcp` advertisement, and from the
  invitation's saved address [C: `discovery.go`, `joining.go`]. A member that
  comes back on a new address announces it; adding a member that is already
  known "updates the server's address" [S3].
- **Clients** (the CLI and the Python client) use any member. `ResolveJoining`
  already browses for candidates, tries each, and accepts the first that passes
  the pinned identity and the credential [C: `joining.go`]. Every member
  presents the same joining identity, so the pin every client already holds
  stays valid. Clients also save the member list from each reply, so a client
  with one remembered address is not tied to that machine.
- **Writes sent to a member that is not coordinating** are passed on by that
  member, as rqlite does: "the node will transparently forward the request to
  the Leader" [S10]. Clients see one request and one reply.
- **The old single address** keeps working while that machine is up, because
  that machine is still a member.

This is the one place consumers see a change (a saved member list in place of
one address), so the Mycelium skill changes with stage 4.

### 5.6 Files

A file is already stored by its SHA-256 under `blobs/` [C: `files.go`], so it
does not matter which machine holds it. The worker's cache is keyed the same way
and "contains only reproducible store copies" [C: `file_cache.go`]. What is
missing is a second copy and a record of who has one.

- **Holder records.** A committed record per file lists the members that have
  verified it. Verification and copying happen outside every lock, as now.
- **Two copies by default**, on different machines, chosen by measured free
  disk. A fleet of one keeps one. The first Linux worker keeps a full set until
  stage 5 is accepted, so rollback needs no gathering.
- **Uploads go to any member.** That member publishes the file locally with
  the existing flush, link and directory sync, and records itself as the first
  holder.
- **Outputs stay where they are made.** A worker hands its outputs to the
  member on its own machine, which is a local link, not an upload. That removes
  the path that timed out on 2026-11-24, where a 333-byte upload waited behind
  the queue lock on another machine [M, #2017]. Copying to a second machine is
  background work.
- **One store per machine.** The member's store and the worker's cache become
  the same directory. An input fetched for a job is both the cached copy and a
  holder's copy, so the second copy is often free.
- **When a result counts.** A job that depends on another can start as soon as
  the output it needs is committed with one holder, because the bytes exist.
  The job is reported as succeeded to its owner once two holders have verified
  the output. If the only holder is lost first, the job goes back in line as an
  infrastructure loss. Status shows `holders: 1/2` until then and never calls
  one copy durable.
- **Serving.** A worker fetches an input from its own machine if it is there,
  otherwise from any holder named in its instruction.
- **Small, critical files go everywhere.** A queue start verifies frozen
  extension bundles before serving traffic [C: `extension_bundle_store.go`].
  Bundles and environment snapshots are small, so every member holds them and
  any member can become coordinator without fetching anything first.
- **Collection.** Reference tracking stays the authority. Retiring a file is a
  committed record; each holder removes its copy when it applies it.
- **Capacity.** Each member has its own store allowance from measured free
  disk, like today's single allowance. If the fleet cannot hold two copies, new
  uploads get the existing capacity refusal and status says which machines are
  full.
- **The NAS** can be an extra holder for finished outputs where one is mounted.
  It is never the only holder of something the queue needs, and no queue state
  lives on it: "WAL does not work over a network filesystem" [S2].
- **Checkpoints.** NAS checkpoints are unchanged. File-store checkpoint
  generations are files and get two holders before a generation counts as
  portable.

A separate storage system was considered and set aside: the store is already
content-addressed with resumable transfer, and a second service on every
machine breaks the one-binary rule.

### 5.7 Partitions and power loss

| Situation | Majority side | Minority side | After it heals |
|---|---|---|---|
| One machine drops off the network | Carries on. If it was the coordinator, a new one in seconds. | Its member answers reads marked stale and refuses writes with a retryable error. Its worker keeps running its jobs, starts nothing new and saves results. | It catches up; saved results are replayed; overlapping copies settle by the late-return rule. |
| The network splits in two | The side with two of three voters carries on. | As above. | As above. |
| No side has a majority | Nobody assigns. All running jobs continue. | | The first majority to re-form carries on. Nothing committed is lost. |
| A deposed coordinator keeps answering its worker | It cannot commit, so it issues nothing new, and it steps down within its lease. Anything it re-sends carries an old term and is refused. | | |
| Whole fleet loses power | Machines boot in any order. Nothing happens until two of the last three voters are up; then one coordinates, opens the restart grace, and workers report. A machine with stale records cannot be chosen. | | Jobs running at the cut are infrastructure losses, as today. |

Every acknowledged change was flushed to disk on a majority first. SQLite with
`synchronous=FULL` in WAL mode already gives that on one machine [C]; the log
extends it to two of three. On macOS a Go file sync issues a full flush because
"SYS_FSYNC doesn't fully flush contents to disk" [S15]; it is slower, and
measured in stage 0.

The approved design's rule for a quiet network (a strict majority of workers
silent means the network failed, so nothing is put back in line) carries over
without change. A coordinator that cannot reach a majority cannot declare
anything in any case.

### 5.8 Upgrades and drains

- **A release rolls member by member.** A member that is not coordinating
  restarts with no effect on the fleet. The coordinator hands over, then
  restarts. The upgrade operation loses its "queue host last" rule and its
  queue-specific phases.
- **Workers are unchanged.** A worker binary update still takes over running
  jobs without stopping them (MY-55). The member is a separate service, so its
  restart never touches a job.
- **Mixed versions.** Only the adjacent release pair has to interoperate
  (MY-42). Members that are not coordinating store bytes without decoding them,
  so an old member can hold a new coordinator's records. New record formats are
  written only after a committed "fleet format" record, which the coordinator
  proposes once every voter runs the new release. Until that record, any member
  can be rolled back.
- **Drains are about jobs, not about the queue.** A drained or disabled machine
  takes no new jobs and stays a full member, voter included. Only a machine
  that is going away hands its roles over.
- **Maintenance on any machine,** the first Linux worker included, is a
  hand-over and then an ordinary power-off. The fleet keeps scheduling.

### 5.9 Cost on machines that also run GPU jobs

The numbers below are the ones that exist. The rest are measured in stage 0 and
stage 3 on the real machines before any of them is given a vote.

| Role | Memory | Disk | CPU | Known today |
|---|---|---|---|---|
| Copy (every member) | A small process. No decoded records; SQLite's page cache and a short log tail. | The database, plus a bounded log. | Writing committed bytes. | Not yet measured. The driver's page cache is 2,000 KiB per connection [M, #2031]. |
| Answering requests | Per request. | None. | TLS, JSON, rendering status. | A status reply can be 23 MB [M]. |
| Holding files | None. | Up to its allowance. | Hashing on verify. | 87.9 GiB in the fleet today [M]. |
| Voter | As a copy. | One flush per batch of changes. | Small. | 4.9 ms median commit on the first Linux worker [M, #2017]. |
| Coordinator | The live queue state. | As a voter. | Planning. | 3.9 GiB live [M, #2021]; 1.37 GiB on the fixture after #2031 [M]. |

How the cost is kept bounded:

- **A ceiling per member.** A member that is not coordinating runs under a
  memory ceiling in its service unit. The installer already writes `MemoryMax`,
  `CPUQuota` and `IOWeight` for services [C: `install.go`]. The ceiling is set
  from the stage 3 measurement, not guessed here.
- **The footprint is charged, not hidden.** Each host's memory ledger already
  measures memory used by things that are not jobs [M, #2021]. The member is in
  that measurement, so job admission sees it.
- **The heavy role goes where memory is spare.** Coordinator placement counts
  the jobs queued for each machine, so the role stays off the machine whose
  job memory is the bottleneck. On this fleet that is the first Linux worker
  [M, #2021], and the Linux arm64 machine has the most memory.
- **A member under pressure sheds work in a fixed order:** stop answering
  reads (pass them on), stop taking new file copies, hand over coordinating,
  hand over its vote. It never sheds its copy.
- **Background work yields to jobs.** File copying and catch-up run at
  background I/O priority, as shared-host commands already do on macOS
  (`taskpolicy -b`) [C: `shared_host.go`], and stop when the machine's disk is
  under job pressure.
- **A slow disk slows only itself.** A commit waits for the fastest two of
  three voters [S1]. A voter whose disk is busy staging a large input is the
  one not waited for. A coordinator whose disk degrades hands the role over.
- **Disk writes are reported.** Each member reports replicated bytes per day,
  so write volume and wear are visible per machine.

The coordinator's memory should fall well below today's figure, because it
stops serving history and reads. That is a prediction to be checked in stage 4.

### 5.10 Sharing the deciding: plan on every machine, confirm once

Through stage 6 the coordinator still plans, and planning is what is left of
the 2026-11-24 contention once the disk write is out of the lock. On the
benchmark fixture a check-in holds the lock about 87 ms [M, #2031]. At that
cost one lock serves about 11 check-ins a second, which is about 11 machines at
today's one-second interval [calculation]. A queue meant for any fleet from one
machine to dozens cannot keep all planning behind one lock.

The last stage moves planning to the machine it is for:

- When a machine has room, **its own member** works out what it should run
  next: capability, qualification, memory fit, ranking. It does this from its
  own copy of the committed records, at a known entry number.
- It sends the coordinator a short proposal: "start this job on me, worked out
  at entry N".
- The coordinator **checks** it against the current records: the job is still
  waiting, the machine's memory still fits, and nothing since entry N changes
  the choice. It commits it, or rejects it with the newer number and the member
  plans again.
- Housekeeping (retention, evidence compaction) runs the same way on any member
  with headroom.

Rules that look across machines (least valuable machine that fits, scarce-host
yield, fair sharing) are worked out by every member from the same committed
records, so two members reach the same answer at the same entry number. Where
they race, the coordinator's check decides.

This is an established pattern. Nomad: "Multiple schedulers are running in
parallel without locking or reservations", each "submits the plan to the
leader", and the plan queue lets "the leader node ... do partial or complete
rejections of a plan" [S12]. Google's Omega uses "parallelism, shared state,
and lock-free optimistic concurrency control" [S16]. It is also the rule
`AGENTS.md` already sets for the queue lock ("snapshot, work outside, then
recheck fences inside"), applied across machines.

It is gated on a measurement: build it when, after stage 4, the share of each
second the coordinator spends holding the lock still limits how fast work
starts. It needs each planning member to keep live work in memory, which is
small (99 active jobs today [M]) and counted in that machine's ceiling.

### 5.11 Laptop testing

- **`myce dev --members N`** runs N members in one process, each with its own
  temporary database and file store, joined by the library's in-memory
  transport, which exists "to allow Raft to be tested in-memory without going
  over a network" [S3]. Fake GPU inventory and the real API and command runner
  stay as they are.
- **Fault controls** in dev: cut a member off (`Disconnect`: "remove the
  ability to route to a given peer" [S3]), kill it and keep its disk, kill it
  and wipe its disk, slow its disk flush, and kill everything and restart in a
  random order.
- **Most tests do not pay for any of this.** The log sits behind one interface.
  The default implementation in unit tests is a one-member log that commits
  immediately, so the existing suite keeps its speed and its fake clock.
- **The clock rule keeps its meaning.** Queue and scheduling decisions still
  use the injected `Clock`. The consensus library's own election timers use
  real time; that is a named exception, confined to the log package, and those
  tests use short real timeouts. If they prove flaky, `etcd-io/raft` is driven
  by an explicit tick and removes the exception [S6].
- **A history check.** One scenario test records every job transition during
  random faults and checks that no job had two current attempts and no result
  was accepted twice.

## 6. Migration

Each stage runs on the real fleet with production work in the queue, is tested
on a laptop first, and has a rollback. The database stays a normal queue
database on every member the whole way, which is what makes the rollbacks
simple. Sizes are relative: S is days of work, L is the largest piece.

**Stage 0. Measure, and list the single-host assumptions. (S, no behaviour
change.)**
Bytes and entries per second by the classes in section 5.2, on the live queue
and on the fixture. Disk flush latency on each fleet machine while it runs a
representative job. Round-trip time and loss between machines over a day.
Database size, time to copy it consistently, and time to load it. A list of
every place the queue assumes it is on one known machine. Found so far: the NAS
probe the queue runs from its own host [C: `storage_probe.go`], output delivery
to the NAS [C: `output_delivery.go`], the co-located disk relief keyed by the
queue's hostname [C: `queue.go`, `diskHost`], startup bundle verification, the
owner-write route ("on the queue host", [operations](operations.md)), and
release manifests read by the queue command.
*Done when* the numbers are on #1176 and the blanks in section 5.9 are filled.

**Stage 1. One write path, committed outside the lock. (L, single host.)**
Every durable write goes through one log interface, still backed by today's
SQLite on today's host. Liveness, telemetry and wait diagnostics stop being
written on every check-in. Replies and instructions are released after the
commit, and the lock is not held during it.
*Done when* the restart and crash tests pass; an injected commit failure shows
that no reply went out before its commit; bytes per check-in and lock hold per
check-in fall against stage 0.
*Rollback:* a flag selects the in-lock commit for one release. Both write the
same records.
*Helps by itself:* this is the disk half of the 2026-11-24 contention.

**Stage 2. The log, with one member. (M, single host.)**
The interface is backed by the consensus library with a single voter. The
existing database is the starting state.
*Done when* `myce dev` passes unchanged, `kill -9` during a fan-out loses no
accepted job, and commit latency is within noise of stage 1.
*Rollback:* the flag from stage 1. The database is the same format.

**Stage 3. A member on every machine. (L.)**
`myce install` puts a member on every enrolled machine. Each joins as a copy,
catches up, answers reads and passes writes on. Workers switch to their local
member. `myce dev --members N` exists from here.
This is the warm-standby option as a waypoint, with no special machine: every
machine now has all the records, a moment behind, and a fenced manual promotion
is possible if the first Linux worker dies.
*Done when* a submission through any member runs; killing a copy changes
nothing; member memory, CPU and bytes written per day are measured on each
fleet machine and a ceiling is set; the first Linux worker's share of read
requests has fallen.
*Rollback:* remove the other members. The first Linux worker carries on.

**Stage 4. Three voters; the coordinator moves by itself. (L.)**
Voters and coordinator are chosen as in section 5.4. Hand-over on restart,
maintenance and power-off. Replacement of a lost voter. The fencing number on
instructions. Clients save the member list; the skill is updated. The upgrade
operation drops its queue-host rules.
*Done when,* on a laptop: the coordinator is killed mid-batch and every
accepted job is present, running attempts finish and are accepted once, and a
copy is promoted; two of three voters are killed and the survivor pauses and
then resumes with nothing lost; an isolated old coordinator's instructions are
refused; the history check passes under random faults. *And on the fleet:*
planned maintenance of the first Linux worker with no failed request; its
queue memory at the member ceiling and its job allowance raised to match; one
unplanned-loss drill on a machine with no production work.
*Rollback:* stop all members but one, chosen as the most up to date, and run
it in single-queue mode. Its database is complete.

**Stage 5. Files on every machine. (L, independent of stage 4.)**
Holder records, two copies, local hand-over of outputs, one store per machine,
any holder serves.
*Done when,* on a laptop: an upload through one member is fetched from another
after the first is killed; an interrupted transfer resumes; a retired file
leaves every holder; status shows `1/2` and never calls it durable. *And on the
fleet:* the first Linux worker powered off and jobs on other machines still
fetch inputs and publish outputs.
*Rollback:* one copy, with the first Linux worker holding the full set it
never gave up.

**Stage 6. Machines that sleep, and larger fleets. (M.)**
Hand-over on the system's sleep notice on macOS and Linux. Five voters where
five are steady. Members that return on a new address.
*Done when,* on a laptop: a member that sleeps and wakes every few minutes is
never chosen to vote while a steady copy exists, and a sleeping coordinator
hands over with no failed request.
*Rollback:* fixed three voters.

**Stage 7. Plan on every machine. (L, gated.)**
Section 5.10. Built only if the stage 4 measurement says the coordinator's
planning still limits throughput.
*Rollback:* planning returns to the coordinator; the proposal check stays.

Stages 4 and 5 can run in parallel once stage 3 is accepted. Until stage 5,
files still depend on the first Linux worker: with stage 4 alone, its
maintenance no longer stops scheduling, but file transfers to and from it wait
and workers retry.

**The memory install on 2026-11-25** comes before any of this. It is a fleet
stop handled as today. The existing relocation runbook is the only tool that
could avoid it, and it is itself a stopped copy.

**Consumers.** API routes, the job model, submission keys, error codes, the
token and the pinned identity do not change. A change of coordinator looks like
a brief queue restart: retryable errors that clients already handle. Visible
additions: the saved member list (stage 4) and `holders` in file status
(stage 5).

## 7. What this deletes or replaces

- **The disk commit under the queue lock** [C: `updateTransactionMode`,
  `persistUsage`]: replaced by commit outside the lock, reply after commit.
- **Direct database writers** in six files (section 5.2): replaced by log
  entries.
- **Per-check-in durable writes of liveness, telemetry and wait diagnostics:**
  removed.
- **The queue as a host-specific install role** [C: `install.go`]: `myce install
  queue` on one chosen machine becomes a member installed everywhere. The queue
  service's unlimited memory setting goes with it.
- **The relocation runbook** ([operations](operations.md#queue-backup-restore-and-relocation))
  and "keep the file store with the queue database": replaced by adding and
  removing members. A backup export stays.
- **"Queue host last" and the queue phases of the fleet upgrade**
  ([fleet upgrades](reference/fleet-upgrades.md)): removed.
- **Workers finding a queue:** the worker's queue address, discovery and
  reconnect targeting are replaced by its local member. Discovery stays for
  clients and members.
- **Workers uploading outputs across the network to the queue's store,** and
  the first-upload timing workaround added in #2025: replaced by a local
  hand-over.
- **The separate worker file cache** [C: `file_cache.go`]: merged into the
  member's store.
- **Queue-local NAS probe, NAS output delivery and co-located disk relief:**
  each becomes something any member with that mount or that disk does.
- **The single file-store allowance:** replaced by one per member.

Kept as they are: the scheduler and its policies, the job model, attempt
identity and stale-result refusal, the worker spool and replay, the restart
grace, checkpoints, the API, the token and the injected clock.

## 8. Risks and unknowns

1. **Write volume.** Without the classification in section 5.2 this ships
   about 42 GB a day to every disk [calculation from #1066]. Stage 1 is a
   prerequisite, not an optimisation.
2. **Reply after commit touches every change path.** Any side effect that
   today runs before the commit returns (file publication, NAS delivery, engine
   commands) has to be found and moved. The existing `committed` callback is
   the seam [C]. This is the largest single piece of work.
3. **Catching up a multi-GB database.** A returning machine may need the whole
   database copied. The copy must not starve jobs on either end. Measure in
   stage 0; rqlite's snapshot handling is the reference [S10].
4. **Disk flush on macOS** is a full flush [S15] and may take tens of
   milliseconds. Batching absorbs it; a Mac that coordinates sets the pace.
   Measured in stage 0 and counted in the ranking.
5. **Single-host assumptions not yet found.** The stage 0 list is the control.
6. **Library timers use real time.** A named exception to the fake-clock rule,
   confined to one package, with a fallback library (section 5.11).
7. **One joining identity on every machine.** Any member holds the key that
   clients pin. The fleet token is already on every machine, so this adds
   little, but a per-member certificate under a fleet authority is the better
   end state and would change what clients pin.
8. **Mixed versions.** The format record in section 5.8 has to be tested with
   the adjacent release on the laptop cluster.
9. **Two-machine fleets** can have durability on both machines or progress
   with one, not both (section 5.4).
10. **Machines that fail together.** Voters on one power strip or one access
    point are lost together, and the fleet cannot see wiring. A majority lost
    at once pauses the queue until one returns.
11. **Manual recovery can lose an acknowledged change** [S3]. It stays manual,
    explicit and loud.
12. **Planning cost remains until stage 7.** Stages 1 to 4 remove the disk
    write, the reads and the files from the coordinator's lock, not the
    planning.
13. **Disk space for second copies.** About 88 GiB of files becomes up to
    176 GiB across the fleet, less what worker caches already hold. Machines
    with small disks hold less; status has to say when the fleet cannot hold
    two copies.
14. **The v1 lesson.** v1 failed by being "too heavy to run in production and
    too hard to test". The guards here are one authority at a time, one library
    protocol in place of home-grown ones, a laptop cluster with fault controls,
    and stages that each ship alone.
15. **Licence.** `hashicorp/raft` and its log store are MPL-2.0 [S3]. The
    repository has no licence file yet [M], which a public release needs
    settled in any case.

## 9. What an approval would change

Not changed by this document:

- **Approved design, "The parts".** "Queue service | One always-on machine"
  becomes "every participating machine; three or five of them save each change
  and one at a time coordinates". "There is no peer network, no leader election
  and no replicated state" becomes "members replicate committed records through
  one consensus log; there is no gossip registry and no deciding by workers".
- **`AGENTS.md`.** "One binary, one SQLite queue ... No peer network, leases,
  replicated registry" becomes "One binary. Every machine holds the queue's
  committed records in its own SQLite file; a few save each change and one
  coordinates at a time. No dedicated queue machines, no gossip registry, no
  v1 peer coordination." Add: "Nothing is replied or sent to a worker before
  it is committed. Commit happens outside `q.mu`."
- **Decisions.** MY-40 moves from direction to an approved design, with the
  2026-11-24 clarification recorded beside it.
- **Skill.** One paragraph on the member list at stage 4 and one on `holders`
  at stage 5.

## 10. Questions for the maintainer

Each is a yes or no. The recommended answer is first.

1. **One final say at a time.** Every machine holds everything, answers
   requests and serves files. But at any instant one machine has the final say
   on which job starts where, because two machines deciding that at once would
   run the same job twice. That role moves by itself in seconds, and the
   planning behind it is spread to every machine in the last stage. **Is that
   what you mean by the queue being shared? (Recommended: yes.)**
2. **Changing the standing rules.** The approved design and `AGENTS.md` say one
   queue on one machine with no replicated state. **May they be changed as in
   section 9 once you approve this? (Recommended: yes.)**
3. **Two copies of every job file.** Each file is kept on two different
   machines, so losing a machine loses no file. That takes up to about twice
   today's 88 GiB across the fleet. **OK? (Recommended: yes.)**
4. **Two voters lost at the same instant.** With three vote-carrying machines,
   losing any one changes nothing. If two lose power at the same moment, the
   queue keeps everything and running jobs continue, but nothing new starts
   until one is back. **Is that pause acceptable, with five voters once five
   machines are steady? (Recommended: yes.)**
5. **A two-machine fleet.** If one of two machines dies without warning, the
   other pauses new starts until it returns or someone runs one recovery
   command, and nothing is lost except possibly the last instant. **Is a pause
   better than carrying on automatically and risking a job running twice?
   (Recommended: yes.)**
6. **A new library in the binary.** This uses the open-source consensus library
   that Consul and Nomad are built on (MPL-2.0 licence). **OK to depend on it?
   (Recommended: yes.)**
7. **Order of work.** Stages 0 to 2 (measure, then take the disk write out of
   the lock) are useful on their own and change nothing about where the queue
   lives. **Start those now, ahead of the rest of the current route?
   (Recommended: yes.)**

## 11. Sources

External pages were fetched on 2026-11-24. Quotes are verbatim.

- S1. Ongaro and Ousterhout, [In Search of an Understandable Consensus
  Algorithm (extended version)](https://raft.github.io/raft.pdf), 2014:
  section 2 (availability with any majority; five servers tolerate two
  failures), Figure 3 (Election Safety, Leader Completeness, State Machine
  Safety), section 5.3 ("a single slow follower will not impact performance"),
  section 5.6 (timing), section 6 (non-voting members), section 8 (client
  serial numbers).
- S2. [SQLite write-ahead logging](https://sqlite.org/wal.html): "All processes
  using a database must be on the same host computer; WAL does not work over a
  network filesystem"; "there can only be one writer at a time".
- S3. [hashicorp/raft package documentation](https://pkg.go.dev/github.com/hashicorp/raft),
  v1.8.0, MPL-2.0: `FSM.Apply`, `AddNonvoter`, `AddVoter`, `DemoteVoter`,
  `RemoveServer`, `LeadershipTransfer`, `Config.LeaderLeaseTimeout`,
  `Config.NoSnapshotRestoreOnStart`, `BatchingFSM`, `InmemTransport`,
  `RecoverCluster`. The log store is
  [hashicorp/raft-boltdb](https://github.com/hashicorp/raft-boltdb), "an
  implementation of both a `LogStore` and `StableStore`", MPL-2.0.
- S4. [hashicorp/raft `config.go`](https://github.com/hashicorp/raft/blob/main/config.go),
  `DefaultConfig`: heartbeat and election timeouts 1000 ms, leader lease
  500 ms, commit timeout 50 ms, 64 entries per append, 10,240 trailing logs.
- S5. [Consul Autopilot](https://developer.hashicorp.com/consul/docs/manage/scale/autopilot):
  `CleanupDeadServers`, `ServerStabilizationTime` (default 10 s).
- S6. [etcd-io/raft README](https://github.com/etcd-io/raft): "the library only
  implements the Raft algorithm; both network and disk IO are left to the
  user"; "the library models Raft as a state machine"; `Node.Tick()`.
  Apache-2.0.
- S7. [Litestream: how it works](https://litestream.io/how-it-works/).
- S8. [Litestream: tips and caveats](https://litestream.io/tips/).
- S9. [LiteFS: how it works](https://docs.fly.io/litefs/how-it-works).
- S10. [rqlite FAQ](https://rqlite.io/docs/faq/) and
  [rqlite design](https://rqlite.io/docs/design/): "rqlite snapshots the SQLite
  database and truncates the Raft log".
- S11. [canonical/dqlite README](https://github.com/canonical/dqlite).
- S12. [Nomad: how scheduling works](https://developer.hashicorp.com/nomad/docs/concepts/scheduling/how-scheduling-works).
- S13. [SQLite VACUUM](https://sqlite.org/lang_vacuum.html), section 2.1,
  `VACUUM INTO`.
- S14. Kleppmann, [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html),
  2016.
- S15. Go source, [`internal/poll/fd_fsync_darwin.go`](https://github.com/golang/go/blob/master/src/internal/poll/fd_fsync_darwin.go):
  "on OS X, SYS_FSYNC doesn't fully flush contents to disk".
- S16. Schwarzkopf, Konwinski, Abd-El-Malek and Wilkes, [Omega: flexible,
  scalable schedulers for large compute clusters](https://research.google/pubs/omega-flexible-scalable-schedulers-for-large-compute-clusters/),
  EuroSys 2013.

Internal: [approved design](design-proposal.md), [decisions](DECISIONS.md)
(MY-40, MY-42, MY-55), [operations](operations.md),
[fleet upgrades](reference/fleet-upgrades.md), [file store](reference/file-store.md),
issues #1176, #1066, #2017, #2021, #2031, #2034 and draft #1183.
