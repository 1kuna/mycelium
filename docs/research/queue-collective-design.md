# Queue collective: a shared, replicated Mycelium queue

Research date: 2026-11-09. Refs #30, #31, #316, #527, #661, #1066. This is a
design study for the maintainer's decision, not an implementation, a change to
the [approved design](../design-proposal.md), or a change to AGENTS.md. Section
8 lists the amendments it proposes; the maintainer approves product direction.
No fleet host, live queue, database or spool was written to; every live fact
below comes from the public repository, its issues and public primary sources.

Every claim is tagged. **[S n]** is sourced, with the link in the Sources list.
**[M]** is a measurement recorded on the linked issue or document. **[I]** is
this study's inference from those facts.

## Summary

The maintainer asked for the queue to be "something the machines share rather
than just running on one machine", load balanced like the jobs, so that "if a
machine goes down, nothing is truly lost" [S1]. Today the queue is one Go
process with one SQLite file on one GPU host; losing that host's disk loses
every job accepted since the last stopped-state backup, and every queue release
drains that GPU host [S2][S3][S4].

Recommendation: keep today's queue logic and persistence format unchanged, and
run it as the state machine of a small embedded Raft group of three always-on,
non-GPU members. One member leads and runs the existing scheduler; the other two
hold byte-identical copies of every committed record and can take over within
an election timeout. Workers, clients, the API, the token and the TLS pin do not
change; any member answers any request and forwards writes to the leader. Blobs
and results become content-addressed objects held by at least two members. This
gives the collective property the maintainer described while keeping exact
custody and release fences, because every decision is still made in one place
and committed to a majority before anyone acts on it [I].

Why this is not v1 again: v1 made every machine a symmetric peer that
coordinated jobs through an eventually consistent registry, per-host leases and
its own discovery and tunnel protocols [S5][S6]. The proposal has one protocol
(Raft, from a library in production use for a decade), one decision maker at a
time, three fixed members, and workers that are not members at all [I].

The first step is a measurement, not code: how many bytes each check-in commits
today, because replication multiplies that volume by the member count. #1066
records about 21 MB/s of queue writes under load, which must fall before any
replication is turned on [M, S7].

## 1. Today, verified

### 1.1 Shape

- One Go process, one SQLite database, one file store directory, running on a
  GPU host that also runs production jobs. The queue host and its workers share
  one NVMe filesystem for the database, WAL, file store and worker staging
  [S3][S8].
- SQLite opens with `journal_mode=WAL`, `synchronous=FULL` and a single writer
  connection; queue state is stored as key/blob records in a `queue_state`
  table (one record per job, submission, extension and observation plus a base
  `state` record), reassembled on start by `readQueueState` and written as a
  diff of changed records by `persistState` (`internal/myce/queue.go`,
  `internal/myce/queue_persistence.go`) [S9].
- Every mutation runs inside `updateTransaction`: lock `q.mu`, mutate the
  in-memory state, compute the record diff, commit it to SQLite, publish the
  read snapshot, unlock. The commit, including `synchronous=FULL`'s fsync, is
  inside the lock. Job log chunks, usage samples, model intervals and
  enrollment tickets are written to SQLite directly by ten files
  (`AppendLog`, `persistUsage`, `persistModelIntervals`, enrollment) [S9].
- Scheduling decisions use an injected `Clock`; `myce dev` runs the real API and
  command runner with fake GPU inventory and an advance-only `devClock`
  [S10][S9].
- Workers are plain-command agents that `POST /api/workers/checkin` about once
  a second, carry attempt-specific execution identities and a per-start session,
  persist each attempt's result in their own spool (`<identity>.result.json`)
  and replay unacknowledged results and log chunks by offset after a reconnect
  [S9][S11].
- Clients and workers hold one queue address (`MYCE_QUEUE` or a joining file)
  and authenticate the queue by the SHA-256 of its self-signed certificate; the
  joining path browses mDNS for `_mycelium._tcp` and tries each candidate
  against that pin. The Go client refuses redirects; the Python client retries
  only GET/HEAD on 429/503 and documents keyed replay for a lost submit reply
  [S9][S12][S13].
- Non-test code in `internal/myce` is about 104,600 lines; the files that touch
  SQLite directly total about 6,400 lines, and ten files reference `q.db` [M].

### 1.2 What already survives loss

- Queue process restart with its disk intact: accepted jobs survive, workers
  reconnect, and a three-minute `RestartGrace` prevents declaring unreported
  workers lost immediately [S2][S9].
- A worker that loses the network keeps running its attempts, saves logs and
  results locally, starts nothing new, and replays on reconnect
  (`TestWorkerNetworkPartitionFakeClock`) [S2][S9].
- Two copies of one job after a split: the queue keeps the resumable copy with
  the newest complete checkpoint generation, or the current attempt, and stops
  the other; output writes must be safe to repeat [S2].
- Stale executions are rejected: results are attempt-specific and a superseded
  attempt cannot publish; at most one attempt's result is published [S2].
- Checkpoint generations are atomic and monotonic; NAS checkpoints are relative
  and each worker resolves them under its own mount [S2][S14].

### 1.3 What does not survive loss, and what hurts today

- Loss of the queue host's disk. The operations guide says: "Mycelium has one
  SQLite writer. Queue loss stops submissions, new starts, acknowledged
  cancellation and queue-mediated routing", and "A cold restore still requires
  positive source fencing; backup age defines possible data loss" [S4]. The
  recorded relocation plan budgeted 5 to 10 minutes of API downtime for a
  rehearsed idle move, excluding bulk copy of a 90 GiB file store [S3].
- Every queue release drains the GPU host that hosts it: a queue-only upgrade
  "still drains that host and waits for fresh exact custody" [S15]. That host
  is production-critical for one accelerator type [S3].
- Commits under the lock couple every control request to the queue host's disk.
  During the 2026-10-31 incident, I/O pressure on that host reached about 86%
  "some" and job GETs waited 7.7 s for the mutex with a 0.07 ms hold; a live
  database copy on the same disk was the likely contributor and the mechanism
  remains unproven, since a later bounded native test did not reproduce it
  [M, S8].
- Under load, a check-in held the mutex 644 ms on average, 317 ms of it
  persistence, while the process wrote about 21 MB/s; check-in HTTP latency was
  p50 9.2 s and p99 16.4 s [M, S7]. Laptop replays show the planning share can
  fall from 309/489 ms to 55/75 ms (p50/p99) with indexing, so about half of
  today's hold is disk, not planning [M, S16].

### 1.4 What v1 did and why it failed

v1 is archived at tag `v1-final`. Its spec says: "every peer is equal (no
permanent server/leader)", "Any peer can receive a request, gather a fleet
snapshot, choose placement, and ask the selected resource owner to commit", a
"replicated, eventually-consistent JOB REGISTRY + HEARTBEATS", and "the
coordinator decides, but the resource owner commits" [S5]. Its decision log
records a "local last-writer-wins table keyed by job id" for the registry,
replication that "is eventually consistent, leaderless", UDP broadcast
discovery, per-peer loopback tunnels, a libp2p overlay later stubbed out, and
heartbeat-driven recovery [S6].

The audits at the tag show where that design broke. The federation capstone
test for the owner race was "scripted ... not two concurrent commits against
real shared `node.Admission`" (F-031, WEAK); the bounded-replan proof did not
survive mutation (F-036, WEAK); "Queued and rescued jobs never deliver results:
drain/rescue load the model, grant a lease, and discard the work" (D2); and the
summary names "registry merge ordering preventing convergence" [S17][S18]. The
v3 design records the conclusion: v1 "tried to do too much at once and never
thought about how people would use or test it. It was too heavy to run in
production and too hard to test" [S2]. The v3 rule that followed is "There is no
peer network, no leader election and no replicated state" [S2], echoed in
AGENTS.md as "No peer network, leases, replicated registry, or restored v1
architecture" [S19].

The lesson this study takes from v1 is specific [I]: the failures were not
"replication is hard" in the abstract. They were (1) many authorities (every
peer coordinated), (2) eventual consistency where uniqueness was required (one
job, one current attempt), (3) three home-grown protocols (discovery, tunnels,
registry merge) and (4) tests that could not exercise real races on a laptop.
A design that avoids those four avoids v1.

Prior research for #30 reached the same staged conclusion on 2026-10-14: keep
one assigning queue first, later "three stable voting hyphae" replicate its
decisions, and "Gossip finds reachable hyphae; it cannot decide safely who
assigns work" [S20]. On 2026-10-15 the maintainer's direction was recorded as a
"peer network where every hypha holds bits and pieces, with each piece kept on
several hyphae so no single machine holds the only copy", with a possible
exception for a designated finished-output location [S21]. The 2026-11-09
direction supersedes the single-host rule; this study is the design that both
directions asked for.

## 2. Goals as failure scenarios

"Durable" below means: accepted submissions and their keys, cancellations,
attempt and release custody, published results, learned curves and policy.
"Nothing durable lost" is the target in every row. Pause figures are for the
recommended design and are inferred until measured; the recommended election
timeout is chosen from measured LAN round trips, following the Raft timing rule
that broadcast time must be an order of magnitude below the election timeout
[S22].

| Scenario | Today | Required outcome | Durable loss | Scheduling pause |
|---|---|---|---|---|
| Queue host dies mid-batch (power, disk) | Control stops. Running attempts finish on their workers and buffer results. Recovery is a manual restore from a stopped-state backup; everything after the backup is lost [S4]. | Another member becomes leader with every committed record. Running attempts keep running, report to the new leader and are accepted once. Workers that have not reported get the existing restart grace. | None. Every acknowledged write was on a majority before the reply [S23][I]. | Election timeout plus one worker check-in interval plus state load on the new leader; seconds, not minutes, once state load is measured [I]. |
| Network partition splits the fleet | The design already treats a majority of workers going quiet as a network event and does not requeue everything [S2]. With one queue, the side without the queue host has no control. | The side holding a majority of members keeps scheduling. The minority side pauses new assignments; its workers keep running, buffer, and replay on heal. Overlapping copies reconcile by the existing newest-checkpoint rule. | None. Raft is a CP protocol: the minority cannot commit [S24][S23]. | Minority: for the partition's duration. Majority: none beyond an election if the leader was on the minority side [I]. |
| Two machines both think they are primary | Impossible by construction today (one process, flock on the database) [S9]. | At most one member can commit in a term. A deposed leader cannot commit, cannot send new directives, steps down after its lease timeout, and reloads from committed records. Directives carry the term so a worker ignores a lower term than one it has acted on. | None [S23][S25][I]. | The deposed leader's lease timeout, overlapping the new election [I]. |
| A member or worker returns after days offline | A worker replays its spool; stale attempts are rejected by exact fences [S2][S9]. A restored old queue copy would be a second authority, which the operations guide forbids [S4]. | A returning worker behaves as today. A returning member catches up from the leader's log or a snapshot and rejoins as a follower; it cannot become leader until caught up. | None [S25]. | None for the fleet. Catch-up time for the member depends on snapshot size; today's database is about 859 MiB, so this must be measured [M, S3][I]. |
| The file store's host dies | Blobs live only on the queue host's disk; "One store is not a backup" [S2]. | Every blob and accepted result exists on at least two members before it counts as durably transferred. Any holder serves it. Checkpoints on the NAS are unchanged. | None for blobs at replication factor two or more; a blob with one holder is reported as such, never as durable [I]. | None; placement needing a blob fetches from a surviving holder [I]. |
| The NAS is unreachable | Checkpoint-dependent work waits; running work continues; status explains it [S2]. | Unchanged. The queue's own state never lives on the NAS, because SQLite WAL "does not work over a network filesystem" [S26]. | None. | None for non-NAS work, as today. |
| Majority of members down (shared power or switch) | Equivalent to queue loss today. | Scheduling pauses with an explained reason; no member accepts writes it cannot commit. Workers keep running. Recovery is automatic when a majority returns. | None [S23]. | The outage's duration. Not worse than today; the design does not promise progress without a majority [I]. |

## 3. Candidate architectures

Common to all candidates: workers stay plain-command agents, the HTTP API and
token stay, and the job model, fences and scheduling policy stay. The question
is only where the authoritative state lives and how a replacement authority is
chosen.

### 3.1 (a) One active queue, replicated durable state, fenced takeover

**a1. Asynchronous replication (Litestream, sqlite3_rsync) plus a fenced promotion.**
Litestream "continuously copies write-ahead log pages from disk to a replica";
it is asynchronous and "trades off durability of recent writes"; "Synchronous
replication is on the Litestream roadmap but has not yet been implemented"; and
"Multiple applications replicating into the same bucket & path can cause
situations where you will be unable to restore" [S27][S28]. SQLite's own
`sqlite3_rsync` makes a consistent point-in-time replica while both databases
are live, one direction only, and since 3.50.0 no longer requires WAL mode on
both sides [S29]. Either gives recovery with a loss window equal to the
replication lag, and neither decides who may be primary: promotion still needs
an external fence, and the operations guide's rule stands that "Never serve two
queues with the same identity" [S4]. Honest description: disaster recovery, not
the collective the maintainer asked for [I].

**a2. rqlite.** Raft around SQLite, but "you must only write to the database via
the HTTP API", "the behavior of rqlite is undefined if you send explicit BEGIN,
COMMIT, or ROLLBACK statements", and it is "a standalone application", not a
library [S30]. Default "Weak" reads "are not Linearizable" [S31]. Adopting it
means a second always-on service per member and rewriting the ten persistence
call sites to HTTP SQL, and it ends the one-binary rule [I].

**a3. dqlite.** "a C library that implements an embeddable and replicated SQL
database engine"; "dqlite runs on Linux and requires a kernel with support for
native async I/O"; Go bindings need the C library installed [S32][S33]. The
development host is a Mac and every feature must be laptop-testable, so dqlite
fails a hard rule before any other comparison [S19][S34][I].

**a4. LiteFS.** A FUSE filesystem that replicates SQLite pages, chooses a primary
through Consul leases, "does not confirm that writes are committed to other
nodes before continuing to the next transaction", resets a diverged node by
snapshot, and is "outside our scope of support ... Use with caution", with a
documented stale-primary data-loss mode [S35][S36]. Rejected [I].

### 3.2 (b) An embedded Raft group running the queue as its state machine

Raft gives one leader per term, a replicated ordered log, and the Leader
Completeness guarantee that a committed entry is present in every later
leader's log [S23]. Three voters tolerate one failure; five tolerate two
[S37][S38]. Nomad and Consul separate voting servers from clients: "Only Nomad
server nodes participate in Raft" and "All client nodes forward requests to
servers" [S37][S38]. NATS JetStream applies the same shape to a message store
with "R3, the production floor" [S39].

Two Go libraries are production-grade. `hashicorp/raft` (v1.8.0, 2026-09-16,
MPL-2.0) supplies the FSM interface, snapshots, a TCP transport, an in-memory
transport for tests with `Disconnect` to simulate partitions, non-voting members
(`AddNonvoter`), leadership transfer and pre-vote [S40][S41]. `etcd-io/raft`
(Apache-2.0) "only implement[s] the core raft algorithm"; "both network and
disk IO are left to the user"; it is used by etcd, CockroachDB and TiKV and
offers pre-vote, CheckQuorum-style step-down and ReadIndex reads [S42].

The design choice inside (b) is **what to replicate**.

**b1. Replicate requests (check-ins, submits) and re-execute the queue logic on
every member.** Raft requires that "Apply must be deterministic and produce the
same result on all peers" [S41]. Today's transaction reads the NAS probe, the
filesystem, learned curves and the clock inside the mutation; making all of that
deterministic across members is a rewrite of the queue's decision code and
reintroduces v1's "every peer computes" shape [I]. Rejected.

**b2. Replicate committed record write-sets.** The leader runs today's
`updateTransaction` exactly as now, up to the point where `persistState`
produces the map of changed records (`key -> bytes`, with deletions). That map
is the Raft log entry. `FSM.Apply` on every member writes those bytes into its
local `queue_state` table, which is trivially deterministic. Followers never
decode records; a member that becomes leader loads state with today's
`readQueueState`, exactly as `Open` does after a restart [S9][I]. This keeps
the persistence format, the recovery path and the whole decision code, and it
makes every follower a byte-identical copy of the leader's committed database.
This is the recommended candidate; section 4 details it.

### 3.3 (c) The "collective": gossip membership, CRDT state, deterministic scheduling

The attraction is that every member accepts updates and the system converges
without a leader. Shapiro et al. define Strong Eventual Consistency so that
"an update does not require synchronisation, and CRDT replicas provably
converge", and "SEC does not require to solve consensus" [S43]. memberlist
provides gossip membership that "is eventually consistent but converges quickly
on average" [S44].

Where it breaks for Mycelium is exactly the fences the project depends on [I].
A CRDT can hold the set of submitted jobs and each attempt's log, but it cannot
decide that exactly one attempt of a job is current, that exactly one result is
published, that a release obligation has been discharged once, or that a
production retry budget was spent once. Those are uniqueness and mutual
exclusion decisions. The CRDT paper is explicit that "conflict arbitration may
require a consensus and a roll-back" and that "sequential consistency requires
consensus" [S43]. Jepsen's etcd analysis draws the general line: "lock services
must sacrifice correctness in order to preserve liveness in asynchronous
systems", and the safe pattern is a fencing token that the resource itself
checks [S45]. Under a partition, a CRDT queue on both sides would assign the
same job twice and publish two results; the existing late-return rule tolerates
duplicate attempts for repeat-safe resumable jobs, but production's
at-most-one-published-result and exact-release contracts do not tolerate two
authorities [S2][I]. v1's eventually consistent LWW registry was this design,
and its audits record non-convergence and work that was never re-executed
[S6][S17][S18].

What (c) gets right is kept in the recommendation [I]: any member can serve
reads and data; blobs are content-addressed so any holder is as good as any
other; and each host remains the authority on its own physical resources
through its reports, as today. Only the decision log needs a single voice.

### 3.4 Comparison

Ratings are this study's judgments from the cited mechanisms [I], except where
a cell cites a measurement.

| Criterion | a1 async copy + fence | a2 rqlite | a3 dqlite | b2 embedded Raft, record write-sets | c gossip/CRDT |
|---|---|---|---|---|---|
| Stability of the authority | Manual or external fence; split-brain is an operator error away [S4][S28] | Raft; mature | Raft; mature | Raft; mature library; one decision maker per term [S23][S41] | No single authority; uniqueness unprovable without consensus [S43] |
| Operational simplicity | Low: one extra process, backups, runbooks | Medium: extra service per member, HTTP SQL | Fails: Linux-only, cgo [S32] | Medium: three members of the same binary, one config flag each | High apparent, low real: conflict handling leaks into every feature |
| Laptop testability | Yes | Partly (needs rqlited) | No (Mac dev host) [S34] | Yes: N members in one process on `InmemTransport`, partitions by `Disconnect` [S41] | Yes for convergence; no for exactness |
| Failover time | Minutes; human in the loop [S3] | Seconds (election) | Seconds | Seconds: election timeout plus reconnection [S22][I] | None needed for reads; undefined for custody |
| Data-loss window | Replication lag; "loss of data in that time window" [S28] | None for committed writes | None | None for committed writes [S23] | None for CRDT state; duplicates instead of loss |
| Write throughput and scheduling latency | As today, plus copy traffic | Each write is a majority round trip | Same | Each commit is a majority round trip and follower fsync, taken outside `q.mu`; batches amortize [I] | Local writes; conflicts later |
| Code that survives | All | All but persistence (10 files) and the one-binary rule | All but persistence; platform rule broken | All; persistence seam (about 6,400 lines touched lightly), new replication package, forwarding, dev members [M][I] | Planner and custody rewritten as replicated deterministic functions |
| Delivers "state lives in the collective" | No: one primary and a copy | Yes | Yes | Yes | Yes, without exactness |

## 4. Recommendation: one voice, many memories

### 4.1 Roles and membership

- A **member** is a `myce queue` process that holds a full copy of committed
  state. Three members are **voters**; they run on always-on, non-GPU machines
  that never sleep and do not drain for releases. Workers are not members and
  need no change [S37][I].
- Optional **non-voting members** (`AddNonvoter`) can run anywhere a local read
  or blob cache is useful; they receive the log but do not vote or count toward
  commitment [S41]. GPU hosts are not voters because they restart with every
  release and drain for production; a voter that is absent half the time halves
  the group's margin [I].
- Membership changes are Raft configuration changes (`AddVoter`, `RemoveServer`,
  `DemoteVoter`), one at a time, driven by `myce queue join|leave` [S41][S23].
  No discovery list is ever treated as membership; mDNS only supplies addresses,
  as today [S9].
- Five voters are supported but not recommended for a fleet this size; three
  tolerate the one simultaneous loss the fleet actually sees [S38][I].

### 4.2 Write path

Today's transaction stays intact up to its record diff. The change is where
the commit happens and when the reply is sent.

1. Lock `q.mu`; run today's mutation; produce the record write-set and the
   read snapshot candidate. Unlock. No network or disk I/O is added under the
   lock, which keeps the #80 rule intact and removes the fsync that is under it
   today [S19][S9][I].
2. Propose the write-set to Raft. The leader appends locally and replicates to
   followers; commitment needs a majority's durable append [S23]. Proposals
   from one leader are a FIFO sequence, so write-sets apply in the same order
   the mutex produced them [I].
3. `Apply` runs on every member and writes the bytes into `queue_state`. On the
   leader, the transaction's reply is released and the read snapshot is
   published only now. No worker directive, no accepted-submission reply and no
   file publication precedes commitment [I].
4. The leader's in-memory state is optimistic: transaction N+1 may run under
   the lock before N has committed. If any commit fails (lost leadership, disk
   failure), the member treats it as a local restart: it discards in-memory
   state, reloads from its committed records with today's `Open` path, and
   every in-flight request receives a retryable 503, as a queue restart does
   today [S9][I]. Clients already handle this: the Python client retries
   GET/HEAD with Retry-After and the skill prescribes keyed replay for a lost
   submit reply [S13][S46].

This is the Raft paper's client contract: "clients ... assign unique serial
numbers to every command", the state machine deduplicates [S23]. Mycelium's
`submission_key` already is that serial number [S2].

Latency: today a check-in pays one local fsync under the lock. Afterwards it
pays one LAN round trip plus the slower of two follower fsyncs, outside the
lock. On macOS, Go's `File.Sync` issues `F_FULLFSYNC` because "SYS_FSYNC doesn't
fully flush contents to disk", so follower fsync cost on Mac members must be
measured before election and batching parameters are chosen [S47][I].

### 4.3 Leadership change is a queue restart

A new leader loads state from its committed records exactly as `Open` does
today, including `RestartGrace` for workers that have not yet reported and the
"queue restarted; occupancy unknown" marking of live timings [S9]. Nothing new
is invented for recovery; the existing restart tests become the leader-change
tests [I].

Fencing [I]: a directive is sent only after its write-set committed, so a
deposed leader, which cannot commit, cannot assign or stop anything. It steps
down after its lease timeout ("If we reach this interval without contact, we
will step down as leader") [S41]. As cheap hardening, every reply carries the
leader's term and each worker remembers the highest term it has acted on and
ignores directives from a lower one; this is the fencing-token pattern Jepsen
recommends for any resource acted on after a lease [S45].

### 4.4 Any member answers

- **Writes**: a follower forwards `POST` requests to the leader over the
  members' authenticated transport and returns the leader's reply. This is how
  Nomad and rqlite behave ("the request is forwarded to the leader") and it
  avoids HTTP redirects, which the Go client refuses [S37][S30][S9].
- **Reads**: a follower serves `GET /api/status` and `GET /api/jobs/{id}` from
  its own committed records with a freshness field, exactly the point-in-time
  snapshot contract #1052 already adopted for the single queue [S48]. A read
  that must be linearizable (custody checks inside `host status --wait`) is
  forwarded to the leader, which verifies leadership first (`VerifyLeader`)
  [S41][I].
- **Worker check-ins**: the hot path. Workers may talk to any member; a
  follower forwards the check-in and relays the directive. Spreading the
  TLS, JSON and blob traffic across members is the "load balance the queue"
  part of the request; the decision itself stays with the leader [I].
- **Model calls** (`/models/{name}/...`) are proxied by whichever member
  receives them to the engine the leader placed; in-flight calls still fail on
  leader change and callers retry, as the design already states [S2][I].

### 4.5 Identity, discovery and trust

Every member holds the same joining identity (certificate and key), so the
fingerprint every worker and client already pins stays valid, and `myce invite`
keeps working from any member [S9]. Every member advertises `_mycelium._tcp`
with that fingerprint; `ResolveJoining` already browses all candidates and
accepts the first that authenticates, so a worker finds a live member with no
change [S9]. Explicit `MYCE_QUEUE` users keep pointing at one member and are
served by forwarding; a later invitation version can carry several addresses,
which is the only consumer-visible change and the only skill change this design
needs [I]. The fleet token stays a bearer credential verified by every member;
members never pass it to jobs, as today [S19].

### 4.6 What is replicated, and what is not

| Data | Today | Proposed | Why |
|---|---|---|---|
| `queue_state` records (jobs, submissions, custody, policy, curves, health) | SQLite on one host | Raft log entries, applied to every member's `queue_state` | This is the durable intent; nothing here may be lost [I] |
| Enrollment tickets, release approvals | SQLite | Same log | Authority state [I] |
| Job log chunks (`logs`, `log_offsets`) | SQLite under `q.mu` | A lazily replicated stream between members, outside the log; the worker keeps the attempt's log file and replays from the highest offset the new leader reports, using the existing idempotent `log_offsets` key | Bulk; the worker is already the durable source and replays by offset [S9][I] |
| Usage samples, model intervals, efficiency telemetry | SQLite | Lazily replicated; loss of the last seconds on failover is accepted and visible | Telemetry, not intent; MY-31 learning tolerates gaps [I] |
| Blobs, results, file-store checkpoint generations | One directory | Content-addressed objects with a replicated holder list and factor R (section 5) | Bulk; replicated by pull outside any lock [I] |
| NAS checkpoints and models | NAS | Unchanged | Already shared storage [S2] |

### 4.7 Why this is not v1

| v1 property that failed | This proposal |
|---|---|
| Every peer coordinates; "no permanent server/leader" [S5] | Exactly one leader per term makes every decision; followers store bytes [S23] |
| Eventually consistent LWW job registry; "registry merge ordering preventing convergence" [S6][S18] | A single ordered log; every member has byte-identical committed state [S23] |
| Per-host lease store, coordinator/owner split, fence races proven only by scripted tests [S17] | Custody stays the existing attempt and release fences, decided in one place; the race tests are kill and partition tests on an in-process cluster [S41] |
| Home-grown UDP discovery, loopback tunnels, libp2p overlay [S6] | One library protocol over the existing authenticated TLS; mDNS for addresses only, as today [S9] |
| Rescue paths that re-ran nothing [S18] | No rescue path: a running attempt reports to whichever member answers and the leader applies today's reconciliation [I] |
| Symmetric daemons on every machine, including GPU hosts | Three small always-on members; GPU hosts stay plain workers [I] |

### 4.8 The biological property

"Any member can serve": reads, uploads, blob fetches and check-ins land on any
member. "State lives in the collective": every committed record exists on a
majority before anyone acts on it, and every blob on at least two holders. "A
machine going down loses nothing": a voter's loss costs an election; a worker's
loss costs what it costs today (requeue or resume); a holder's loss costs
nothing while another holder exists. "Information is part of the collective":
no machine, including the former queue host, is special. What stays singular
is the *voice*, one leader per term, because exactness has no other known
construction in an asynchronous network [S43][S45][I].

### 4.9 Which machines hold state

Voters: three always-on small machines that do not sleep, drain or host GPU
production, wired if possible. The recorded relocation survey found such
machines on Wi-Fi and asked for a verified wired route; that still applies, and
Raft's timing rule makes jitter a liveness concern, not a safety one [S3][S22].
GPU hosts hold no queue state. The NAS holds no queue state (WAL rule) [S26]
and is one optional blob holder among several. The development host may run a
voter for dev and gates (MY-37) but a production voter there competes with
orchestration load; measure before choosing [S34][S3][I].

## 5. File store

Today a blob is `<root>/blobs/<sha256>`, published by hard link from a verified
tus upload, with references and retired-file sets tracked in queue state and
collected asynchronously [S9][S14]. Content addressing already makes a blob
location-independent; what is missing is a second copy and a record of who has
it [I].

Proposed [I]:

- **Holder records.** A replicated record `blob_holders/<sha256>` lists the
  members that have verified the blob. The member that completes an upload
  publishes locally, verifies, then proposes itself as the first holder.
- **Replication factor R**, default `min(2, members)`, configurable per
  project tier. Other members pull the blob over the existing resumable blob
  GET route, verify the SHA-256, and propose themselves as holders. Pulls are
  background work with measured concurrency and headroom (MY-34), never under
  `q.mu` or the file-store lifecycle lock [S19].
- **Durability semantics.** An input blob is usable at one holder, as today;
  status shows `holders: 1/2` until replicated. A *result* is "accepted" only
  when R holders have verified it; the existing verified-transfer step gains
  one condition. A blob with fewer than R holders after its grace is a health
  alert naming the shortfall, not a silent risk.
- **Serving.** Workers fetch inputs from any holder listed in their directive;
  the leader lists holders nearest the worker first when telemetry knows.
- **Garbage collection.** Today's reference tracking stays authoritative;
  deletion is proposed as a record and each holder removes its copy on apply.
- **The NAS** can be configured as a holder for finished outputs, meeting the
  maintainer's 2026-10-15 note about a designated finished-output location for
  provenance [S21]; it is never the only holder of anything the queue needs.
- **Checkpoints.** NAS checkpoints are unchanged. File-store checkpoint
  generations are blobs and get the same R copies before the generation counts
  as portable.

Nothing bulk moves inside the queue lock; that rule came from #80 and stays
[S19].

## 6. Migration plan

Each step is independently testable on a laptop, reversible, and leaves clients
and the skill unchanged unless the step says otherwise. Steps 1 to 3 are
valuable on their own even if the maintainer stops there.

**Step 0. Measure (no product code).**
Record, on the retained-history fixture already used by #661 (6,500 executions,
110 queued jobs, 1,400 curves): bytes committed per check-in and per submit,
commits per second under the fan-out replay, state-load time of `Open` for a
database of today's size, and `F_FULLFSYNC` latency on each candidate member's
disk [S16][S47]. Acceptance: numbers posted on #1066 and this study's issue.
Rollback: none.

**Step 1. One persistence seam.**
Introduce a `recordStore` interface with `Append(writeSet) (index, error)` and
`Load()`, implemented by today's SQLite code; route the ten direct `q.db`
users (`AppendLog`, usage, intervals, tickets) through typed auxiliary record
kinds so exactly one place touches SQLite. Acceptance: the full gate is
unchanged; a round-trip test shows `readQueueState(persistState(x)) == x` for
the fixture. Consumers: no change. Rollback: revert; the on-disk format is
unchanged.

**Step 2. Commit outside the lock, reply after commit.**
`updateTransaction` unlocks after computing the write-set; the SQLite commit
runs outside; the reply and read-snapshot publication wait for the commit; a
failed commit triggers the local-restart path (section 4.2). Acceptance:
existing restart and crash tests pass; a new fake-clock test injects a commit
failure and proves (i) the reply never preceded the commit, (ii) state after
reload equals the committed records, (iii) in-flight requests got retryable
503s. Expected benefit before any replication: the 317 ms persistence share of
the 644 ms check-in hold leaves the lock [M, S7]. Consumers: no change.
Rollback: a one-release flag selects the in-lock commit; both paths write
identical records.

**Step 3. A one-voter Raft group.**
The `recordStore` becomes a Raft log of write-sets with a single voter (self),
`raft-boltdb` as the log store, snapshots taken from the `queue_state` table
(raft-wal "is still considered experimental" and is not chosen) [S49][S41].
Acceptance: `myce dev` and the dev restart probe pass unchanged; `kill -9`
during a fan-out loses no accepted job; commit latency is within the step-0
noise of the SQLite baseline. Consumers: no change. Rollback: flag back to
step 2; both read the same `queue_state` table.

**Step 4. Followers, forwarding, shared identity, dev members.**
`myce queue join` adds a non-voter; followers serve reads with freshness and
forward writes; every member advertises mDNS with the shared identity;
`myce dev --members 3` runs three members in one process on `InmemTransport`
under the shared dev clock, with dev-only endpoints to kill, restart,
partition and heal members. Acceptance: submit through a follower and the job
runs; kill a follower and nothing changes; kill the single voter and scheduling
pauses then resumes on its return with every job present (the pause is expected
at this step). Consumers: no change; `MYCE_QUEUE` may point at any member.
Rollback: `RemoveServer` on the follower; the single voter continues.

**Step 5. Three voters.**
Promote two followers; enable pre-vote and lease-based step-down; add the term
to directives; add the capable-leader rule (a member whose binary cannot decode
the committed schema transfers leadership away instead of leading) [S41].
Acceptance on the dev cluster, fake clock and in-memory partitions:
(i) kill the leader mid-batch: a new leader within the configured election
timeout, every accepted job present, running attempts complete and are accepted
once; (ii) partition a minority holding a worker: no new assignment on the
minority, result replay and the newest-checkpoint rule hold after heal;
(iii) isolate the old leader while it keeps answering a worker: no directive
from the stale term is acted on; (iv) a member returns after 30 simulated days:
catch-up by snapshot, and its worker's stale attempts are rejected by the
existing fences; (v) add and remove a voter under load with no lost submission;
(vi) a history checker over the dev cluster's job-state transitions finds them
linearizable (a Go checker such as Porcupine is a candidate tool; choice open).
Consumers: no change. Rollback: `DemoteVoter` the two followers and continue
as step 4; the leader's local records are the single-node database.

**Step 6. Blob holders and replication factor.**
Section 5. Acceptance on the dev cluster: upload through member A, kill A, a
worker fetches the input from B; a result upload interrupted by a leader change
resumes (tus) and the accepted result shows two holders; a deleted reference
removes the blob from every holder; `holders: 1/2` is reported, never
"durable". Consumers: a new status field; the skill gains one sentence.
Rollback: R=1 restores today's semantics with holder records still kept.

**Step 7. Fleet rollout.**
Run voters on three always-on members. The GPU host stops hosting the queue;
its queue-only releases become a `LeadershipTransfer` followed by one member's
restart, so a queue release no longer drains a production host (MY-35) [S41].
Acceptance: the #316 criterion, control-request latency recorded before and
after under a comparable fan-out, plus one hour with no check-in flap [S3][S16].
Rollback: stop two members, run the remaining one as a one-voter group (step 3
mode) or in plain-SQLite mode (step 2); its records are complete.

**Consumer compatibility, summarized.** API routes, job model, submission keys,
error codes, the token and the TLS pin are unchanged through step 7. Leader
changes surface as retryable 503s that clients already handle [S13][S46]. The
only additions are a `holders` status field and, later, an optional
multi-address invitation; the skill changes only for those [I].

## 7. Risks, unknowns, what to measure first

1. **Write volume.** #1066 measured about 21 MB/s of queue writes under load
   and attributes it to re-serialization; replication multiplies committed bytes
   by the voter count and adds follower fsyncs [M, S7]. Measure bytes per
   commit first (step 0) and land #1066 before step 3. Without that, the design
   would ship today's write amplification to three disks.
2. **Follower fsync on macOS.** `F_FULLFSYNC` is slower than `fsync` by design
   [S47]. If measured latency is tens of milliseconds, commits still clear the
   lock but reply latency rises; batching proposals per leader tick is the
   standard answer and must be measured, not assumed.
3. **Election timing on home networks.** Raft needs `broadcastTime <<
   electionTimeout << MTBF` [S22]. Members on Wi-Fi add jitter; pre-vote and
   lease step-down limit churn [S41][S42]. Derive timeouts from measured RTT
   and fsync distributions per MY-34, never from library defaults copied in.
4. **Sleeping voters.** A sleeping Mac is a failed voter. Polite part-time
   participation rules apply to workers, not members; voters must be configured
   never to sleep, and that is an operator fact to verify, not a code change.
5. **Snapshot size and catch-up.** The database is about 859 MiB; a returning
   member may need a full snapshot [M, S3]. Retention already prunes history;
   measure snapshot size and transfer time on the LAN before step 5.
6. **Mixed versions.** Followers store bytes, so a newer leader's records are
   safe on older followers; an older binary that became leader would fail
   closed on an unknown schema marker, as today's "fails closed in older
   binaries" comment intends [S9]. The capable-leader rule (step 5) and an
   upgrade order of followers first, leader last with leadership transfer,
   cover it; test it on the dev cluster.
7. **The injected-clock rule meets library timers.** Raft election and
   heartbeat timers inside the library use wall time; dev tests will run them
   with short real timeouts while every scheduling decision still uses the
   injected `Clock`. This is a bounded exception that the study names rather
   than hides; it should be recorded as such if accepted [S19].
8. **Side effects after commit.** File publication, NAS delivery tasks and
   engine commands must run only after the write-set that authorizes them has
   committed. The existing `committed` callbacks are the seam; an audit of
   every post-transaction side effect belongs to step 2.
9. **Minority-side production.** A production start window can expire while
   its worker is on the minority side; this is today's behaviour during a queue
   outage and is not worsened, but it should be stated in status.
10. **Planning cost stays on the leader.** Replication removes disk time from
    the lock, not planning time (283 ms per check-in in #1066); #661 and #1031
    continue independently [M, S7][S16].
11. **Library choice.** `hashicorp/raft` brings transport, snapshots and the
    in-memory test transport; `etcd-io/raft` brings more control at the cost of
    writing transport and storage [S40][S42]. Start with `hashicorp/raft`;
    switching later is a contained change behind the `recordStore` seam.
12. **Licensing.** `hashicorp/raft` and `memberlist` are MPL-2.0, `etcd-io/raft`
    Apache-2.0 [S40][S42][S44]. The repository currently has no LICENSE file
    [M]; choosing one is needed for the public release regardless of this
    design.

Measure first, in order: bytes per commit and commits per second (step 0);
`F_FULLFSYNC` latency on candidate members; member-to-member RTT and loss over
a day, wired and wireless; `Open` state-load time; candidate members' sleep and
power behaviour.

## 8. Proposed amendments (for the maintainer's approval)

These are proposals; neither file is changed by this study.

- **Design proposal, "The parts" and the single-host sentence.** Replace "One
  always-on machine" with "A small group of always-on members (three
  recommended), one leading at a time" and replace "There is no peer network, no
  leader election and no replicated state" with "Queue members replicate
  committed state through one consensus log; there is no gossip registry, no
  per-host lease store and no coordination by workers." Add the failure table of
  section 2 to "Power and network loss are normal, not rare".
- **AGENTS.md.** Replace "One binary, one SQLite queue, plain-command workers.
  No peer network, leases, replicated registry, or restored v1 architecture"
  with "One binary. The queue is a consensus group of one to five always-on
  members, each with its own local SQLite; plain-command workers are not
  members. No gossip registry, per-host leases or v1 peer coordination." Keep
  the injected-clock rule, the laptop rule, the no-bulk-I/O-under-`q.mu` rule
  and the token rule verbatim; add "Replication and commit happen outside
  `q.mu`; no reply or directive precedes commitment."
- **Decisions.** Record the maintainer's 2026-11-09 direction verbatim as a
  ruling that supersedes the single-queue scope of MY-03's "one v3 runtime"
  only where it concerns topology, and note that #31's revisit trigger has been
  met by the maintainer's resumption.
- **Skill.** No change until the multi-address invitation or the `holders`
  field ships; then one sentence each.

## 9. Sources

Internal facts were read on 2026-11-09 from the repository at the base of this
branch and from public issues; external pages were fetched on 2026-11-09 and
their stated last-modified dates are given where the page supplies one.

- S1. The maintainer's direction, 2026-11-09, as relayed in this study's brief:
  "i want the queue to be something the machines share rather than just running
  on one machine ... if a machine goes down, nothing is truly lost."
- S2. [Approved design](../design-proposal.md): "Why a redesign", "The parts",
  "Machines leaving", "Power and network loss are normal, not rare",
  "Production infrastructure retries", "Files with or without a NAS".
- S3. Queue relocation plan, 2026-10-29, retained in history:
  [docs/plan-queue-relocation.md at 12bb6b7^](https://github.com/1kuna/mycelium/blob/12bb6b7826153c38d356555b711c9ba4a11f856a%5E/docs/plan-queue-relocation.md)
  (downtime estimate, file-store and database sizes, Wi-Fi members, restart
  grace); consolidated into #527 via
  [#316](https://github.com/1kuna/mycelium/issues/316).
- S4. [Operations: queue backup, restore and relocation](../operations.md#queue-backup-restore-and-relocation).
- S5. v1 project spec at tag `v1-final`:
  [01-project-spec.md](https://github.com/1kuna/mycelium/blob/v1-final/01-project-spec.md)
  and [README.md](https://github.com/1kuna/mycelium/blob/v1-final/README.md).
- S6. v1 decision log at tag `v1-final`:
  [DECISIONS.md](https://github.com/1kuna/mycelium/blob/v1-final/DECISIONS.md).
- S7. [#1066 Check-in persistence dominates queue mutex time](https://github.com/1kuna/mycelium/issues/1066),
  live measurement 2026-11-08.
- S8. [#527 Disk I/O from jobs on the queue host stalls queue commits](https://github.com/1kuna/mycelium/issues/527)
  and [operations: queue disk contention](../operations.md#queue-disk-contention-527).
- S9. Repository source at the branch base: `internal/myce/queue.go` (`Open`,
  `updateTransaction`, `AppendLog`, `RestartGrace`),
  `internal/myce/queue_persistence.go` (`readQueueState`, `persistState`),
  `internal/myce/usage.go` (`persistUsage`), `internal/myce/joining.go`
  (`ResolveJoining`, `JoiningIdentity`), `internal/myce/discovery.go`,
  `internal/myce/files.go` (`publish`, `blob`), `internal/myce/worker.go`
  (`replay`), `internal/myce/dev.go`, `internal/myce/dev_clock.go`,
  `internal/myce/network_test.go` (`TestWorkerNetworkPartitionFakeClock`),
  `internal/myce/http.go` (routes, `Client.Call`).
- S10. [AGENTS.md](../../AGENTS.md), clock and dev rules.
- S11. [docs/development.md](../development.md), current worker protocol and
  keyed attempt wire revision.
- S12. `clients/python/src/mycelium/installed.py` and `client.py` (pinned
  handler, `MYCE_QUEUE`, retry policy).
- S13. [skills/mycelium/SKILL.md](../../skills/mycelium/SKILL.md), "Observe and
  recover" and keyed replay guidance.
- S14. [docs/files.md](../files.md), resumable jobs and checkpoint generations.
- S15. [Operations: rolling fleet upgrades](../operations.md#rolling-fleet-upgrades),
  queue-only upgrade behaviour.
- S16. [#661 Workers on several hosts flap stale under queue load](https://github.com/1kuna/mycelium/issues/661),
  synthetic replay measurements; [#706 postmortem](https://github.com/1kuna/mycelium/issues/706).
- S17. v1 verification findings at `v1-final`:
  [FINDINGS.md](https://github.com/1kuna/mycelium/blob/v1-final/FINDINGS.md),
  rows F-031 and F-036.
- S18. v1 conformance audit at `v1-final`:
  [FABLE-FINDINGS.md](https://github.com/1kuna/mycelium/blob/v1-final/FABLE-FINDINGS.md),
  top-line and D2.
- S19. [AGENTS.md](../../AGENTS.md): "One binary, one SQLite queue ...", "No
  network, remote-storage or bulk I/O ... inside `q.mu`", token rule, laptop
  rule.
- S20. [#30 research, surviving lost hyphae](https://github.com/1kuna/mycelium/blob/research/surviving-lost-hyphae/docs/research/surviving-lost-hyphae.md),
  2026-10-14, and the [#30 resolution](https://github.com/1kuna/mycelium/issues/30).
- S21. [#31 Where coordination lives](https://github.com/1kuna/mycelium/issues/31),
  comment of 2026-10-15 recording the maintainer's direction, and the
  2026-11-01 deferral.
- S22. Ongaro and Ousterhout, [In Search of an Understandable Consensus
  Algorithm (extended)](https://raft.github.io/raft.pdf), 2014, §5.6: "broadcastTime
  ≪ electionTimeout ≪ MTBF", broadcast time "0.5ms to 20ms", election timeout
  "likely to be somewhere between 10ms and 500ms", "When the leader crashes,
  the system will be unavailable for roughly the election timeout".
- S23. Same paper: §1 "fully functional (available) as long as any majority of
  the servers are operational"; Figure 3 "Leader Completeness"; §5.2
  randomized election timeouts "(e.g., 150–300ms)"; §6 joint consensus; §8
  "clients ... assign unique serial numbers to every command".
- S24. [rqlite FAQ](https://rqlite.io/docs/faq/), last modified 2026-11-04:
  "Raft is a Consistency/Partition-tolerance (CP) protocol."
- S25. hashicorp/raft `Config.LeaderLeaseTimeout`, `AddNonvoter`, snapshot and
  catch-up documentation, [pkg.go.dev](https://pkg.go.dev/github.com/hashicorp/raft),
  v1.8.0.
- S26. [SQLite write-ahead logging](https://sqlite.org/wal.html): "All
  processes using a database must be on the same host computer; WAL does not
  work over a network filesystem."
- S27. [Litestream: how it works](https://litestream.io/how-it-works/).
- S28. [Litestream tips](https://litestream.io/tips/): asynchronous
  replication, "loss of data in that time window", "Synchronous replication is
  on the Litestream roadmap but has not yet been implemented", multiple
  applications warning.
- S29. [sqlite3_rsync](https://sqlite.org/rsync.html).
- S30. [rqlite FAQ](https://rqlite.io/docs/faq/): HTTP API only, explicit
  transactions undefined, standalone application, write forwarding.
- S31. [rqlite read consistency](https://rqlite.io/docs/api/read-consistency/),
  last modified 2026-11-04.
- S32. [canonical/dqlite README](https://github.com/canonical/dqlite): C library,
  "runs on Linux and requires a kernel with support for native async I/O".
- S33. [canonical/go-dqlite README](https://github.com/canonical/go-dqlite):
  requires the dqlite C library installed.
- S34. MY-37, the maintainer's 2026-11-09 instruction that development must not
  depend on fleet availability and should run on the development Mac, recorded
  on [#1086](https://github.com/1kuna/mycelium/issues/1086).
- S35. [LiteFS: how it works](https://docs.fly.io/litefs/how-it-works):
  Consul leases, asynchronous replication, snapshot reset of a diverged node.
- S36. [LiteFS overview](https://docs.fly.io/litefs/): "outside our scope of
  support", pre-1.0, stale-lease data-loss warning.
- S37. [Nomad consensus protocol](https://developer.hashicorp.com/nomad/docs/architecture/cluster/consensus):
  "Only Nomad server nodes participate in Raft", "All client nodes forward
  requests to servers", deployment table, "3 or 5 Nomad servers".
- S38. [Consul consensus](https://developer.hashicorp.com/consul/docs/architecture/consensus):
  quorum arithmetic, "We recommend either 3 or 5 servers".
- S39. [NATS JetStream clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering):
  odd counts, R3 "the production floor", "An R1 stream on a cluster still has
  no HA".
- S40. [hashicorp/raft README](https://github.com/hashicorp/raft): FSM,
  snapshots, LogStore/StableStore, non-voting servers, MPL-2.0.
- S41. [hashicorp/raft package documentation](https://pkg.go.dev/github.com/hashicorp/raft),
  v1.8.0 (2026-09-16): `FSM.Apply` "must be deterministic", `InmemTransport`
  `Connect`/`Disconnect`/`DisconnectAll`, `AddVoter`/`AddNonvoter`/
  `RemoveServer`/`DemoteVoter`, `LeadershipTransfer`, `VerifyLeader`,
  `Barrier`, `LeaderCh`, `PreVoteDisabled`, `Restore` warnings,
  `SnapshotThreshold`/`TrailingLogs`.
- S42. [etcd-io/raft README](https://github.com/etcd-io/raft): minimal core,
  user-supplied transport and storage, users, ReadIndex and lease reads,
  step-down on lost quorum, Apache-2.0.
- S43. Shapiro, Preguiça, Baquero, Zawirski, [Conflict-free Replicated Data
  Types](https://inria.hal.science/inria-00609399v2), INRIA RR-7687, 2011:
  "conflict arbitration may require a consensus and a roll-back", Definition
  2.3 (SEC), "SEC does not require to solve consensus", "sequential consistency
  requires consensus".
- S44. [hashicorp/memberlist README](https://github.com/hashicorp/memberlist):
  SWIM-based, "eventually consistent but converges quickly on average", MPL-2.0.
- S45. [Jepsen: etcd 3.4.3](https://jepsen.io/analyses/etcd-3.4.3): "lock
  services must sacrifice correctness in order to preserve liveness in
  asynchronous systems"; fencing tokens.
- S46. [skills/mycelium/SKILL.md](../../skills/mycelium/SKILL.md): "a lost reply
  ... calls for the original command/key and receipt"; Retry-After handling.
- S47. Go source [internal/poll/fd_fsync_darwin.go](https://github.com/golang/go/blob/master/src/internal/poll/fd_fsync_darwin.go):
  "on OS X, SYS_FSYNC doesn't fully flush contents to disk", `F_FULLFSYNC`.
- S48. [#1052 Serve status and job reads from a cheap published snapshot](https://github.com/1kuna/mycelium/issues/1052),
  point-in-time read contract.
- S49. [hashicorp/raft-wal README](https://github.com/hashicorp/raft-wal):
  "This library is still considered experimental!"; raft-boltdb named as the
  long-standing store.
