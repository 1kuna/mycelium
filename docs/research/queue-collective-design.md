# Queue collective: the queue lives on the fleet itself

Research date: 2026-11-09, revised the same day after the maintainer rejected
the first draft's dedicated-member constraint. Refs #1176 (tracking), #1179,
#30, #31, #316, #527, #661, #1066. This is a design study for the maintainer's
decision, not an implementation, a change to the [approved design](../design-proposal.md),
or a change to AGENTS.md. Section 9 lists the amendments it proposes. No fleet
host, live queue, database or spool was written to; every live fact below comes
from the public repository, its issues and public primary sources.

Every claim is tagged. **[S n]** is sourced, with the link in the Sources list.
**[M]** is a measurement recorded on the linked issue or document. **[I]** is
this study's inference from those facts.

## 0. The governing constraint

The maintainer's direction, 2026-11-09, verbatim [S1]:

> "i want the queue to be something the machines share rather than just running
> on one machine. just like we load balance the jobs, i think we should load
> balance the queue as well. ask fable about the best way to do that with
> stability so mycelium truly works as a biological mycelium could. where
> information is part of the collective in a way. that way if a machine goes
> down, nothing is truly lost."

The maintainer's rejection of the first draft's "three always-on, non-GPU
members", 2026-11-09, verbatim [S50]:

> "nope. the design is already a fail. i'm not doing "always-on machines that
> don't run gpu jobs" they need to be the same machines that the fleet is
> comprised of. no exceptions. people don't have extra machines they can just
> as specific queue devices. i'm trying to make mycelium as universal and
> adaptive as possible."

So the hard constraint for everything below: **queue state lives on the ordinary
fleet machines**. Those are GPU hosts that run production and drain, Macs and
laptops that sleep, machines on Wi-Fi, and machines that join and leave. There
are no dedicated or always-on queue devices, no special hardware and no
hand-picked stable boxes. The design must work for any fleet from one machine
to dozens, with whatever machines the user has. The maintainer's earlier
2026-10-15 note, that every hypha should hold "bits and pieces" kept on several
hyphae, points the same way [S21]. MY-40 records the direction and #1176 tracks
it; the approved design's single-queue rule stays in force until the maintainer
approves a design [S51].

## Summary

**What changes.** Every fleet machine runs the same binary and holds a complete
copy of the committed queue state. Among the machines present, a small odd
number (one, three or five, chosen from fleet size and measured fitness) are
voters that carry commitment; the fittest voter leads and runs today's
scheduler unchanged. Roles move automatically: before a machine sleeps,
drains, upgrades or leaves, it hands leadership and its vote to the fittest
other machine; when a machine vanishes, the remaining majority removes it and
promotes a replica. Workers, clients, the API, the token and the TLS pin do not
change; any machine answers any request [I].

**Why consensus still.** Exact custody, one current attempt and one published
result are uniqueness decisions, and uniqueness cannot be decided without
agreement among a majority; a CRDT or lease-based collective can converge but
cannot promise "exactly one" during a partition [S43][S45]. What the constraint
changes is not the decision rule but who may hold it: anyone fit, re-chosen
continuously. The "collective" part is where memory lives (every machine) and
who may speak (any fit machine), not how many speak at once [I].

**Degenerate fleets.** One machine: a one-voter group, today's queue with a
smaller write volume. Two machines: one voter plus one replica, roles moving to
whichever is awake. Everything asleep but one: if the others slept through the
sleep hook, the awake machine is the sole voter and runs alone; if power was cut
without warning to a majority of voters, the survivor keeps every record and
waits for one more voter to return, because acting alone could contradict a
decision the lost voters already committed [S23][I].

**Coexisting with GPU work.** Commitment needs the fastest majority, so one
machine's stalled disk does not stall the fleet; the leader's own disk is on
the path, so leadership moves off a machine whose fsync latency or I/O pressure
degrades, and members' I/O is isolated with mechanisms the code already has on
Linux and macOS [S61][S59][S60][S64]. The write-volume problem in #1066 is the
gating prerequisite: today's check-in persistence would be multiplied by the
voter count, so liveness and telemetry stop being replicated records at all
[M, S7][I].

**Whole-fleet power loss.** Every acknowledged write was flushed to disk on a
majority before the reply. Machines come back in any order; the group elects a
leader when a majority of the last voter set is back; workers report what they
still hold; nothing committed is lost [S23][S26][I].

**First step.** A measurement, not code: bytes committed per check-in by record
class, fsync latency on each fleet machine during real jobs, round-trip times
between machines, and each machine's availability history from the contact
records the queue already keeps.

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
  diff of changed records by `persistState` [S9].
- Every mutation runs inside `updateTransaction`: lock `q.mu`, mutate the
  in-memory state, compute the record diff, commit it to SQLite, publish the
  read snapshot, unlock. The commit, including `synchronous=FULL`'s fsync, is
  inside the lock. Job log chunks, usage samples, model intervals and
  enrollment tickets are written to SQLite directly by ten files [S9].
- Scheduling decisions use an injected `Clock`; `myce dev` runs the real API and
  command runner with fake GPU inventory and an advance-only `devClock` [S10][S9].
- Workers are plain-command agents that `POST /api/workers/checkin` about once
  a second, carry attempt-specific execution identities and a per-start
  session, persist each attempt's result in their own spool and replay
  unacknowledged results and log chunks by offset after a reconnect [S9][S11].
- Workers already observe owner signals that this design reuses: power is
  "desktop (no battery), ac, battery, or unknown", a laptop flag, heat and
  owner idle time; polite participation waits on battery or without external
  power [S64].
- Linux services can already carry a systemd `IOWeight`, marked "requires
  native qualification"; macOS shared-host payloads already launch under
  `nice -n 10 taskpolicy -b` [S64].
- Clients and workers hold one queue address (`MYCE_QUEUE` or a joining file)
  and authenticate the queue by the SHA-256 of its self-signed certificate; the
  joining path browses mDNS for `_mycelium._tcp` and tries each candidate
  against that pin. The Go client refuses redirects; the Python client retries
  only GET/HEAD on 429/503 and documents keyed replay for a lost submit reply
  [S9][S12][S13].
- Non-test code in `internal/myce` is about 104,600 lines; the files that touch
  SQLite directly total about 6,400 lines [M].

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
- Blob publication already syncs the file, hard-links it into `blobs/` and
  syncs the directory [S9].

### 1.3 What does not survive loss, and what hurts today

- Loss of the queue host's disk. "Mycelium has one SQLite writer. Queue loss
  stops submissions, new starts, acknowledged cancellation and queue-mediated
  routing"; "backup age defines possible data loss" [S4]. The recorded
  relocation plan budgeted 5 to 10 minutes of API downtime for a rehearsed
  idle move, excluding bulk copy of a 90 GiB file store [S3].
- Every queue release drains the GPU host that hosts it: a queue-only upgrade
  "still drains that host and waits for fresh exact custody" [S15].
- Commits under the lock couple every control request to one disk. During the
  2026-10-31 incident, I/O pressure on that host reached about 86% "some" and
  job GETs waited 7.7 s for the mutex with a 0.07 ms hold; a live database copy
  on the same disk was the likely contributor; the mechanism remains unproven,
  since a bounded native test did not reproduce it [M, S8].
- Under load, a check-in held the mutex 644 ms on average, 317 ms of it
  persistence, while the process wrote about 21 MB/s; check-in HTTP latency was
  p50 9.2 s and p99 16.4 s [M, S7]. Laptop replays show the planning share can
  fall from 309/489 ms to 55/75 ms (p50/p99) with indexing [M, S16].

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
production and too hard to test" [S2].

The lesson this study takes from v1 is specific [I]: the failures were (1) many
authorities (every peer coordinated), (2) eventual consistency where uniqueness
was required, (3) three home-grown protocols (discovery, tunnels, registry
merge) and (4) tests that could not exercise real races on a laptop. The
revised design keeps one authority at a time, one library protocol, and a
laptop cluster with fault injection; what it adds over the first draft is that
the authority and its voters are chosen from whatever machines are present.

Prior research for #30 reached a staged conclusion on 2026-10-14: keep one
assigning queue first, later let stable hyphae replicate its decisions, and
"Gossip finds reachable hyphae; it cannot decide safely who assigns work" [S20].

## 2. Goals as failure scenarios

"Durable" means: accepted submissions and their keys, cancellations, attempt
and release custody, published results, learned curves and policy. "Nothing
durable lost" is the target in every row. Pause figures are inferred until
measured; the election timeout is chosen from measured round trips, following
the Raft timing rule that broadcast time must be an order of magnitude below
the election timeout [S22].

| Scenario | Today | Required outcome | Durable loss | Scheduling pause |
|---|---|---|---|---|
| A voter sleeps (lid closed, idle sleep) | If it is the queue host, control stops until it wakes. | The sleep hook hands off leadership and the vote before sleep (section 4.4). The machine's worker leaves politely as today. | None [I]. | None beyond a leadership transfer if it led [S41][I]. |
| A voter drains for production or restarts for a release | The queue host's release drains the host and restarts control [S15]. | Drain and upgrade hooks hand off first; the machine remains a replica while draining; the release restarts one replica, not the authority. | None. | None if it was a follower; one leadership transfer if it led [S41]. |
| A voter vanishes (power cut, crash, cable pulled) | If it is the queue host: control stops; manual restore; everything after the last backup is lost [S4]. | If a majority remains, it elects a leader, removes the lost voter from the configuration and promotes the fittest replica. Running attempts keep running and report to the new leader. | None [S23][I]. | Election timeout plus one check-in interval plus state load; seconds once state load is measured [I]. |
| A majority of voters vanishes at once | Equivalent to queue loss today. | Survivors keep every committed record and wait; no survivor acts alone. Progress resumes automatically when any one lost voter returns (three-voter case). A documented manual recovery exists for permanent loss (section 7). | None while waiting; manual recovery can lose entries committed only on the lost voters, and says so [S63]. | Until a majority of the last configuration returns [S23]. |
| Network partition splits the fleet | Only the side with the queue host has control. | The side with a voter majority schedules; the minority pauses new assignments; minority workers keep running, buffer and replay; overlapping copies reconcile by the newest-checkpoint rule [S2]. | None [S24][S23]. | Minority: the partition's duration. Majority: an election if the leader was on the minority side [I]. |
| Two machines both think they lead | Impossible today (one process, flock on the database) [S9]. | At most one leader can commit per term; a deposed leader cannot commit or send new directives, steps down after its lease timeout, and reloads from committed records; directives carry the term so a worker ignores a lower term than one it has acted on. | None [S23][S41][I]. | The deposed leader's lease timeout, overlapping the new election [I]. |
| A machine returns after days or weeks offline | A worker replays its spool; stale attempts are rejected [S2][S9]. | As a worker: unchanged. As a replica: catches up from the leader's trailing log or a snapshot; it cannot win an election with a stale log [S65][S41]. | None. | None for the fleet; catch-up time for the member depends on snapshot size (database about 859 MiB today) [M, S3]. |
| The file store's host dies | Blobs live only there; "One store is not a backup" [S2]. | Every blob and accepted result exists on at least two members before it counts as durably transferred; any holder serves it. | None at replication factor two or more; a one-holder blob is reported, never called durable [I]. | None [I]. |
| The NAS is unreachable | Checkpoint-dependent work waits; running work continues [S2]. | Unchanged; no queue state lives on the NAS (WAL rule) [S26]. | None. | None for non-NAS work. |
| The whole fleet loses power (#1179) | Queue host reboots; database intact (FULL sync); workers rejoin; running attempts were lost and are requeued or resumed [S2]. | Section 7: members restart in any order; a leader is elected when a majority of the last voter set is up; workers clean leftovers and report fresh; replication of blobs and results resumes from worker spools and holders. | None committed. Attempts running at the cut are infrastructure losses, as today [S2][S52]. | Until a majority of the last voter set has booted, then election and state load [I]. |
| One-machine fleet | Today's queue. | A one-voter group: identical behaviour, smaller write volume. | As today. | As today. |
| Two-machine fleet, the voter dies suddenly | If it hosted the queue: as queue loss. | The replica holds every committed record except entries still in flight to it; it waits for the voter's disk to return, because promoting itself could contradict a committed decision. If the voter's disk is destroyed, those in-flight entries are lost and the clients' keyed replay and workers' reports rebuild them. | At most the replication lag, and only with the disk destroyed [I]. | Until the voter returns or the operator runs manual recovery [S63]. |

## 3. Candidate architectures, re-examined under the constraint

Common to all candidates: workers stay plain-command agents, the HTTP API and
token stay, and the job model, fences and scheduling policy stay. The question
is where the authoritative state lives, how a replacement authority is chosen,
and whether that still works when every machine is also a worker that sleeps,
drains and reboots.

### 3.1 (a) One active queue, replicated durable state, fenced takeover

**a1. Asynchronous replication (Litestream, sqlite3_rsync) plus a fenced promotion.**
Litestream "continuously copies write-ahead log pages from disk to a replica";
it is asynchronous and "trades off durability of recent writes"; "Synchronous
replication is on the Litestream roadmap but has not yet been implemented"; and
"Multiple applications replicating into the same bucket & path can cause
situations where you will be unable to restore" [S27][S28]. SQLite's own
`sqlite3_rsync` makes a consistent point-in-time replica while both databases
are live [S29]. Either gives recovery with a loss window equal to the lag, and
neither decides who may be primary; "Never serve two queues with the same
identity" still has to be enforced by someone [S4]. Under the constraint, the
primary is a GPU host that drains and restarts, so the promotion decision would
fire routinely, by hand [I]. Rejected as the core.

**a2. rqlite.** Raft around SQLite, but "you must only write to the database via
the HTTP API", explicit transactions are undefined, and it is "a standalone
application", not a library [S30]. A second service on every fleet machine, and
the end of the one-binary rule [I]. Rejected.

**a3. dqlite.** A C library that "runs on Linux and requires a kernel with
support for native async I/O" [S32][S33]. The fleet includes macOS machines
and the development host is a Mac [S34]. Rejected.

**a4. LiteFS.** FUSE replication with Consul leases, asynchronous, snapshot-reset
of a diverged node, "outside our scope of support", with a documented
stale-primary data-loss mode [S35][S36]. Rejected.

### 3.2 (b) An embedded Raft group running the queue as its state machine

Raft gives one leader per term, a replicated ordered log, and the Leader
Completeness guarantee that a committed entry is present in every later
leader's log [S23]. Three voters tolerate one failure; five tolerate two
[S37][S38]. Nomad and Consul separate voting servers from clients [S37][S38],
and both ship **Autopilot**, which keeps the voter set healthy without an
operator: "Dead servers will periodically be cleaned up and removed from the
Raft peer set", a new server "must be healthy and stable for a certain amount
of time before being promoted to a full, voting member" (default 10 s), and a
`MinQuorum` floor [S53][S54]. That is the precedent for adaptive membership:
the voter set is a committed configuration that a reconciler continuously
adjusts toward the healthy machines present [I].

Two Go libraries are production-grade. `hashicorp/raft` (v1.8.0, 2026-09-16,
MPL-2.0) supplies the FSM interface, snapshots, a TCP transport, an in-memory
transport with `Disconnect` for partition tests, non-voting members
(`AddNonvoter`), `DemoteVoter`, `LeadershipTransfer`, pre-vote and a manual
`RecoverCluster` [S40][S41][S63]. `etcd-io/raft` (Apache-2.0) implements only
the core algorithm, leaving transport and storage to the user, and lists
"Leader writing to disk in parallel with followers" among its optimizations
[S42].

The design choice inside (b) is **what to replicate**.

**b1. Replicate requests and re-execute the queue logic on every member.** Raft
requires that "Apply must be deterministic and produce the same result on all
peers" [S41]. Today's transaction reads the NAS probe, the filesystem, learned
curves and the clock inside the mutation; making that deterministic is a
rewrite of the decision code and reintroduces v1's "every peer computes" shape
[I]. Rejected.

**b2. Replicate committed record write-sets.** The leader runs today's
`updateTransaction` up to the point where `persistState` produces the map of
changed records. That map is the log entry; `Apply` on every member writes the
bytes into its local `queue_state`, which is trivially deterministic. Followers
never decode records; a member that becomes leader loads state with today's
`readQueueState`, exactly as `Open` does after a restart [S9][I]. This keeps
the persistence format, the recovery path and the decision code, and makes
every member a byte-identical copy of committed state. **Recommended**, with
the membership and coexistence rules of section 4.

### 3.3 (c) The collective: gossip membership, CRDT state, deterministic scheduling

The attraction is that every member accepts updates and the system converges
without a leader. Shapiro et al. define Strong Eventual Consistency so that "an
update does not require synchronisation, and CRDT replicas provably converge",
and "SEC does not require to solve consensus" [S43]. memberlist provides gossip
membership that "is eventually consistent but converges quickly on average"
[S44].

Where it breaks for Mycelium is the fences the project depends on [I]. A CRDT
can hold the set of submitted jobs and each attempt's log, but it cannot decide
that exactly one attempt of a job is current, that exactly one result is
published, that a release obligation was discharged once, or that a production
retry budget was spent once. Those are uniqueness and mutual exclusion. The
CRDT paper is explicit that "conflict arbitration may require a consensus and a
roll-back" and that "sequential consistency requires consensus" [S43]. Jepsen's
etcd analysis draws the general line: "lock services must sacrifice correctness
in order to preserve liveness in asynchronous systems", and the safe pattern is
a fencing token checked by the resource itself [S45]. Under a partition, a CRDT
queue on both sides would assign the same job twice and publish two results;
the late-return rule tolerates duplicate attempts for repeat-safe resumable
jobs, but at-most-one-published-result does not tolerate two authorities
[S2][I]. v1's LWW registry was this design [S6][S17][S18].

### 3.4 More collective shapes that the constraint invites, examined

The constraint makes it fair to ask whether something other than a majority
group now fits better. Three shapes were examined [I]:

**A lease-holder elected by measured fitness over a replicated log, without a
majority.** Someone has to grant the lease. A time-based lease needs agreement
on time, and Jepsen shows that even a perfect lock service cannot stop a paused
or partitioned holder from acting after its lease ended; two holders will
exist for a while and both will publish [S45]. The fix is a fencing token
checked by every resource, which workers can do for directives, but a
*published result* is consumed by clients, which cannot check anything. So the
arbiter for publication still needs agreement. Fitness election is kept, but
inside the group: `LeadershipTransfer` to the fittest voter [S41].

**Per-job ownership spread across machines.** Each job gets an owner machine
that holds its custody and decides its attempts; the load of deciding is spread.
But placement, priority tiers, fair-share deficits, preemption and host memory
allowances are decisions across jobs that need one view of the fleet; owner
failover needs agreement about who the new owner is; and this is v1's
"coordinator per job" with its "owner commits" split, whose registry did not
converge [S5][S6][S18]. Per-job *fences* already exist as attempt identities;
per-job *authority* adds coordination without removing the global decision.

**Flexible quorums ("majority of the awake").** Flexible Paxos shows that
"intersection is required only across phases": an election quorum and a
replication quorum need only overlap each other [S62]. One could replicate to
every live voter and let any single survivor lead. But then a write needs every
voter, so one sleeping voter blocks all writes, the opposite of what a fleet of
laptops needs. The practical form of "majority of the awake" is standard
majorities over a voter set that tracks the awake machines, which is the
Autopilot pattern [S53][S54][I].

A relaxation worth keeping for later: a disconnected machine could *accept* a
submission locally as a CRDT-style add and forward it when it reconnects, since
a submission is idempotent under its key. That is offline intake, not
scheduling, and the client's keyed replay already provides it [S13][I].

### 3.5 Comparison

Ratings are this study's judgments from the cited mechanisms [I], except where
a cell cites a measurement.

| Criterion | a1 async copy + fence | a2 rqlite | a3 dqlite | b2 Raft, adaptive voters, record write-sets | c gossip/CRDT | 3.4 lease by fitness |
|---|---|---|---|---|---|---|
| Stability of the authority | Manual or external fence [S4][S28] | Raft | Raft | Raft; one leader per term; voters re-chosen by a reconciler [S23][S53] | None; uniqueness unprovable [S43] | Two holders possible during pauses [S45] |
| Works on sleeping, draining, Wi-Fi fleet machines | Primary restarts constantly | Yes with membership tooling | Linux only | Yes: roles move before sleep/drain; dead voters removed while a majority remains [S53][S56][S58] | Yes | Yes, unsafely for publication |
| Operational simplicity | Backups and runbooks | Extra service per machine | Fails platform rule | One binary; one config flag per machine; no special machines | Conflict handling in every feature | Lease service plus fencing everywhere |
| Laptop testability | Yes | Partly | No [S34] | Yes: N members in one process on `InmemTransport`; partitions by `Disconnect`; fault injection for sleep, drain, vanish, slow disk, power loss [S41] | Convergence yes, exactness no | Yes |
| Failover time | Minutes, human | Seconds | Seconds | Seconds: election plus reconnection; zero for planned leave (transfer first) [S22][S41] | n/a | Lease length |
| Data-loss window | Replication lag [S28] | None committed | None | None committed; manual recovery after permanent majority loss is explicit [S23][S63] | Duplicates instead of loss | Lease overlap |
| Write throughput and latency | As today | Majority round trip | Same | Majority round trip and the fastest majority's fsync, outside `q.mu`; batches amortize; record classification cuts volume [I] | Local | Local |
| Code that survives | All | All but persistence and one-binary | Platform rule broken | All; persistence seam, new replication package, forwarding, hooks, dev members [M][I] | Planner and custody rewritten | Planner survives; custody rewritten |
| "State lives in the collective" | No | Yes | Yes | Yes: every machine holds it | Yes without exactness | Yes without exactness |

## 4. Recommendation: collective memory, rotating voice

### 4.1 Roles

- A **member** is any fleet machine running `myce`, worker and member in one
  process or two services on the same machine. Every member holds a complete
  copy of committed state as a Raft replica (voter or non-voter) [S41].
- **Voters** are the k fittest present members; k is 1, 3 or 5 (section 4.3).
  The **leader** is the fittest voter and runs today's scheduler. Everyone else
  is a **replica**: it receives every entry, serves reads and blobs, forwards
  writes, and is promotable in one configuration change because it is already
  caught up [S41].
- No machine is special. The former queue host becomes one member among the
  others; a laptop that joins for an afternoon becomes a replica, and if the
  fleet is small and the laptop is the fittest machine present, it may lead
  for that afternoon and hand off before it closes its lid [I].

### 4.2 Write path (unchanged from the first draft)

1. Lock `q.mu`; run today's mutation; produce the record write-set and the
   read snapshot candidate. Unlock. No network or disk I/O under the lock,
   which keeps the #80 rule and removes today's in-lock fsync [S19][S9][I].
2. Propose the write-set. The leader appends locally and replicates; commitment
   needs a majority's durable append [S23]. Proposals from one leader are FIFO,
   so write-sets apply in mutex order [I].
3. `Apply` writes the bytes into `queue_state` on every member. On the leader,
   the reply is released and the read snapshot published only now. No worker
   directive, no accepted-submission reply and no file publication precedes
   commitment [I].
4. The leader's in-memory state is optimistic; if any commit fails (lost
   leadership, disk failure), the member discards in-memory state, reloads from
   its committed records with today's `Open` path, and every in-flight request
   receives a retryable 503, as a queue restart does today [S9][I]. Clients
   already handle this [S13][S46]. `submission_key` is the Raft paper's client
   serial number [S23][S2].

### 4.3 Adaptive membership

**Fitness** is a ranking computed by the leader from measurements every member
already makes or can cheaply add, exchanged in the existing check-in and in
Raft heartbeats [I]:

- availability: the fraction of a sliding window during which the machine was
  reachable and awake, from the contact records the queue already keeps, plus
  the count of recent sleep and reboot events;
- planned unavailability: battery power, the laptop flag, polite daily-use
  policy, a pending drain, a pending release, all of which the worker already
  reports or the queue already knows [S64][S15];
- storage: fsync latency p99 of the member's own log store, I/O pressure (PSI
  on Linux; sampled rates on macOS), free headroom for log and snapshots;
- network: round-trip time p99 and loss to the current voters, link kind when
  detectable;
- load: CPU share and memory pressure left for the member process.

The score orders machines; it has no fixed thresholds, in line with MY-34. A
machine on battery, under drain, or with a pending release ranks below any
machine without those signals; among the rest, measured availability dominates,
then storage latency, then network [I].

**Voter count k.** Default before measurement: 1 when one or two members are
present, 3 when three or more. With measured availability, k is the largest odd
number up to 5 such that losing a majority of the k fittest at once is less
likely than losing the single fittest alone; if no k greater than 1 satisfies
that, k is 1 and the fittest machine leads alone with every other machine as a
replica. A fleet of one desktop and several flaky laptops therefore runs a
one-voter group on the desktop, which is strictly safer than three voters two
of which sleep [S37][I]. Five is used only when at least five machines are
measurably stable, following the 3-or-5 guidance [S38].

**Reconciliation** runs on the leader, the way Autopilot does, and every change
is a Raft single-server configuration change, so the voter set is always a
committed configuration [S53][S23][S41]:

- *Join*: a new machine becomes a replica and catches up (trailing log or
  snapshot). It becomes voter-eligible only after a stabilization period of
  continuous health, Autopilot's "server stabilization time" [S53][S54].
- *Grow*: while fewer than k voters are present, promote the fittest eligible
  replica (`AddVoter`); one at a time.
- *Shrink*: when fewer than k machines are present or fit, demote the least
  fit voter (`DemoteVoter`) so the set never needs an absent machine; one at a
  time, and never below one.
- *Replace*: when a voter ranks below a stable replica by more than a hysteresis
  margin for longer than the stabilization period, promote the replica, then
  demote the voter. Hysteresis prevents churn from momentary load [I].
- *Suspected loss*: when a voter misses heartbeats beyond the measured contact
  allowance (the same RFC 6298 estimator #661 introduced for worker loss), and
  the leader still has a majority, it removes the voter (`RemoveServer`) and
  promotes the fittest replica, Autopilot's dead-server cleanup [S53][S2]. If
  the lost voter was the leader, the surviving voters elect first.
- *Leader fitness*: when the leader's own fsync p99 or I/O pressure rises above
  the voters' measured distribution, or when it receives a drain, release or
  sleep intent, it calls `LeadershipTransfer` to the fittest voter [S41].

A **MinQuorum-style floor** [S53] is not a fixed number here: the reconciler
never removes a voter when doing so would leave the group unable to tolerate
the loss the measured availability predicts, unless the machine is already
gone [I].

### 4.4 Lifecycle hooks: leave the group before leaving the machine

The common ways a fleet machine goes away are planned, and each already has a
code path that can hand roles off first [I]:

- **`myce leave`, drain, disable, release restart**: the worker side already
  stops taking work and waits for exact custody [S2][S15]. The member side adds:
  transfer leadership if leading; ask the leader to demote this vote if voting;
  only then proceed. The machine stays a replica while draining, so it keeps
  receiving entries and can take roles back when it resumes.
- **Sleep, Linux**: take a systemd-logind `sleep` inhibitor in `delay` mode.
  The lock "can only delay the requested operations for a limited time", bounded
  by `InhibitDelayMaxSec`, which "Defaults to 5" seconds [S56][S57]. Five
  seconds is enough for a leadership transfer and one configuration change on a
  LAN, and must be measured in step 5.
- **Sleep, macOS**: register with `IORegisterForSystemPower`; on
  `kIOMessageCanSystemSleep` or `kIOMessageSystemWillSleep` the member hands
  off, then calls `IOAllowPowerChange`; "Power Management waits up to 30
  seconds" for the acknowledgement [S58]. Forced sleep (lid closed) can only be
  delayed, not prevented, which is enough [S58].
- **Wake**: the member rejoins as a replica; its worker reports fresh; the
  reconciler re-ranks it after stabilization.
- **Sudden loss** (battery dead, power cut, cable pulled): no hook runs. This is
  the one case the suspected-loss rule handles, and the one case where losing a
  majority at once pauses scheduling until a lost voter returns (section 7).

### 4.5 Degenerate fleets

| Fleet | Voter set | Planned sleep or leave of a voter | Sudden loss of a voter | Everyone asleep but one |
|---|---|---|---|---|
| 1 machine | {A} | The queue stops with the machine, as today. | As today: reboot resumes from the local database. | n/a |
| 2 machines | {A}, B replica | A hands leadership and its vote to B before sleeping; B leads alone; A rejoins as replica. | B holds every committed record except entries in flight to it and waits for A's disk; if A's disk is destroyed, the in-flight tail is lost and clients' keyed replay plus workers' reports rebuild it. B does not promote itself, because A may be partitioned and still committing [S36][S63]. | If the sleeper handed off, the awake machine leads alone. |
| 3 machines | {A,B,C} | The sleeper hands off; the reconciler shrinks to {A,B} then, if fitness says so, to {A}; or keeps {A,B} and tolerates nothing until a third returns. Default: shrink to 1 when only two machines remain. | The two survivors elect, remove the lost voter, and continue with k=1 or 3 as fitness dictates. | If two slept through hooks: the awake one is sole voter. If two lost power at once: the survivor waits for one of them. |
| Many machines | k fittest, rest replicas | Hand-off; the reconciler promotes the next fittest replica. | Majority elects; dead-server cleanup; promotion. | Hooks shrink the set as machines sleep; power loss to a majority pauses until one returns. |

The two-machine row is the honest limit of exactness: with two machines you can
have durability on both or progress with one, not both at once; two Raft voters
"can tolerate 0 failures" [S37]. The default chooses progress with one and
durability on the fittest machine's disk, because planned departures hand off
and sudden disk destruction is the rare case; the alternative (two voters) would
pause the fleet every time either machine slept [I].

### 4.6 Leadership change is a queue restart

A new leader loads state from its committed records exactly as `Open` does
today, including `RestartGrace` for workers that have not yet reported and the
"queue restarted; occupancy unknown" marking of live timings [S9]. The existing
restart tests become the leader-change tests [I].

Fencing [I]: a directive is sent only after its write-set committed, so a
deposed leader cannot assign or stop anything. It steps down after its lease
timeout [S41]. Every reply carries the leader's term; each worker remembers the
highest term it has acted on and ignores directives from a lower one, the
fencing-token pattern [S45].

### 4.7 Any member answers

- **Writes** are forwarded by any member to the leader over the members'
  authenticated transport, as Nomad and rqlite do [S37][S30]; the Go client's
  refusal of redirects is untouched [S9].
- **Reads** are served by any member from its own committed records with a
  freshness field, the point-in-time contract #1052 adopted [S48]; reads that
  must be linearizable are forwarded to the leader, which verifies leadership
  (`VerifyLeader`) [S41].
- **Worker check-ins** may hit any member; the member forwards and relays the
  directive. This spreads TLS, JSON parsing, blob serving and long polls across
  the fleet, which is the load-balancing half of the maintainer's request; the
  decision stays with one leader [I].
- **Model calls** are proxied by whichever member receives them to the engine
  the leader placed; in-flight calls still fail on leader change and callers
  retry, as the design already states [S2].

### 4.8 Identity, discovery and trust

Every member holds the same joining identity (certificate and key), so the
fingerprint every worker and client already pins stays valid, and `myce invite`
works from any member [S9]. Every member advertises `_mycelium._tcp`;
`ResolveJoining` already browses all candidates and accepts the first that
authenticates [S9]. Explicit `MYCE_QUEUE` users keep one address and are served
by forwarding; a later invitation version can carry several addresses, the only
consumer-visible change and the only skill change this design needs [I]. The
fleet token stays a bearer credential verified by every member and never passed
to jobs [S19].

### 4.9 What is replicated, and the write-volume prerequisite

This classification is the answer to #1066 as much as to replication [I]:

| Class | Examples | Replication | Why |
|---|---|---|---|
| Intent | submit, cancel, pause, resume, policy, pins, maintenance, release approvals, enrollment tickets | Synchronous (log entry before reply) | The user's durable intent [I] |
| Outcome and custody | attempt created and assigned, Started, completed, result accepted, release confirmed, retry and loss counters | Synchronous (log entry before directive or reply) | One current attempt, one published result, exact release [S2] |
| Learned | curves, memory envelopes, efficiency baselines, comparable observations | Lazy, batched entries | Loss of the last seconds is visible and acceptable; MY-31 learning tolerates gaps [I] |
| Liveness and telemetry | contact and session evidence, device telemetry, tool inventories, usage samples, model intervals | **Not replicated**; leader-local soft state, rebuilt from the next check-in; frozen contact evidence for offline conversion kept as a lazy record | The workers are the source of truth and report every second; this is most of today's check-in persistence [S9][M, S7] |
| Job log chunks | `logs`, `log_offsets` | Lazy stream between members; the worker keeps the attempt log and replays from the highest offset the new leader reports | Bulk; the worker already replays by offset with an idempotent key [S9] |
| Blobs and results | file store | Content-addressed holders, factor R (section 6) | Bulk; pulled outside any lock [I] |

Consequence: replication traffic becomes proportional to job transitions, not
to check-ins. The current 21 MB/s is write amplification that must be removed
before any entry is shipped to a second machine; step 0 measures it by class
[M, S7].

## 5. Coexisting with GPU and heavy I/O work

A member that is also running a production attempt staging a large input,
writing a large output or copying a model is the normal case under the
constraint. The design must make such a machine a *worse candidate*, never a
stalled fleet [I].

**What consensus actually waits for.** Commitment needs the leader plus enough
followers to form a majority; the slowest follower is not on the path when
k = 3, so one stalled disk slows nothing [S23][I]. The leader's own append is
on the path: `hashicorp/raft` writes the entry locally before notifying
replicators, and a failed local write steps the leader down [S61]. `etcd-io/raft`
can write the leader's disk in parallel with followers [S42]. Either way the
leader must have a healthy disk, so leader fitness tracks fsync p99 and I/O
pressure and leadership moves when they degrade (section 4.3) [I].

**Isolation that already exists, extended to members** [I]:

- Linux: the member runs in its own systemd unit with `IOAccounting` and
  `IOWeight`, which the installer already supports but marks "requires native
  qualification" [S64]. `io.weight` only has effect through the iocost
  controller or the BFQ scheduler, and #527 found neither active on the queue
  host [S59][S8]. `io.latency` is a separate cgroup v2 controller that gives
  "a latency target" and "will throttle any peers" when the protected group
  misses it, and is "work conserving"; it does not depend on the scheduler
  [S59]. The member's cgroup gets an `io.latency` target derived from its
  measured idle fsync latency; attempts run in the worker's delegated cgroup
  (`Delegate=memory cpu` already) with a lower `io.weight` and `cpu.weight`
  [S64][S59]. Native qualification of these controllers is a step-6 measurement.
- macOS: attempts already launch under `nice -n 10 taskpolicy -b` on shared
  hosts; `taskpolicy` "uses the setiopolicy_np(3) and setpriority(2) APIs" and
  "All children of the specified program also inherit these policies" [S64][S60].
  Extend that to every attempt on a machine that is currently a voter, and run
  the member at default policy.
- Both: the member's log store and SQLite live on the machine's fastest local
  volume, never on the NAS [S26].

**Which jobs make a machine a worse voter.** The ranking reads measurements,
not job types: write rate and I/O pressure during input staging and output
writes, memory pressure and swap, a pending drain or release, Docker image
pulls. Pure accelerator compute barely moves fsync latency; bulk copies do
[S8][I]. A production host that is about to be drained for a release is
demoted *before* the drain by the hook, not by the ranking [I].

**Interaction with #527 and #1066.** Yes, directly [I]. #527's hazard, heavy
I/O on the queue host stalling every check-in through the in-lock commit,
becomes "this member's fsync got slow", which the ranking turns into a
leadership move and the out-of-lock commit turns into latency on that member's
forwarded requests only. The bounded native test in #527 (22.7 MiB/s with
fsync every 4 MiB) did not move check-in p99 past about 1.1 s; the incident had
a 27 GB copy; the fitness inputs must be calibrated against both [M, S8].
#1066's 21 MB/s is the prerequisite in section 4.9: without the record
classification, replication ships write amplification to k disks and the
leader's fsync budget is spent on telemetry [M, S7].

## 6. File store

Today a blob is `<root>/blobs/<sha256>`, published by hard link from a verified
tus upload after `fsync` and a directory sync, with references and retired-file
sets tracked in queue state and collected asynchronously [S9][S14]. Content
addressing already makes a blob location-independent; what is missing is a
second copy and a record of who has it [I].

Proposed [I]:

- **Holder records.** A replicated record `blob_holders/<sha256>` lists the
  members that have verified the blob. The member that completes an upload
  publishes locally, verifies, then proposes itself as the first holder.
- **Replication factor R**, default `min(2, members)`; tiers may ask for more.
  Other members pull over the existing resumable blob GET route, verify the
  SHA-256, and propose themselves as holders. Pulls are background work with
  measured concurrency and headroom (MY-34), never under `q.mu` or the
  file-store lifecycle lock [S19]. Holder choice prefers machines with measured
  disk headroom and availability, so a laptop that leaves nightly is a holder
  of last resort, not first.
- **Durability semantics.** An input is usable at one holder, as today; status
  shows `holders: 1/2` until replicated. A *result* is "accepted" only when R
  holders have verified it; the worker keeps the result in its spool until that
  acknowledgement, as it already keeps unacknowledged results. A blob below R
  after its grace raises a health alert naming the shortfall.
- **Serving.** Workers fetch inputs from any holder listed in their directive,
  nearest first when telemetry knows.
- **Garbage collection.** Today's reference tracking stays authoritative;
  deletion is a record and each holder removes its copy on apply.
- **The NAS** can be configured as a holder for finished outputs, meeting the
  maintainer's 2026-10-15 note about a designated finished-output location for
  provenance [S21]; it is never the only holder of anything the queue needs.
- **Checkpoints.** NAS checkpoints are unchanged. File-store checkpoint
  generations are blobs and get R copies before a generation counts as portable.

## 7. Durability when the whole fleet loses power (#1179)

#1179 asks that "Every part of Mycelium survives sudden power loss, kernel
panic or `kill -9` at any instant without corruption, and resumes" [S52]. For
the queue under this design:

**What is guaranteed** [I, from S23, S26, S41, S49]:

- Every acknowledged write (reply sent, directive issued, result accepted) was
  appended with fsync on a majority of voters before the acknowledgement. Raft
  log stores sync on append; SQLite with `synchronous=FULL` in WAL mode is
  "atomic, consistent, isolated, and durable (ACID)" across power loss
  [S26][S66]. `raft-boltdb` is the chosen log store; `raft-wal` "is still
  considered experimental" [S49].
- Blob publication is fsync, link, directory sync [S9]; a blob with R holders
  survives the loss of R−1 disks.
- Worker spools hold unacknowledged results; #1179's audit covers their
  atomicity and is a prerequisite this design relies on, not something it
  re-specifies [S52].
- No wall clock is trusted for safety: Raft safety does not depend on clocks,
  and scheduling uses the injected and control clocks [S23][S9].

**Recovery, machines returning in any order** [I]:

1. Each member boots, finds existing state (`HasExistingState`) and starts as a
   follower in the last committed configuration [S63].
2. Nothing happens until a majority of that configuration is up. A member with
   a stale log cannot win: Raft's election restriction makes a voter deny its
   vote to a less up-to-date candidate [S65]. So the first machine to boot does
   not become a leader with missing entries.
3. When a majority is up, an election completes within the election timeout;
   the leader loads state from its committed records, marks occupancy unknown,
   and opens the restart grace for unreported workers [S9].
4. Workers boot, "clean up any job processes left over from before the power
   cut", and report fresh [S2]. Attempts that were running are infrastructure
   losses: resumable jobs continue from their last complete checkpoint
   generation, others restart under their retry rules, production within its
   bounded budget [S2].
5. Replicas that were not in the majority catch up from the trailing log or a
   snapshot. Blob replication resumes toward R; results still in worker spools
   are re-uploaded.
6. If a voter's disk did not survive, the majority of the rest proceeds and the
   reconciler replaces it. If a *majority* of voters' disks did not survive,
   the survivors wait, and the operator may run the manual recovery below.

**Manual recovery, stated honestly.** `hashicorp/raft` provides `RecoverCluster`
"to recover from a loss of quorum where the current configuration cannot be
restored, such as when several servers die at the same time". Its warning:
"this is an extremely unsafe operation", because it "implicitly commits all
entries in the Raft log", including entries that were replicating but not yet
committed, and it must be run with all servers stopped and an identical
configuration [S63]. The design exposes it as `myce queue recover --force` with
exactly that text, requiring the operator to name the survivors. What such a
recovery can lose: entries committed only on the destroyed disks (a submission
acknowledged in the last milliseconds before the cut). What rebuilds them:
clients' keyed replay and workers' custody reports [S13][S2][I]. This is the
only path that can lose an acknowledged write, it is never automatic, and it
exists only for the case where a majority of voters' disks are destroyed at
once.

## 8. Migration plan

Each step is independently testable on a laptop, reversible, and leaves clients
and the skill unchanged unless the step says otherwise. Steps 0 to 3 are
valuable alone. Fault injection in `myce dev` covers: *sleep* (member stops
heartbeating and serving, disk kept, later "wakes"), *drain* (planned leave
with hooks), *vanish* (kill, disk kept), *disk loss* (kill, wipe), *partition*
(`InmemTransport.Disconnect`), *slow disk* (fsync latency injected into the log
store), *power loss* (kill every member, restart in random order) [S41][I].

**Step 0. Measure (no product code).** On the retained-history fixture already
used by #661 (6,500 executions, 110 queued jobs, 1,400 curves): bytes committed
per check-in and per submit, split by the classes of section 4.9; commits per
second under the fan-out replay; `Open` state-load time for a database of
today's size [S16]. On each fleet machine, read-only: fsync latency p99 while a
representative staging job runs, member-to-member round-trip and loss over a
day, and each machine's availability history from the contact records the queue
already keeps [S47]. Acceptance: numbers posted on #1066 and #1176. Rollback:
none.

**Step 1. Record classification and one persistence seam.** Introduce a
`recordStore` with `Append(writeSet, class) (index, error)` and `Load()`,
implemented by today's SQLite code; route the ten direct `q.db` users through
typed record kinds; stop persisting liveness and telemetry as durable records,
rebuilding them from the next check-in on restart. Acceptance: the full gate is
unchanged; a round-trip test shows `readQueueState(persistState(x)) == x` for
intent, outcome and learned records; a restart test shows liveness rebuilt
within one check-in interval; bytes per check-in fall against step 0.
Consumers: no change. Rollback: revert; the on-disk format of retained records
is unchanged.

**Step 2. Commit outside the lock, reply after commit.** As section 4.2.
Acceptance: existing restart and crash tests pass; a fake-clock test injects a
commit failure and proves the reply never preceded the commit, state after
reload equals the committed records, and in-flight requests got retryable 503s.
Expected benefit before any replication: the persistence share of the check-in
hold leaves the lock [M, S7]. Consumers: no change. Rollback: a one-release
flag selects the in-lock commit; both paths write identical records.

**Step 3. A one-voter group.** The `recordStore` becomes a Raft log of
write-sets with a single voter, `raft-boltdb` as the log store, snapshots from
`queue_state` [S49][S41]. Acceptance: `myce dev` and the dev restart probe pass
unchanged; `kill -9` during a fan-out loses no accepted job; power-loss
injection (kill, restart) resumes; commit latency is within step-0 noise of the
SQLite baseline. Consumers: no change. Rollback: flag back to step 2.

**Step 4. Every machine a replica; any member answers.** `myce join` on a
machine that already runs a worker also starts the member (or one process does
both); it joins as a non-voter, catches up, serves reads with freshness,
forwards writes, and advertises mDNS with the shared identity.
`myce dev --members N` runs N members in one process on `InmemTransport` under
the shared dev clock, with dev-only endpoints for the fault injections above.
Acceptance: submit through a replica and the job runs; vanish a replica and
nothing changes; vanish the single voter and scheduling pauses, then resumes
on its return with every job present (expected at this step); a replica that
"slept" for 30 simulated days catches up by snapshot. Consumers: no change.
Rollback: `RemoveServer` on the replicas.

**Step 5. Adaptive voters and lifecycle hooks.** Fitness ranking, the
reconciler (grow, shrink, replace, suspected loss, leader fitness), the
hand-off hooks for leave, drain, release and sleep (logind delay inhibitor,
IOKit power notifications), the term in directives, and the capable-leader rule
(a member whose binary cannot decode the committed schema transfers leadership
away) [S53][S56][S58][S41]. Acceptance on the dev cluster, fake clock and
in-memory partitions, each as a scripted scenario:
(i) planned sleep of the leader: hand-off completes within the injected
inhibitor budget (5 s default on Linux) and no request fails;
(ii) vanish the leader mid-batch: a new leader within the election timeout,
every accepted job present, running attempts complete and are accepted once,
the lost voter is removed and a replica promoted;
(iii) two of three voters vanish at once: the survivor serves reads, refuses
writes with an explained pause, and resumes when one returns, with no lost
record;
(iv) the two-machine fleet: the voter sleeps through the hook and the replica
leads; the voter vanishes and the replica waits; on return no record is lost;
(v) partition a minority holding a worker: no new assignment there, replay and
the newest-checkpoint rule hold after heal;
(vi) isolate the old leader while it keeps answering a worker: no directive
from a stale term is acted on;
(vii) flapping: a member that sleeps and wakes every few minutes never becomes
a voter while a stable replica exists (hysteresis), and a fleet of one desktop
and three flapping laptops settles on k = 1;
(viii) add and remove machines under load with no lost submission;
(ix) a history checker over job-state transitions finds them linearizable (a
Go checker such as Porcupine is a candidate; choice open).
Consumers: no change. Rollback: `DemoteVoter` to a single voter; the leader's
records are the single-node database.

**Step 6. Coexistence.** Member I/O and CPU isolation (`io.latency` and weights
on Linux, `taskpolicy` on macOS), leadership moves on degraded fsync, and the
fitness inputs from PSI and I/O rates [S59][S60]. Acceptance: on the dev
cluster, inject fsync latency into the leader's log store and observe a
leadership transfer with no failed request; on one real Linux machine
(separately authorized native measurement), a staging-sized write load against
the member's volume does not push the member's fsync p99 past its measured
idle distribution with the controllers configured, and does with them off.
Consumers: no change. Rollback: controllers off; the ranking still moves
leadership.

**Step 7. Blob holders and replication factor.** Section 6. Acceptance on the
dev cluster: upload through member A, vanish A, a worker fetches the input from
B; a result upload interrupted by a leader change resumes (tus) and the
accepted result shows two holders; a deleted reference removes the blob from
every holder; `holders: 1/2` is reported, never "durable"; power-loss injection
with a result still in a worker spool ends with the result accepted at R.
Consumers: a new status field; the skill gains one sentence. Rollback: R = 1
with holder records kept.

**Step 8. Fleet rollout on the machines that exist.** Every fleet machine runs
the member; the former queue host is one member among others; its releases no
longer drain the fleet's control (a member release is a hand-off and one
restart, MY-35) [S41]. Acceptance: the #316 criterion, control-request latency
before and after under a comparable fan-out, plus one hour with no check-in
flap and at least one observed automatic hand-off during a real drain or sleep
[S3][S16]. Rollback: stop all but one member and run it in step-3 mode; its
records are complete.

**Consumer compatibility, summarized.** API routes, job model, submission keys,
error codes, the token and the TLS pin are unchanged through step 8. Leader
changes surface as retryable 503s that clients already handle [S13][S46]. The
only additions are a `holders` status field and, later, an optional
multi-address invitation; the skill changes only for those [I].

## 9. Risks, unknowns, what to measure first

1. **Write volume.** About 21 MB/s today, attributed to re-serialization; any
   replication multiplies committed bytes by k and adds follower fsyncs
   [M, S7]. Step 0 measures by class; step 1 removes liveness from the durable
   set. Without that, the design ships today's amplification to every disk.
2. **Follower fsync on macOS.** Go's `File.Sync` issues `F_FULLFSYNC` because
   "SYS_FSYNC doesn't fully flush contents to disk" [S47]. If measured latency
   is tens of milliseconds, commit latency rises on Mac-heavy fleets; batching
   per leader tick is the standard answer and must be measured.
3. **Hand-off budgets.** Linux delay inhibitors default to 5 s [S57]; macOS
   waits up to 30 s [S58]. A hand-off is one leadership transfer and at most
   one configuration change; its duration on a loaded fleet must be measured
   (step 5). If 5 s is too short on some fleets, the member can still demote
   itself on wake and the reconciler handles the interval as a suspected loss.
4. **Churn.** Laptops that sleep and wake often could cause configuration
   churn; stabilization time and hysteresis bound it, and the k rule prefers
   one stable voter over three flapping ones [S53][I]. The exact windows are
   measured, not fixed.
5. **Correlated loss.** Voters on one power strip or one Wi-Fi access point
   fail together, and the fleet cannot know circuits. Fitness can prefer voters
   that differ in observed power kind and link, a weak analogue of Autopilot's
   redundancy zones [S53]; the honest statement is that a correlated loss of a
   majority pauses until one returns, and the manual recovery exists [S63].
6. **The manual recovery path** can lose acknowledged writes and is the only
   such path; it must stay manual, loud and documented [S63].
7. **Snapshot size and catch-up.** About 859 MiB today; a laptop returning
   after weeks needs a snapshot [M, S3]. Retention already prunes history;
   measure snapshot size and LAN transfer before step 5.
8. **Mixed versions.** Followers store bytes, so a newer leader's records are
   safe on older replicas; an older binary that became leader would fail closed
   on an unknown schema marker [S9]. The capable-leader rule and an upgrade
   order of replicas first, leader last, cover it; test it on the dev cluster.
9. **Library timers and the injected clock.** Raft election and heartbeat
   timers inside the library use wall time; dev tests run them with short real
   timeouts while every scheduling decision keeps the injected `Clock`. A
   bounded, named exception to the clock rule [S19].
10. **Side effects after commit.** File publication, NAS delivery tasks and
    engine commands must run only after their authorizing write-set committed;
    the `committed` callbacks are the seam; auditing them belongs to step 2.
11. **I/O controller qualification.** `io.weight` needs iocost or BFQ, which
    the queue host did not have; `io.latency` is scheduler-independent [S59][S8].
    Native qualification is a separately authorized measurement (step 6).
12. **Planning cost stays on the leader.** Replication removes disk time from
    the lock, not planning time (283 ms per check-in in #1066); #661 and #1031
    continue [M, S7][S16].
13. **Library choice.** `hashicorp/raft` brings transport, snapshots, in-memory
    transport and `RecoverCluster`; `etcd-io/raft` brings parallel leader disk
    writes at the cost of writing transport and storage [S40][S42][S61]. Start
    with `hashicorp/raft`; the `recordStore` seam contains a later switch.
14. **Licensing.** `hashicorp/raft` and `memberlist` are MPL-2.0; `etcd-io/raft`
    Apache-2.0 [S40][S42][S44]. The repository has no LICENSE file [M]; choosing
    one is needed for the public release regardless of this design.

**Measure first, in order:** bytes per commit by record class and commits per
second (step 0); fsync p99 on each fleet machine during a representative
staging job; member-to-member round trip and loss over a day; `Open` state-load
time and snapshot size; per-machine availability history from existing contact
records, which also decides k.

## 10. Proposed amendments (for the maintainer's approval)

Neither file is changed by this study.

- **Design proposal, "The parts" and the single-host sentence.** Replace "One
  always-on machine" with "Every participating machine; a small odd subset of
  the fittest present machines carries commitment and one of them leads at a
  time" and replace "There is no peer network, no leader election and no
  replicated state" with "Queue members replicate committed state through one
  consensus log whose voters are chosen continuously from measured fitness;
  there is no gossip registry, no per-host lease store and no coordination by
  workers." Add the failure table of section 2 to "Power and network loss are
  normal, not rare".
- **AGENTS.md.** Replace "One binary, one SQLite queue, plain-command workers.
  No peer network, leases, replicated registry, or restored v1 architecture"
  with "One binary. Every machine holds the queue's committed state in its own
  local SQLite; a measured subset votes and one leads at a time; plain-command
  workers. No dedicated queue machines, gossip registry, per-host leases or v1
  peer coordination." Keep the injected-clock rule, the laptop rule, the
  no-bulk-I/O-under-`q.mu` rule and the token rule verbatim; add "Replication
  and commit happen outside `q.mu`; no reply or directive precedes commitment;
  a machine hands off its roles before it sleeps, drains or restarts."
- **Decisions.** MY-40 already records the direction [S51]; add the 2026-11-09
  rejection quoted in section 0 as its scope clarification, and note that
  #31's revisit trigger has been met.
- **Skill.** No change until the multi-address invitation or the `holders`
  field ships; then one sentence each.

## 11. Sources

Internal facts were read on 2026-11-09 from the repository at this branch and
from public issues; external pages were fetched on 2026-11-09 and their stated
last-modified dates are given where the page supplies one.

- S1. The maintainer's direction, 2026-11-09, as relayed in this study's brief
  and recorded in [docs/DECISIONS.md](../DECISIONS.md) (MY-40).
- S2. [Approved design](../design-proposal.md): "Why a redesign", "The parts",
  "Machines leaving", "Power and network loss are normal, not rare",
  "Production infrastructure retries", "Files with or without a NAS".
- S3. Queue relocation plan, 2026-10-29, retained in history:
  [docs/plan-queue-relocation.md at 12bb6b7^](https://github.com/1kuna/mycelium/blob/12bb6b7826153c38d356555b711c9ba4a11f856a%5E/docs/plan-queue-relocation.md);
  consolidated into #527 via [#316](https://github.com/1kuna/mycelium/issues/316).
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
- S9. Repository source at this branch: `internal/myce/queue.go` (`Open`,
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
- S15. [Operations: rolling fleet upgrades](../operations.md#rolling-fleet-upgrades).
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
  comment of 2026-10-15 recording the maintainer's direction.
- S22. Ongaro and Ousterhout, [In Search of an Understandable Consensus
  Algorithm (extended)](https://raft.github.io/raft.pdf), 2014, §5.6:
  "broadcastTime ≪ electionTimeout ≪ MTBF"; broadcast time "0.5ms to 20ms";
  "When the leader crashes, the system will be unavailable for roughly the
  election timeout".
- S23. Same paper: §1 "fully functional (available) as long as any majority of
  the servers are operational"; Figure 3 "Leader Completeness"; §5.2
  randomized election timeouts "(e.g., 150–300ms)"; §6 configuration changes;
  §8 "clients ... assign unique serial numbers to every command".
- S24. [rqlite FAQ](https://rqlite.io/docs/faq/), last modified 2026-11-04:
  "Raft is a Consistency/Partition-tolerance (CP) protocol."
- S25. hashicorp/raft `Config.LeaderLeaseTimeout`, `AddNonvoter`, snapshot and
  catch-up documentation, [pkg.go.dev](https://pkg.go.dev/github.com/hashicorp/raft), v1.8.0.
- S26. [SQLite write-ahead logging](https://sqlite.org/wal.html): "All
  processes using a database must be on the same host computer; WAL does not
  work over a network filesystem."
- S27. [Litestream: how it works](https://litestream.io/how-it-works/).
- S28. [Litestream tips](https://litestream.io/tips/).
- S29. [sqlite3_rsync](https://sqlite.org/rsync.html).
- S30. [rqlite FAQ](https://rqlite.io/docs/faq/): HTTP API only, explicit
  transactions undefined, standalone application, write forwarding.
- S31. [rqlite read consistency](https://rqlite.io/docs/api/read-consistency/).
- S32. [canonical/dqlite README](https://github.com/canonical/dqlite).
- S33. [canonical/go-dqlite README](https://github.com/canonical/go-dqlite).
- S34. MY-37, the maintainer's 2026-11-09 instruction that development must not
  depend on fleet availability and runs on the development Mac, recorded on
  [#1086](https://github.com/1kuna/mycelium/issues/1086).
- S35. [LiteFS: how it works](https://docs.fly.io/litefs/how-it-works).
- S36. [LiteFS overview](https://docs.fly.io/litefs/): "outside our scope of
  support", stale-lease data-loss warning.
- S37. [Nomad consensus protocol](https://developer.hashicorp.com/nomad/docs/architecture/cluster/consensus):
  "Only Nomad server nodes participate in Raft", "All client nodes forward
  requests to servers", deployment table ("2 ... 0" failure tolerance),
  "3 or 5 Nomad servers".
- S38. [Consul consensus](https://developer.hashicorp.com/consul/docs/architecture/consensus):
  quorum arithmetic, "We recommend either 3 or 5 servers".
- S39. [NATS JetStream clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering):
  odd counts, R3 "the production floor".
- S40. [hashicorp/raft README](https://github.com/hashicorp/raft): FSM,
  snapshots, LogStore/StableStore, non-voting servers, MPL-2.0.
- S41. [hashicorp/raft package documentation](https://pkg.go.dev/github.com/hashicorp/raft),
  v1.8.0 (2026-09-16): `FSM.Apply` "must be deterministic", `InmemTransport`
  `Connect`/`Disconnect`/`DisconnectAll`, `AddVoter`/`AddNonvoter`/
  `RemoveServer`/`DemoteVoter`, `LeadershipTransfer`, `VerifyLeader`,
  `LeaderCh`, `LeaderLeaseTimeout` ("If we reach this interval without contact,
  we will step down as leader"), `PreVoteDisabled`, `SnapshotThreshold`,
  `TrailingLogs`.
- S42. [etcd-io/raft README](https://github.com/etcd-io/raft): minimal core,
  user-supplied transport and storage, "Leader writing to disk in parallel with
  followers", step-down on lost quorum, Apache-2.0.
- S43. Shapiro, Preguiça, Baquero, Zawirski, [Conflict-free Replicated Data
  Types](https://inria.hal.science/inria-00609399v2), INRIA RR-7687, 2011.
- S44. [hashicorp/memberlist README](https://github.com/hashicorp/memberlist).
- S45. [Jepsen: etcd 3.4.3](https://jepsen.io/analyses/etcd-3.4.3): "lock
  services must sacrifice correctness in order to preserve liveness in
  asynchronous systems"; fencing tokens.
- S46. [skills/mycelium/SKILL.md](../../skills/mycelium/SKILL.md): keyed replay
  and Retry-After handling.
- S47. Go source [internal/poll/fd_fsync_darwin.go](https://github.com/golang/go/blob/master/src/internal/poll/fd_fsync_darwin.go):
  "on OS X, SYS_FSYNC doesn't fully flush contents to disk", `F_FULLFSYNC`.
- S48. [#1052 Serve status and job reads from a cheap published snapshot](https://github.com/1kuna/mycelium/issues/1052).
- S49. [hashicorp/raft-wal README](https://github.com/hashicorp/raft-wal):
  "This library is still considered experimental!"
- S50. The maintainer's rejection of the first draft, 2026-11-09, verbatim, as
  relayed in the revision brief (quoted in section 0).
- S51. [docs/DECISIONS.md](../DECISIONS.md), MY-40 (2026-11-09), and
  [#1176](https://github.com/1kuna/mycelium/issues/1176).
- S52. [#1179 Every part of Mycelium survives power loss without corruption and resumes](https://github.com/1kuna/mycelium/issues/1179).
- S53. [Consul Autopilot](https://developer.hashicorp.com/consul/docs/manage/scale/autopilot):
  `CleanupDeadServers` "Enables periodic dead server removal from the Raft peer
  set"; `ServerStabilizationTime` "Time duration that a new server must remain
  healthy before it can become a voting member" (default 10s); `MinQuorum`;
  redundancy zones.
- S54. [Nomad Autopilot](https://developer.hashicorp.com/nomad/docs/manage/autopilot):
  "Dead servers will periodically be cleaned up and removed from the Raft peer
  set"; "must be healthy and stable for a certain amount of time before being
  promoted to a full, voting member".
- S55. [hashicorp/raft-autopilot](https://github.com/hashicorp/raft-autopilot),
  MPL-2.0; the Go library behind the Consul and Nomad features above (README
  body was not retrievable on the research date; the repository page shows the
  license and the promoter/reconcile sources).
- S56. [systemd-inhibit(1)](https://man7.org/linux/man-pages/man1/systemd-inhibit.1.html):
  delay mode "can only delay the requested operations for a limited time";
  "delay" is available for "sleep" and "shutdown".
- S57. [logind.conf(5)](https://man7.org/linux/man-pages/man5/logind.conf.5.html):
  `InhibitDelayMaxSec` "Specifies the maximum time a system shutdown or sleep
  request is delayed"; "Defaults to 5."
- S58. Apple Technical Q&A [QA1340](https://developer.apple.com/library/archive/qa/qa1340/_index.html):
  `IORegisterForSystemPower`, `kIOMessageCanSystemSleep`,
  `kIOMessageSystemWillSleep`, "Power Management waits up to 30 seconds",
  "it is not possible to prevent forced sleep, only delay it".
- S59. [Linux cgroup v2, IO controller](https://docs.kernel.org/admin-guide/cgroup-v2.html):
  weight-based distribution "is provided by the iocost cost model controller"
  or BFQ; `io.latency` "You provide a group with a latency target, and if the
  group misses its target the controller will throttle any peers", "work
  conserving".
- S60. [taskpolicy(8)](https://keith.github.io/xcode-man-pages/taskpolicy.8.html):
  "uses the setiopolicy_np(3) and setpriority(2) APIs"; "All children of the
  specified program also inherit these policies"; `-b` sets `PRIO_DARWIN_BG`.
- S61. hashicorp/raft [raft.go](https://github.com/hashicorp/raft/blob/main/raft.go),
  `dispatchLogs`: "Write the log entry locally" before "Notify the replicators
  of the new log"; a failed local write responds with the error and
  `setState(Follower)`.
- S62. Howard, Malkhi, Spiegelman, [Flexible Paxos: Quorum intersection
  revisited](https://arxiv.org/abs/1608.06696), 2016: "Majority quorums are not
  necessary as intersection is required only across phases."
- S63. hashicorp/raft `RecoverCluster` documentation,
  [pkg.go.dev](https://pkg.go.dev/github.com/hashicorp/raft): "recover from a
  loss of quorum where the current configuration cannot be restored, such as
  when several servers die at the same time"; "this is an extremely unsafe
  operation"; "implicitly commits all entries in the Raft log"; also
  `HasExistingState`.
- S64. Repository source: `internal/myce/availability.go` (`OwnerSignals`:
  power "desktop (no battery), ac, battery, or unknown", laptop, heat, idle;
  waits "on battery" and "external power required"), `internal/myce/install.go`
  and `install_settings.go` (`IOWeight`, "requires native qualification",
  `Delegate=memory cpu`), `internal/myce/shared_host.go`
  (`nice -n 10 taskpolicy -b`).
- S65. Raft paper §5.4.1 (election restriction): a "voter denies its vote if
  its own log is more up-to-date than that of the candidate".
- S66. [SQLite PRAGMA synchronous](https://sqlite.org/pragma.html#pragma_synchronous):
  "FULL is atomic, consistent, isolated, and durable (ACID) in WAL mode"; with
  NORMAL a committed transaction "might roll back following a power loss or
  system crash".
