# How a mycelium keeps working when hyphae die

Research for [issue #30](https://github.com/1kuna/mycelium/issues/30), under [map #17](https://github.com/1kuna/mycelium/issues/17). **As of 2026-10-14.** Research only; this report proposes decisions for the future spec and does not change the approved design. [1–4]

## Question

How can a home mycelium of a few to a few dozen hyphae survive losing any one hypha, including the spore, without recreating v1's complicated peer network? The glossary defines the spore as the first hypha, not a permanent authority. The agreed sequence is one designated always-on hypha now, automatic standby later, and peer coordination eventually; each stage must remain simple to run and test on a laptop. [1–4]

## Short answer

**Keep one place assigning work first; make that role replaceable later.** Today's queue can recover when its process restarts with its SQLite state intact, but that is different from continuing after the queue hypha and its disk disappear. Backups give recovery; automatic takeover also needs durable shared decisions and a way to prevent competing assignment authorities. Gossip alone provides neither. [4–9]

**Recommendation:** first retain local SQLite and establish a simple, explicit recovery path using SQLite's backup facilities or Litestream. Describe this honestly as recovery with an interruption and possible loss of recent writes. Later, if surviving any single hypha must be automatic without losing acknowledged jobs, use an existing Raft implementation with three stable voting hyphae and one active assignment role. Do not invent a two-hypha election protocol. Eventually let eligible hyphae take that role, with gossip only if discovery actually needs it. These are recommendations, not approved implementation changes. [2,4–12]

## What must survive

The current approved design has one queue process, one local SQLite file, plain-command workers, no peer network, no leader election, and no replicated state. Operations specifies one-second worker check-ins and a restart reconciliation period. The design documents reconnecting workers and persisted batch state; operations explicitly excludes exactly-once external side effects and durable replay of started model streams. These are documented contracts, not new runtime measurements. [4,5]

The useful distinction is between four failures:

- **A working hypha disappears:** return eligible work to the queue, resume complete checkpoints where possible, reject stale results, and reconcile overlapping attempts. Today's design already addresses this. Losing the only hypha with the required resource still means an explained wait; replacing coordination cannot create that resource. [4,5]
- **The assignment process stops but its hypha and storage remain:** restart against the same durable state and reconcile before replacing unreported work. This is today's recovery path. [4,5]
- **The assigning hypha or its storage disappears:** another hypha needs recoverable queue state, a discoverable endpoint, and exclusive authority to assign work. A backup alone supplies only the state-recovery part. [6–9,11]
- **Communication breaks while both sides keep running:** silence does not prove death. A majority-based authority can prevent conflicting committed queue decisions, but disconnected commands may still run alongside replacements. Job output needs separate retry safety. [5,8,10,13]

For the next spec, define “survive” explicitly: time until new assignments resume; which acknowledged submissions may be lost; how unfinished work resumes; and what happens when no safe authority or compatible resource is reachable. Recommended default: preserve authoritative state and show an explained pause rather than assign from conflicting histories. This is a design inference from the existing contracts and Raft's partition behavior. [4,5,8]

## Findings from primary sources

### 1. Leader election must protect state, not just select a name

Raft combines election, an ordered replicated log, and rules ensuring a newly elected leader contains committed history. Each voter grants at most one vote per term; a candidate needs a majority; randomized timeouts help resolve split votes. Committed state remains available through a leader change while a communicating majority remains. An isolated old leader may still believe it is in charge, but cannot commit new decisions without a majority. [8,11]

For a fixed voting set, majority arithmetic means **two voters tolerate zero losses; three tolerate one; five tolerate two**. Adding a few dozen working hyphae need not enlarge the voting set: Nomad and Consul separate voting servers from clients. Three stable voting hyphae are therefore a plausible later-stage choice; daily-use laptops that sleep should not be required votes. The suitability judgment is specific to the home-use goal, not a benchmark result. [8,11,12]

A “witness” is not a magic escape from this requirement: ordinary Raft voters persist the replicated log. Three voters can supply the deciding majority; an external lock service instead moves the authority dependency elsewhere. Safe changes to voting membership also need the consensus protocol's configuration-change rules, not independent edits to a discovery list. [8,12]

Raft's client section addresses a committed command whose response is lost: retries can duplicate it unless the state machine records unique request identifiers and prior responses. That protects replicated commands; it does not guarantee that an arbitrary shell command publishes an external result once. For Mycelium, retain attempt identities and retry-safe outputs, and propose stable submission identifiers and a durable assignment generation before automatic takeover. The latter are proposed requirements, not existing features established here. [5,8]

### 2. Gossip answers “who seems reachable?”, not “who may assign work?”

memberlist describes gossip-based membership and failure detection as eventually consistent. Serf describes its discovery, membership and event service as masterless. Consul uses Serf for membership, discovery and failure detection, with a separate Raft layer for authoritative decisions; Nomad follows the same separation. Thus a masterless membership service does not make a job queue masterless or prove that a suspected-dead spore has stopped. [9–12]

**Implication for simplicity:** keep the current check-in model first. Consider Serf/memberlist later only for a concrete discovery requirement that direct check-ins cannot meet. Do not turn every resource report or job transition into a gossip conflict-resolution problem. This recommendation follows v1's documented simplicity lesson and the distinction between membership and consensus. [4,9–12]

### 3. Nomad/Consul show the pattern, but would add another platform

Nomad uses a small Raft server set and clients that register, heartbeat, and execute allocations. Its documented deployment table recommends three or five servers. Consul likewise requires a majority and recommends three or five servers; gossip discovery is separate. This is a useful pattern to borrow, without adopting a complete scheduler and service-discovery system. [11,12]

Nomad explicitly documents disconnected allocations continuing while replacement allocations start, followed by reconciliation. Its client-side stop timer also has clock-drift limitations. This confirms that consensus over assignment state and control of already-running work are separate problems. Mycelium's existing attempt-specific reconciliation and cleanup obligations must survive takeover; an expired check-in must not erase a physical resource hold. [5,13]

**Assessment:** deploying Nomad or Consul solely for this ticket would add server operation and recovery responsibilities to the existing Mycelium program. It is reasonable only if that platform is independently wanted; otherwise an existing embedded consensus library is a narrower eventual candidate. This is an architectural judgment based on their documented components, not measured operating cost. [4,11,12]

### 4. Ray illustrates why working-hypha recovery is not spore recovery

Ray retries tasks after working-server failures, and reconstructing lost result objects can rerun a task that already finished. Its documentation distinguishes this from losing the head server, which fails the whole installation absent appropriate recovery. Configured GCS recovery can preserve existing tasks while management operations are unavailable. Ray still requires that only one GCS be alive at a time. [14–16]

As read on the research date, Ray's GCS documentation lists external highly available Redis and an embedded RocksDB option marked alpha, Linux-only, requiring durable reattachable storage. Its supported external-Redis recovery setup is scoped to KubeRay for Ray Serve; other setups need their own failure detection and restart mechanisms. A durable metadata backend is therefore not itself general automatic head replacement. [16]

**Assessment:** Ray is useful evidence about separate recovery layers, but replacing plain-command workers with its runtime does not remove durable-state, exclusive-authority or external-output questions. That would widen this research ticket into a runtime redesign. [4,14–16]

### 5. SQLite backup and replication options solve different problems

**SQLite itself.** The online backup API can create a consistent snapshot of a live database; the official page also describes `VACUUM INTO`. WAL requires processes on the same machine and does not work over an ordinary network filesystem. Separating a live database from its WAL can lose committed transactions or corrupt the copied database. Do not “solve” standby by sharing the current SQLite file over a NAS or copying just its main file while it is active. [6,7]

**Litestream: simple recovery, not automatic authority transfer.** Its background process copies SQLite WAL changes to a replica asynchronously. The official tips describe a recent-write loss window and warn that multiple applications replicating into the same destination can prevent restoration. Its SFTP destination allows backups to remain on home-owned storage; that is an option, not authorization to configure or access it here. Prefer the supported backup tool over custom WAL shipping. Restore/promotion still needs an explicit rule ensuring the former assigning process cannot remain authoritative. [17–19]

The one-second S3 replication default documented by Litestream is **not a guaranteed one-second loss bound**: asynchronous replication can lag or be unavailable. A locally acknowledged job may not yet exist in the replica. Consequently, restoring an asynchronous backup cannot promise preservation of every acknowledged job after permanent loss of the original disk. Do not import Litestream's performance-oriented `synchronous=NORMAL` suggestion into Mycelium's local durability contract; SQLite documents that NORMAL omits per-commit WAL synchronization. [4,7,17,18]

**LiteFS: live copies, but still asynchronous.** LiteFS intercepts SQLite filesystem writes, captures transaction page sets, and replicates them to other machines. Its documented automatic role selection uses Consul leases. It does not wait for replicas before completing a local transaction; a former writer with unreplicated changes can be reset from the new writer. Consul's authority does not make the separate SQLite replication synchronous. [20,21]

LiteFS adds a FUSE filesystem and, for its documented automatic failover, Consul. The current Fly documentation says it cannot provide support or guidance, describes pre-1.0 status, and warns that Fly autostop/autostart can let stale machines win a lease and discard newer state. That warning is explicitly about Fly's autoscaler, not a demonstrated home-network failure; it is still a reason to investigate freshness and support before selecting LiteFS. This report does not establish native cross-platform suitability. [20–22]

**rqlite: majority-backed database decisions, with an interface change.** rqlite uses Raft around SQLite and is explicitly not a drop-in SQLite replication tool. Applications send SQL through its API rather than modifying the underlying SQLite file. Transactions are grouped within API requests; explicit application-managed `BEGIN`/`COMMIT`/`ROLLBACK` sequences are unsupported. Adopting it would require inspecting and changing Mycelium's database access patterns. [23,24]

Normal rqlite writes wait for majority commitment, whereas queued writes can acknowledge before persistence and can duplicate after a lost response. Its default weak reads rely on a server's belief that it is leader; linearizable reads verify leadership through the read. A future design should use committed writes and authoritative reads for assignments, not confuse fast queue admission or a stale status read with a durable decision. Even then, database consensus alone does not fence shell processes or replicate checkpoints and uploads. [5,8,23–26]

## Options for a home mycelium

The ratings below are comparative design judgments based on the cited mechanisms, not measured RAM, throughput or recovery times.

| Option | Single-hypha loss behavior | Trade-off and fit |
|---|---|---|
| Current one queue + local SQLite | Same-storage restart recovers; permanent storage loss has no replica in the approved design. | Smallest present system. Retain first, but do not claim automatic spore-loss survival. [4,5] |
| One queue + consistent snapshots or Litestream + manual replacement | Recover on another hypha from the available backup; interruption and recent-write loss depend on backup freshness. | Simplest proposed recovery improvement. Requires separate storage and explicit exclusive promotion; a backup is not a live standby. [6,17,18] |
| Two queue-capable hyphae + asynchronous replica | Live replica may shorten recovery, but cannot preserve every recent acknowledged write; safe automatic ownership needs another authority or an independently proven fence. | Manual standby is possible; reject “whichever misses a heartbeat loses” as the automatic rule. Two Raft voters cannot tolerate one loss. [8,18,21] |
| LiteFS + Consul | Automatic database-writer replacement with a majority-backed lease service; asynchronous SQLite state may lose recent writes. | Filesystem and Consul dependencies, freshness risks and current support caveats make this a poor default. [12,20–22] |
| Three stable voting hyphae using rqlite or existing embedded Raft | Can preserve committed queue decisions and replace the active role after one voter disappears, while a majority remains reachable. | Best eventual safety fit; rqlite adds a database service/API migration, embedded Raft requires deterministic state-machine and snapshot integration. Compare those integrations before choosing. [8,11,23–26] |
| Nomad/Consul or Ray adoption | Recovery depends on their documented server/GCS setup, not merely installing working clients. | Broader platform change than this question needs; consider only for an independent requirement. [11–16] |
| Peer discovery with Serf/memberlist | Discovery and failure suspicion continue without one central membership service. | Useful eventual discovery; insufficient alone for durable queue ownership. Keep consensus distinct. [9–12] |

For a few to a few dozen hyphae, **the number doing work and the number deciding authoritative history should be separate**. A fixed three-hypha voting core can serve many nonvoting hyphae. A two-hypha home can retain one authority and manual recovery, but ordinary Raft cannot offer safe automatic continuation after either voter is lost. A single remaining hypha cannot both preserve majority safety and keep accepting new decisions in a larger fixed voting set. [8,11,12]

## Staged recommendation

### First: keep today's shape and make recovery understandable

1. Retain one designated always-on assigning hypha, local SQLite, existing check-ins and attempt-specific reconciliation. The spore is an origin, not a permanently required machine. Do not add gossip, leases or replicated registries to today's implementation. [2–5]
2. Propose consistent backups to a different storage failure domain. Compare SQLite snapshots with Litestream based on acceptable loss of recent submissions and recovery effort; use the existing tool rather than building replication. Show backup freshness and an explained recovery pause. A backup on the same failed disk does not meet the goal. [6,17–19]
3. Specify manual replacement: establish that the former authority cannot issue accepted assignments, restore a consistent copy, reconnect through a stable discoverable queue identity, and reconcile exact attempts before replacing work. If exclusive authority is uncertain, remain paused. This is a proposed recovery contract derived from the cited failure cases. [5,8,13,18]

**Honest limit:** this stage improves recovery; it does not fully meet automatic survival of any hypha. Losing unreplicated acknowledged submissions remains possible after permanent disk loss. Preserve that gap explicitly for the standby decision ticket rather than rename recovery as high availability. [2,17,18]

### Later: automatic standby without conflicting histories

Propose three stable voting hyphae, with one active assignment role and the others ready to take it. Every accepted submission, cancellation, assignment and unresolved cleanup obligation must be preserved in authoritative replicated state before it is acknowledged or dispatched. Use one mature consensus implementation; do not write custom elections or bolt an election onto an asynchronously copied queue and assume equivalent guarantees. This is the recommended acceptance contract, grounded in Raft's committed-history guarantees and Mycelium's existing custody rules. [5,8]

Before selecting rqlite versus embedded Raft, compare the actual transaction/API migration against deterministic state-machine, snapshot, restore and schema-upgrade work. rqlite supplies the database's replication; embedded Raft can retain the one-binary goal but leaves those integration obligations to Mycelium. Neither is a one-line switch. This report recommends the decision boundary, not an unverified implementation winner. [4,8,23,24]

Proposed laptop-only acceptance criteria for that later implementation: lose any one voter including the spore; lose the active assigning process just after commit but before reply; isolate the old authority; reconnect an old attempt; change voting membership safely; and retain physical cleanup holds. Demonstrate that only committed decisions dispatch, acknowledged submissions survive one voting-hypha loss, and lost replies do not create duplicate submissions. Timing and scheduling tests must keep the injected-clock rule. **These are future checks, not experiments performed for this report.** [5,8,27]

Queue replication must be paired with a separate plan for inputs, checkpoints, model artifacts and uploads. A recoverable queue cannot resume a job whose only checkpoint or required resource died with a hypha. The vision does not require a NAS; optional storage and file movement need their own survival contract. Keep that work with the map's storage decisions. [2,4,5]

### Eventually: no indispensable hypha

Let eligible hyphae hold and transfer the assignment role without a privileged spore. Add gossip discovery only when needed; keep authoritative ownership in consensus. Growing peer participation is not permission for every isolated partition to schedule independently. Revisit this stage once the standby contract works simply and unattended, or if fixed always-on voting hyphae cannot fit home participation patterns. [2,3,8–12]

“Survive losing any one hypha” is achievable with sufficient replicated state and remaining resources. “Continue safely through every partition or after all but one hypha disappear” is a different requirement and is not promised by Raft. Shared power, network and storage can also remove the communicating majority; the safe response is an explained pause. [4,8]

## Confidence, limits and current state

**High confidence** in the documented distinction between recovery backups, gossip membership, majority-backed authoritative state, and job retry safety. **Moderate confidence** in the staged home-use recommendation: integration and operating burden have not been measured. [5–26]

This was a primary-source review only. No source changes, SSH, credentials, fleet access or experiments were used. No failover was qualified. I could not verify Mycelium integration with these tools, actual home-network recovery times, replica lag, restore behavior, hardware/filesystem durability, native operating-system coverage, or application-specific output safety. Moving documentation was read on the date above; version-specific behavior must be checked again before implementation.

**Current state:** research complete; proposed direction is one simple recoverable queue first, majority-backed automatic replacement later, optional peer discovery eventually. **Next step:** the orchestrator uses this report in the map's coordination decision and defines acceptable interruption/data loss, stable voting participation, and storage survival before revising the approved spec. Issue #30 remains open for the orchestrator; this research branch is never merged. [1,2]

## Sources and dates

All sources are primary: project decisions/docs, official vendor/project docs, project-owned source repositories, or the original Raft paper. **All external pages were accessed 2026-10-14.** Publication/update dates are stated below only where the source supplies one; undated pages are not assigned an invented publication date. Source terminology such as “server,” “leader,” and “client” is used when describing those systems, not to rename Mycelium's hyphae.

1. [Mycelium issue #30: research question](https://github.com/1kuna/mycelium/issues/30), project ticket, read 2026-10-14.
2. [Map #17](https://github.com/1kuna/mycelium/issues/17) and [vision round 2, Q9/Q10](https://github.com/1kuna/mycelium/issues/16#issuecomment-6175064702), project decisions, comment dated 2026-10-14.
3. [GLOSSARY.md](https://github.com/1kuna/mycelium/blob/2587b223b8d711080fb4a6ace3182a6e56fa0b62/GLOSSARY.md), vision branch snapshot, commit dated 2026-10-14.
4. [Approved design](https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/design-proposal.md), approved 2026-10-08, main snapshot dated 2026-10-12; especially “The parts,” leaving/loss, power/network loss and “Not in the first version.”
5. [Operations](https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/operations.md), same main snapshot; especially monitoring/failures, restart defaults and Docker cleanup custody. Private operational examples are not reproduced here.
6. [SQLite Online Backup API](https://sqlite.org/backup.html), SQLite documentation, undated.
7. [SQLite Write-Ahead Logging](https://sqlite.org/wal.html), SQLite documentation, undated page; WAL network-filesystem, synchronization and sidecar requirements.
8. [Ongaro and Ousterhout, *In Search of an Understandable Consensus Algorithm*, extended version](https://raft.github.io/raft.pdf), 2014-05-20; §§2, 5–6 and 8.
9. [memberlist README](https://github.com/hashicorp/memberlist/blob/master/README.md) and [Serf README](https://github.com/hashicorp/serf/blob/master/README.md), project-owned moving source documentation, undated.
10. [Consul gossip architecture](https://developer.hashicorp.com/consul/docs/architecture/gossip), official documentation, undated.
11. [Nomad consensus](https://developer.hashicorp.com/nomad/docs/concepts/consensus), [architecture](https://developer.hashicorp.com/nomad/docs/concepts/architecture), and [gossip](https://developer.hashicorp.com/nomad/docs/concepts/gossip), official documentation, undated.
12. [Consul consensus architecture](https://developer.hashicorp.com/consul/docs/architecture/consensus), official documentation, undated.
13. [Nomad `disconnect` job specification](https://developer.hashicorp.com/nomad/docs/job-specification/disconnect), official documentation, undated.
14. [Ray node fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/nodes.html), official latest documentation, undated.
15. [Ray task fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html), official latest documentation, undated.
16. [Ray GCS fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/gcs.html), official latest documentation, undated; current Redis/RocksDB options and single-GCS requirement.
17. [Litestream: how it works](https://litestream.io/how-it-works/), official documentation, undated; current page describes v0.5/LTX behavior.
18. [Litestream tips](https://litestream.io/tips/), official documentation, undated; data-loss window, synchronization and competing-writer warnings.
19. [Litestream SFTP replication](https://litestream.io/guides/sftp/), official documentation, undated; read only, no SSH performed.
20. [LiteFS overview and project status](https://fly.io/docs/litefs/), official Fly documentation, undated; current support and Fly-autoscaler warnings.
21. [How LiteFS works](https://fly.io/docs/litefs/how-it-works/), official documentation, undated; Consul leases and asynchronous divergence recovery.
22. [LiteFS FAQ](https://fly.io/docs/litefs/faq/), official documentation, undated; FUSE and recovery trade-offs.
23. [rqlite FAQ](https://rqlite.io/docs/faq/), official documentation, last modified 2026-07-25; consensus, direct-file access and transaction limitations.
24. [rqlite Data API](https://rqlite.io/docs/api/api/), official documentation, last modified 2026-10-12; transaction batches and request forwarding.
25. [rqlite read consistency](https://rqlite.io/docs/api/read-consistency/), official documentation, last modified 2026-10-01; weak versus linearizable reads.
26. [rqlite queued writes](https://rqlite.io/docs/api/queued-writes/), official documentation, undated publication here; pre-commit acknowledgement and retry trade-offs.
27. [Mycelium AGENTS.md](https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/AGENTS.md), main snapshot dated 2026-10-12; one-binary/local-testing and injected-clock constraints.
