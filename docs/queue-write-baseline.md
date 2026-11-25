# Queue write baseline and SQLite transaction changes

This records the single-queue work for [#2092](https://github.com/1kuna/mycelium/issues/2092), under [MY-40](DECISIONS.md#my-40). The migration proposal and decision questions are in [#2040](https://github.com/1kuna/mycelium/pull/2040).

## Measurement method

The laptop fixture extends the [#2087](https://github.com/1kuna/mycelium/pull/2087) reproduction with real SQLite `synchronous=FULL` commits and 11,500 retained jobs, two running, ten queued and 19 paused. Five synthetic workers check in once per second and keep event polls open. Three status clients and four batch-wait clients make concurrent HTTP requests. The setup and WAL truncation precede the sample. Automatic WAL recycling is disabled during the 30-second window. CPU includes the loopback load driver and the queue's background workers.

The comparison reports bound values for inserts and upserts in the queue-state and usage writers, with eight bytes per numeric argument. WAL bytes include SQLite page writes and transaction framing. Commits per class count transactions containing at least one such write, so a transaction can count toward several classes. The compact control row includes intent, custody, contact and usage fields. The detached admission-wait field can be attributed directly. The job row can still carry small diagnostics alongside its custody and outcome fields. Idle reproduction produces neither a representative submission or log stream nor a representative retention sweep. Deletes, writes in auxiliary tables, SQLite index amplification and the diagnostic side database are outside the comparable value-byte rates. SQLite commit latency includes the synchronous barrier and SQLite's own commit work.

Run the current fixture with:

```sh
GOMAXPROCS=4 go test ./internal/myce -run '^$' -bench '^BenchmarkQueueIdleTraffic$' -benchtime=1x
```

For the comparison, the before revision was `53d2427b7f35d5468b25f26fdc43646290d6f668` with only the fixture's observational callbacks ported. Run three 30-second windows per revision, alternating revisions on the same Darwin arm64 laptop with Go 1.25.6. Report the median of each window's value.

## Laptop results

| Measure | Before | After |
| --- | ---: | ---: |
| Check-in mutex hold p50 / p95 (ms) | 6.64 / 12.71 | 4.84 / 5.58 |
| Check-in HTTP latency p50 / p95 (ms) | 7.99 / 26.49 | 8.29 / 26.19 |
| Queue CPU (CPU-s/min) | 12.31 | 11.85 |
| WAL (B/s) | 598,362 | 494,469 |
| SQLite commits/s | 11.59 | 11.54 |
| SQLite FULL commit p50 / p95 (ms) | 1.33 / 14.78 | 2.25 / 15.90 |

Each window completed 150 HTTP check-ins. Check-in p95 by window was 29.23, 25.12 and 26.49 ms before, and 26.19, 324.8 and 21.89 ms after. The 324.8 ms window also had the highest commit time per wall second (110.9 ms/s). It remains in the comparison. The measurements establish a shorter physical mutex hold. They leave end-to-end tail latency sensitive to storage and the writer turn.

| SQLite write class | Before B/s | Before commits/s | After B/s | After commits/s |
| --- | ---: | ---: | ---: | ---: |
| Compact control state and its fields | 56,508 | 11.59 | 50,092 | 11.54 |
| Detached wait diagnostics | 605,164 | 5.62 | 585,105 | 5.56 |
| Job records and other job fields | 1,264 | 0.10 | 1,265 | 0.10 |
| Usage accounting rows | 2,702 | 11.59 | 2,684 | 11.54 |
| Submission, machine, learned-catalog, health, enrollment and log rows | 0 | 0 | 0 | 0 |

Detached wait diagnostics dominated the fixture's write payload. The concurrent [#2082](https://github.com/1kuna/mycelium/issues/2082) follow-up owns their serialization and evaluation cadence. These measurements precede that change.

A supplementary fixture copy/reopen measurement stopped when the startup preflight reported `legacy_state_present` for the scratch copy. The copy/reopen timing remains unverified. Crash and restart tests reopen their private queue database in its original layout.

## Read-only queue-host sample

At 18:20:23–18:20:38 UTC on 2026-11-25, the queue used 24.51 CPU-seconds in 15 wall seconds (163.4% of one core). The co-located worker used 15.70 CPU-seconds (104.7%). These are process CPU deltas from `/proc/PID/stat` at 100 ticks per second. The non-loopback interface totals rose by 3,468 bytes received and 2,119 sent, or about 231 B/s received and 141 B/s sent. The interface drop counters did not advance. The byte rates describe this host's interfaces during this window, with all processes included.

Three five-second `iostat` intervals showed SSD flush rates of 14.4, 13.2 and 10.2 per second and average flush waits of 3.51, 17.36 and 24.88 ms. Corresponding write rates were 271,650, 159,526 and 68,341 KiB/s across the device. These are shared-device observations. The queue's process I/O counters were inaccessible to the sampling account. [#2017](https://github.com/1kuna/mycelium/issues/2017) separately recorded a 4.9 ms median SQLite commit in 43 live check-ins on 2026-11-24 and a 487 ms commit in the longest sampled hold. The windows had different traffic and measure different layers.

Live record-class bytes and commit counts, database size and copy/reopen time, queue-specific network bytes, other machines' flush and network rates, and inter-machine RTT and packet loss remain unmeasured. The authorized sample covered passive OS counters on the queue host. A fixture result cannot fill those fleet measurements. The [#2082](https://github.com/1kuna/mycelium/issues/2082) post-release live comparison is separate.

## Single-host assumptions in the code

| Area | Current dependency |
| --- | --- |
| Queue serving and persistence | The queue command opens one database and starts its HTTP/TLS listeners and one maintenance loop. `Open` owns a local advisory lock, one SQLite writer, a local WAL and a separate read connection. Startup reconstructs the planner and the immutable read index from those records and marks prior sessions unreported. [Queue startup](../cmd/myce/main.go), [database ownership](../internal/myce/queue.go), [startup reader](../internal/myce/queue_persistence_read.go), [maintenance loop](../internal/myce/http.go) |
| Ordering and in-process state | One process orders submissions, assignments, launch and Started authorization, release receipts, outcomes and policy revisions. Mutable state, the fair writer turn, wait revisions, stop events, caches, health delivery, file lifecycle locks and the injected control opportunity clock share its address space. Worker report latency, freshness, restart grace and network-quiet observations come from that process. [Queue](../internal/myce/queue.go), [writer turn](../internal/myce/queue_mutex.go), [waits](../internal/myce/wait_index.go), [stop events](../internal/myce/events.go), [worker contact](../internal/myce/queue_contact.go) |
| Routing, discovery and trust | Workers, CLI/SDK clients and observers use one configured/discovered queue address. Discovery publishes a queue endpoint and TLS pin. Workers reconnect to that authority, buffer reports locally and reconcile against it. Joining identity, TLS keys, invitation material and owner credentials live with the queue installation. Enrollment tickets are one-use rows in its database. [Discovery](../internal/myce/discovery.go), [joining identity](../internal/myce/joining.go), [tickets](../internal/myce/enrollment_ticket.go), [installed clients](../cmd/myce/installed_client.go), [worker](../internal/myce/worker.go) |
| Installation, backup and relocation | Queue and worker are separate service roles. Queue startup receives the local database, file, spool, identity and checkpoint paths. Service readiness, backup and restore copy the queue's authority as a set. The worker's normal update route has a queue-owner-local path. [Install](../internal/myce/install.go), [state roots](../internal/myce/queue_roots.go), [readiness](../cmd/myce/queue_startup.go), [operations](operations.md#queue-backup-restore-and-relocation) |
| Owner provisioning and releases | Owner writes use the local queue role. Queue startup reads local model, environment and host/project policy inputs, signing trust, release manifest and certificate inputs. Release approvals bind once to provisioned trust. Fleet-update coordination runs in the queue process and treats the queue host specially, with its own installed service proof and rollback phases. [Command startup](../cmd/myce/main.go), [release approval](../internal/myce/enrollment_release.go), [fleet coordinator](../internal/myce/fleet_queue_runtime.go), [upgrade procedure](reference/fleet-upgrades.md) |
| File storage and transfer | The queue verifies one file store at startup, including bundle custody. Uploads, download URLs, staging fingerprints, file-backed checkpoints and results point at that store. The queue has a single capacity and reservation ledger, a local upload spool, local physical-space probes, and a collector synchronized with local reference publication. Workers have a separate local file cache and replay spool. [File store](../internal/myce/files.go), [bundle custody](../internal/myce/extension_bundle_store.go), [quota](../internal/myce/file_store_quota.go), [upload spool](../internal/myce/upload_spool.go), [retention](../internal/myce/file_retention.go), [worker cache](../internal/myce/file_cache.go) |
| NAS mounts and asynchronous publication | Queue-local paths and its NAS identity drive submission-time NAS checks, checkpoint availability and expiry, storage compatibility and asynchronous result delivery. Delivery takes a local operation lock and writes to the queue's configured destination root. Exact result/checkpoint fences live in the queue database. Workers can mount the same share at different local paths. [Availability probe](../internal/myce/storage_probe.go), [NAS resolution](../internal/myce/resolve_nas.go), [NAS identity](../internal/myce/onboarding_nas_identity.go), [checkpoint expiry](../internal/myce/checkpoint_expiry.go), [output delivery](../internal/myce/output_delivery.go) |
| Co-located storage and resource measurements | The queue records its physical hostname and uses it to link file-store pressure to a worker on that machine. Request memory admission and capacity observations reflect the queue process and host. Local diagnostics include its profiler, idle history, process health and notifier. [Queue](../internal/myce/queue.go), [co-located pressure](../internal/myce/file_store_worker_pressure.go), [request budgeting](../internal/myce/request_budget.go), [idle evidence](../internal/myce/idle_storage.go), [health alerts](../internal/myce/health_alerts.go), [profiling](../cmd/myce/queue_debug.go) |
| Queue-local input resolution | Inline environment paths and startup catalogs are read relative to the queue environment. Materialized recipes, bundles and submission fingerprints depend on frozen source bytes. Moving the request to another machine would need equivalent input and policy there. [Resolution](../internal/myce/resolve.go), [environment recipes](../internal/myce/environments.go), [command discovery](../internal/myce/command_discovery.go), [model source policy](../internal/myce/model_snapshots.go) |

## Transaction and rollback decisions

The queue prepares SQLite row operations while it owns the writer turn. It releases the physical mutex for the SQLite transaction and keeps the writer turn until publication or rollback. Only the writer sees its speculative journal. Status, job and batch-wait reads can use their committed view during a flush. Reads that need mutable state still wait for the journal to settle. Writer-turn latency therefore remains part of request latency even when physical mutex hold falls. Existing attempt, wait-episode and file custody rechecks run before a batch can enter SQLite.

The contact checkpoint uses the existing three-minute restart window. The running queue keeps current contact and telemetry in memory. Assignment, start and recovery commit the session's restart evidence with job custody. An accepted check-in that raises a current custodian's recovery allowance commits the increase without advancing the periodic checkpoint. Clean close saves the latest restart evidence, and recovery still requires current session evidence before offering work. A commit error restores the in-memory journal and returns retryable HTTP 503 through the existing `request_failed` response. No assignment or acknowledgement from that failed transaction is published.

For one release, starting the queue with `--queue-commit-under-lock` commits queue state under the mutex and saves contact on every accepted check-in, restoring the prior contact durability. Detached history and log cleanup still use the shared writer turn and commit outside the mutex. Both modes use the same SQLite format and keep custody and recovery evidence atomic. A clean restart is enough to switch either way. The local idle-evidence cache remains an independent database. The row batch carries explicit values and local SQL operations, including log sequence allocation and conditional retention deletes. A future log needs stable entry IDs and a defined replicated representation of those operations.
