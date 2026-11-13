# Queue disk I/O

Measured queue disk contention, SQLite backup and commit limits, systemd I/O
weighting and the opt-in I/O experiment. For backups use the
[stopped-state procedure](../operations.md#queue-backup-restore-and-relocation).

## Queue disk contention (#527)

The 2026-11-05 native test on queue release s074-r2 (`b71c29ca`) did not reproduce
the check-in deadline failure under bounded gate-like I/O. Test-class job
`1179401000178688d5384d7083878797` ran four writers on the queue's backing device,
inside the ordinary worker attempt cgroup: 2 GiB written over 90.001 seconds
(22.755 MiB/s), fsync every 4 MiB, at most 256 MiB of disposable payload, with
90-second before/after phases. Payload occupied less than 0.15% of measured free
space. The experiment finished at 20:59:38Z, with zero attempt losses; its
accepted result was retrieved and verified, payloads removed and processes exited.
No service configuration or production database was changed or accessed.

Before / during / after check-in p50 was 316.812 / 322.027 / 319.888 ms;
p95 was 909.931 / 744.890 / 958.325 ms; p99 was
1035.339 / 969.471 / 1124.875 ms. These are the API's rolling 256-sample windows
read near each phase end, not independent phase histograms; windows can retain
nearby phase samples. Journal counts use the exact phase timestamps. Slow
control requests (>1 second) numbered 4 / 1 / 15, or 2.67 / 0.67 / 10.00 per
minute, excluding intentional event/wait long polls and the three status reads
used to observe latency. Slow check-ins numbered 4 / 1 / 10, with maxima
1080.377 / 1136.772 / 1399.770 ms. No check-in over the unchanged ten-second
deadline or local worker check-in timeout was observed. Background work was
uncontrolled, and recovery had more slow requests; this is bounded native
deadline evidence, not proof of causal isolation or unlimited I/O headroom.

This supports removing CI's architecture-wide full-gate exclusion: the existing
Linux profile's amd64 and arm64 targets are both eligible again in CI, without
host pins or resource-policy changes. It does not qualify live backups, larger
bursts, or sustained bulk maintenance. Keep those off the queue's backing device
until separately measured. Another directory or partition on the same device
provides no physical isolation. No effective I/O weighting was present in this
test (no delegated io controller, scheduler `none`, no configured io.cost).
Native protection remains unqualified; [#527](https://github.com/1kuna/mycelium/issues/527)
retains that broader acceptance and the control options below.

## SQLite backup and commit limits

Python `Connection.backup(pages=256, sleep=0.05)` does not throttle successful
steps: CPython sleeps only on BUSY/LOCKED. Concurrent source changes can restart
the copy and increase writes beyond the database size. See
[CPython's implementation](https://github.com/python/cpython/blob/v3.14.0/Modules/_sqlite/connection.c)
and [SQLite backup semantics](https://sqlite.org/c3ref/backup_finish.html).
Use a progress callback or external I/O controls when bounding an online copy;
use the [stopped-state backup procedure](../operations.md#queue-backup-restore-and-relocation)
for a complete queue recovery generation.

Queue mutations commit under the queue mutex with WAL and `synchronous=FULL`.
Synchronization and automatic checkpoints can delay other control calls.
Unlocking before commit would break durable publication/rollback semantics.
Mutable state and WAL checkpointing can cause sustained write amplification
without database growth. Process counters do not distinguish database, WAL,
logs and payload writes; whole-disk averages do not establish a safe I/O ceiling.

## Slow check-in body attribution (#1568)

The existing `slow_queue_mutex_hold` event logs body phase totals for check-in
transactions, after releasing the mutex. `fn_body_ms` includes both the check-in
callback and shared transaction work that follows it. The non-overlapping
`body_checkin_inventory_ms`, `body_checkin_memory_ms`,
`body_checkin_reconcile_ms`, `body_checkin_maintain_ms`,
`body_checkin_attempts_ms`, `body_checkin_plans_ms` and
`body_transaction_tail_ms` attribute that time. Inventory includes accepted
health/inventory and session/release handling; reconcile includes preparation,
checkpoint-expiry, model and environment reports. Plans includes deferred reply
construction, including admission budgets and preparation plans.

Additional `body_maintain_ms`, `body_placement_ms`, `body_environment_plan_ms`,
`body_qualification_plan_ms`, `body_model_plan_ms`, `body_copy_plan_ms`,
`body_health_ms`, `body_efficiency_ms` and `body_retained_index_ms` are inclusive
helper timings: do not add them to the enclosing phases. For example, health
detection is inside the transaction tail. All timings use an observational clock
and never affect scheduling. The logger's existing adaptive admission remains
unchanged; timestamps and fixed counters are collected without per-job logs.

Reproduce the incident shape on a laptop with
`go test ./internal/myce -run '^$' -bench '^BenchmarkCheckinProductionScale$'
-benchtime=20x -cpuprofile=checkin.cpu -mutexprofile=checkin.mutex`. The synthetic
fixture has 10,200 retained executions and submission bindings, 75 current jobs,
five occupied workers, a 29-piece production fan-out, 40 waiting production
stages and 1,400 learned curves. It advances the injected decision clock through
worker rounds, measures body/all holds and their maxima, and verifies running
custody and dependency waits survive. CPU samples labelled `work=checkin-body`
include post-unlock diagnostics; a CPU hotspot alone does not prove mutex hold
time. Compare it with the measured locked phases.

The multi-second live body cause remains unknown until reproduced or attributed
by a representative live slow hold. Release acceptance must identify the
dominating body phase, measure submission p90 from all submissions (target under
one second), and inspect wait errors and 429/503 rates separately from normal
long-poll duration. Synthetic laptop results alone cannot establish that proof.

## Standard systemd policy

The installer accepts `--io-weight=1..10000` for Linux queue and worker services,
emitting `IOAccounting=yes` and `IOWeight=VALUE` on the existing service. Zero
leaves system defaults, emits neither directive and clears an explicit setting.
Omitted settings survive upgrades; existing installations default to zero.
macOS rejects nonzero values rather than silently claiming enforcement.

A worker weight of 10 versus the queue's system default 100 is an initial
relative-priority candidate, **not a measured latency guarantee**. Weighting
costs competing jobs throughput under contention; measure the ratio natively.
The aggregate service policy covers preparation, output collection and existing
attempt descendants, without adding more share per additional job. It also
lowers worker control-file write priority; qualify contact and those writes too.
Configure every worker service instance sharing the device through the installer.

Weights require an available/enabled cgroup-v2 I/O controller and an effective
weighting implementation such as BFQ or configured io.cost. `none`/`mq-deadline`
alone do not supply proportional weighting. Check the common-ancestor hierarchy:
weights compare siblings and do not prioritize a service over an uncontrolled
competing branch. A `systemctl show` property is not enforcement evidence.
An SSH backup outside the worker subtree is not covered by its policy.

If effective weights miss the existing deadline, use standard `io.max`/systemd
`IOWriteBandwidthMax` and `IOReadBandwidthMax` at the aggregate worker subtree
on the actual backing device. Derive caps from a measured latency sweep under
representative read/write bursts and fsync, rather than advertised sequential
bandwidth. Caps cost stage time: measure that too. Put bulk operator tools in
controlled scopes or keep them off that disk. No unmeasured cap is proposed.
For relocation, use the [queue recovery procedure](../operations.md#queue-backup-restore-and-relocation).

## Laptop experiment and native acceptance

Check free space, memory and active work before opting in:

```sh
MYCE_TEST_QUEUE_DISK_IO=1 GOMAXPROCS=4 go test ./internal/myce \
  -run '^TestQueueDiskIOReproduction$' -count=1 -v
```

The real dev HTTP API, SQLite queue and CPU command runner share one filesystem.
Four finite writers fsync every 4 MiB, write 2 GiB total and occupy at most
256 MiB of payload space. Test-owned scratch is removed after worker shutdown.
The two-minute harness context bounds a broken local experiment's resource
cost; it does not change ordinary fleet-job timeouts. Plain native execution
uses existing unsandboxed dev behavior. A passing test proves execution/cleanup,
not reproduction of the incident or native I/O protection.

Run an authorized Linux experiment through Mycelium with platform
targets and no host pins, with a disposable queue on the job's filesystem. Record
device/controller/scheduler readback, actual job I/O rates, commit duration,
check-in p99/max and losses before/after policy. Include an isolated, bounded
live-backup reproduction and verify competing writers' scope membership.
The unchanged ten-second check-in deadline must hold under representative
production bursts, without false quiet losses. Collect every workload failure,
retain a compact receipt and remove disposable payloads after process release.
Native Linux proof and #527's measured-protection acceptance remain open.
