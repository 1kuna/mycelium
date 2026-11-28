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

Queue mutations commit with WAL and `synchronous=FULL`. The writer keeps its
turn through the SQLite transaction, even when the physical mutex is released.
Synchronization and automatic checkpoints can delay subsequent turns. Committed
read views can coexist with the flush. Publishing before commit would break
durable publication and rollback semantics.

For [#2257](https://github.com/1kuna/mycelium/issues/2257), the r6.63 journal's
52.778 s input-claim transaction held the physical mutex for 66.9 ms. It spent
24.527 s in SQLite statements and 28.185 s in commit while keeping the writer
turn. The 16.534 s submission held the physical mutex for 1.020 s, including
0.524 s of persistence preparation, and spent 15.514 s on the off-lock write.
Older `slow_request` events counted the entire acquired turn as a mutex hold.
`mutex_hold_ms` now reports physical hold time and `off_lock_turn_ms` reports
time spent with the writer turn held and the mutex released. Check-in
`mutex_wait_ms` still includes waiting for the writer turn. On
`slow_queue_mutex_hold`, `off_lock_prepare_ms` and `off_lock_write_ms` separate
bulk preparation from SQLite work outside the mutex.

Issuing and settling claims scan the immutable claim generation before the
writer turn and recheck it before applying edits. A repeated submission keeps
the original settlement timestamp. A later read-only lookup measured the live
legacy claim field at 3,553,299 bytes, with 4,096-byte SQLite pages. The three
logged claim turns during the incident submitted 3,578,979–3,580,069 bytes each
to SQLite. The legacy writer replaces the entire field on each changed claim,
so logical write bytes scale with retained claims. These totals include other
queue-state rows for the turn. They measure SQLite payload, with physical I/O and
fsync duration still dependent on the device and WAL checkpointing.

The incremental layout writes each changed claim as an individual queue-state
row, in the same transaction as admission, allocation, or job settlement. Ship
the reader in the release before enabling the writer (`inputClaimRowsDefault`)
so queue-only rollback can read and write the new rows (MY-42). The first open
with the writer enabled atomically replaces the old field before serving work.
The adjacent-release rollback fixture verifies claim custody in both directions.
After activation, compare claim write bytes and writer-turn duration during a
busy hour. Durable SQLite work can still delay other writers during an I/O stall.
Mutable state and WAL checkpointing can cause sustained write amplification
without database growth. Process counters do not distinguish database, WAL,
logs and payload writes; whole-disk averages do not establish a safe I/O ceiling.

In the [#2245](https://github.com/1kuna/mycelium/issues/2245) incident, I/O PSI
`full avg60` was 54%. One sampled check-in spent 3.64 s inside SQLite commit
out of a 4.43 s writer turn. Another check-in waited 48.7 s for its turn.
`persist_sync_commit_ms` includes SQLite work as well as the sync. The 429
requires a queued mutex wait beyond the learned uncontended interval and a
competing observation in flight. It can therefore occur while the writer is
blocked on storage. Ordinary status, including `--sections machines` with the
current jobs, can use the committed publication without that admission step.
The failed release capture itself had no completed phase measurement.

The subsequent release's `status --sections machines --json` reads spent
6.02–7.41 s in three matching server requests with no measured mutex wait.
They returned about 213 active jobs. The route loaded the retained publication
and ran the job placement projection before reducing those rows to summaries.
A 12,400-retained-job fixture with a busy writer reproduced that unrelated
history decode. The machine-only section now reads detailed machine telemetry,
current job summaries and their named custody references from committed state.
`--sections jobs,machines` still calculates placement diagnostics.

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
detection is inside the transaction tail. `body_maintain_models_ms`,
`body_maintain_deadlines_ms`, `body_maintain_reservations_ms` and
`body_maintain_pool_yield_ms` are inclusive children of `body_maintain_ms`
(#2179). `body_maintain_settle_ms` sums relation settlement, which also runs
outside maintenance. All timings use an observational clock
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
The worker **service** policy covers preparation and output collection in its
cgroup. Installed detached attempts run in independent scopes under the
worker's jobs slice, outside the service. Apply aggregate payload policy at the
jobs slice so each new attempt shares one device budget.

Weights require an available/enabled cgroup-v2 I/O controller and an effective
weighting implementation such as BFQ or configured io.cost. `none`/`mq-deadline`
alone do not supply proportional weighting. Check the common-ancestor hierarchy:
weights compare siblings and do not prioritize a service over an uncontrolled
competing branch. A `systemctl show` property is not enforcement evidence.
An SSH backup outside the worker subtree is not covered by its policy.

### Bound detached job writes on a shared device

On cgroup v2, `IOWriteBandwidthMax` programs `io.max` even with the `none`
scheduler. Use it when detached jobs are the competing writers and effective
weighting is unavailable. Installed Linux workers put their custodian scopes
under `myce-jobs.slice` by default. For a custom worker slice, confirm the jobs
slice using the writer's `/proc/PID/cgroup`. Confirm the queue is outside that
slice. Locate the queue and job-files backing device with `findmnt --target`
and `lsblk`. Verify the actual device number against the jobs slice's `io.stat`
while buffered writes are active. Linux needs filesystem writeback attribution
to charge those writes to their originating cgroup.

The user service manager needs the `io` controller delegated. Check
`systemctl show "user@$(id -u).service" -p DelegateControllers` and the jobs
slice's kernel files. If `io` is missing, the system administrator can add a
drop-in for the service account's `user@UID.service`, with `[Service]` and
`Delegate=cpu io memory pids` on separate lines for a manager currently delegated
`cpu memory pids`. Retain any other controllers already delegated. Reload the
system manager and activate the change during an approved idle maintenance
window. A user manager restart can terminate its queue, worker and attempt
units. Check delegation and active attempt scope membership again afterward.

On a disposable queue first, sweep write rates using representative buffered
writes and fsync bursts. Pick a rate from observed queue commit and read latency
on the backing device. Record job throughput at each rate. Begin with a runtime
property on the jobs slice, run as the installed service user:

```sh
# Set jobs_slice, device and bytes_per_second from the checks above.
systemctl --user set-property --runtime "$jobs_slice" \
  "IOWriteBandwidthMax=$device $bytes_per_second"
group=$(systemctl --user show "$jobs_slice" -p ControlGroup --value)
cat "/sys/fs/cgroup$group/io.max"
cat "/sys/fs/cgroup$group/io.stat"
```

Check `wbps` on the expected device and measure the change in buffered write
throughput. On a disposable Linux ext4 system with the `none` scheduler, a
60,000,000 B/s trial yielded 60.0 MB/s for a buffered writer with `fdatasync`.
The virtual disk and short uncapped write window qualify enforcement only.
Record any existing device caps before a sweep so they can be restored.
For a previously uncapped slice, use `IOWriteBandwidthMax=` to roll back and
verify that `wbps` is `max` or that the device has no row. Persist a qualified
cap with the same property without `--runtime` and verify it after the next
startup. If another branch is the writer, scope the limit to that branch. #2245
requires at least 40 MB/s of job throughput alongside the read latency target.
For physical isolation use the
[queue recovery procedure](../operations.md#queue-backup-restore-and-relocation).

Measure the baseline before deployment and the qualified policy afterward in
comparable windows. Use existing authorized job traffic or a separately
authorized bounded writer on the same device. Measure each window's writer
`io.stat` byte deltas over wall time (at least 40 MB/s), disk `w_await`, I/O PSI,
and the queue journal's `off_lock_write_ms`, `persist_sync_commit_ms` and request
wait times. From an authenticated client, time `myce status --json` and
`myce status --sections machines --json` once at a time at spaced intervals.
Record the maximum elapsed time and any 429 or 503. Each must finish in under
two seconds during the write window. Time one `myce status --history --json`
capture during that window, writing to a private file on a separate device or
the client. Check exit status, parse the complete JSON and verify the returned
job count matches `jobs_page.returned` and `jobs_page.total`, and the priority
policy is present. Use
the [release after-check](../operations.md#upgrade-the-approved-release) to
compare retained jobs and attempt custody. Keep #2245 open until the live
measurements are recorded.

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
