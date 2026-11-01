# Queue disk contention

Keep bulk copies, heavy tests and maintenance off the queue's backing device
while qualifying I/O protection. Another directory or partition on the same
device provides no physical isolation. Native protection remains unqualified;
[#527](https://github.com/1kuna/mycelium/issues/527) owns measured acceptance.

## SQLite backup and commit limits

Python `Connection.backup(pages=256, sleep=0.05)` does not throttle successful
steps: CPython sleeps only on BUSY/LOCKED. Concurrent source changes can restart
the copy and increase writes beyond the database size. See
[CPython's implementation](https://github.com/python/cpython/blob/v3.14.0/Modules/_sqlite/connection.c)
and [SQLite backup semantics](https://sqlite.org/c3ref/backup_finish.html).
Use a progress callback or external I/O controls when bounding an online copy;
use the [stopped-state backup procedure](operations.md#queue-backup-restore-and-relocation)
for a complete queue recovery generation.

Queue mutations commit under the queue mutex with WAL and `synchronous=FULL`.
Synchronization and automatic checkpoints can delay other control calls.
Unlocking before commit would break durable publication/rollback semantics.
Mutable state and WAL checkpointing can cause sustained write amplification
without database growth. Process counters do not distinguish database, WAL,
logs and payload writes; whole-disk averages do not establish a safe I/O ceiling.

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
For relocation, use the [queue recovery procedure](operations.md#queue-backup-restore-and-relocation).

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
