# Queue disk contention (#527)

Status: investigation and opt-in policy support. **Native protection is not
qualified.** Keep controller/scheduler/service changes and native qualification
after the current production hold.

## Evidence and limits

The queue DB, WAL, file store and job staging shared one NVMe filesystem during
the 2026-10-31 incident. Requests waited seconds for the queue mutex; check-in
holds reached 4.64 seconds. The operator identified and stopped an SSH-launched
online SQLite backup writing tens of GB. Pressure fell while full-gate jobs
continued. This identifies a strong contributor, not individual fsync durations
or exclusive causation.

The copy used Python `Connection.backup(pages=256, sleep=0.05)` without a progress
callback. CPython sleeps only on BUSY/LOCKED, not between successful steps.
Concurrent source changes can restart the copy, increasing writes far beyond the
database size. See [CPython's implementation](https://github.com/python/cpython/blob/v3.14.0/Modules/_sqlite/connection.c)
and [SQLite backup semantics](https://sqlite.org/c3ref/backup_finish.html).
The sleep argument is not a bandwidth limit.

`Queue.updateRequest` holds `q.mu` through `persistUsage`, `persistState` and
`queueUpdateTiming.commit`; `Queue.AppendLog` also commits under the mutex.
SQLite uses WAL and `synchronous=FULL`. Commit may include synchronization and
automatic checkpoint work. A waiting GET's brief hold does not measure the
preceding transaction; several slow commits can build a longer backlog.
Unlocking before commit would break durable publication/rollback semantics.

A later passive three-second sample found about 21 MB of queue-process writes,
four WAL checkpoint-generation changes and a constant 74 MB WAL allocation.
The database was about 1.57 GB; its mutable base-state record was about 816 KB.
That record includes machine/contact state and is rewritten when changed. A few
updates per second plus WAL-to-database checkpoint writes can explain sustained
multi-MB/s traffic without database growth. This is expected write amplification
of the current layout, not proof that every lifetime write was a checkpoint.
Process counters cannot split WAL, database, logs and payload writes. Worker
counters include staging, persistence and reaped descendants; they are not
per-production-job measurements.

The 12:47–12:55 UTC canary journal had no slow check-in records or sampled long
holds; retained stage receipts had zero losses but no per-attempt disk counters.
Whole-disk ten-minute samples were:

| UTC interval | Write MiB/s | Read MiB/s | Mean request latency |
| --- | ---: | ---: | ---: |
| 12:40–12:50 | 10.7 | 21.6 | 1.32 ms |
| 23:00–23:10 | 20.5 | 0.8 | 39.99 ms |

These are mixed-workload averages, not isolated production throughput, burst
capacity or an I/O ceiling. Small compressed input/output receipts exclude
temporary audio, model/cache reads and repeated writes. Canary success does not
establish full-batch headroom. Before qualification, exclude heavy tests and bulk
operator copies/maintenance from the queue disk. Another directory or partition
on that device provides no physical isolation.

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

Incident-host inspection found no I/O controller in the user-service hierarchy,
NVMe scheduler `none`, and no io.cost configuration. IOWeight alone is insufficient
there. Enabling controllers or changing schedulers requires separately authorized
native work after the hold. There was no second installed SSD; removable
installation media is unsuitable for queue isolation.

If effective weights miss the existing deadline, use standard `io.max`/systemd
`IOWriteBandwidthMax` and `IOReadBandwidthMax` at the aggregate worker subtree
on the actual backing device. Derive caps from a measured latency sweep under
representative read/write bursts and fsync, rather than advertised sequential
bandwidth. Caps cost stage time: measure that too. Put bulk operator tools in
controlled scopes or keep them off that disk. No unmeasured cap is proposed.
Queue relocation remains [#316](https://github.com/1kuna/mycelium/issues/316).

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

First APFS run: 11 baseline check-ins, p50 4.722 ms/p99 11.901 ms; nine loaded
check-ins, p50 5.102 ms/p99 17.114 ms. Job/wait window about two seconds; one
successful attempt, zero losses. **This bounded run did not reproduce the Linux
deadline failure** and does not qualify the proposed weight.

After the hold, run an authorized Linux experiment through Mycelium with platform
targets and no host pins, with a disposable queue on the job's filesystem. Record
device/controller/scheduler readback, actual job I/O rates, commit duration,
check-in p99/max and losses before/after policy. Include an isolated, bounded
live-backup reproduction and verify competing writers' scope membership.
The unchanged ten-second check-in deadline must hold under representative
production bursts, without false quiet losses. Collect every workload failure,
retain a compact receipt and remove disposable payloads after process release.
Native Linux proof and #527's measured-protection acceptance remain open.
