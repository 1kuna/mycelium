# SQLite writer durability: fixture cost and fault evidence

Work for [#1179](https://github.com/1kuna/mycelium/issues/1179). Measurements taken
2026-11-09; baseline `7663d789b7134ca907ebb46a6a9bcadf4f261be9`, implementation
`e2a244361f77fb7984ca7916cee50705f0650e80`. Later documentation changes do not
change the measured implementation.

Darwin fullfsync has a material measured cost: the retained-history fixture's
median commit rises from 3.60 to 23.82 ms and check-in throughput falls 28%.
This tradeoff needs the orchestrator/maintainer's decision before merge. Linux
keeps FULL/WAL and receives no fullfsync request.

## Method

Use the unchanged `BenchmarkCheckinPersistence` with real SQLite commits:

```sh
go test ./internal/myce -run '^$' -bench '^BenchmarkCheckinPersistence$' -benchtime=20x -count=3 -parallel=1
```

Run baseline then implementation serially on the same machine per platform,
with private HOME, TMPDIR and GOCACHE, `GOMAXPROCS=2` and `GOFLAGS=-p=2`.
No other owned Go process runs concurrently. The shared machines are not
exclusive; these short ordered samples describe observed cost, not a controlled
causal estimate or production capacity. Do not compare throughput between the
two platforms as hardware equivalence.

The fixture has 6,500 retained jobs, 110 queued jobs, 1,400 observations and five
synthetic occupied workers. The manifests variant adds 768 input references per
running job. It disables automatic checkpointing to measure WAL growth; ordinary
production checkpoint stalls are therefore outside these timings. Fixture setup
and cleanup are excluded. Throughput is `1e9 / ns/op`, including all timed
check-in work; commit and mutex hold are separately instrumented phases.

The baseline already uses incremental persistence. These numbers do not repeat
or replace #1066's live observation of approximately 317 ms/check-in and 21 MB/s.
No live queue database, worker spool or customer data was read.

## Darwin

Medians of three samples, each containing 20 check-ins:

| Fixture | Commit ms before → after | Check-ins/s before → after | Throughput change | Mutex hold ms before → after |
| --- | ---: | ---: | ---: | ---: |
| Retained | 3.599 → 23.82 | 15.149 → 10.895 | −28.1% | 62.17 → 85.66 |
| Manifests | 9.627 → 12.20 | 13.510 → 12.756 | −5.6% | 69.41 → 74.66 |

Individual samples in execution order (before / after):

| Fixture | Commit ms samples | Check-ins/s samples |
| --- | --- | --- |
| Retained before | 3.599, 3.484, 4.541 | 16.103, 15.070, 15.149 |
| Retained after | 22.37, 23.82, 45.16 | 11.449, 10.895, 8.955 |
| Manifests before | 9.627, 8.716, 10.11 | 13.468, 13.999, 13.510 |
| Manifests after | 25.11, 11.21, 12.20 | 10.536, 12.756, 13.276 |

Observed commit ranges overlap for the manifests fixture but not the retained
fixture. Wide after-sample variation and shared load limit precision. No SQL
commit, checkpoint or other I/O was moved into the queue mutex; the Darwin
flush policy makes its existing commit wait more expensive.

## Linux: measurement blocked

No permitted Linux before/after number is available yet. An earlier completed
comparison is superseded following the orchestrator's placement correction and
is excluded from these results. The replacement comparison carried the required
private machine exclusion, but its first attempt was interrupted for production
and the queue reported the remaining capacity reserved for production. That
owned job was cancelled before trying the authorized local VM fallback.

The admitted gate's Linux VM benchmark refused with
`STOP local gate: host memory pressure is not normal; no work started`.
No VM Go process started, and the refused operation was not bypassed. A later
orchestrator ruling suspended all direct Go runs outside gate admission. Future
measurement must use the admitted gate or a permitted Mycelium placement.
The Linux cost comparison remains a required follow-up before acceptance;
unchanged Linux FULL/WAL policy is not a measurement of zero cost. Deployment
identities and reservation details stay out of this public report.

## Fault and configuration proof

The focused race selection is:

```sh
go test -race ./internal/myce -run '^TestQueueSQLite|^TestCrashDurableSubmission$|^TestQueuePersistence|^TestMachineMigration' -count=1 -parallel=1
```

Darwin passed in 3.614 s before the direct-run suspension. A focused Linux race
run passed in 2.711 s in the superseded placement; retain that as technical
observation only, not permitted-placement acceptance. The permitted replacement
was interrupted before returning accepted outputs. Fresh normal-gate evidence
is pending.
The tests are ordinary package tests without GPU, NAS, external services or
special gate switches. Quick related-test selection includes them; full gates
run the normal race suite.

- Force physical connection replacement for both the queue and offline
  conversion; read back WAL/FULL, busy timeout and Darwin fullfsync again.
- Refuse SQLite's successful-but-non-WAL response and an ignored synchronous
  assignment. Conversion refuses a non-WAL database without changing its mode.
- In isolated child processes intercept SQLite's actual WAL VFS `xSync` and
  inject sync failure, a lost unsynced suffix, a 100-byte torn suffix, or kill
  immediately after successful sync. Reopen, run `integrity_check`, retain an
  acknowledged job and cancellation, reject torn/lost new state and verify a
  subsequent write. A sync error must return an error and roll back the in-memory
  check-in; recovery may see either complete disk state after that uncertain
  commit. The fully synced check-in must recover.
- Assert the VFS sync request is FULL on Darwin and NORMAL on Linux (SQLite's
  VFS flag terminology differs from the SQL synchronous level).

This models WAL crash images and process death, not a physical power cut or
kernel panic. PRAGMA readback and VFS flags establish the requested policy;
SQLite can fall back to ordinary fsync when Darwin fullfsync is unsupported.
They do not qualify a filesystem, drive cache or storage controller. The other
write families in #1179, directory publication and the physical drill remain
separate work. Inventory [PR #1184](https://github.com/1kuna/mycelium/pull/1184) remains a draft.
