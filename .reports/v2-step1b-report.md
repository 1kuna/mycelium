# Mycelium v2 step 1b: power and network recovery

Implemented locally on `v2-step1`, starting from step 1 (`a48c9b9`). The branch
was checked before work and again before completion. Read `AGENTS.md`, the full
approved `docs/design-proposal.md`, and `.reports/v2-step1-report.md` first. The
approved design is unchanged. This task's step 1b means power/network recovery;
model calls remain outside this task.

## Independent review and dispositions

Reviewed job validation, priority/readiness/deadline/wait ordering, preemption,
leave/loss/cancel, persistence, HTTP routes, command execution, and existing
unit/end-to-end tests against the whole design.

1. **Restart immediately ran normal loss maintenance.** Workers absent for 60
   seconds would be requeued before reporting surviving commands/results.
   Added a configurable three-minute startup recovery window for persisted
   machines. Running reports and buffered results preserve their attempts;
   only unreported machines expire at the end of the window.
2. **Fleet-wide silence requeued every job.** Added network-outage detection:
   when all active workers are silent, assignments remain in place. The first
   reconnect grants the others a full 60-second reconnect window. A healthy
   peer then provides evidence for an individual worker's subsequent loss.
3. **Worker log upload blocked reading stdout during an outage.** Sufficient
   output filled the pipe and blocked the command. Logs now go directly to a
   local disk file; completion is fsynced independently of the HTTP loop.
   Reconnection checks ownership before replaying saved logs/results.
4. **Worker restart forgot its processes and pending output.** Added a durable
   spool, exclusive owner lock, persisted execution identity, and a gated
   supervisor. Actual command execution begins only after the group identity
   is fsynced. Startup validates identity before killing the old group and
   waits until no live group members remain before checking in fresh. Reused
   PIDs are never killed just because their number matches.
5. **Late returns were always killed, ignoring newest checkpoints.** Persist
   issued copy identities and reconcile returning resumable attempts by newest
   complete NAS checkpoint. The losing copy is revoked and stopped on checkin.
   Attempt numbers remain monotonic even when an older copy wins. Each attempt
   writes in its own folder, preventing disconnected copies overwriting one
   another. NAS failure defers conflict decisions and uploads until comparison
   is possible. Cancelled/terminal or explicitly stopped copies cannot revive.
6. **NAS access failed only after assignment/startup.** Queue and worker now
   probe their checkpoint root and prevent starts when unavailable. Neither
   creates a missing fleet root. Status/HTML show the NAS problem plainly.
   Running commands remain running; their checkpoint retry contract is in the
   guide. Dev creates only its explicitly local checkpoint root.
7. **Failed snapshot writes did not fully restore in-memory state.** JSON
   unmarshal into existing maps retained newly inserted keys, leaving phantom
   jobs after failed submissions. Rollback now resets the state before restore,
   including transient recovery bookkeeping. Returned job/machine values and
   incoming specifications are detached from internal maps/slices.
8. **Two queue processes could overwrite one another's snapshots.** Added an
   exclusive database owner lock. Worker spools also have exclusive ownership;
   an unrelated fresh same-name worker cannot steal a live worker session.
   Restart replacement requires proof of the recorded previous session.
9. **Lost acknowledgements duplicated logs or rejected completed success.**
   Durable per-attempt byte offsets deduplicate log chunks transactionally;
   repeated completion acknowledgements are idempotent. Worker replay persists
   chunk boundaries so an interrupted upload resends identical bytes. Valid
   log replay refreshes worker liveness while a large backlog is delivered.
10. **Planned pauses exhausted the three-attempt limit.** Separated command
    attempt numbering from infrastructure interruptions. Planned resumptions
    are unlimited; non-resumable jobs fail after three infrastructure losses.
    Resumable work continues from checkpoints. Command failures still fail
    immediately; non-resumable jobs remain protected from priority preemption.
11. **Durability relied on an implicit SQLite setting.** WAL was already used;
    now synchronous FULL is explicitly requested and verified. A subprocess
    crash test exits without Close and verifies the submitted job survives.
12. **New supervisor exposed a relative-command regression during review.**
    Fixed resolution of `./run.sh` against the job's workdir and tested it.

Existing scheduling ordering, eligibility, allowed machines, queued give-up,
planned leave choices, cancellation, and stale-result checks were otherwise
supported by the existing tests. Kept the single-command worker, plain commands,
one SQLite snapshot, and existing scheduler. No leases, peer registry, model
server, environment installer, hardware probing, or generic recovery framework.
Disk spool/attempt isolation are the necessary additions for the loss contract.

## Power/network cases and tests

All decision clocks are injected. Fake clocks drive scheduling, startup grace,
loss, fleet silence, NAS decisions, checkpoint timestamps, and retry boundaries.
Real timers only wake loops; actual subprocess tests supplement those decisions.
No `time.Now` or `time.Sleep` occurs in queue/scheduling/worker decision logic;
`RealClock.Now` is the sole wall-time adapter.

- **Durable submission:** `TestCrashDurableSubmission` uses fake queue time,
  abrupt subprocess exit, reopen, and PRAGMA checks (`wal`, synchronous `2`).
- **Queue restart:** `TestRestartGraceReportsAndExpiry` advances fake time
  through 119/120-second configured recovery boundaries, reports a running
  job, uploads a finished result, and requeues only the unreported job.
- **Worker restart:** `TestWorkerFreshRestartRetriesCleanedSession` exercises
  fresh-session recovery with fake queue time; real
  `TestWorkerRestartCleansOrphanBeforeReassignment` kills/restarts a worker
  subprocess and proves cleanup precedes attempt 2. Identity, reused PID,
  lock/corrupt state, and gated launch have separate worker tests.
- **Worker network partition:** `TestWorkerNetworkPartitionFakeClock` runs the
  real worker/API/command with fake worker and queue clocks and a pretend
  disconnected transport. It finishes offline, retains result/output, starts
  no queued successor, and uploads on reconnect. Large-output coverage writes
  4 MiB while disconnected without blocking the command. Saved-result replay
  verifies old-session checkin -> logs -> completion -> fresh session.
- **Backoff:** `TestWorkerRetryFakeClockBoundaries` checks each exponential
  delay, one nanosecond before/at each deadline, the 30-second cap, and reset.
- **Competing copies:** `TestLateReturnCheckpointWinner` tests both winners
  with fake time/progress. Old loss tests cover non-resumable stale rejection.
  `TestCheckpointReconciliationWaitsForNAS` holds decisions without NAS access.
- **Whole network:** `TestWholeNetworkSilenceWaitsAndReconnects` advances ten
  minutes, proves both jobs remain assigned, then reconnects both safely.
- **NAS:** `TestNASUnavailableBlocksAndRecovers` uses a reversible directory
  rename and fake time, tests queue/worker blocking and status, then recovery.
  `TestNASLossDoesNotStopRunningJob` protects running work from new preemption;
  the worker missing-root test proves no local mount substitute is created.
- **Atomic/newest complete saves:** `TestWorkerAttemptCheckpointIsolation`
  uses fake timestamps, chooses the newest complete save, excludes `.tmp`,
  preserves timestamps, and isolates later writes. Legacy root checkpoints
  are retained; checkpoint symlinks fail visibly. The guide now specifies
  temporary write -> fsync -> rename -> directory fsync and save retries.
- **Autostart:** Linux queue/worker systemd units and a Mac launchd plist are
  in `deploy/`, with private token setup and mount/state instructions. Plist
  syntax was checked locally. Units were not installed or boot-tested.

Additional regressions cover failed-write rollback/external copies, single
queue ownership, worker name ownership, duplicate logs/completions, repeated
planned pauses, and relative commands. The original executable checkpoint
preemption/resumption end-to-end test still passes with attempt folders.

## Final verification

All requested checks exited 0 on the local laptop:

```text
$ go build ./...
[no output; exit 0]
$ go vet ./...
[no output; exit 0]
$ go test ./... -race
ok  	mycelium/cmd/myce	12.871s
ok  	mycelium/internal/myce	7.341s
$ git diff --check
[no output; exit 0]
$ plutil -lint deploy/local.mycelium.worker.plist
deploy/local.mycelium.worker.plist: OK
```

No fleet hardware, GPU, LibreOffice, remote deployment, or push was used.
Staged explicit source/doc/example/report paths only. Local runtime spools are
ignored. `.reports/` is normally ignored; this requested report is explicitly
force-staged.

## Boundaries and deferred work

- A partition cannot carry a stop signal: command copies can overlap until
  reconnection. Checkpoint reconciliation ends overlap when communication
  returns; exactly-once external side effects are not claimed. Jobs must make
  their output safe to retry. Checkpoint progress uses shared NAS modification
  timestamps, not a semantic understanding of arbitrary job state.
- Missing/unwritable roots are detected. If an unmounted NAS exposes a usable
  local directory, the operator must configure the mount to fail closed;
  directory probes cannot establish remote-server identity.
- Completed local spool files are retained for inspection and recovery.
  Automatic retention/rotation is deferred; monitor and archive disk usage.
- SQLite FULL/WAL relies on storage honoring fsync. Tests simulate process
  crashes and pretend mounts; no real power cut or NAS outage was performed.
- Boot services are examples, not installed services. LaunchAgents start at
  login; the README describes LaunchDaemons for boot before login.
- Linux/macOS process groups are supported. Windows, hardware acceptance,
  model calls/loading, and environment installation remain later work.

## DIGEST

- Reviewed step 1 against the full approved design and fixed the recovery bugs.
- Added durable writes, restart grace, safe worker cleanup, and disk-backed replay.
- Added network-outage waiting, checkpoint winner selection, and clear NAS status.
- Fixed phantom jobs, duplicate delivery, unsafe ownership, and planned retry limits.
- Added fake-clock recovery tests and real crash, offline-output, and dev tests.
- Added atomic checkpoint guidance and Linux/Mac boot service examples.
- Build, vet, race tests, diff check, and plist validation passed.
- No hardware, LibreOffice, deployment, or push; partition overlap remains possible until reconnect.
