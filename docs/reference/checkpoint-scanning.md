# Checkpoint metadata access

Job status and progress come from the executing worker's local observations
and publication signals. Status reads never inspect the NAS. See
[the maintainer's source](../DECISIONS.md#my-43) and
[#1670](https://github.com/1kuna/mycelium/issues/1670).

## Save notification

The updated Python `mycelium.Checkpoint` writes and syncs its atomic NAS save as
before, then atomically publishes `{"name":"checkpoint.json","generation":N}`
to `MYCE_CHECKPOINT_NOTIFY`. This is a session-specific file in the attempt's
private **local** work directory, owned by its existing retention/cleanup.
The helper discovers restored generations once and keeps its generation counter
locally; subsequent saves do not enumerate the checkpoint directory.
Custom writers can publish the same notification after their atomic JSON save
or manifest is durable. Names are plain `.json` filenames and generations are
positive int64s. Use a single writer and atomic local replacement. The worker
stats only this local file, reads a changed marker once, and sends its generation
to the queue. Several saves before observation may coalesce to the latest.
Neither worker nor queue opens a NAS marker or directory to produce status.
An explicitly different Python checkpoint directory does not signal the attempt.

Arbitrary writers without notifications report progress **unknown** (`-1`)
between lifecycle events. Root mtime cannot detect nested or in-place writes, so
it is not polled. There is no periodic checkpoint-root stat either.

Resume discovery, recovered-process reconciliation and stop/release/finalization
can inspect the exact attempt's durable bytes. The worker retains a single
outstanding operation, consumes its late completion, and caches by event identity
rather than elapsed time. Repeated heartbeats cannot restart a completed event,
even after an error. Queue-side reads are limited to a known recoverable copy's
resume decision and finalization/release, outside `q.mu`, with identity rechecked
before acceptance. Finalization does not reuse an observation predating a save.

`report.Progress` supplies status, recovery comparisons, checkpoint-origin
tracking and the initial scarce-host-yield generation. Same-attempt reports now
come directly from the worker. Recovery still compares fresh durable progress at
the resume event. Normal stop/deadline/cancellation decisions do not require
live checkpoint scans. Arbitrary writers without publication evidence cannot
be selected for portable scarce-host yield until a positive generation is
known; the helper supplies that evidence. Yield still requires a newer verified
final save after stop, plus exact process release. Finalization/release discovers
unreported saves and records the resume target before retry. If a writer is lost
while progress is unknown, its last executing target is retained conservatively;
absence of status evidence never proves that its checkpoint folder is empty.

`local_first` is unchanged: local `generation.json`, cooperative acknowledgement,
queue-portable generations and asynchronous NAS mirrors keep their protocol.
Existing no-NAS file-transport capture continues to scan **local** storage.

## Output trees are excluded from checkpoint discovery

A 600-second live trace of r6.19.1 on 2026-11-16 (03:32–03:42 UTC), supplied by
the orchestrator, attributed 96.55% of one host's CIFS read submissions to its
worker and queue services: about 297 reads/s, 165 directory reads/s and 378
opens/s. Nearly all observed progress discovery traversed one running attempt's
nested output data; NAS disk busy was about 95%. This is a supplied baseline,
not a measurement of this PR or proof that deployment fixes the NAS hangs.

Neither status consumer opens that tree:

| Consumer | Code and regression evidence |
| --- | --- |
| Running worker | `worker_attempts.go` calls `checkpointReport`; its NAS live branch in `checkpoint_progress.go` reads only the local notification and returns unknown without one. `TestCheckpointProgressEventDriven` asserts zero NAS reads through a simulated minute and one local marker read. |
| Queue check-in (`queue.go`) | `processCheckinContext` leaves the NAS path empty for same-attempt reports. Only an exact known recoverable copy can request a resume comparison. `TestQueueCheckpointProgressFromWorker` fails on any NAS status scan. |
| Resume and stop/release/finalization, worker and queue | Both use `checkpointProgress` / `scanCheckpointProgressWalk` in `worker_recovery.go`. The scanner prunes the top-level output directory before metadata inspection or enumeration. `TestCheckpointProgressNeverEnumeratesOutput` rejects either operation and verifies output JSON cannot become checkpoint progress, with the production walker too. |

Keep checkpoints outside the top-level output directory. Local-first continues
to read its local manifest; its protocol is unchanged. Restore copies and
result publication may read output bytes as part of their requested operation;
these are not status discovery. Checkpoint reads remain save/resume/recovery or
stop/release/finalization operations. The separate shared-root health/admission
probes and explicit input/retention operations below still exist; this PR does
not claim that every NAS access is a checkpoint lifecycle operation.

## NAS access audit (#1670)

The #1671 fixture used three directories, a 100 ms actor and five-second reports:
**36 → 0 NAS directory reads per running attempt per minute** (12 → 0 walks).
The older r6.19 baseline was 1,800 reads/minute in that fixture. The isolated
published save causes exactly **one local marker read**, zero NAS reads. These
are local counting-filesystem results to be qualified, not live CIFS counts.

Idle root probes use five minutes: ten times the existing 30-second operation
stall allowance, based on observed successful 17.73s/23.22s NAS operations (#234).
The margin reduces worst-observed idle-probe duty below 10%; mount sampling also
waits at least ten times its measured whole-sequence cost. Cost: an otherwise
idle failed share may remain cached for five minutes. Actual I/O failures and
local OS mount inventory still fence immediately; metadata success cannot clear
a data-read fault. Pending calls retain one owner and the existing 30s
no-operation-progress fence. Failed mount probes back off from at least five
minutes. These admission/host-health checks never inspect job progress.

| Path / consumer | #1671 | This change / remaining rate |
| --- | --- | --- |
| Worker NAS attempt progress | `60D/N` directory reads/minute | **0 periodic NAS accesses**, no checkpoint-root stat; local notification only |
| Queue same-attempt progress / portable yield status | One verification walk per sender interval | **0 NAS accesses for status**; worker report only |
| Resume/recovery, stop, release, finalization | Fresh lifecycle reads | Event-only; one owned observation per event, no timed refresh |
| Restore/source discovery and staging errors before launch | Retry each report interval | Failed NAS preparation retries at most once/5 min; pending copies joined promptly |
| Worker checkpoint admission (`worker_attempts.go`) | One fixed-file probe/check-in | At most **0.2 fixed-file sequences/minute/worker**, no enumeration |
| Queue checkpoint availability (`storage_probe.go`) | One fixed-file probe/shortest check-in interval | At most **0.2 fixed-file sequences/minute/queue**, longer if the shortest measured check-in exceeds 5 min |
| Mount health: NAS root (`host_hygiene_mount.go`) | Stat/open/`ReadDir(1)` each health sample | **0 directory reads**; at most **0.2 root stats/minute/host**, plus local mount-table checks |
| Mount health: checkpoint root | One fixed-file sequence/health sample | At most **0.2 fixed-file sequences/minute/host**, no enumeration; longer after slow sequences/errors |
| Requested input inspection (`resolve_nas.go`) | One requested path chain/heartbeat | Request-driven only, no autonomous scan; one `Lstat` per path component and cached diagnostic reply |
| Shared-input/model staging | Cache-miss declared paths; one copy owner | Event-only on cache miss; verification of copied trees is local |
| Checkpoint shard setup | Latched startup; failed setup at most once/minute | Unchanged: at most 257 metadata operations/setup, no recurring access once ready |
| Settled checkpoint retention | Indexed expiry and assigned-folder removal | Unchanged: 72h grace, batches of eight; failed claim retry after 30 min, no share enumeration |
| Checkpoint/output publication | Event/manifest-driven | Unchanged: payload saves and accepted-generation/result publishing; owned retries under existing publication pacing |
| Storage/liveness sampling | Five-second samples of managed local roots | Unchanged: external checkpoint paths excluded; no NAS reads |

A fixed-file sequence stats its configured root, opens/writes/closes one fixed
probe file and reopens/reads/closes it; the queue additionally fsyncs the file.
The checkpoint-health and worker-admission lanes remain independent and their
rates add (at most 0.4 sequences/minute/worker). None is per attempt. Installation
probes are explicit operations. Cache/environment/log/file-store retention uses
local managed stores. Active I/O fault recovery may reopen the exact failed
source twice (at most one page per read), at the mount recovery cadence; it never
walks the share. Payload-authored scans remain the payload's responsibility.

## Reproducible checks

`TestCheckpointProgressEventDriven` runs the real actor for a simulated minute
with a counting filesystem and a local marker-read counter. It checks every
100 ms prefix, compares #1671's 36 reads with zero, publishes a nested save that
does not change root mtime and observes exactly one local marker read, then
checks a final arbitrary save at finalization. Queue reports, pending events,
idle probe cadence and existing resume/stop/local-first tests cover the other
paths. All filesystem fixtures are local; no test needs a NAS.

The release owner must repeat the existing `kprobe:cifs_readdir` measurement on
the queue host's worker before and after rollout, using the same running
attempts and report cadence. Obtain current worker and queue PIDs through their
service managers; do not reuse old PIDs. Run the same 30-second observation,
with six five-second buckets, keyed by PID, command, parent directory and
directory (names only; no full private paths). On Linux with bpftrace/BTF:

```sh
sudo bpftrace -e '
kprobe:cifs_readdir /pid == $1 || pid == $2/ {
  $file = (struct file *)arg0;
  $entry = $file->f_path.dentry;
  @reads[pid, comm, str($entry->d_parent->d_name.name),
         str($entry->d_name.name)] = count();
}
interval:s:5 { print(@reads); clear(@reads); }
interval:s:30 { exit(); }
END { clear(@reads); }
' "$WORKER_PID" "$QUEUE_PID"
```

Keep the per-attempt rows distinct. Baseline: approximately 104 CIFS calls per
attempt root in five seconds; a separate 30-second window measured 626–627.
Expected steady-state after: zero per-attempt CIFS directory calls, even with
one-second reports. Saves signal locally and never trigger NAS status reads;
resume/stop/release/finalization events must be identified separately. Also retain whole-worker and queue totals, report
interval, attempt identity and directory depth; nested directories add calls.
This probe observes existing I/O and never initiates a NAS walk. Its exact
after counts remain release-owner evidence, not a claim from local tests.

Compare NAS disk busy and macOS SMB stall/reconnect observations with the
recorded November 14–15 baselines after deployment. Reduced scan load alone
does not prove that the intermittent NAS hangs are fixed.
