# Checkpoint metadata access

Checkpoint progress is consumed once per worker report. Scanning more often
cannot improve that delivery cadence. Worker actors still observe stop and
release at up to 100 ms, but NAS progress walks run at most once per configured
report interval. The queue reuses each attempt's progress verification for the
sender's advertised or learned check-in interval. Legacy senders without any
cadence evidence retain scan-on-check-in behavior until a cadence is learned.

An unfinished filesystem call keeps its original single-flight owner. Late
completion is consumed promptly, without starting another walk; pending refresh
does not reset already observed progress. Completed errors are also reused for
the interval so a fast failure cannot turn into a metadata retry loop. No
checkpoint I/O runs under the queue mutex.

Root-directory mtime is not a sufficient change signal for arbitrary saves:
in-place writes and nested-directory publications can leave it unchanged. Those
checkpoints retain a full scan at the consumer cadence. Local-first uses its
existing `generation.json` publication on local storage; its prompt capture,
acknowledgement, queue portability and asynchronous NAS mirror are unchanged.
Resume source selection and final yield verification still read fresh data;
final verification does not reuse a heartbeat observation that predates a save.

## NAS access audit (#1670)

Here `N` is the configured report interval in seconds, `C` the sender's advertised
or learned interval, `H` the adaptive host-health sample period (at least the
worker control/report interval), and `D` the number of directories in the
attempt. A recursive progress walk enumerates `D` directories, stats entries
and parses JSON saves. Kernel CIFS calls also include EOF iterations, so they
are not numerically interchangeable with walks or directory enumerations.

| Path / consumer | Before | After |
| --- | --- | --- |
| Worker NAS attempt progress (`worker_attempts.go`, `worker_recovery.go`) | Up to `1/min(N, 0.1)` walks/s/attempt | At most `1/N` walks/s/attempt; `60D/N` directory enumerations/min |
| Queue heartbeat durable-progress verification for stale/portable attempts (`queue.go`) | One walk per matching report, including duplicate/rapid reports | At most one walk per path per `C`; ordinary same-attempt nonportable reports need no queue walk |
| Restore/source discovery and input staging retried by the actor (`worker.go`, `checkpoint_launch.go`) | A completed retryable failure can restart discovery on the next actor tick (up to 10/s with `N >= 0.1`) | Completed failure retries at most once per `N`; pending copies retain one owner and are joined, not restarted |
| Worker checkpoint admission probe (`worker_attempts.go`, `storage_probe.go`) | One fixed-file create/write/read/close probe per check-in, no directory enumeration | Unchanged: one per check-in |
| Queue checkpoint availability (`storage_probe.go`) | One single-flight fixed-file probe per shortest measured worker interval; without cadence, per transaction | Unchanged; no directory enumeration |
| Mount health (`host_hygiene_mount.go`, `host_hygiene.go`) | One single-flight probe per adaptive health sample, at most `1/H`; NAS root uses stat/open/`ReadDir(1)`, checkpoint root uses fixed-file access; failed recovery uses observed-cost backoff | Unchanged; checkpoint health never enumerates the checkpoint root |
| Requested input-path inspection (`resolve_nas.go`) | One requested path chain per heartbeat; component `Lstat`, no recursive walk | Unchanged; no autonomous share discovery |
| Shared-input/model staging (`shared_input_cache.go`, `model_files.go`) | Declared paths/manifest entries on cache miss; copies single-flight under the staging/cache owner. Recursive verification walks local staged data | Unchanged on success; actor retries after completed retryable failures now bounded by `N` |
| Checkpoint shard setup (`checkpoint_layout.go`) | Startup/check-in kick until ready; failed attempts no faster than once/minute; at most 257 metadata operations per setup, no recursive share walk | Unchanged; successful setup is latched |
| Settled checkpoint retention (`checkpoint_expiry.go`) | Queue uses indexed terminal/release records, no NAS scan. Worker removes only assigned settled folders after 72-hour grace; batches of eight, failed claims wait 30 minutes | Unchanged; no periodic enumeration of the NAS share |
| Checkpoint/output publication (`checkpoint_local.go`, `output_delivery.go`) | Event/manifest driven per accepted generation/result, one owned asynchronous publisher, failed publications use existing retry pacing | Unchanged; not a progress polling walk |
| Storage and liveness sampling (`attempt_storage.go`, `attempt_liveness_worker.go`) | Five-second metadata sampling of managed local roots; external checkpoint paths excluded; symlinks not followed | Unchanged; no NAS progress polling |

Installation/onboarding probes are explicit operations, not periodic scans.
Cache, environment, log and file-store retention enumerate their local managed
stores; they do not enumerate the configured NAS share. The user-supplied
checkpoint and NAS roots are the remote paths covered above.

## Reproducible checks

`TestCheckpointProgressReportCadence` runs the real actor loop with an injected
clock and a counting filesystem over three local checkpoint directories. It
compares 600 old-style walks in a simulated minute with a five-second report
interval, checks every 100 ms prefix for scan bursts, and publishes a new nested
generation without changing the root mtime. Queue, pending-refresh and existing
local-first/resume regressions cover the other consumers. It never needs a NAS.

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
Expected after: scale by `0.1/N` for unchanged directory contents and I/O
behavior (about two root CIFS calls in five seconds when `N=5` and each walk
needs a data read plus EOF). Also retain whole-worker and queue totals, report
interval, attempt identity and directory depth; nested directories add calls.
This probe observes existing I/O and never initiates a NAS walk. Its exact
after counts remain release-owner evidence, not a claim from local tests.

Compare NAS disk busy and macOS SMB stall/reconnect observations with the
recorded November 14–15 baselines after deployment. Reduced scan load alone
does not prove that the intermittent NAS hangs are fixed.
