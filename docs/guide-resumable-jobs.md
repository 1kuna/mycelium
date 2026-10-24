# Resumable jobs

On v3, [files without a NAS](guide-files-v3.md) adds local saves transferred to
the queue hypha before publication. The atomic generation contract below is the
same; newer local-only saves cannot resume elsewhere. Explicit NAS configuration
keeps the path described below.

Stored job inputs (`files`, including a profile's frozen source archive) and
their verified results use the queue file store in both modes. Uploading inputs
does not switch NAS checkpoints to file-store generations; workers fetch and
verify those inputs through the queue API. See [file transport](guide-files-v3.md).

Set `resumable: true` only when the command can safely restart. Mycelium sends
SIGTERM to the command's process group, allows 10 seconds for production preemption (configurable), or two minutes for
deadline preemption and planned leave, to save and exit,
then sends SIGKILL if necessary. Do not daemonize or leave that group. Save
periodically too: a machine that loses power cannot save on demand.

Each attempt receives `MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR`.
Each worker sets `--checkpoint-root` to its own local mount of the same NAS
checkpoint directory, configured privately per host. For example, use a generic
`NAS_ROOT/checkpoints` under each machine's verified mount. Queue `--checkpoints`
uses the queue's local path. Jobs carry relative checkpoint paths; the worker
resolves them before setting `MYCE_CHECKPOINT_DIR`. Create the root on the
mounted NAS before starting the queue. Dev creates a local root.
The stable job folder holds `attempt-1`, `attempt-2`, etc.; the environment points
to the current attempt's folder. Before starting, the worker copies checkpoints
from the attempt with the highest durable checkpoint generation.
Temporary files and directories ending in `.tmp` never count as complete and are never copied.
Workers stage a complete attempt copy in an ignored temporary directory, then
rename it into place, so a failed copy cannot publish a manifest without its payloads.
Failed staging directories remain available for inspection; retries preserve an
already prepared attempt's saves. If a writer publishes a new generation during
copying, preparation retries rather than combining its manifest with an older payload list.
This isolates disconnected copies so they cannot overwrite each other's saves.
Old step-1 checkpoint files directly in the job folder are also preserved when
resuming. Checkpoints remain after completion or failure.

**Every save must be atomic:** write to a temporary file in the same directory,
flush and fsync it, rename it into place, then fsync the directory (except the
Linux CIFS no-op described below). Use one
self-contained checkpoint file, or a single atomic manifest referencing immutable
files, so readers never combine pieces from different saves. The newest complete
save is determined by a positive monotonic `generation` recorded inside the
atomic JSON save, never by file times or wall clocks:

```json
{"myce_checkpoint": {"generation": 9}, "value": {"step": 42}}
```

Increment the highest retained generation on every save, including after resume.
The Python `Checkpoint` helper does this automatically and returns only `value`
when loading. Keep a single writer per attempt. For non-JSON payloads, atomically
publish one JSON manifest with the same `myce_checkpoint` metadata referencing
immutable payload files; publish the manifest only after its payload is durable.
Temporary `.tmp` files never count. Unsequenced legacy saves still load at
generation zero: Python chooses the lexicographically greatest JSON filename on
a tie; worker resume chooses the greatest numeric prior attempt (direct job-folder
saves lose a tie). Reconciliation retains the current execution on a tie. These
are deterministic compatibility rules, not estimates of legacy progress.
Invalid generation metadata fails visibly instead of using timestamps. A job must also make its output writes safe to retry.

On Linux CIFS/SMB mounts, the Go checkpoint-copy path and Python helper skip
only directory fsync: Linux `cifs_dir_fsync` is explicitly a dummy operation
because directory operations are synchronous. Go identifies CIFS on the opened
directory with `fstatfs`; Python uses the most specific resolved mount in
`/proc/self/mountinfo`. Unknown filesystems and other platforms retain directory
fsync. File fsync and same-directory atomic rename remain mandatory. A mount
using `nostrictsync` may omit the server flush for files; avoid that option for
durable checkpoints. Server acknowledgements do not independently prove power-loss
survival of NAS caches: qualify server/storage guarantees and power-loss behavior
separately. Neither helper returns early from a still-running file flush.

Resume copies now sync each copied file, then each staged directory once,
children before parents, before atomically publishing the entire attempt and
syncing its parent. This removes per-file directory barriers on local filesystems
without weakening publication ordering. This does not establish a fixed
latency bound for a large checkpoint.
File flushes, copying, generation scanning and remote I/O can still be slow or
block. A timeout after rename on a filesystem requiring directory fsync must not
be reported as a durable save; visibility alone does not prove durability.

Size checkpoint intervals from measured worst-case/p99 save duration and
acceptable lost work, including time spent doing the next bounded work unit.
Avoid overlapping saves. Measure resume-copy latency separately, including file
count and payload size. Stop grace must exceed the longest bounded work unit plus
a complete final save and process-exit margin; the 10-second production default
is not a NAS latency guarantee. Increase configured grace only within the agreed
production-start budget; otherwise reduce checkpoint payload/file count or save
more frequently so the previous complete generation is sufficient after forced
stop. The queue's 25 ms wait budget does not bound a command's save syscall.
Run the large ext4 fixture separately with a qualified storage-duration budget
and memory/cache accounting; preserve all file-count/hash/heap assertions.

This complete Python command atomically replaces one checkpoint and retries NAS
failures while continuing to run:

```python
import json, os, pathlib, signal, time
root = pathlib.Path(os.environ["MYCE_CHECKPOINT_DIR"])
p = root / "checkpoint.json"
old = json.loads(p.read_text()) if p.exists() else 0
n = old["value"] if isinstance(old, dict) else old  # legacy scalar JSON
sequence = old["myce_checkpoint"]["generation"] if isinstance(old, dict) else 0
stopping = False
def stop(*_):
    global stopping
    stopping = True
signal.signal(signal.SIGTERM, stop)
def save():
    global sequence
    generation = sequence + 1
    tmp = p.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump({"myce_checkpoint": {"generation": generation}, "value": n}, f)
        f.flush(); os.fsync(f.fileno())
    tmp.replace(p)
    fd = os.open(root, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
    sequence = generation
while n < 100 and not stopping:
    time.sleep(0.1); n += 1  # replace with useful work
    try: save()
    except OSError as e: print(f"checkpoint save will retry: {e}", flush=True)
while True:  # keep retrying the final save until NAS access returns
    try: save(); break
    except OSError as e:
        print(f"checkpoint save will retry: {e}", flush=True); time.sleep(1)
print(f"saved {n}; attempt {os.environ['MYCE_ATTEMPT']}", flush=True)
```

Non-resumable production (`priority: production`, `resumable: false`) starts
without NAS checkpoint access and receives a local scratch checkpoint directory
under the worker state directory. Declare `resumable: true` when production needs
NAS checkpoints. Experiments and resumable production still wait for NAS recovery.

The worker heartbeat allows 750 ms for its checkpoint-root probe, including
creating and removing a temporary file; slower or blocked probes report the root
unavailable. One-shot onboarding allows ten seconds per filesystem operation
and has a cancellable 15-second total validation limit, including mount inventory.
Standalone mount selection retains its five-second limit, including dry runs.
Probe failures and recovery are logged on state changes. Worker
attempt preparation also allows 250 ms for network-filesystem metadata and cold
automount access. Queue I/O and checkpoint progress reads retain their 25 ms
waits; the queue performs them outside its lock. An outstanding filesystem
operation is reused on retry rather than duplicated. A blocked filesystem syscall
may continue in the background, with at most one outstanding call per operation;
commands never start from an incomplete preparation. Status reports the NAS as
unreachable while a probe fails or exceeds its budget.

Production preemption has a 10-second default grace; deadline preemption and
planned leave have two minutes. If the NAS stays down beyond that allowance,
the worker force-stops the displaced process group, reports it interrupted, and
production proceeds after physical release is reported. The next experiment
attempt resumes the previous complete save. Planned pauses do not exhaust
retries. All jobs fail loudly after three counted infrastructure losses;
a command's own failure fails immediately.

Workers keep commands running through network loss. Logs and results are spooled
on local disk, then delivered after reconnect. Acknowledged per-execution `.log` and
`.result.json` files remain in the local spool for inspection; monitor disk usage
and archive them according to your retention needs. The worker does not start another
command while disconnected. A restarted worker kills its recorded old process
group before checking in fresh. If two copies meet after a partition, the queue
keeps the one with the newest complete checkpoint and stops the other. If the NAS is unavailable,
conflict reconciliation and uploads wait until checkpoints can be compared. No system
can send a stop through a disconnected network: copies can overlap until they
reconnect. Commands must not rely on exactly-once external side effects.

For a laptop demonstration, use `myce dev --workers 1`, submit the script as an
experiment, then submit production requiring the same memory. Follow the logs to
see stop/save and resumption. See `deploy/README.md` for optional boot services.
