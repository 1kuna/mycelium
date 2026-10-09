# Resumable jobs

Set `resumable: true` only when the command can safely restart. Mycelium sends
SIGTERM to the command's process group, allows 10 seconds for production preemption (configurable), or two minutes for
deadline preemption and planned leave, to save and exit,
then sends SIGKILL if necessary. Do not daemonize or leave that group. Save
periodically too: a machine that loses power cannot save on demand.

Each attempt receives `MYCE_JOB_ID`, `MYCE_ATTEMPT`, and `MYCE_CHECKPOINT_DIR`.
Each worker sets `--checkpoint-root` to its own local mount of the same NAS
checkpoint directory: for example `/mnt/nas/mycelium/checkpoints` on
Spark, `/mnt/nas/mycelium/checkpoints` on B70 when that is its mount,
and `/Volumes/NAS/mycelium/checkpoints` on Macs. Queue `--checkpoints`
uses the queue's local path. Jobs carry relative checkpoint paths; the worker
resolves them before setting `MYCE_CHECKPOINT_DIR`. Create the root on the
mounted NAS before starting the queue. Dev creates a local root.
The stable job folder holds `attempt-1`, `attempt-2`, etc.; the environment points
to the current attempt's folder. Before starting, the worker copies checkpoints
from the attempt with the newest complete save, preserving file timestamps.
Temporary files ending in `.tmp` never count as complete and are never copied.
This isolates disconnected copies so they cannot overwrite each other's saves.
Old step-1 checkpoint files directly in the job folder are also preserved when
resuming. Checkpoints remain after completion or failure.

**Every save must be atomic:** write to a temporary file in the same directory,
flush and fsync it, rename it into place, then fsync the directory. Use one
self-contained checkpoint file, or a single atomic manifest referencing immutable
files, so readers never combine pieces from different saves. The newest complete
save is determined by the NAS file modification time. Preserve those timestamps
when moving existing checkpoints; do not artificially touch files or assign future
timestamps. If the job keeps numbered checkpoints, resume from the newest complete
one and ignore `.tmp` files. A job must also make its output writes safe to retry.

This complete Python command atomically replaces one checkpoint and retries NAS
failures while continuing to run:

```python
import os, pathlib, signal, time
root = pathlib.Path(os.environ["MYCE_CHECKPOINT_DIR"])
p = root / "progress"
n = int(p.read_text()) if p.exists() else 0
stopping = False
def stop(*_):
    global stopping
    stopping = True
signal.signal(signal.SIGTERM, stop)
def save():
    tmp = p.with_suffix(".tmp")
    with tmp.open("w") as f:
        f.write(str(n)); f.flush(); os.fsync(f.fileno())
    tmp.replace(p)
    fd = os.open(root, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
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

Production preemption has a 10-second default grace; deadline preemption and
planned leave have two minutes. If the NAS stays down beyond that allowance,
the next attempt resumes the previous complete save. Planned pauses do not exhaust
retries. Non-resumable jobs fail loudly after three infrastructure interruptions;
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
