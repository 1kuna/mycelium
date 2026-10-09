# Resumable jobs

Set `resumable: true` only when the command can safely restart. Mycelium sends
SIGTERM to the command's process group, allows two minutes to save and exit,
then sends SIGKILL if necessary. The command must stay in that process group;
do not daemonize it. Save progress periodically as well: a lost machine cannot
save on demand.

Each attempt receives:

- `MYCE_JOB_ID`: the stable job ID.
- `MYCE_ATTEMPT`: the attempt number, starting at 1.
- `MYCE_CHECKPOINT_DIR`: a stable folder for this job under the queue's
  `--checkpoints` root. The worker creates it before starting the command.

Mount the shared NAS at the **same absolute path on every fleet machine**.
In `myce dev`, a local folder provides the same checkpoint contract.
Checkpoints belong to the job and are preserved after completion or failure.
Use atomic replacements so an interrupted write leaves the previous checkpoint
usable. A real job should also make its output writes safe to retry.

Here is a complete small Python command (`resume.py`, 11 lines):

```python
import os, pathlib, signal, time
p = pathlib.Path(os.environ["MYCE_CHECKPOINT_DIR"]) / "progress"
n = int(p.read_text()) if p.exists() else 0
stopping = False
def stop(*_):
    global stopping; stopping = True
signal.signal(signal.SIGTERM, stop)
while n < 100 and not stopping:
    time.sleep(0.1); n += 1  # replace with one unit of useful work
    tmp = p.with_suffix(".tmp"); tmp.write_text(str(n)); tmp.replace(p)
print(f"saved {n}; attempt {os.environ['MYCE_ATTEMPT']}", flush=True)
```

A planned stop puts the job at the front of its priority line. On its next
attempt this script reads the same file and continues. A command failure is
reported as failed; infrastructure loss or requested restart retries up to
three total attempts. After the third interrupted attempt the queue fails
loudly rather than looping forever.

For a quick preemption demonstration, start `myce dev --workers 1`, submit this
script as an experiment, then submit a production command requiring the same
worker memory. Follow the experiment's logs to see its second attempt.
