"""Checkpoint/stop diagnostic, not a GPU workload; safe on a laptop."""
import json
import os
from pathlib import Path
import signal
import time

root = Path(os.environ["MYCE_CHECKPOINT_DIR"])
path = root / "checkpoint.json"
legacy = root / "progress"
saved = json.loads(path.read_text()) if path.exists() else (int(legacy.read_text()) if legacy.exists() else 0)
unit = saved["value"] if isinstance(saved, dict) else saved
sequence = saved["myce_checkpoint"]["generation"] if isinstance(saved, dict) else 0
stopping = False


def stop(*_):
    global stopping
    stopping = True


def save():
    global sequence
    generation = sequence + 1
    temporary = root / "checkpoint.tmp"
    with temporary.open("w") as stream:
        json.dump({"myce_checkpoint": {"generation": generation}, "value": unit}, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    fd = os.open(root, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    sequence = generation


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
print(f"START attempt={os.environ['MYCE_ATTEMPT']} resume={unit}", flush=True)
while unit < 60 and not stopping:
    time.sleep(1)  # diagnostic only: replace with bounded GPU work in a real job
    unit += 1
    try:
        save()
        print(f"checkpoint={unit}", flush=True)
    except OSError as error:
        print(f"checkpoint retry: {error}", flush=True)
while True:
    try:
        save()
        break
    except OSError as error:
        print(f"final checkpoint retry: {error}", flush=True)
        time.sleep(1)
print(f"STOP saved={unit}" if stopping else f"DONE saved={unit}", flush=True)
