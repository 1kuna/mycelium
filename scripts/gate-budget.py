#!/usr/bin/env python3
"""Bound the entire quick gate, including builds and cache setup, on macOS/Linux."""
import os
import signal
import subprocess
import sys
import time


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


signal.signal(signal.SIGTERM, interrupted)
started = time.monotonic()
status = 1
timed_out = False
child_env = dict(os.environ, MYCE_GATE_BUDGET='1')
child = subprocess.Popen(['bash', *sys.argv[1:]], env=child_env, start_new_session=True)
try:
    status = child.wait(timeout=175)
    if status < 0:
        status = 128 - status
except subprocess.TimeoutExpired:
    status, timed_out = 124, True
    print('quick: three-minute budget exhausted; run ./scripts/gate.sh full',
          file=sys.stderr, flush=True)
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    # Also stop grandchildren if the shell exited before them.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
except (KeyboardInterrupt, SystemExit) as error:
    status = error.code if isinstance(error, SystemExit) else 130
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    child.wait()
finally:
    detail = ' (timed out; run full)' if timed_out else ''
    print(f'gate quick: exit={status}{detail} wall={int(time.monotonic() - started)}s', flush=True)
sys.exit(status)
