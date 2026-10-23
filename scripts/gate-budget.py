#!/usr/bin/env python3
"""Bound the entire quick gate, including builds and cache setup, on macOS/Linux."""
import os
import signal
import subprocess
import sys

child_env = dict(os.environ, MYCE_GATE_BUDGET='1')
child = subprocess.Popen(['bash', *sys.argv[1:]], env=child_env, start_new_session=True)
try:
    sys.exit(child.wait(timeout=175))
except subprocess.TimeoutExpired:
    print('quick: three-minute budget exhausted; run ./scripts/gate.sh full',
          file=sys.stderr, flush=True)
    os.killpg(child.pid, signal.SIGTERM)
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    # Also stop grandchildren if the shell exited before them.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    sys.exit(124)
except KeyboardInterrupt:
    os.killpg(child.pid, signal.SIGTERM)
    child.wait()
    sys.exit(130)
