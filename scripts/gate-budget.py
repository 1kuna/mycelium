#!/usr/bin/env python3
"""Bound quick and stop its fixtures before removing their working files."""
import os
from pathlib import Path
import shutil
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def owned_processes(marker, scratch):
    # Fixtures deliberately create new sessions/groups, and can be reparented
    # before the gate exits. An inherited, unique marker survives both. Read ps
    # privately: its environment output may contain credentials; never log it.
    output = subprocess.check_output(
        ['ps', 'eww', '-axo', 'pid=,uid=,stat=,command='], text=True)
    owned = set()
    # Real worker commands allowlist their environment (#219), retaining only
    # HOME/TMPDIR from our isolation. Their roots still prove this run's scope.
    state = re.compile(r'(?:^|\s)(?:HOME|TMPDIR)=' + re.escape(scratch) + r'(?:/|\s|$)')
    for line in output.splitlines():
        fields = line.split(None, 3)
        if (len(fields) == 4 and fields[1] == str(os.getuid()) and
                not fields[2].startswith('Z') and
                (marker in fields[3].split() or state.search(fields[3]))):
            owned.add(int(fields[0]))
    return owned


def signal_owned(marker, scratch, sig):
    # Signal individual members, never a group which may include someone else.
    for pid in owned_processes(marker, scratch):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            print(f'quick: cannot signal owned fixture {pid}; scratch retained if still active',
                  file=sys.stderr)


def stop_fixtures(marker, scratch, child):
    signal_owned(marker, scratch, signal.SIGTERM)
    until = time.monotonic() + 5
    while owned_processes(marker, scratch) and time.monotonic() < until:
        child.poll()
        time.sleep(.05)
    signal_owned(marker, scratch, signal.SIGKILL)
    # SIGKILL delivery is asynchronous. Verify exit before deleting working files.
    until = time.monotonic() + 1
    while owned_processes(marker, scratch) and time.monotonic() < until:
        child.poll()
        time.sleep(.05)
    child.poll()
    return not owned_processes(marker, scratch)


def remove_scratch(path):
    def writable(function, name, error):
        os.chmod(os.path.dirname(name), 0o700)
        os.chmod(name, 0o700)
        function(name)
    shutil.rmtree(path, onerror=writable)


def main():
    signal.signal(signal.SIGTERM, interrupted)
    started = time.monotonic()
    status, timed_out = 1, False
    root = Path(sys.argv[1]).resolve().parent.parent / '.gate'
    root.mkdir(exist_ok=True)
    scratch = tempfile.mkdtemp(prefix='run.', dir=root)
    marker = 'GATE_RUN_ID=' + uuid.uuid4().hex
    child_env = dict(os.environ, MYCE_GATE_BUDGET='1', GATE_SCRATCH=scratch)
    child_env['GATE_RUN_ID'] = marker.split('=', 1)[1]
    child = subprocess.Popen(['bash', *sys.argv[1:]], env=child_env, start_new_session=True)
    try:
        # Reported related tests alone take 156s, before builds/vet. Allow five
        # minutes for quick feedback; reserve the last five seconds for TERM.
        status = child.wait(timeout=295)
        if status < 0:
            status = 128 - status
    except subprocess.TimeoutExpired:
        status, timed_out = 124, True
        print('quick: five-minute budget exhausted; run ./scripts/gate.sh full',
              file=sys.stderr, flush=True)
    except (KeyboardInterrupt, SystemExit) as error:
        status = error.code if isinstance(error, SystemExit) else 130
    finally:
        # Keep scratch available for TERM handlers/checkpoint saves. This also
        # catches leaked fixtures after an otherwise successful shell exit.
        if stop_fixtures(marker, scratch, child):
            remove_scratch(scratch)
        else:
            print(f'gate scratch retained: {scratch}', file=sys.stderr)
            if status == 0:
                status = 1
        detail = ' (timed out; run full)' if timed_out else ''
        print(f'gate quick: exit={status}{detail} wall={int(time.monotonic() - started)}s', flush=True)
    return status


if __name__ == '__main__':
    sys.exit(main())
