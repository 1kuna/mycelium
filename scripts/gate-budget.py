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


def scratch_processes(scratch, pids):
    """Use OS file handles for reparented fixtures, including env -i payloads."""
    def inside(name):
        # OS handle inventories give resolved paths. Do not stat every path in
        # lsof's output: unrelated network mounts must not stall local cleanup.
        return name == scratch or name.startswith(scratch + os.sep)
    found = set()
    if sys.platform.startswith('linux'):
        for pid in pids:
            for entry in ('cwd', 'exe', 'fd/0', 'fd/1', 'fd/2'):
                try:
                    if inside(os.readlink(f'/proc/{pid}/{entry}')):
                        found.add(pid)
                except (FileNotFoundError, PermissionError):
                    pass
    else:
        # macOS ships lsof; inspect only working/executable/standard handles.
        # A peer merely reading a log on another descriptor is not a fixture.
        output = subprocess.check_output(
            ['lsof', '-b', '-nP', '-a', '-u', str(os.getuid()), '-d', 'cwd,txt,0,1,2', '-Fpn'],
            text=True, stderr=subprocess.DEVNULL)
        pid = None
        for line in output.splitlines():
            if line.startswith('p'):
                pid = int(line[1:])
            elif pid in pids and line.startswith('n') and inside(line[1:]):
                found.add(pid)
    return found


def owned_processes(marker, scratch, known, inspect_handles=False):
    # Read privately: ps environments can contain credentials; never log them.
    # Retain start identities once ownership is proven, so TERM cannot hide a
    # scrubbed child by killing its parent or closing its inherited log handles.
    # GNU ps rejects BSD -ax combined with eww; -e selects all on Linux.
    selection = '-eo' if sys.platform.startswith('linux') else '-axo'
    output = subprocess.check_output(
        ['ps', 'eww', selection, 'pid=,ppid=,uid=,stat=,lstart=,command='],
        text=True, env=dict(os.environ, LC_ALL='C'))
    processes = {}
    state = re.compile(r'(?:^|\s)(?:HOME|TMPDIR)=' + re.escape(scratch) + r'(?:/|\s|$)')
    owned = set()
    for line in output.splitlines():
        fields = line.split(None, 9)
        if len(fields) != 10 or fields[2] != str(os.getuid()) or fields[3].startswith('Z'):
            continue
        pid, parent = int(fields[0]), int(fields[1])
        birth = ' '.join(fields[4:9])
        processes[pid] = (parent, birth)
        if (known.get(pid) == birth or marker in fields[9].split() or state.search(fields[9])):
            owned.add(pid)
    if inspect_handles:
        owned.update(scratch_processes(scratch, processes))
    # Capture descendants before delivering any signal, even when they scrub
    # all environment and put their own cwd/output outside the scratch tree.
    while True:
        descendants = {pid for pid, (parent, _) in processes.items() if parent in owned}
        if descendants <= owned:
            break
        owned.update(descendants)
    known.update({pid: processes[pid][1] for pid in owned})
    return owned


def signal_owned(marker, scratch, sig, known):
    # Re-inventory identities before each signal phase; never signal a group.
    pids = owned_processes(marker, scratch, known, inspect_handles=True)
    for pid in pids:
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            print(f'quick: cannot signal owned fixture {pid}; scratch retained if still active',
                  file=sys.stderr)
    return bool(pids)


def stop_fixtures(marker, scratch, child):
    known = {}
    if not signal_owned(marker, scratch, signal.SIGTERM, known):
        return True
    until = time.monotonic() + 5
    while owned_processes(marker, scratch, known) and time.monotonic() < until:
        child.poll()
        time.sleep(.05)
    signal_owned(marker, scratch, signal.SIGKILL, known)
    # SIGKILL delivery is asynchronous. Verify exit before deleting working files.
    until = time.monotonic() + 1
    while owned_processes(marker, scratch, known) and time.monotonic() < until:
        child.poll()
        time.sleep(.05)
    child.poll()
    return not owned_processes(marker, scratch, known, inspect_handles=True)


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
        try:
            stopped = stop_fixtures(marker, scratch, child)
        except (OSError, subprocess.SubprocessError):
            stopped = False
            print('quick: fixture inventory failed; scratch retained', file=sys.stderr)
        if stopped:
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
