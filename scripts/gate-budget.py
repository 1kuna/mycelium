#!/usr/bin/env python3
"""Supervise gate custody and stop fixtures before removing working files."""
import os
import fcntl
import json
from pathlib import Path
import shutil
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid


MACOS_HANDLE_PROBE = [sys.executable, str(Path(__file__).with_name('gate-handles.py'))]


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
        # Apple's lsof cannot exclude network mounts, and even +D scans can
        # block on unrelated storage. libproc reads process paths directly.
        if not pids:
            return found
        command = [*MACOS_HANDLE_PROBE, scratch, *map(str, sorted(pids))]
        # Keep native inspection in a bounded subprocess: an unavailable kernel
        # probe costs retained scratch, never fixture signals or deletion.
        with tempfile.TemporaryFile(mode='w+t') as output_file:
            probe = subprocess.Popen(command, stdout=output_file, stderr=subprocess.DEVNULL)
            try:
                status = probe.wait(timeout=5)
            except subprocess.TimeoutExpired:
                probe.kill()  # Only the probe we just launched, never candidates.
                try:
                    probe.wait(timeout=.2)
                except subprocess.TimeoutExpired:
                    pass  # Do not wait for uninterruptible kernel I/O to return.
                raise
            if status != 0:
                raise subprocess.CalledProcessError(status, command)
            output_file.seek(0)
            output = output_file.read()
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


def process_start(pid):
    """Return a live process's birth identity, not just a reusable PID."""
    output = subprocess.check_output(
        ['ps', '-axo', 'pid=,stat=,lstart='], text=True,
        env=dict(os.environ, LC_ALL='C'))
    for line in output.splitlines():
        fields = line.split(None, 2)
        if len(fields) != 3:
            raise ValueError('incomplete process identity inventory')
        if int(fields[0]) == pid:
            return None if fields[1].startswith('Z') else fields[2]
    return None


def write_full_custody(slot, marker=None, scratch=None):
    record = dict(holder_pid=os.getpid(), holder_start=process_start(os.getpid()), phase='reserved')
    if not record['holder_start']:
        raise RuntimeError('cannot establish full-suite holder identity')
    if marker is not None:
        record.update(marker=marker, scratch=scratch, phase='running')
    # Publish all-or-nothing, including the reservation before scratch exists.
    # A crash before this rename cannot have started any fixtures; a crash after
    # it always leaves either the reservation or the complete run identity.
    path = Path(slot.name).with_suffix('.custody')
    staging = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        staging.write_text(json.dumps(record) + '\n')
        staging.replace(path)
    finally:
        staging.unlink(missing_ok=True)


def reconcile_full_custody(slot):
    """Reclaim an unlocked abandoned record only after fixture-exit proof."""
    path = Path(slot.name).with_suffix('.custody')
    if not path.exists():
        return True
    try:
        record = json.loads(path.read_text())
        if 'holder_pid' in record:
            pid, birth = record['holder_pid'], record['holder_start']
            if not isinstance(pid, int) or pid <= 0 or not isinstance(birth, str) or not birth:
                return False
            if (record.get('phase') not in ('reserved', 'running')
                    or (record['phase'] == 'running' and 'marker' not in record)
                    or (record['phase'] == 'reserved' and ('marker' in record or 'scratch' in record))):
                return False
            if process_start(pid) == birth:
                return False
        elif 'marker' not in record:
            return False  # Old incomplete/unknown records need owner recovery.
        if 'marker' in record:
            marker, scratch = record['marker'], record['scratch']
            if (not isinstance(marker, str) or not re.fullmatch(r'GATE_RUN_ID=[0-9a-f]{32}', marker)
                    or not isinstance(scratch, str) or not os.path.isabs(scratch)
                    or Path(scratch).name in ('', '.', '..') or not Path(scratch).name.startswith('run.')):
                return False
            # A cancelled job or dead supervisor may leave detached fixtures.
            # Inventory only: reconciliation never signals another run or
            # removes its scratch. Legacy records use the same exit proof.
            if owned_processes(marker, scratch, {}, inspect_handles=True):
                return False
        # The caller holds both admission and slot locks. Retain the old record
        # as evidence; age never grants admission or proves fixture exit.
        path.rename(path.with_name(path.name + '.reconciled-' + uuid.uuid4().hex))
        print('gate: reconciled abandoned full-suite custody', flush=True)
        return True
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError):
        return False  # Unknown ownership/inventory keeps the slot occupied.


def acquire_full_slot():
    """Hold a cross-checkout slot for the whole full gate, not each package."""
    cache = Path(subprocess.check_output(['go', 'env', 'GOCACHE'], text=True).strip())
    cache.mkdir(parents=True, exist_ok=True)
    announced = False
    while True:
        # Serialize the availability snapshot. Slot numbers do not encode how
        # many suites are active: slot 1 can remain held after slot 0 exits.
        with (cache / 'mycelium-full-gate-admission.lock').open('a') as admission:
            fcntl.flock(admission, fcntl.LOCK_EX)
            processes = subprocess.check_output(['ps', '-axo', 'pid=,command='], text=True)
            legacy = sum(bool(re.search(r'\s(?:\S*/)?go test .*?-race', line)) and '-run' not in line
                         for line in processes.splitlines())
            available = []
            try:
                # Two full suites is the repository's observed Mac safety
                # boundary, independent of package count and CPU defaults.
                for index in range(2):
                    handle = (cache / f'mycelium-full-gate-{index}.lock').open('a')
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        if not reconcile_full_custody(handle):
                            handle.close()
                        else:
                            available.append(handle)
                    except BlockingIOError:
                        handle.close()
                occupied = 2 - len(available)
                if available and occupied + legacy < 2:
                    handle = available[0]
                    # Persist occupancy before starting work. A supervisor exit
                    # releases flock even when its fixtures have not exited.
                    write_full_custody(handle)
                    return available.pop(0)
            finally:
                for handle in available:
                    handle.close()
        if not announced:
            print('waiting for a full-race slot (two-suite custody boundary)', flush=True)
            announced = True
        time.sleep(.25)  # Local semaphore readiness check, not a work deadline.


def release_full_slot(slot, stopped):
    if stopped:
        Path(slot.name).with_suffix('.custody').unlink()
    else:
        print('gate: full-suite custody unconfirmed; admission slot retained', file=sys.stderr)
    slot.close()


def main():
    signal.signal(signal.SIGTERM, interrupted)
    started = time.monotonic()
    status = 1
    mode = 'full' if 'full' in sys.argv[2:] else 'quick'
    # The repository's two-suite safety rule is specific to Macs. Linux fleet
    # jobs are already admitted against declared CPU and learned host memory;
    # an additional fixed suite count would strand those allocated resources.
    slot = acquire_full_slot() if (mode == 'full' and sys.platform == 'darwin'
                                   and os.environ.get('GATE_LOCAL_CAPACITY_HELD') != '1') else None
    root = Path(sys.argv[1]).resolve().parent.parent / '.gate'
    root.mkdir(exist_ok=True)
    scratch = tempfile.mkdtemp(prefix='run.', dir=root)
    marker = 'GATE_RUN_ID=' + uuid.uuid4().hex
    if slot is not None:
        write_full_custody(slot, marker, scratch)
    child_env = dict(os.environ, MYCE_GATE_BUDGET='1', GATE_SCRATCH=scratch)
    child_env['GATE_RUN_ID'] = marker.split('=', 1)[1]
    child = subprocess.Popen(['bash', *sys.argv[1:]], env=child_env, start_new_session=not bool(os.environ.get('MYCE_PROCESS_SESSION')))
    try:
        # Per-command progress watchdogs decide stalls; no gate wall deadline.
        status = child.wait()
        if status < 0:
            status = 128 - status
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
            status = 125  # Unconfirmed custody prevents capacity reuse.
        if slot is not None:
            release_full_slot(slot, stopped)
        print(f'gate {mode}: exit={status} wall={int(time.monotonic() - started)}s', flush=True)
    return status


if __name__ == '__main__':
    sys.exit(main())
