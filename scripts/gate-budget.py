#!/usr/bin/env python3
"""Supervise gate custody and stop fixtures before removing working files."""
import os
import fcntl
import hashlib
import importlib.util
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
import traceback
from email.utils import parsedate


MACOS_HANDLE_PROBE = [sys.executable, str(Path(__file__).with_name('gate-handles.py'))]


def admission_module():
    # Package watchdogs import this module without using host admission.
    spec = importlib.util.spec_from_file_location('gate_capacity', Path(__file__).with_name('gate-capacity.py'))
    capacity = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(capacity)
    return capacity


def archive_custody(*args):
    return admission_module().archive_custody(*args)


def interrupted(signum, frame):
    # Ignore a second guard signal before unwinding into the cleanup block.
    for ignored in (signal.SIGTERM, signal.SIGINT):
        signal.signal(ignored, signal.SIG_IGN)
    raise SystemExit(128 + signum)


def scratch_processes(scratch, pids, all_handles=False, protected_roots=None, evidence=None):
    """Use OS file handles for reparented fixtures, including env -i payloads."""
    def inside(name):
        # OS handle inventories give resolved paths. Do not stat every path in
        # lsof's output: unrelated network mounts must not stall local cleanup.
        return name == scratch or name.startswith(scratch + os.sep)
    found = set()
    if sys.platform.startswith('linux'):
        for pid in pids:
            entries = ['cwd', 'exe', 'fd/0', 'fd/1', 'fd/2']
            if all_handles:
                try:
                    entries = ['cwd', 'exe'] + [str(p.relative_to(Path('/proc') / str(pid)))
                                                for p in (Path('/proc') / str(pid) / 'fd').iterdir()]
                except FileNotFoundError:
                    continue
            for entry in entries:
                try:
                    if inside(os.readlink(f'/proc/{pid}/{entry}')):
                        found.add(pid)
                except FileNotFoundError:
                    pass
                except PermissionError:
                    if all_handles:
                        raise
            if all_handles:
                try:
                    with open(f'/proc/{pid}/maps') as mappings:
                        for line in mappings:
                            fields = line.rstrip('\n').split(None, 5)
                            if len(fields) < 5:
                                raise RuntimeError('mapped-file inventory is incomplete')
                            if len(fields) == 6:
                                name = re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), fields[5])
                                if inside(name):
                                    found.add(pid)
                except FileNotFoundError:
                    pass  # Process exited before its mapping inventory.
    else:
        # Apple's lsof cannot exclude network mounts, and even +D scans can
        # block on unrelated storage. libproc reads process paths directly.
        if not pids:
            return found
        command = [*MACOS_HANDLE_PROBE, *(['--all-fds'] if all_handles else []),
                   *(['--protected-roots', ','.join(map(str, sorted(protected_roots)))]
                     if protected_roots is not None else []),
                   scratch, *map(str, sorted(pids))]
        # Keep native inspection in a bounded subprocess: an unavailable kernel
        # probe costs retained scratch, never fixture signals or deletion.
        with tempfile.TemporaryFile(mode='w+t') as output_file, tempfile.TemporaryFile(mode='w+t') as error_file:
            probe = subprocess.Popen(command, stdout=output_file, stderr=error_file)
            failure, status = None, None
            try:
                # Complete recovery inventories exceeded the old 5s cap under
                # ordinary load. The measured host probe maximum was 5.39s;
                # 30s rounds up roughly 4x that observation (MY-34). Timeout
                # still refuses custody release; ordinary cleanup stays 5s.
                status = probe.wait(timeout=30 if all_handles else 5)
            except subprocess.TimeoutExpired as error:
                failure = error
                probe.kill()  # Only the probe we just launched, never candidates.
                try:
                    probe.wait(timeout=.2)
                except subprocess.TimeoutExpired:
                    pass  # Do not wait for uninterruptible kernel I/O to return.
            output_file.seek(0)
            output = output_file.read()
            error_file.seek(0)
            probe_error = error_file.read()
        pid = None
        for line in output.splitlines():
            if line.startswith('p'):
                pid = int(line[1:])
            elif pid in pids and line.startswith('n') and inside(line[1:]):
                found.add(pid)
            elif line.startswith('x'):
                if evidence is None:
                    raise RuntimeError('protected process exclusion lacks an evidence receipt')
                evidence.append(json.loads(line[1:]))
        if failure is not None:
            raise failure
        if status != 0:
            raise subprocess.CalledProcessError(status, command, stderr=probe_error)
    return found


def owned_processes(marker, scratch, known, inspect_handles=False, complete_handles=False, born_after=None):
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
        if complete_handles:
            # A fixture cannot predate its holder. In particular Linux's
            # non-dumpable sd-pam session helper is not a gate fixture; probing
            # it would make every ordinary user's scratch inventory fail.
            cutoff = parsedate(born_after or '')
            candidates = {pid for pid, (_, birth) in processes.items()
                          if not cutoff or not parsedate(birth) or parsedate(birth) >= cutoff}
            owned.update(scratch_processes(scratch, candidates, all_handles=True,
                                          protected_roots=set(known), evidence=[]))
        else:
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


def full_custody_paths(slot):
    host = getattr(slot, 'host_custody', None)
    return ([host] if host else []) + [Path(slot.name).with_suffix('.custody')]


def write_full_custody(slot, marker=None, scratch=None):
    record = dict(holder_pid=os.getpid(), holder_start=process_start(os.getpid()), phase='reserved')
    if not record['holder_start']:
        raise RuntimeError('cannot establish full-suite holder identity')
    if marker is not None:
        record.update(marker=marker, scratch=scratch, phase='running')
    # Each record publishes atomically, host first, before fixtures can start.
    # A partial publication leaves conservative host custody; complete records
    # retain either the reservation or the run identity after supervisor death.
    for path in full_custody_paths(slot):
        staging = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        try:
            staging.write_text(json.dumps(record) + '\n')
            staging.replace(path)
        finally:
            staging.unlink(missing_ok=True)
    slot.custody_record = record


def full_custody_idle(record, dead_releases=True):
    if 'holder_pid' in record:
        pid, birth = record['holder_pid'], record.get('holder_start')
        if type(pid) is not int or pid <= 0:
            return False
        current = process_start(pid)
        if dead_releases:
            return current is None or isinstance(birth, str) and bool(birth.strip()) and current != birth
        if not isinstance(birth, str) or not birth:
            return False
        if (record.get('phase') not in ('reserved', 'running')
                or (record['phase'] == 'running' and 'marker' not in record)
                or (record['phase'] == 'reserved' and ('marker' in record or 'scratch' in record))):
            return False
        if current == birth:
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
    return True


def reconcile_full_custody(slot, local=True):
    """Reclaim dead holders automatically; preserve live/unknown ownership."""
    try:
        paths = [path for path in full_custody_paths(slot) if path.exists()]
        if not all(full_custody_idle(json.loads(path.read_text()), dead_releases=local) for path in paths):
            return False
        # The caller holds host/cache admission and the cache-slot lock. Keep
        # both old records as evidence; age alone never releases custody.
        if paths:
            row = json.loads(paths[-1].read_text())
            archive_custody(Path(slot.name).parent, paths, row, 'dead full holder')
        return True
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError):
        return False  # Unknown ownership/inventory keeps the slot occupied.


def try_full_slot(cache, control):
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
                if control:
                    key = hashlib.sha256(str(Path(handle.name).resolve()).encode()).hexdigest()
                    handle.host_custody = control / ('local-go-native-full-' + key + '.custody')
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    if not reconcile_full_custody(handle, local=control is not None):
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
    return None


def acquire_full_slot():
    """Legacy native fulls share host admission; fleet jobs remain background."""
    cache = Path(subprocess.check_output(['go', 'env', 'GOCACHE'], text=True).strip())
    cache.mkdir(parents=True, exist_ok=True)
    admission_module()._cache.trim_backups(cache / 'selfheal-backups')
    worker_files = (Path.home() / '.local/share/mycelium/worker/state/job-files').resolve()
    fleet = Path.cwd().resolve().is_relative_to(worker_files)
    control = Path.home() / '.cache/mycelium-gates/control'
    if not fleet:
        control.mkdir(parents=True, exist_ok=True)
    announced = False
    while True:
        if fleet:
            slot = try_full_slot(cache, None)
        else:
            # Same lock/order as gate-capacity: a resource group cannot start
            # between the group check and publication of native reservation.
            with (control / 'local-go-admission.lock').open('a') as admission:
                fcntl.flock(admission, fcntl.LOCK_EX)
                slot = (None if (control / 'local-go-resource-group.custody').exists()
                        else try_full_slot(cache, control))
        if slot is not None:
            slot.local_development = not fleet
            return slot
        if not announced:
            print('waiting for a full-race slot (two-suite custody boundary)', flush=True)
            announced = True
        time.sleep(.25)  # Local semaphore readiness check, not a work deadline.


def release_full_slot(slot, stopped):
    if not stopped and not getattr(slot, 'local_development', True):
        print('gate: fleet full cleanup unconfirmed; custody retained', file=sys.stderr)
        slot.close()
        return
    paths = full_custody_paths(slot)
    expected = getattr(slot, 'custody_record', None)
    if expected is not None and any(json.loads(path.read_text()) != expected for path in paths):
        raise RuntimeError('native full custody changed; retained')
    if stopped:
        # Removing cache custody first leaves host custody binding until last.
        for path in reversed(paths):
            path.unlink()
    else:
        archive_custody(Path(slot.name).parent, paths, expected or {},
                        'cleanup unconfirmed; verdict retained')
    slot.close()
    admission_module()._cache.trim_backups(Path(slot.name).parent / 'selfheal-backups')


def local_resource_admission():
    """Only the canonical host admission can replace the legacy suite count."""
    path = os.environ.get('GATE_LOCAL_SLOT')
    if not path:
        return False
    lane = Path(path).resolve()
    control = (Path.home() / '.cache/mycelium-gates/control').resolve()
    row = json.loads((lane / 'owner.json').read_text())
    group = json.loads((control / 'local-go-resource-group.custody').read_text())
    guards = [control / name / 'owner.json' for name in ('local-go-slot', 'local-go-slot-2')]
    expected_guard = dict(protocol_guard=True, resource_group=group.get('resource_group'))
    if (lane.parent != control or not lane.name.startswith('local-go-slot-') or
            row.get('admission_protocol') != 2 or
            group.get('admission_protocol') != 2 or
            not isinstance(group.get('admission_config_sha256'), str) or
            len(group['admission_config_sha256']) != 64 or
            row.get('resource_group') != group.get('resource_group') or
            row.get('admission_config_sha256') != group.get('admission_config_sha256') or
            row.get('run_id') != os.environ.get('GATE_LOCAL_SLOT_ID') or
            row.get('source_root') != str(Path.cwd().resolve()) or
            any(json.loads(p.read_text()) != expected_guard for p in guards)):
        raise RuntimeError('local resource admission identity is unconfirmed')
    # The admitting owner must still be an ancestor, not a reused/dead PID.
    parents = dict(map(int, line.split()) for line in subprocess.check_output(
        ['ps', '-axo', 'pid=,ppid='], text=True).splitlines() if line.strip())
    pid = os.getpid()
    visited = set()
    while pid in parents and pid != row['pid'] and pid not in visited:
        visited.add(pid)
        pid = parents[pid]
    if pid != row['pid']:
        raise RuntimeError('local resource owner is not an execution ancestor')
    return row


def main():
    signal.signal(signal.SIGTERM, interrupted)
    started = time.monotonic()
    status = 1
    mode = 'full' if 'full' in sys.argv[2:] else 'quick'
    # The repository's two-suite safety rule is specific to Macs. Linux fleet
    # jobs are already admitted against declared CPU and learned host memory;
    # an additional fixed suite count would strand those allocated resources.
    try:
        admitted = local_resource_admission() if sys.platform == 'darwin' else False
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print('STOP gate: local resource admission unconfirmed:', error, file=sys.stderr)
        return 125
    slot = acquire_full_slot() if mode == 'full' and sys.platform == 'darwin' and not admitted else None
    root = Path(sys.argv[1]).resolve().parent.parent / '.gate'
    root.mkdir(exist_ok=True)
    if isinstance(admitted, dict) and admitted.get('run_marker') and admitted.get('scratch'):
        scratch = str(root / ('run.' + admitted['run_marker']))
        if scratch != admitted['scratch']:
            raise RuntimeError('admitted scratch identity changed')
        Path(scratch).mkdir(mode=0o700)
        marker = 'GATE_RUN_ID=' + admitted['run_marker']
    else:
        identity = os.environ.get('GATE_CUSTODY_ID')
        if identity and re.fullmatch('[0-9a-f]{32}', identity):
            scratch = str(root / ('run.' + identity))
            if scratch != os.environ.get('GATE_SCRATCH'):
                raise RuntimeError('admitted scratch identity changed')
            Path(scratch).mkdir(mode=0o700)
            marker = 'GATE_RUN_ID=' + identity
        else:
            scratch = tempfile.mkdtemp(prefix='run.', dir=root)
            marker = 'GATE_RUN_ID=' + uuid.uuid4().hex
    if slot is not None:
        write_full_custody(slot, marker, scratch)
    child_env = dict(os.environ, MYCE_GATE_BUDGET='1', GATE_SCRATCH=scratch)
    child_env.pop('GATE_CUSTODY_ID', None)
    child_env.pop('GATE_LOCAL_SLOT', None)
    child_env.pop('GATE_LOCAL_SLOT_ID', None)
    child_env['GATE_RUN_ID'] = marker.split('=', 1)[1]
    runtime_dir = root.parent / '.reports/gate-runtime' / child_env['GATE_RUN_ID']
    runtime_dir.mkdir(parents=True, mode=0o700)
    child_env['GATE_RUNTIME_DIR'] = str(runtime_dir)
    # Receipt authority belongs only to this supervisor, never nested fixtures.
    receipt_path = child_env.pop('GATE_CLEANUP_RECEIPT', None)
    receipt_identity = child_env.pop('GATE_CLEANUP_ID', None)
    custody_path = child_env.pop('GATE_EXECUTION_CUSTODY', None)
    if custody_path:
        birth = subprocess.check_output(['ps', '-p', str(os.getpid()), '-o', 'lstart='],
                                        text=True, env=dict(os.environ, LC_ALL='C')).strip()
        if not birth:
            raise RuntimeError('budget supervisor identity unavailable')
        path = Path(custody_path)
        pending = path.with_name(path.name + '.' + uuid.uuid4().hex)
        pending.write_text(json.dumps(dict(schema=1, identity=receipt_identity,
            run_marker=marker.split('=', 1)[1], scratch=scratch,
            supervisors=[dict(pid=os.getpid(), process_identity=birth)])) + '\n')
        pending.replace(path)
    child = subprocess.Popen(['bash', *sys.argv[1:]], env=child_env, start_new_session=not bool(os.environ.get('MYCE_PROCESS_SESSION')))
    try:
        # Per-command progress watchdogs decide stalls; no gate wall deadline.
        status = child.wait()
        if status < 0:
            status = 128 - status
    except (KeyboardInterrupt, SystemExit) as error:
        status = error.code if isinstance(error, SystemExit) else 130
    finally:
        # Host and native measurement guards can deliver TERM independently.
        # Once cleanup starts, another signal must not interrupt its custody
        # inventory or prevent the affirmative receipt from being published.
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, signal.SIG_IGN)
        # Keep scratch available for TERM handlers/checkpoint saves. This also
        # catches leaked fixtures after an otherwise successful shell exit.
        try:
            stopped = stop_fixtures(marker, scratch, child)
            (runtime_dir / 'cleanup.json').write_text(json.dumps(dict(stopped=stopped)) + '\n')
            if stopped:
                remove_scratch(scratch)
        except Exception as error:
            stopped = False
            diagnostic = dict(error_type=type(error).__name__, error=str(error),
                              traceback=traceback.format_exc(),
                              stderr=getattr(error, 'stderr', None))
            try:
                (runtime_dir / 'cleanup-error.json').write_text(json.dumps(diagnostic) + '\n')
            except OSError:
                print(json.dumps(diagnostic), file=sys.stderr)
            print('quick: fixture inventory failed; scratch retained: ' + str(error), file=sys.stderr)
        if not stopped:
            print(f'gate scratch retained: {scratch}', file=sys.stderr)
            status = 125  # Cleanup failure is a real verdict, not a capacity reservation.
        if slot is not None:
            release_full_slot(slot, stopped)
        if receipt_path and receipt_identity:
            # Publish after cleanup and admission custody have both settled.
            # A killed supervisor cannot leave a success-shaped receipt.
            path = Path(receipt_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            pending = path.with_name(path.name + '.' + uuid.uuid4().hex)
            pending.write_text(json.dumps(dict(schema=1, identity=receipt_identity,
                                               stopped=stopped, status=status)) + '\n')
            pending.replace(path)
        print(f'gate {mode}: exit={status} wall={int(time.monotonic() - started)}s', flush=True)
    return status


if __name__ == '__main__':
    sys.exit(main())
