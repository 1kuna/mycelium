#!/usr/bin/env python3
"""Local heavy-work admission shared across checkouts and release builds."""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid

GIB = 1024 ** 3
ENTRY_FLOOR = 45 * GIB
STOP_FLOOR = 42 * GIB


def process_identity(pid):
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart='],
                            capture_output=True, text=True, timeout=5)
    if result.returncode not in (0, 1):
        raise RuntimeError('waiting process identity is unknown')
    return result.stdout.strip() or None


def full_waiters(directory):
    rows = []
    for path in directory.glob('local-go-wait-*.json'):
        row = json.loads(path.read_text())
        if process_identity(row['pid']) != row['process_identity']:
            # This is a never-admitted queue ticket, not execution custody.
            path.unlink()
            continue
        rows.append(({'calibration': 0, 'release': 1, 'normal': 2}[row['priority']],
                     row['sequence'], path))
    return [path for _, _, path in sorted(rows)]


def root(repo):
    # Caches, VM capacity and full concurrency are host-wide, including clones.
    return Path.home() / '.cache/mycelium-gates/control'


def legacy_directories(repo):
    directory = root(repo)
    registry = directory / 'legacy-roots.json'
    roots = set(json.loads(registry.read_text())) if registry.exists() else set()
    common = Path(subprocess.check_output(['git', '-C', str(repo), 'rev-parse',
                                          '--git-common-dir'], text=True).strip())
    if not common.is_absolute():
        common = repo / common
    roots.add(str(common.resolve().parent / '.reports'))
    pending = registry.with_suffix('.tmp')
    pending.write_text(json.dumps(sorted(roots)) + '\n')
    pending.replace(registry)
    return [Path(path) for path in roots]


def occupied_lanes(directory):
    return [p for p in directory.glob('local-go-slot*') if p.is_dir()]


def native_full_custody():
    caches = {Path.home() / 'Library/Caches/mycelium/gates/go-build'}
    caches.add(Path(subprocess.check_output(['go', 'env', 'GOCACHE'], text=True).strip()))
    return any(list(cache.glob('mycelium-full-gate-[0-9].custody')) for cache in caches)


def available_memory():
    if os.uname().sysname == 'Darwin':
        # vm_stat excludes compressed pages: do not count them as usable RAM.
        output = subprocess.check_output(['vm_stat'], text=True)
        import re
        page = int(re.search(r'page size of (\d+)', output).group(1))
        fields = dict((k, int(v)) for k, v in re.findall(r'^([^:]+):\s+(\d+)\.', output, re.M))
        return page * sum(fields.get(k, 0) for k in
                          ('Pages free', 'Pages inactive', 'Pages speculative'))
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) * 1024
    raise RuntimeError('available memory is unknown')


def check_pressure():
    if os.uname().sysname == 'Darwin':
        level = int(subprocess.check_output(['sysctl', '-n',
                    'kern.memorystatus_vm_pressure_level'], text=True, timeout=5))
        if level != 1:
            raise RuntimeError('host memory pressure is not normal; no work started')


class Slot:
    def __init__(self, directory, owner, full):
        self.directory, self.owner, self.full = directory, owner, full

    def release(self, stopped=True):
        if not stopped:
            print('gate: custody uncertain; local capacity retained', flush=True)
            return
        with admission_lock(self.directory.parent):
            path = self.directory / 'owner.json'
            if json.loads(path.read_text()) != self.owner:
                raise RuntimeError('local gate slot ownership changed; retained')
            path.unlink()
            self.directory.rmdir()
            if self.full:
                self.full.unlink()



@contextlib.contextmanager
def admission_lock(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'local-go-admission.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def memory_requirement(config, platform):
    measured = config.get('memory_measurement')
    if not measured:
        return 22 * GIB  # Unqualified hosts retain the initial conservative value.
    if not measured.get('evidence'):
        raise RuntimeError('memory qualification is missing its evidence')
    native = int(measured['native_peak_bytes'])
    headroom = int(measured['headroom_bytes'])
    if native <= 0 or headroom <= 0:
        raise RuntimeError('memory qualification has invalid peak/headroom')
    if platform == 'native':
        return native + headroom
    linux = int(measured.get('linux_peak_bytes') or measured['linux_provisional_bytes'])
    vm = int(measured['vm_peak_bytes'])
    return max(native, linux + vm) + headroom


def acquire(repo, *, mode='quick', priority='normal', cores=4, wait=False, platform='native', after_pid=None):
    """Conservative initial 2 heavy/1 full slots; legacy mkdir owners still bind.

    Increasing full concurrency requires a recorded measurement in gate-capacity.json.
    A release waiter takes the next free heavy slot ahead of normal gates.
    No stale owner is removed automatically, including after supervisor death.
    """
    directory = root(repo)
    directory.mkdir(parents=True, exist_ok=True)
    if priority == 'calibration' and mode != 'full':
        raise ValueError('calibration priority reserves a full lane only')
    prefix = 'local-go-calibration-wait-' if priority == 'calibration' else 'local-go-release-wait-'
    ticket = directory / (prefix + uuid.uuid4().hex)
    queue_ticket = None
    try:
        if priority in ('release', 'calibration'):
            ticket.write_text(str(os.getpid()) + '\n')
        if mode == 'full':
            with admission_lock(directory):
                sequence_path = directory / 'queue-sequence'
                sequence = int(sequence_path.read_text()) + 1 if sequence_path.exists() else 1
                sequence_path.write_text(str(sequence))
                queue_ticket = directory / ('local-go-wait-' + uuid.uuid4().hex + '.json')
                queue_ticket.write_text(json.dumps(dict(pid=os.getpid(), process_identity=process_identity(os.getpid()),
                    priority=priority, sequence=sequence)) + '\n')
        after_identity = process_identity(after_pid) if after_pid else None
        announced = None
        while True:
            check_pressure()
            if shutil.disk_usage(repo).free < ENTRY_FLOOR:
                raise RuntimeError('local disk below 45 GiB entry floor')
            with admission_lock(directory):
                legacy = [p for folder in legacy_directories(repo) for p in occupied_lanes(folder)]
                config_path = directory / 'gate-capacity.json'
                config = json.loads(config_path.read_text()) if config_path.exists() else {}
                # Two legacy lanes are authorized; full remains serialized until measured.
                slots = min(2, max(1, int(config.get('heavy_slots', 2))))
                fulls = min(2, max(1, int(config.get('full_slots', 1))))
                pending_release = priority not in ('release', 'calibration') and any(directory.glob('local-go-release-wait-*'))
                pending_calibration = (mode == 'full' and priority != 'calibration'
                                       and any(directory.glob('local-go-calibration-wait-*')))
                queue = full_waiters(directory) if mode == 'full' else []
                queue_first = not queue or queue[0] == queue_ticket
                batch_done = not after_identity or process_identity(after_pid) != after_identity
                full_marker = None
                if mode == 'full' and not legacy and not native_full_custody():
                    for index in range(fulls):
                        candidate = directory / f'local-go-full-{index}.custody'
                        if not candidate.exists():
                            full_marker = candidate
                            break
                needed_memory = memory_requirement(config, platform)
                memory_ok = available_memory() >= needed_memory
                if not memory_ok:
                    raise RuntimeError(f'available memory below {needed_memory / GIB:.2f} GiB entry requirement; no work started')
                occupied = len(legacy) + len(occupied_lanes(directory))
                if (not pending_release and not pending_calibration and queue_first and batch_done and occupied < slots
                        and (mode != 'full' or full_marker)):
                    for index in range(slots):
                        slot = directory / ('local-go-slot' + (f'-{index + 1}' if index else ''))
                        try:
                            slot.mkdir(mode=0o700)
                        except FileExistsError:
                            continue
                        owner = dict(pid=os.getpid(), run_id=uuid.uuid4().hex,
                                     mode=mode, priority=priority, cores=cores)
                        (slot / 'owner.json').write_text(json.dumps(owner) + '\n')
                        if full_marker:
                            full_marker.write_text(json.dumps(owner) + '\n')
                        return Slot(slot, owner, full_marker)
            if not wait:
                reason = (f'available memory below {needed_memory / GIB:.2f} GiB measured entry requirement'
                          if not memory_ok else 'capacity busy or reserved for release/calibration')
                raise RuntimeError('local ' + reason + '; no work started')
            position = queue.index(queue_ticket) + 1 if queue_ticket else None
            if announced != (position, batch_done):
                detail = f'full queue position {position}' if position else 'waiting for local capacity'
                if not batch_done:
                    detail += '; waiting for the named comparison batch to exit'
                print('gate: ' + detail + ' (calibration/release priority first)', flush=True)
                announced = (position, batch_done)
            time.sleep(.5)
    finally:
        with admission_lock(directory):
            ticket.unlink(missing_ok=True)
            if queue_ticket:
                queue_ticket.unlink(missing_ok=True)
