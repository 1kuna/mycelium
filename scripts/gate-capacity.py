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


def acquire(repo, *, mode='quick', priority='normal', cores=4, wait=False):
    """Conservative initial 2 heavy/1 full slots; legacy mkdir owners still bind.

    Increasing full concurrency requires a recorded measurement in gate-capacity.json.
    A release waiter takes the next free heavy slot ahead of normal gates.
    No stale owner is removed automatically, including after supervisor death.
    """
    directory = root(repo)
    directory.mkdir(parents=True, exist_ok=True)
    ticket = directory / ('local-go-release-wait-' + uuid.uuid4().hex)
    if priority == 'release':
        ticket.write_text(str(os.getpid()) + '\n')
    announced = False
    try:
        while True:
            if shutil.disk_usage(repo).free < ENTRY_FLOOR:
                raise RuntimeError('local disk below 45 GiB entry floor')
            with admission_lock(directory):
                legacy = [p for folder in legacy_directories(repo) for p in occupied_lanes(folder)]
                config_path = directory / 'gate-capacity.json'
                config = json.loads(config_path.read_text()) if config_path.exists() else {}
                # Two legacy lanes are authorized; full remains serialized until measured.
                slots = min(2, max(1, int(config.get('heavy_slots', 2))))
                fulls = min(2, max(1, int(config.get('full_slots', 1))))
                pending_release = priority != 'release' and any(directory.glob('local-go-release-wait-*'))
                full_marker = None
                if mode == 'full' and not legacy and not native_full_custody():
                    for index in range(fulls):
                        candidate = directory / f'local-go-full-{index}.custody'
                        if not candidate.exists():
                            full_marker = candidate
                            break
                # Unmeasured starting values: 16 GiB reserve + profile-declared
                # 6 GiB gate need. Replace after the requested host calibration.
                memory_ok = available_memory() >= (16 + 6) * GIB
                occupied = len(legacy) + len(occupied_lanes(directory))
                if (not pending_release and memory_ok and occupied < slots
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
                reason = 'available memory below 22 GiB' if not memory_ok else 'capacity busy or reserved for release'
                raise RuntimeError('local ' + reason + '; no work started')
            if not announced:
                print('gate: waiting for local capacity (release waiters first)', flush=True)
                announced = True
            time.sleep(.5)
    finally:
        ticket.unlink(missing_ok=True)
