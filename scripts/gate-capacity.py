#!/usr/bin/env python3
"""Local heavy-work admission shared across checkouts and release builds."""
import contextlib
import fcntl
import hashlib
import importlib.util
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

_spec = importlib.util.spec_from_file_location('gate_memory', Path(__file__).with_name('gate-memory.py'))
_memory = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_memory)
local_worker_usage = _memory.local_worker_usage
_spec = importlib.util.spec_from_file_location('gate_cache', Path(__file__).with_name('gate-cache.py'))
_cache = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cache)


def process_identity(pid):
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'stat=,lstart='],
                            capture_output=True, text=True, timeout=5,
                            env=dict(os.environ, LC_ALL='C'))
    if result.returncode not in (0, 1):
        raise RuntimeError('waiting process identity is unknown')
    fields = result.stdout.strip().split(None, 1)
    if not fields or fields[0].startswith('Z'):
        return None  # Waiting tickets have no execution custody to retain.
    if len(fields) != 2:
        raise RuntimeError('waiting process identity is incomplete')
    return fields[1].strip()


def publish_waiting(path, text):
    pending = path.with_name('.' + path.name + '.' + uuid.uuid4().hex)
    pending.write_text(text)
    pending.replace(path)


def priority_waiting(directory, priority):
    active = False
    for path in directory.glob('local-go-' + priority + '-wait-*'):
        raw = path.read_text()
        try:
            row = json.loads(raw)
        except ValueError:
            row = None
        if isinstance(row, dict):
            dead = process_identity(row['pid']) != row['process_identity']
        elif raw.strip().isdigit():
            # Older waiting records contained only PID: absence proves death;
            # an extant/reused PID lacks start identity and remains unknown.
            dead = process_identity(int(raw)) is None
        else:
            dead = False  # Preserve unknown legacy waiting records.
        if dead:
            path.unlink()
        else:
            active = True
    return active


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


def dead_holder(row):
    """Absence works for old PID-only records; reuse needs a start identity."""
    pid = row.get('pid', row.get('holder_pid'))
    birth = row.get('process_identity', row.get('holder_start'))
    if type(pid) is not int or pid <= 0:
        return False
    current = process_identity(pid)
    return current is None or isinstance(birth, str) and bool(birth.strip()) and current != birth


def archive_custody(directory, paths, row, reason):
    """Caller holds admission_lock. A crash leaves independently reclaimable records."""
    archive = directory / 'selfheal-backups' / uuid.uuid4().hex
    archive.mkdir(parents=True, mode=0o700)
    (archive / 'recovery.json').write_text(json.dumps(dict(owner=row, reason=reason)) + '\n')
    if row.get('scratch'):
        (archive / 'scratch-pending.json').write_text(json.dumps(row) + '\n')
    for path in paths:
        path.rename(archive / path.name)
    # Scratch is separate from admission. A live child or failed observation
    # leaves it in place, with a diagnostic, but cannot hold other gates' slots.
    if row.get('scratch'):
        try:
            _cache.collect_scratch(row, archive)
        except Exception as error:
            (archive / 'scratch-retained.txt').write_text(str(error) + '\n')
    print('gate: released local custody (' + reason + ')', flush=True)
    return archive


def clear_empty_group(directory):
    group = directory / 'local-go-resource-group.custody'
    if not group.exists():
        return
    row = json.loads(group.read_text())
    expected = dict(protocol_guard=True, resource_group=row.get('resource_group'))
    guards = []
    for lane in occupied_lanes(directory):
        owner = lane / 'owner.json'
        if not owner.exists() or json.loads(owner.read_text()) != expected:
            return  # Live or unknown execution, never a waiter, keeps the guards.
        guards.append(lane)
    for lane in guards:
        (lane / 'owner.json').unlink()
        lane.rmdir()
    group.unlink()


def reclaim_dead(directory):
    """Admission-lock transaction: reclaim dead holders without global quiet."""
    for lane in [*occupied_lanes(directory), *directory.glob('.slot-pending-*')]:
        owner = lane / 'owner.json'
        if lane.is_symlink() or not owner.is_file() or owner.is_symlink():
            continue
        try:
            row = json.loads(owner.read_text())
            if not isinstance(row, dict) or not dead_holder(row):
                continue
        except (ValueError, OSError, subprocess.SubprocessError):
            continue  # Unknown liveness is not evidence of death.
        paths = [lane]
        for full in directory.glob('local-go-full-*.custody'):
            try:
                if not full.is_symlink() and json.loads(full.read_text()) == row:
                    paths.append(full)
            except (OSError, ValueError):
                continue
        archive_custody(directory, paths, row, 'dead holder')
    for path in [*directory.glob('local-go-full-*.custody'),
                 *directory.glob('local-go-native-full-*.custody')]:
        if path.is_symlink():
            continue
        try:
            row = json.loads(path.read_text())
            if isinstance(row, dict) and dead_holder(row):
                archive_custody(directory, [path], row, 'dead holder')
        except (ValueError, OSError, subprocess.SubprocessError):
            continue
    clear_empty_group(directory)


def execution_lanes(directory):
    """Protocol guards occupy both old lane names so frozen older clients wait."""
    group = directory / 'local-go-resource-group.custody'
    if not group.exists():
        return occupied_lanes(directory)
    record = json.loads(group.read_text())
    expected = dict(protocol_guard=True, resource_group=record['resource_group'])
    guards = [directory / name for name in ('local-go-slot', 'local-go-slot-2')]
    if record.get('admission_protocol') != 2 or any(
            not (p / 'owner.json').exists() or
            json.loads((p / 'owner.json').read_text()) != expected for p in guards):
        raise RuntimeError('resource admission compatibility custody changed; retained')
    return [p for p in occupied_lanes(directory) if p not in guards]


def resource_qualification(config):
    """Check the private measurement before enabling concurrent full admission."""
    qualification = config.get('resource_admission')
    if qualification is None:
        return None
    measured = config['memory_measurement']
    if qualification.get('schema') != 1:
        raise ValueError('unknown resource qualification schema')
    for key in ('measured_cores', 'vm_cores', 'vm_memory_bytes'):
        if type(qualification.get(key)) is not int or qualification[key] <= 0:
            raise ValueError('invalid measured resource capacity')
    rows = []
    for evidence in measured['evidence']:
        rows.extend(json.loads(line) for line in _memory.evidence_bytes(evidence).splitlines())
    native = [r for r in rows if r.get('mode') == 'full' and
              r.get('phase') in (None, 'native') and r.get('status') in (0, 1) and not r.get('refusal') and
              r.get('max_tree_rss_sum_bytes')]
    linux = [r for r in rows if r.get('mode') == 'full' and r.get('phase') == 'host'
             and r.get('status') == 0 and not r.get('refusal') and r.get('samples') and
             any(g.get('phase') == 'linux' and g.get('mode') == 'full' and
                 g.get('status') == 0 and not g.get('refusal') and
                 g.get('run_id') == r.get('run_id') for g in rows)]
    native_cores = qualification.get('native_measured_cores', qualification['measured_cores'])
    if type(native_cores) is not int or native_cores < qualification['measured_cores']:
        raise ValueError('invalid native measured core count')
    if native_cores > qualification['measured_cores'] and not any(
            r.get('admission', {}).get('cores') == native_cores and
            r.get('admission', {}).get('exclusive') is True for r in native):
        raise ValueError('native core profile lacks its exclusive calibration')
    if not native or not linux:
        raise ValueError('concurrent admission requires completed native and Linux measurements')
    if measured['native_peak_bytes'] != max(r['max_tree_rss_sum_bytes'] for r in native):
        raise ValueError('native resource peak does not match its evidence')
    if not any(r['max_tree_rss_sum_bytes'] == measured['linux_peak_bytes'] and
               max(s.get('host_memory', {}).get('all_vz_rss_sum_bytes', 0)
                   for s in r['samples']) == measured['vm_peak_bytes'] for r in linux):
        raise ValueError('Linux/VM resource peaks do not match their evidence')
    return qualification


def admission_config_digest(config):
    """Bind behavioral limits and their evidence; ignore descriptive metadata."""
    measured = config.get('memory_measurement', {})
    normalized = dict(resource_admission=config.get('resource_admission'),
        full_slot_measurement=config.get('full_slot_measurement'),
        memory_measurement={key: measured.get(key) for key in (
            'native_peak_bytes', 'linux_peak_bytes', 'linux_provisional_bytes',
            'vm_peak_bytes', 'headroom_bytes', 'evidence')},
        workstation_cpu_reserve_cores=config.get('workstation_cpu_reserve_cores', 0),
        fleet_worker_cpu_reserve_cores=config.get('fleet_worker_cpu_reserve_cores', 0),
        swap_baseline=config.get('swap_baseline'))
    return hashlib.sha256(json.dumps(normalized, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def native_full_custody(*, include_fleet=False):
    """Local custody binds local admission; installed-worker custody is separate.

    Dead local cache records are reclaimed under their cache/slot locks. Fleet
    custody remains outside local development recovery.
    """
    control = Path.home() / '.cache/mycelium-gates/control'
    if any(control.glob('local-go-native-full-*.custody')):
        return True  # Host-wide reservations include custom-cache native gates.
    caches = {Path.home() / 'Library/Caches/mycelium/gates/go-build'}
    caches.add(Path(subprocess.check_output(['go', 'env', 'GOCACHE'], text=True).strip()))
    worker_files = Path.home() / '.local/share/mycelium/worker/state/job-files'
    for cache in caches:
        for path in cache.glob('mycelium-full-gate-[0-9].custody'):
            try:
                row = json.loads(path.read_text())
                fleet = bool(row.get('scratch')) and Path(row['scratch']).resolve().is_relative_to(worker_files.resolve())
            except (OSError, ValueError, KeyError, TypeError):
                fleet = False  # Unknown custody remains binding.
                row = {}
            if not fleet and not include_fleet and dead_holder(row):
                with (cache / 'mycelium-full-gate-admission.lock').open('a') as admission:
                    fcntl.flock(admission, fcntl.LOCK_EX)
                    with path.with_suffix('.lock').open('a') as slot:
                        try:
                            fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            return True
                        if path.exists() and json.loads(path.read_text()) == row and dead_holder(row):
                            archive_custody(cache, [path], row, 'dead native cache holder')
                            continue
            if include_fleet or not fleet:
                return True
    return False


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


def total_memory():
    if os.uname().sysname == 'Darwin':
        return int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'],
                                          text=True, timeout=5))
    return os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')


def check_pressure():
    if os.uname().sysname == 'Darwin':
        try:
            level = int(subprocess.check_output(['sysctl', '-n',
                        'kern.memorystatus_vm_pressure_level'], text=True, timeout=5))
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print('WARNING gate: admission pressure observation unavailable: ' + str(error), flush=True)
            return
        if level == 4:
            raise RuntimeError('host memory pressure is critical; no work started')


class Slot:
    def __init__(self, directory, owner, full, group=None):
        self.directory, self.owner, self.full, self.group = directory, owner, full, group

    def release(self, stopped=True):
        with admission_lock(self.directory.parent):
            if self.group:
                execution_lanes(self.directory.parent)  # Verify guards before any removal.
                group_path = self.directory.parent / 'local-go-resource-group.custody'
                if json.loads(group_path.read_text()) != self.group:
                    raise RuntimeError('resource group custody changed; retained')
            path = self.directory / 'owner.json'
            if json.loads(path.read_text()) != self.owner:
                raise RuntimeError('local gate slot ownership changed; retained')
            if self.full and json.loads(self.full.read_text()) != self.owner:
                raise RuntimeError('local full custody changed; retained')
            if stopped:
                path.unlink()
                self.directory.rmdir()
                if self.full:
                    self.full.unlink()
            else:
                archive_custody(self.directory.parent,
                    [self.directory] + ([self.full] if self.full else []),
                    self.owner, 'cleanup unconfirmed; verdict retained')
            if self.group and not execution_lanes(self.directory.parent):
                for name in ('local-go-slot', 'local-go-slot-2'):
                    guard = self.directory.parent / name
                    (guard / 'owner.json').unlink()
                    guard.rmdir()
                group_path.unlink()



@contextlib.contextmanager
def admission_lock(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'local-go-admission.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def full_slot_profile(config):
    """Qualify extra I/O-overlap slots from identified native and VM observations."""
    profile = config.get('full_slot_measurement')
    if profile is None:
        return None
    slots = profile['slots']
    if type(slots) is not int or slots != 8:
        raise ValueError('full slot measurement must identify the eight-slot comparison')
    rows = [json.loads(line) for item in profile['evidence']
            for line in _memory.evidence_bytes(item).splitlines()]
    rows = [r for r in rows if r.get('mode') == 'full' and r.get('status') in (0, 1)
            and not r.get('refusal') and r.get('admission', {}).get('cores') == slots]
    native = [r for r in rows if r.get('phase') == 'native' and r.get('max_tree_rss_sum_bytes')]
    hosts = [r for r in rows if r.get('phase') == 'host' and r.get('max_tree_rss_sum_bytes')
             and any(s.get('host_memory', {}).get('all_vz_rss_sum_bytes', 0) > 0 for s in r.get('samples', []))
             and any(g.get('phase') == 'linux' and g.get('run_id') == r.get('run_id') for g in rows)]
    if not native or not hosts:
        raise ValueError('eight-slot admission requires native and Linux/VM measurements')
    base = config['memory_measurement']
    # Older envelopes mixed an eight-slot native peak into the four-slot
    # requirement. Recover the actual four-slot observations for the fallback.
    baseline = [json.loads(line) for item in base['evidence']
                for line in _memory.evidence_bytes(item).splitlines()]
    four_native = [r['max_tree_rss_sum_bytes'] for r in baseline
                   if r.get('mode') == 'full' and r.get('phase') in (None, 'native')
                   and r.get('status') in (0, 1) and not r.get('refusal')
                   and r.get('max_tree_rss_sum_bytes') and
                   r.get('admission', {}).get('cores', base.get('measured_cores', 4)) == 4]
    return dict(slots=slots,
        four_native_peak_bytes=max(four_native, default=base['native_peak_bytes']),
        native_peak_bytes=max(max(four_native, default=0), max(r['max_tree_rss_sum_bytes'] for r in native)),
        linux_peak_bytes=max(base['linux_peak_bytes'], max(r['max_tree_rss_sum_bytes'] for r in hosts)),
        vm_peak_bytes=max(base['vm_peak_bytes'], max(s.get('host_memory', {}).get('all_vz_rss_sum_bytes', 0)
                                                  for r in hosts for s in r['samples'])))


def memory_requirement(config, platform, mode='full', cores=None, slot_profile=None):
    # MY-41: quick/focused work uses its own measured envelope, not a full peak.
    if mode == 'quick' and config.get('quick_memory_measurement'):
        measured = config['quick_memory_measurement']
        rows = [json.loads(line) for item in measured['evidence']
                for line in _memory.evidence_bytes(item).splitlines()]
        peaks = [r['max_tree_rss_sum_bytes'] for r in rows
                 if r.get('mode') == 'quick' and r.get('status') == 0
                 and not r.get('refusal') and r.get('max_tree_rss_sum_bytes')]
        if not peaks or measured['peak_bytes'] != max(peaks):
            raise RuntimeError('quick memory qualification does not match evidence')
        vm = config.get('memory_measurement', {}).get('vm_peak_bytes', 0) if platform != 'native' else 0
        return measured['peak_bytes'] * (2 if platform == 'both' else 1) + vm + config['memory_measurement']['headroom_bytes']
    measured = config.get('memory_measurement')
    if not measured:
        return (44 if platform == 'both' else 22) * GIB  # Initial envelope per simultaneous leg.
    if not measured.get('evidence'):
        raise RuntimeError('memory qualification is missing its evidence')
    if slot_profile and cores == slot_profile['slots']:
        measured = dict(measured, **{k: slot_profile[k] for k in
                                   ('native_peak_bytes', 'linux_peak_bytes', 'vm_peak_bytes')})
    elif slot_profile and cores == 4:
        measured = dict(measured, native_peak_bytes=slot_profile['four_native_peak_bytes'])
    native = int(measured['native_peak_bytes'])
    headroom = int(measured['headroom_bytes'])
    if native <= 0 or headroom <= 0:
        raise RuntimeError('memory qualification has invalid peak/headroom')
    if platform == 'native':
        return native + headroom
    linux = int(measured.get('linux_peak_bytes') or measured['linux_provisional_bytes'])
    vm = int(measured['vm_peak_bytes'])
    return (native + linux + vm if platform == 'both' else max(native, linux + vm)) + headroom


def acquire(repo, *, mode='quick', priority='normal', cores=4, wait=False, platform='native',
            after_pid=None, exclusive=False, source_root=None):
    """Admit measured CPU/RAM demand; preserve unqualified and legacy custody.

    Reserve each active run's entire peak in addition to live available memory:
    this deliberately overcounts its current RSS to cover future working-set
    growth without confusing shared pages with reclaimable private memory.
    """
    automatic = cores is None and mode == 'full'
    if automatic:
        cores = 4
    if type(cores) is not int or cores <= 0:
        raise ValueError('local gate cores must be a positive integer')
    host_cores = cores * (2 if platform == 'both' else 1)
    directory = root(repo)
    directory.mkdir(parents=True, exist_ok=True)
    _cache.trim_backups(directory / 'selfheal-backups')
    if priority == 'calibration' and mode != 'full':
        raise ValueError('calibration priority reserves a full lane only')
    prefix = 'local-go-calibration-wait-' if priority == 'calibration' else 'local-go-release-wait-'
    ticket = directory / (prefix + uuid.uuid4().hex)
    queue_ticket = None
    checked_config, resources, slot_profile = None, None, None
    host_memory = total_memory()
    if type(host_memory) is not int or host_memory <= 0:
        raise RuntimeError('host physical memory capacity is unknown')
    try:
        if priority in ('release', 'calibration'):
            with admission_lock(directory):
                publish_waiting(ticket, json.dumps(dict(pid=os.getpid(),
                    process_identity=process_identity(os.getpid()))) + '\n')
        if mode == 'full' or exclusive:
            with admission_lock(directory):
                sequence_path = directory / 'queue-sequence'
                sequence = int(sequence_path.read_text()) + 1 if sequence_path.exists() else 1
                publish_waiting(sequence_path, str(sequence))
                queue_ticket = directory / ('local-go-wait-' + uuid.uuid4().hex + '.json')
                publish_waiting(queue_ticket, json.dumps(dict(pid=os.getpid(), process_identity=process_identity(os.getpid()),
                    priority=priority, sequence=sequence, exclusive=exclusive)) + '\n')
        after_identity = process_identity(after_pid) if after_pid else None
        announced = None
        while True:
            check_pressure()
            if shutil.disk_usage(repo).free < ENTRY_FLOOR:
                raise RuntimeError('local disk below 45 GiB entry floor')
            with admission_lock(directory):
                legacy_roots = legacy_directories(repo)
                for folder in [directory, *legacy_roots]:
                    reclaim_dead(folder)
                legacy = [p for folder in legacy_roots for p in occupied_lanes(folder)]
                config_path = directory / 'gate-capacity.json'
                config = json.loads(config_path.read_text()) if config_path.exists() else {}
                if config != checked_config:
                    resources = resource_qualification(config)
                    slot_profile = full_slot_profile(config) if resources else None
                    checked_config = config
                if not resources and (directory / 'local-go-resource-group.custody').exists():
                    raise RuntimeError('resource qualification removed during active custody; retained')
                worker = local_worker_usage()
                worker_cores = config.get('fleet_worker_cpu_reserve_cores', 0)
                if type(worker_cores) is not int or worker_cores < 0:
                    raise ValueError('fleet worker CPU reserve must be a nonnegative integer')
                config_digest = admission_config_digest(config)
                group_path = directory / 'local-go-resource-group.custody'
                if group_path.exists() and json.loads(group_path.read_text()).get(
                        'admission_config_sha256') != config_digest:
                    raise RuntimeError('capacity configuration changed during active custody; no work started; '
                                       'waiting ticket removed, retry after active group cleanup with current helpers')
                qualified_cores = (resources.get('native_measured_cores', resources['measured_cores'])
                                   if resources and platform == 'native' else
                                   resources['measured_cores'] if resources else cores)
                calibration = (resources and mode == 'full' and platform != 'both'
                               and priority == 'calibration' and exclusive and cores > qualified_cores)
                if (resources and cores > qualified_cores and not calibration and not automatic
                        and not (mode == 'full' and slot_profile and cores == slot_profile['slots'])):
                    raise RuntimeError('requested cores exceed the measured local gate budget')
                if exclusive and not resources:
                    raise RuntimeError('exclusive timing requires qualified resource admission')
                # Unqualified hosts preserve the previous two-heavy/one-full rule.
                slots = ((os.cpu_count() or 1) + 2 if resources else
                         min(2, max(1, int(config.get('heavy_slots', 2)))))
                fulls = min(2, max(1, int(config.get('full_slots', 1))))
                release_waiting = priority_waiting(directory, 'release')
                calibration_waiting = priority_waiting(directory, 'calibration')
                pending_release = priority not in ('release', 'calibration') and release_waiting
                pending_calibration = (mode == 'full' and priority != 'calibration'
                                       and calibration_waiting)
                queue = full_waiters(directory)
                queue_first = not queue_ticket or not queue or queue[0] == queue_ticket
                front = json.loads(queue[0].read_text()) if queue else {}
                rank = {'calibration': 0, 'release': 1, 'normal': 2}
                exclusive_waiting = (front.get('exclusive') and
                                     rank[priority] >= rank[front['priority']])
                if exclusive_waiting and queue_ticket != queue[0]:
                    queue_first = False
                if queue_ticket is None and any(
                        row.get('exclusive') and rank[priority] >= rank[row['priority']]
                        for row in (json.loads(path.read_text()) for path in queue)):
                    # An earlier full can precede an exclusive ticket, but
                    # newly arriving quick work must not keep its drain busy.
                    queue_first = False
                batch_done = not after_identity or process_identity(after_pid) != after_identity
                full_marker = None
                if not resources and mode == 'full' and not legacy and not native_full_custody():
                    for index in range(fulls):
                        candidate = directory / f'local-go-full-{index}.custody'
                        if not candidate.exists():
                            full_marker = candidate
                            break
                available = available_memory()
                lanes = legacy + execution_lanes(directory)
                occupied = len(lanes)
                reserved_cores = 0
                reserved_memory, vm_cores, vm_memory = 0, 0, 0
                owners = []
                for lane in lanes:
                    owner_path = lane / 'owner.json'
                    # Preserve unknown legacy occupancy and its four-core allowance.
                    row = json.loads(owner_path.read_text()) if owner_path.exists() else {}
                    owners.append(row)
                    demand = row.get('cores', 4)
                    if type(demand) is not int or demand <= 0:
                        raise RuntimeError('recorded local lane has invalid CPU demand; retained')
                    reserved_cores += demand
                    if resources and row.get('admission_protocol') == 2:
                        peak = row.get('peak_bytes')
                        if type(peak) is not int or peak <= 0:
                            raise RuntimeError('recorded local lane has invalid memory demand; retained')
                        reserved_memory += peak
                        if row.get('platform') != 'native':
                            vm_cores += row.get('per_leg_cores', demand)
                            vm_memory += row.get('vm_peak_bytes', peak)
                if automatic:
                    cores = 4
                    vm_fits = (platform == 'native' or slot_profile and
                               vm_cores + slot_profile['slots'] <= resources['vm_cores'] and
                               vm_memory + memory_requirement(config, 'linux', mode,
                                   slot_profile['slots'], slot_profile) - config['memory_measurement']['headroom_bytes']
                               <= resources['vm_memory_bytes'])
                    if slot_profile and vm_fits and available >= reserved_memory + memory_requirement(
                            config, platform, mode, slot_profile['slots'], slot_profile):
                        cores = slot_profile['slots']
                gate_memory = memory_requirement(config, platform, mode, cores, slot_profile)
                if calibration:
                    # Scale the existing platform observation for an identified
                    # calibration; only measured profiles enable normal defaults.
                    measured = config['memory_measurement']
                    peak = (measured['native_peak_bytes'] if platform == 'native' else
                            measured['linux_peak_bytes'] + measured['vm_peak_bytes'])
                    gate_memory = (peak * cores + qualified_cores - 1) // qualified_cores + measured['headroom_bytes']
                needed_memory = gate_memory
                if needed_memory > host_memory:
                    raise RuntimeError(f'{needed_memory / GIB:.2f} GiB entry requirement exceeds '
                                       f'{host_memory / GIB:.2f} GiB host physical memory; no work started')
                # Slots overlap subprocess and filesystem waits. The qualified
                # full keeps its existing CPU share while using measured extra
                # slots; it cannot admit another lane into that reserved share.
                cpu_share = (min(cores, resources['measured_cores'])
                             if mode == 'full' and slot_profile and cores == slot_profile['slots'] else cores)
                host_cores = cpu_share * (2 if platform == 'both' else 1)
                reserve = int(config.get('workstation_cpu_reserve_cores', 0))
                if reserve < 0:
                    raise ValueError('workstation CPU reserve must not be negative')
                cpu_ok = reserved_cores + worker_cores + host_cores + reserve <= (os.cpu_count() or 1)
                group, resource_ok = None, True
                # Available RAM already reflects active RSS. Quick's measured peak plus
                # shared host headroom covers its growth without recharging other runs.
                admission_memory = needed_memory + (reserved_memory if resources and mode == 'full' else 0)
                memory_ok = available >= admission_memory
                if resources:
                    headroom = config['memory_measurement']['headroom_bytes']
                    peak = gate_memory - headroom
                    group_path = directory / 'local-go-resource-group.custody'
                    existing_group = json.loads(group_path.read_text()) if group_path.exists() else {}
                    expected_fulls = {directory / ('local-go-full-' + row['run_id'] + '.custody'): row
                                      for row in owners if row.get('admission_protocol') == 2
                                      and row.get('mode') == 'full'}
                    full_custody_ok = (set(directory.glob('local-go-full-*.custody')) == set(expected_fulls)
                        and all(json.loads(p.read_text()) == row for p, row in expected_fulls.items()))
                    resource_ok = (not legacy and not native_full_custody() and
                        full_custody_ok and
                        all(row.get('admission_protocol') == 2 and
                            row.get('resource_group') == existing_group.get('resource_group')
                            for row in owners) and
                        not any(row.get('exclusive') for row in owners) and
                        (not exclusive or not owners))
                    vm_peak = (memory_requirement(config, 'linux', mode, cores, slot_profile) - headroom
                               if platform != 'native' else 0)
                    if calibration and platform == 'linux':
                        vm_peak = gate_memory - headroom
                    if platform != 'native':
                        resource_ok = (resource_ok and vm_cores + cores <= resources['vm_cores']
                                       and vm_memory + vm_peak <= resources['vm_memory_bytes'])
                ready = (not pending_release and not pending_calibration and queue_first and batch_done and occupied < slots
                        and cpu_ok and memory_ok and resource_ok
                        and (resources or mode != 'full' or full_marker))
                if ready:
                    if resources:
                        group_path = directory / 'local-go-resource-group.custody'
                        if group_path.exists():
                            group = json.loads(group_path.read_text())
                        else:
                            group = dict(admission_protocol=2, resource_group=uuid.uuid4().hex,
                                         admission_config_sha256=config_digest)
                            publish_waiting(group_path, json.dumps(group) + '\n')
                            for name in ('local-go-slot', 'local-go-slot-2'):
                                guard = directory / name
                                pending = directory / ('.guard-pending-' + uuid.uuid4().hex)
                                pending.mkdir(mode=0o700)
                                (pending / 'owner.json').write_text(json.dumps(dict(
                                    protocol_guard=True, resource_group=group['resource_group'])) + '\n')
                                pending.rename(guard)
                    for index in range(2 if resources else 0, slots):
                        slot = directory / ('local-go-slot' + (f'-{index + 1}' if index else ''))
                        if slot.exists():
                            continue
                        owner = dict(pid=os.getpid(), run_id=uuid.uuid4().hex,
                                     process_identity=process_identity(os.getpid()),
                                     mode=mode, priority=priority, cores=host_cores, per_leg_cores=cores,
                                     automatic_slots=automatic, cpu_share_per_leg=cpu_share,
                                     local_worker_usage_at_entry=worker,
                                     fleet_worker_cpu_reserve_cores=worker_cores,
                                     workstation_cpu_reserve_cores=reserve,
                                     available_memory_at_entry_bytes=available,
                                     memory_requirement_at_entry_bytes=admission_memory,
                                     host_physical_memory_bytes=host_memory)
                        marker = uuid.uuid4().hex
                        owner.update(source_root=str((source_root or repo).resolve()), run_marker=marker,
                                     scratch=str((source_root or repo).resolve() / '.gate' / ('run.' + marker)))
                        if resources:
                            owner.update(admission_protocol=2, uid=os.getuid(), resource_group=group['resource_group'],
                                         admission_config_sha256=config_digest,
                                         platform=platform, peak_bytes=peak, vm_peak_bytes=vm_peak, exclusive=exclusive,
                                         calibration=bool(calibration))
                            if mode == 'full':
                                full_marker = directory / ('local-go-full-' + owner['run_id'] + '.custody')
                        owner = json.loads(json.dumps(owner))  # Freeze observation dictionaries too.
                        # Publish a complete identity atomically. Death between
                        # mkdir and owner.json must not create an unknown slot.
                        pending = directory / ('.slot-pending-' + owner['run_id'])
                        pending.mkdir(mode=0o700)
                        (pending / 'owner.json').write_text(json.dumps(owner) + '\n')
                        pending.rename(slot)
                        if full_marker:
                            publish_waiting(full_marker, json.dumps(owner) + '\n')
                        if automatic:
                            print(f'gate: full selected {cores} slots per leg; CPU share {cpu_share} per leg; '
                                  f'{admission_memory / GIB:.2f} GiB measured memory required, '
                                  f'{available / GIB:.2f} GiB available', flush=True)
                        return Slot(slot, owner, full_marker, group)
            if not wait:
                reason = (f'capacity busy: available memory below {admission_memory / GIB:.2f} GiB measured entry requirement'
                          if not memory_ok else 'capacity busy or reserved for release/calibration')
                raise RuntimeError('local ' + reason + '; no work started')
            position = queue.index(queue_ticket) + 1 if queue_ticket else None
            if announced != (position, batch_done, memory_ok, admission_memory):
                detail = f'full queue position {position}' if position else 'waiting for local capacity'
                if not memory_ok:
                    detail += (f'; waiting for memory: {available / GIB:.2f} GiB available, '
                               f'{admission_memory / GIB:.2f} GiB required (pressure normal)')
                if not batch_done:
                    detail += '; waiting for the named comparison batch to exit'
                print('gate: ' + detail + ' (calibration/release priority first)', flush=True)
                announced = (position, batch_done, memory_ok, admission_memory)
            time.sleep(.5)
    finally:
        with admission_lock(directory):
            ticket.unlink(missing_ok=True)
            if queue_ticket:
                queue_ticket.unlink(missing_ok=True)
