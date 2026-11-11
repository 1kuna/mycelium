#!/usr/bin/env python3
"""Host memory observations, without treating compressed pages as free bytes."""
import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tempfile
from typing import Optional


def swap_used_bytes(output: str) -> int:
    match = re.search(r'used\s*=\s*([\d.]+)([KMGT])', output)
    if not match:
        raise ValueError('host swap use is unknown')
    return int(float(match[1]) * 1024 ** ('KMGT'.index(match[2]) + 1))


def local_worker_usage() -> Optional[dict]:
    try:
        return _local_worker_usage()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print('WARNING gate: worker resource observation unavailable: ' + str(error), file=sys.stderr, flush=True)
        return None


def _local_worker_usage() -> Optional[dict]:
    """Read local launchd/OS usage, independent of queue access or credentials."""
    if sys.platform != 'darwin':
        return None
    rows = subprocess.check_output(['launchctl', 'list'], text=True, timeout=5)
    pid = None
    for row in rows.splitlines():
        fields = row.split()
        if len(fields) == 3 and fields[2] == 'local.mycelium.worker' and fields[0].isdigit():
            pid = int(fields[0])
    if pid is None:
        return dict(pid=None, process_count=0, rss_sum_bytes=0, cpu_cores=0)
    output = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,rss=,pcpu='], text=True, timeout=30)
    processes = [row.split() for row in output.splitlines() if row.strip()]
    owned = {pid}
    while True:
        descendants = {int(p) for p, parent, _, _ in processes if int(parent) in owned}
        if descendants <= owned:
            break
        owned.update(descendants)
    selected = [row for row in processes if int(row[0]) in owned]
    if not any(int(row[0]) == pid for row in selected):
        raise ValueError('local worker identity changed during resource measurement')
    return dict(pid=pid, process_count=len(selected),
        rss_sum_bytes=sum(int(row[2]) * 1024 for row in selected),
        cpu_cores=sum(float(row[3]) / 100 for row in selected),
        accounting='OS process-tree RSS sum and ps averaged CPU; shared pages may repeat')


def snapshot():
    if sys.platform != 'darwin':
        return None  # The outer Darwin supervisor protects the VM backing host.
    output = subprocess.check_output(['vm_stat'], text=True, timeout=5)
    page = int(re.search(r'page size of (\d+)', output).group(1))
    fields = {k: int(v) for k, v in re.findall(r'^([^:]+):\s+(\d+)\.', output, re.M)}
    pressure = int(subprocess.check_output(['sysctl', '-n',
        'kern.memorystatus_vm_pressure_level'], text=True, timeout=5).strip())
    processes = subprocess.check_output(['ps', '-axo', 'rss=,comm='], text=True, timeout=30)
    vm_rss = sum(int(line.split(None, 1)[0]) * 1024 for line in processes.splitlines()
                 if 'com.apple.Virtualization.VirtualMachine.xpc/Contents/MacOS/' in line)
    swap_use = subprocess.check_output(['sysctl', '-n', 'vm.swapusage'], text=True, timeout=5)
    return dict(monotonic_seconds=time.monotonic(), page_bytes=page, pages=fields, pressure_level=pressure,
        available_bytes=page * sum(fields.get(k, 0) for k in
            ('Pages free', 'Pages inactive', 'Pages speculative')),
        file_backed_bytes=page * fields.get('File-backed pages', 0),
        purgeable_bytes=page * fields.get('Pages purgeable', 0),
        compressor_bytes=page * fields.get('Pages occupied by compressor', 0),
        swapouts=fields.get('Swapouts', 0), swap_used_bytes=swap_used_bytes(swap_use),
        all_vz_rss_sum_bytes=vm_rss, local_worker_usage=local_worker_usage())


def idle_baseline() -> Optional[dict]:
    path = Path.home() / '.cache/mycelium-gates/control/gate-capacity.json'
    if not path.exists():
        return None
    baseline = json.loads(path.read_text()).get('swap_baseline')
    if baseline is not None:
        try:
            verify_baseline(baseline)
        except FileNotFoundError:
            print('WARNING gate: swap calibration evidence is missing; using the uncalibrated swap guard',
                  file=sys.stderr, flush=True)
            return None
    return baseline


def measured_rates(rows: list) -> dict:
    """Derive the rate envelope from locally gate-free measured intervals."""
    if len(rows) < 3:
        raise ValueError('swap baseline needs two measured intervals')
    periods, rates, growth = [], [], []
    for row in rows:
        if (row.get('local_gate_owners') != [] or row.get('local_native_custody') is not False
                or row.get('observation_only') is not True or row['memory']['pressure_level'] != 1):
            raise ValueError('swap baseline contains local work or non-normal pressure')
        for key in ('page_bytes', 'swapouts', 'swap_used_bytes'):
            if type(row['memory'][key]) is not int or row['memory'][key] < 0:
                raise ValueError('invalid swap baseline counter')
    for previous, current in zip(rows, rows[1:]):
        elapsed = current['monotonic_seconds'] - previous['monotonic_seconds']
        a, b = previous['memory'], current['memory']
        if (not math.isfinite(elapsed) or elapsed <= 0 or
                type(b['page_bytes']) is not int or b['page_bytes'] <= 0 or
                b['page_bytes'] != a['page_bytes'] or b['swapouts'] < a['swapouts']):
            raise ValueError('invalid swap baseline interval')
        periods.append(elapsed)
        rates.append((b['swapouts'] - a['swapouts']) * b['page_bytes'] / elapsed)
        growth.append(max(0, b['swap_used_bytes'] - a['swap_used_bytes']) / elapsed)
    return dict(window_seconds=max(periods), peak_swapout_bytes_per_second=max(rates),
                peak_swap_use_growth_bytes_per_second=max(growth),
                sample_count=len(rows), duration_seconds=sum(periods),
                mean_swapout_bytes_per_second=sum(
                    (b['memory']['swapouts'] - a['memory']['swapouts']) * b['memory']['page_bytes']
                    for a, b in zip(rows, rows[1:])) / sum(periods))


def evidence_directory() -> Path:
    return Path.home() / '.cache/mycelium-gates/control/evidence'


def evidence_bytes(evidence: dict) -> bytes:
    if not isinstance(evidence, dict) or not evidence.get('sha256') or not evidence.get('path'):
        raise ValueError('swap qualification lacks digest-bound measurement evidence')
    digest = evidence['sha256']
    if not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest):
        raise ValueError('invalid calibration evidence digest')
    # Pin immutable bytes in gate-owned state before admitting work. The source
    # worktree and its reports can then be removed without losing qualification.
    retained = evidence_directory() / digest
    data = retained.read_bytes() if retained.exists() else Path(evidence['path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != evidence['sha256']:
        raise ValueError('swap qualification evidence digest changed')
    if not retained.exists():
        retained.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=retained.parent, delete=False) as pending:
            pending.write(data)
        try:
            os.replace(pending.name, retained)
        finally:
            Path(pending.name).unlink(missing_ok=True)
    return data


def retained_calibration(config: dict) -> dict:
    """Retain every pinned input; a lost qualification uses default admission."""
    result = copy.deepcopy(config)

    def retain(value):
        if isinstance(value, dict):
            if 'path' in value and 'sha256' in value:
                evidence_bytes(value)
            else:
                for item in value.values():
                    retain(item)
        elif isinstance(value, list):
            for item in value:
                retain(item)

    for section in ('memory_measurement', 'quick_memory_measurement', 'full_slot_measurement', 'swap_baseline'):
        if section not in result:
            continue
        try:
            retain(result[section])
        except FileNotFoundError:
            print(f'WARNING gate: {section} evidence is missing; dropping that calibration and using defaults',
                  file=sys.stderr, flush=True)
            del result[section]
            if section == 'memory_measurement':
                # Resource/slot qualifications and quick headroom depend on it.
                for dependent in ('resource_admission', 'full_slot_measurement', 'quick_memory_measurement'):
                    result.pop(dependent, None)
    return result


def verify_baseline(baseline: dict) -> None:
    """Verify samples and fleet-background identity, never querying the queue."""
    if not isinstance(baseline, dict) or baseline.get('schema') != 1:
        raise ValueError('invalid idle swap qualification schema')
    rows = [json.loads(line) for line in evidence_bytes(baseline.get('evidence')).splitlines()]
    expected = measured_rates(rows)
    for key, value in expected.items():
        actual = baseline.get(key)
        if not isinstance(actual, (int, float)) or not math.isfinite(actual) or actual != value:
            raise ValueError('swap qualification rates do not match measurement samples')
    background = baseline.get('fleet_background')
    if not isinstance(background, dict) or not isinstance(background.get('job_ids'), list):
        raise ValueError('swap qualification lacks fleet-background job identity')
    worker_pids = sorted({row['memory']['local_worker_usage']['pid'] for row in rows
                          if row['memory'].get('local_worker_usage', {}).get('pid') is not None})
    if worker_pids != sorted(background.get('worker_pids', [])):
        raise ValueError('fleet-background worker does not match measured samples')
    observed = set()
    snapshots = background.get('status_snapshots', [])
    if worker_pids and not snapshots:
        raise ValueError('swap qualification lacks worker-background evidence')
    for evidence in snapshots:
        status = json.loads(evidence_bytes(evidence))
        observed.update(job['id'] for job in status['jobs']
                        if job.get('machine') == background.get('machine_id')
                        and job.get('state') in ('starting', 'running'))
    if sorted(observed) != sorted(background['job_ids']):
        raise ValueError('fleet-background jobs do not match measured status evidence')


class SwapGuard:
    """Two successive excess windows distinguish continued swapping from a batch.

    Windows use the measured idle sampling period. Two windows are the minimum
    evidence of continuation; the cost is two sampling periods before a swap-only
    stop. Critical or unknown memory pressure remains an immediate stop (MY-41).
    """

    def __init__(self, before: Optional[dict], baseline: Optional[dict] = None):
        self.before = before
        self.baseline = baseline
        self.previous = before
        self.previous_working_set = (before or {}).get('all_vz_rss_sum_bytes', 0)
        self.excess_windows = self.growth_windows = 0
        self.windows = []

    def check(self, current: Optional[dict], owned_rss_bytes: int = 0) -> Optional[str]:
        if current is None:
            return None  # MY-41: a failed observer is not evidence of pressure.
        if current['pressure_level'] == 4:
            return 'host memory pressure is critical'
        if self.before is None:
            self.before = self.previous = current
            self.previous_working_set = current.get('all_vz_rss_sum_bytes', 0)
            return None
        if self.baseline is None:
            return refusal(self.before, current)  # Unqualified hosts fail closed.
        elapsed = current['monotonic_seconds'] - self.previous['monotonic_seconds']
        if elapsed < 0 or current['swapouts'] < self.previous['swapouts']:
            print('WARNING gate: swap observation reset; re-establishing baseline', file=sys.stderr, flush=True)
            self.previous = current
            return None
        if elapsed < self.baseline['window_seconds']:
            return None
        rate = (current['swapouts'] - self.previous['swapouts']) * current['page_bytes'] / elapsed
        growth = (current['swap_used_bytes'] - self.previous['swap_used_bytes']) / elapsed
        working_set = current.get('all_vz_rss_sum_bytes', 0) + owned_rss_bytes
        excess = rate > self.baseline['peak_swapout_bytes_per_second']
        growing = (growth > self.baseline['peak_swap_use_growth_bytes_per_second'] and
                   working_set > self.previous_working_set)
        self.excess_windows = self.excess_windows + 1 if excess else 0
        self.growth_windows = self.growth_windows + 1 if growing else 0
        self.windows.append(dict(elapsed_seconds=elapsed, swapout_bytes_per_second=rate,
            swap_use_growth_bytes_per_second=growth, working_set_bytes=working_set,
            excess_windows=self.excess_windows, growth_windows=self.growth_windows))
        self.previous, self.previous_working_set = current, working_set
        if self.excess_windows >= 2:
            return 'host sustained swapout rate exceeds measured idle baseline'
        if self.growth_windows >= 2:
            return 'host swap use is growing with gate working set'
        return None


def refusal(before, current):
    if before is None:
        return None
    if current is None:
        return None
    if current['pressure_level'] == 4:
        return 'host memory pressure is critical'
    if (current['swapouts'] > before['swapouts'] or
            current.get('swap_used_bytes', 0) > before.get('swap_used_bytes', 0)):
        return 'host began swapping during gate'
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observe', type=int, help='observe an existing owner; never signal it')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if not args.observe:
        print(json.dumps(snapshot()))
        return
    args.report.parent.mkdir(parents=True, exist_ok=True)
    # Observation only. Owner lifetime bounds this calibration sidecar.
    with args.report.open('a', buffering=1) as stream:
        while True:
            try:
                os.kill(args.observe, 0)
            except ProcessLookupError:
                break
            row = dict(timestamp=time.time(), owner_pid=args.observe, memory=snapshot())
            stream.write(json.dumps(row) + '\n')
            time.sleep(2)


if __name__ == '__main__':
    main()
