#!/usr/bin/env python3
"""Host memory observations, without treating compressed pages as free bytes."""
import argparse
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Optional


def swap_used_bytes(output: str) -> int:
    match = re.search(r'used\s*=\s*([\d.]+)([KMGT])', output)
    if not match:
        raise ValueError('host swap use is unknown')
    return int(float(match[1]) * 1024 ** ('KMGT'.index(match[2]) + 1))


def local_worker_usage() -> Optional[dict]:
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
    output = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,rss=,pcpu='], text=True, timeout=5)
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
    processes = subprocess.check_output(['ps', '-axo', 'rss=,comm='], text=True, timeout=5)
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
        values = [baseline[key] for key in ('window_seconds', 'peak_swapout_bytes_per_second',
                                            'peak_swap_use_growth_bytes_per_second')]
        if (not all(isinstance(value, (int, float)) and math.isfinite(value) for value in values) or
                baseline.get('schema') != 1 or not baseline.get('evidence') or
                baseline['window_seconds'] <= 0 or
                baseline['peak_swapout_bytes_per_second'] < 0 or
                baseline['peak_swap_use_growth_bytes_per_second'] < 0):
            raise ValueError('invalid idle swap qualification')
    return baseline


class SwapGuard:
    """Two successive excess windows distinguish continued swapping from a batch.

    Windows use the measured idle sampling period. Two windows are the minimum
    evidence of continuation; the cost is two sampling periods before a swap-only
    stop. Elevated memory pressure remains an immediate stop.
    """

    def __init__(self, before: Optional[dict], baseline: Optional[dict] = None):
        self.before = before
        self.baseline = baseline
        self.previous = before
        self.previous_working_set = (before or {}).get('all_vz_rss_sum_bytes', 0)
        self.excess_windows = self.growth_windows = 0
        self.windows = []

    def check(self, current: Optional[dict], owned_rss_bytes: int = 0) -> Optional[str]:
        if self.before is None:
            return None  # The guest's outer host supervisor owns memory safety.
        if current is None:
            return 'host memory measurement unavailable'
        if current['pressure_level'] != 1:
            return 'host memory pressure is not normal'
        if self.baseline is None:
            return refusal(self.before, current)  # Unqualified hosts fail closed.
        elapsed = current['monotonic_seconds'] - self.previous['monotonic_seconds']
        if elapsed < 0 or current['swapouts'] < self.previous['swapouts']:
            return 'host swap measurement reset during gate'
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
        return 'host memory measurement unavailable'
    if current['pressure_level'] != 1:
        return 'host memory pressure is not normal'
    if current['swapouts'] > before['swapouts']:
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
