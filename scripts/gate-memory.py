#!/usr/bin/env python3
"""Host memory observations, without treating compressed pages as free bytes."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


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
    return dict(page_bytes=page, pages=fields, pressure_level=pressure,
        available_bytes=page * sum(fields.get(k, 0) for k in
            ('Pages free', 'Pages inactive', 'Pages speculative')),
        file_backed_bytes=page * fields.get('File-backed pages', 0),
        purgeable_bytes=page * fields.get('Pages purgeable', 0),
        compressor_bytes=page * fields.get('Pages occupied by compressor', 0),
        swapouts=fields.get('Swapouts', 0), all_vz_rss_sum_bytes=vm_rss)


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
