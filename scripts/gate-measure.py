#!/usr/bin/env python3
"""Measure a local gate and stop its supervisor at the disk floor."""
import argparse
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
import importlib.util

_spec = importlib.util.spec_from_file_location('gate_memory', Path(__file__).with_name('gate-memory.py'))
memory = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(memory)

GIB = 1024 ** 3


def cache_bytes(path):
    if not path.exists():
        return 0
    return int(subprocess.check_output(['du', '-sk', str(path)], text=True).split()[0]) * 1024


def tree_rss(pid):
    lines = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,rss='], text=True)
    processes = [tuple(map(int, line.split())) for line in lines.splitlines() if line.strip()]
    owned = {pid}
    while True:
        descendants = {p for p, parent, rss in processes if parent in owned}
        if descendants <= owned:
            break
        owned.update(descendants)
    # RSS sum includes shared pages more than once; report that qualification.
    return sum(rss * 1024 for p, parent, rss in processes if p in owned)


def measure(command, report, mode, source_sha, *, floor=42 * GIB,
            phase=None, source_info=None, disk_root=Path("."), run_id=None):
    cache = Path(subprocess.check_output(['go', 'env', 'GOCACHE'], text=True).strip())
    modules = Path(subprocess.check_output(['go', 'env', 'GOMODCACHE'], text=True).strip())
    before = dict(build_cache_bytes=cache_bytes(cache), module_cache_bytes=cache_bytes(modules),
                  free_bytes=shutil.disk_usage(disk_root).free)
    before['host_memory'] = memory.snapshot()
    guard = memory.SwapGuard(before['host_memory'],
                             memory.idle_baseline() if before['host_memory'] else None)
    reason = guard.check(before['host_memory'])
    if reason:
        raise RuntimeError('gate: ' + reason + '; no work started')
    if before['free_bytes'] < 45 * GIB:
        raise RuntimeError('gate: below 45 GiB disk entry floor; no work started')
    samples = []
    stopped = threading.Event()
    refusal = []
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    child = subprocess.Popen(command)

    def monitor():
        while not stopped.wait(.5):
            now = time.monotonic()
            try:
                free = shutil.disk_usage(disk_root).free
            except OSError:
                refusal.append('host free-space measurement unavailable')
                try:
                    child.terminate()
                except ProcessLookupError:
                    pass
                return
            if free < floor:
                refusal.append('disk below 42 GiB stop floor')
                print('STOP gate: disk below 42 GiB; terminating owned supervisor',
                      file=sys.stderr, flush=True)
                try:
                    child.terminate()
                except ProcessLookupError:
                    pass
                samples.append(dict(elapsed_seconds=now - started, free_bytes=free,
                                    load1=os.getloadavg()[0]))
                return
            sample = dict(elapsed_seconds=now - started, free_bytes=free,
                          load1=os.getloadavg()[0])
            try:
                sample['host_memory'] = memory.snapshot()
                current = sample['host_memory']
                # Pressure/unqualified-host stops do not wait on RSS probes.
                reason = (guard.check(current) if guard.baseline is None or
                          current is None or current['pressure_level'] != 1 else None)
                if not reason:
                    sample['tree_rss_sum_bytes'] = tree_rss(child.pid)
                    if guard.baseline is not None:
                        reason = guard.check(current, sample['tree_rss_sum_bytes'])
            except (OSError, ValueError, subprocess.SubprocessError):
                reason = 'host memory measurement unavailable'
            if reason:
                refusal.append(reason)
                print('STOP gate: ' + reason, file=sys.stderr, flush=True)
                try:
                    child.terminate()
                except ProcessLookupError:
                    pass
                samples.append(sample)
                return
            try:
                launch = time.monotonic()
                subprocess.run([sys.executable, '-c', 'pass'], check=True, timeout=5,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                sample['foreground_spawn_seconds'] = time.monotonic() - launch
            except (OSError, subprocess.SubprocessError):
                sample['foreground_probe_failed'] = True
            samples.append(sample)
    observer = threading.Thread(target=monitor, daemon=True)
    observer.start()
    previous = {}
    def interrupt(signum, frame):
        try:
            child.send_signal(signum)
        except ProcessLookupError:
            pass
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, interrupt)
    try:
        status = child.wait()
    finally:
        stopped.set()
        observer.join(timeout=6)
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    elapsed = time.monotonic() - started
    used = resource.getrusage(resource.RUSAGE_CHILDREN)
    after = dict(build_cache_bytes=cache_bytes(cache), module_cache_bytes=cache_bytes(modules),
                 free_bytes=shutil.disk_usage(disk_root).free)
    if refusal:
        status = 125
    row = dict(schema=1, run_id=run_id or uuid.uuid4().hex, source_sha=source_sha,
               platform=platform.system().lower() + '/' + platform.machine(), mode=mode,
               status=status, wall_seconds=elapsed,
               user_seconds=used.ru_utime - usage.ru_utime,
               system_seconds=used.ru_stime - usage.ru_stime,
               max_tree_rss_sum_bytes=max((s.get('tree_rss_sum_bytes', 0) for s in samples), default=0),
               rss_accounting='sampled process-tree sum; shared pages may be counted twice',
               before=before, after=after,
               disk_delta_bytes=before['free_bytes'] - after['free_bytes'],
               min_free_bytes=min([before['free_bytes'], after['free_bytes']] +
                                  [s['free_bytes'] for s in samples]),
               samples=samples, refusal=refusal, phase=phase,
               swap_guard=dict(baseline=guard.baseline, windows=guard.windows))
    if source_info and source_info.exists():
        row.update(json.loads(source_info.read_text()))
    report.parent.mkdir(parents=True, exist_ok=True)
    # Each write is one record; append mode preserves simultaneous platform reports.
    with report.open('a') as stream:
        import fcntl
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(row) + '\n')
    print(f'gate measurement: {row["platform"]} {mode} exit={status} '
          f'wall={elapsed:.3f}s disk_delta={row["disk_delta_bytes"]}B', flush=True)
    return status if status >= 0 else 128 - status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, default=Path('.reports/gate-local.jsonl'))
    parser.add_argument('--mode', default='quick')
    parser.add_argument('--source-sha', default='working-tree')
    parser.add_argument('--source-info', type=Path)
    parser.add_argument('--phase')
    parser.add_argument('--run-id')
    parser.add_argument('--disk-root', type=Path, default=Path('.'))
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('command required after --')
    return measure(command, args.report, args.mode, args.source_sha, phase=args.phase,
                   source_info=args.source_info, disk_root=args.disk_root, run_id=args.run_id)


if __name__ == '__main__':
    sys.exit(main())
