#!/usr/bin/env python3
"""Learn host/selection timings and stop silent commands, not progressing work."""
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import selectors
import signal
import subprocess
import sys
import time
import uuid


# Two missed expected completions after the normal completion interval. This is
# a tolerance for transient contention, not a promise about run wall time.
STALL_INTERVALS = 3


def cpu_count():
    """Respect affinity, the worker's CPU contract and Linux cgroup quotas."""
    cores = float(len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity')
                  else os.cpu_count() or 1)
    # A worker may place us in a nested cgroup. Ancestor limits still bind;
    # checking only the mount root misses the actual execution quota.
    candidates = [(Path('/sys/fs/cgroup'), '')]
    try:
        for line in Path('/proc/self/cgroup').read_text().splitlines():
            _, controllers, relative = line.split(':', 2)
            if '..' in Path(relative).parts:
                continue
            if not controllers:
                candidates.append((Path('/sys/fs/cgroup'), relative.lstrip('/')))
            elif 'cpu' in controllers.split(','):
                for mount in ('cpu', 'cpu,cpuacct'):
                    candidates.append((Path('/sys/fs/cgroup') / mount, relative.lstrip('/')))
    except (OSError, ValueError):
        pass
    for root, relative in candidates:
        directory = root / relative
        while True:
            try:
                quota, period = (directory / 'cpu.max').read_text().split()
                if quota != 'max':
                    cores = min(cores, int(quota) / max(1, int(period)))
            except (OSError, ValueError):
                pass
            try:
                quota = int((directory / 'cpu.cfs_quota_us').read_text())
                period = int((directory / 'cpu.cfs_period_us').read_text())
                if quota > 0:
                    cores = min(cores, quota / max(1, period))
            except (OSError, ValueError):
                pass
            if directory == root:
                break
            directory = directory.parent
    if os.environ.get('GOMAXPROCS', '').isdigit() and int(os.environ['GOMAXPROCS']) > 0:
        cores = min(cores, int(os.environ['GOMAXPROCS']))
    return max(1, math.floor(cores))


class Progress:
    def __init__(self, expected, now):
        self.expected = expected
        self.last = self.started = now
        self.longest_gap = 0.0

    def output(self, now):
        self.longest_gap = max(self.longest_gap, now - self.last)
        self.last = now

    def remaining(self, now):
        if self.expected is None:
            return None
        return max(0, STALL_INTERVALS * self.expected - (now - self.last))


def load_history(path):
    try:
        value = json.loads(path.read_text())
        if (value['count'] > 0 and math.isfinite(value['expected_seconds'])
                and value['expected_seconds'] > 0):
            return value
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def run(label, command, cache, report, clock=time.monotonic):
    # The cache belongs to this execution environment, alongside Go's cache.
    # No hostname, address, source path or credentials are persisted in telemetry.
    identity = [label, platform.system(), platform.machine(), cpu_count(),
                os.environ.get('GOVERSION', '')]
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / (hashlib.sha256(json.dumps(identity).encode()).hexdigest() + '.json')
    history = load_history(path)
    expected = history['expected_seconds'] if history else None
    is_test = command[:2] == ['go', 'test']
    if is_test:
        # Cold runs retain the existing toolchain backstop. Warm runs can take
        # arbitrarily long provided they keep making progress.
        command = [*command[:2], '-timeout=0' if history else '-timeout=1h', *command[2:]]
    basis = f'{expected:.3f}s × {STALL_INTERVALS} silence' if history else 'cold learning (Go tests: 1h backstop)'
    print(f'gate progress {label}: cpu={cpu_count()} {basis}', flush=True)
    # Compilation/cache misses precede Go's package-start event. A warm test
    # measurement must not become a deadline for a cold compiler/linker.
    progress = Progress(None, clock())
    pending = b''
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    status = 1
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            while True:
                remaining = progress.remaining(clock())
                if remaining == 0:
                    status = 124
                    print(f'gate stall {label}: no output for {STALL_INTERVALS * expected:.3f}s',
                          file=sys.stderr, flush=True)
                    # The outer custody supervisor stops this command and all
                    # fixtures, verifies exit, then removes scratch. Never wait
                    # here for inherited output handles held by a hung fixture.
                    break
                events = selector.select(remaining)
                if not events:
                    continue
                data = os.read(child.stdout.fileno(), 65536)
                if not data:
                    status = child.wait()
                    if status < 0:
                        status = 128 - status
                    break
                progress.output(clock())
                if is_test and progress.expected is None and expected is not None:
                    pending += data
                    lines = pending.split(b'\n')
                    pending = lines.pop()
                    for line in lines:
                        try:
                            event = json.loads(line)
                        except (ValueError, UnicodeDecodeError):
                            continue
                        if event.get('Action') == 'start':
                            progress.expected = expected
                            break
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
    except (KeyboardInterrupt, SystemExit) as error:
        status = error.code if isinstance(error, SystemExit) else 130
    finally:
        child.stdout.close()
        elapsed = clock() - progress.started
        record = dict(label=label, cpu=cpu_count(), elapsed_seconds=elapsed,
                      longest_gap_seconds=max(progress.longest_gap, clock() - progress.last),
                      expected_seconds=expected, exit=status)
        # Every run is evidence, including failures, but censored/failed runs
        # must not teach a shorter timeout or ratchet a hang into the baseline.
        with report.open('a') as output:
            output.write(json.dumps(record) + '\n')
        if status == 0 and elapsed > 0:
            learned = dict(count=(history['count'] if history else 0) + 1,
                           expected_seconds=elapsed)
            temporary = path.with_suffix('.' + uuid.uuid4().hex)
            temporary.write_text(json.dumps(learned) + '\n')
            os.replace(temporary, path)
    return status


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    if sys.argv[1:] == ['--cpus']:
        print(cpu_count())
    else:
        label, *command = sys.argv[1:]
        sys.exit(run(label, command, Path(os.environ['GATE_TIMING_CACHE']),
                     Path(os.environ['GATE_TIMING_REPORT'])))
