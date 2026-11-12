#!/usr/bin/env python3
"""Learn host/selection timings and stop commands without semantic progress."""
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import selectors
import shutil
import signal
import subprocess
import sys
import time
import tempfile
import uuid


_custody_spec = importlib.util.spec_from_file_location('gate_custody', Path(__file__).with_name('gate-budget.py'))
custody = importlib.util.module_from_spec(_custody_spec)
_custody_spec.loader.exec_module(custody)


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


def history_identity(label):
    # The cache belongs to this execution environment, alongside Go's cache.
    # No hostname, address, source path or credentials are persisted in telemetry.
    identity = [label, platform.system(), platform.machine(), cpu_count(),
                os.environ.get('GOVERSION', '')]
    # Full shards each use one Go CPU. Four and eight competing shards must
    # still learn separately; otherwise both appear to be the same CPU profile.
    if slots := os.environ.get('GATE_TEST_SLOTS'):
        identity.append(slots)
    return identity


def run(label, command, cache, report, clock=time.monotonic):
    identity = history_identity(label)
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
    active_tests = set()
    package_scratch = None
    child_env = None
    if is_test:
        package_scratch = tempfile.mkdtemp(prefix='gate-package.', dir=os.environ.get('TMPDIR'))
        home = Path(package_scratch) / 'home'
        temporary = Path(package_scratch) / 'tmp'
        home.mkdir()
        temporary.mkdir()
        child_env = dict(os.environ, HOME=str(home), USERPROFILE=str(home), TMPDIR=str(temporary),
                         XDG_CONFIG_HOME=str(home / '.config'), XDG_CACHE_HOME=str(home / '.cache'),
                         XDG_DATA_HOME=str(home / '.local/share'), GATE_PACKAGE_ID=uuid.uuid4().hex)
        # Compile once, but keep each executable in its package's custody root.
        # A reparented/env-scrubbed fixture must still be discoverable by its
        # executable/mapping path without matching another shard's processes.
        if source := os.environ.get('GATE_TEST_CLI_BINARY'):
            private = temporary / 'gate_test_cli_binary'
            shutil.copy2(source, private)
            child_env['GATE_TEST_CLI_BINARY'] = str(private)
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=child_env)
    status = 1
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            while True:
                remaining = progress.remaining(clock())
                if remaining == 0:
                    status = 124
                    kind = 'test progress' if is_test else 'output'
                    print(f'gate stall {label}: no {kind} for {STALL_INTERVALS * expected:.3f}s',
                          file=sys.stderr, flush=True)
                    for name in sorted(active_tests):
                        print(f'gate stalled test: {name}', file=sys.stderr, flush=True)
                    # Stop this package below before releasing its execution
                    # slot; sibling progress must not postpone custody cleanup.
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
                if is_test:
                    pending += data
                    lines = pending.split(b'\n')
                    pending = lines.pop()
                    for line in lines:
                        try:
                            event = json.loads(line)
                        except (ValueError, UnicodeDecodeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        action = event.get('Action')
                        # Diagnostic output (including reconnect warnings) is
                        # not evidence that a test advanced. Only test2json
                        # lifecycle transitions renew the learned watchdog.
                        if action not in ('start', 'run', 'pause', 'cont', 'pass', 'fail', 'skip'):
                            continue
                        if action == 'start':
                            progress.expected = expected
                        name = event.get('Test')
                        if isinstance(name, str):
                            if action == 'run':
                                active_tests.add(name)
                            elif action in ('pass', 'fail', 'skip'):
                                active_tests.discard(name)
                        progress.output(clock())
                else:
                    progress.output(clock())
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
    except (KeyboardInterrupt, SystemExit) as error:
        status = error.code if isinstance(error, SystemExit) else 130
    finally:
        child.stdout.close()
        if package_scratch is not None:
            try:
                stopped = custody.stop_fixtures('GATE_PACKAGE_ID=' + child_env['GATE_PACKAGE_ID'],
                                               package_scratch, child)
            except (OSError, subprocess.SubprocessError):
                stopped = False
            if stopped:
                custody.remove_scratch(package_scratch)
            else:
                print('gate: package custody unconfirmed; scratch retained', file=sys.stderr, flush=True)
                status = 125
        elapsed = clock() - progress.started
        record = dict(label=label, cpu=cpu_count(), elapsed_seconds=elapsed,
                      shard_slots=os.environ.get('GATE_TEST_SLOTS'),
                      longest_gap_seconds=max(progress.longest_gap, clock() - progress.last),
                      expected_seconds=expected, exit=status,
                      progress_kind='test lifecycle' if is_test else 'output',
                      active_tests=sorted(active_tests))
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
