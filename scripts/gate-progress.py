#!/usr/bin/env python3
"""Learn host/selection timings and stop commands without semantic progress."""
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import re
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
_receipt_spec = importlib.util.spec_from_file_location('gate_receipt', Path(__file__).with_name('gate-receipt.py'))
receipts = importlib.util.module_from_spec(_receipt_spec)
_receipt_spec.loader.exec_module(receipts)


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


def history_identity(label, command=()):
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
    compiled = command[:1] == ['--test-binary']
    image_sha = None
    if compiled:
        digest = hashlib.sha256()
        with Path(command[2]).open('rb') as image:
            for block in iter(lambda: image.read(1024 * 1024), b''):
                digest.update(block)
        image_sha = digest.hexdigest()
    # Three recent first-launch successes match the existing three-interval
    # contention tolerance. Warm repeats do not evict cold-start evidence.
    startup_samples = (history or {}).get('startup_samples', [])
    startup_samples = [x for x in startup_samples if isinstance(x, (int, float))
                       and math.isfinite(x) and x >= 0][-STALL_INTERVALS:]
    startup_expected = max(startup_samples, default=0)
    # Only compiled images identify first launches. Ordinary go test emits start
    # before launching its binary, and warm runs can erase its cold-start cost.
    # Keep the existing one-hour cold backstop for that unidentified startup;
    # the first named test still switches to the learned in-test silence bound.
    # Legacy compiled histories bootstrap once with that same backstop.
    startup_limit = (STALL_INTERVALS * startup_expected
                     if compiled and startup_expected > 0 else 3600)
    first_launch = image_sha is None or image_sha != (history or {}).get('image_sha256')
    is_test = compiled or command[:2] == ['go', 'test'] and '-c' not in command[2:]
    is_build = label == 'build' or label.startswith(('build-', 'compile-test:'))
    is_unittest = label.startswith('python-client') or label.endswith('-regressions') or label == 'upgrade-compat-supervisor'
    directory = None
    if compiled:
        _, package, image, directory, *arguments = command
        command = ['go', 'tool', 'test2json', '-t', '-p', package, image,
                   '-test.v=test2json', '-test.paniconexit0',
                   '-test.timeout=0' if history else '-test.timeout=1h', *arguments]
    elif is_test:
        # Cold runs retain the existing toolchain backstop. Warm runs can take
        # arbitrarily long provided they keep making progress.
        command = [*command[:2], '-timeout=0' if history else '-timeout=1h', *command[2:]]
    basis = f'{expected:.3f}s × {STALL_INTERVALS} silence' if history else 'cold learning (Go tests: 1h backstop)'
    if is_test:
        basis += f'; startup {startup_limit:.3f}s'
    print(f'gate progress {label}: cpu={cpu_count()} {basis}', flush=True)
    # Compilation/cache misses precede Go's package-start event. A warm test
    # measurement must not become a deadline for a cold compiler/linker.
    progress = Progress(None, clock())
    pending = b''
    active_tests = set()
    failed_tests = set()
    failed_builds = set()
    termination = None
    named_test_started = False
    package_started = None
    startup_gap = None
    phase = 'compile'
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
        if compiled:
            # Self-exec helpers and env-scrubbed descendants must retain this
            # shard's executable-path custody, not a shared image's identity.
            private = temporary / 'package.test'
            shutil.copy2(image, private)
            command[6] = str(private)
            child_env['PWD'] = directory
    # The watchdog owns the receipt. Regression fixtures run intentional
    # failures, and their nested watchdogs must never write to the real gate.
    if 'GATE_FAILURE_REPORT' in os.environ:
        if child_env is None:
            child_env = dict(os.environ)
        child_env.pop('GATE_FAILURE_REPORT', None)
    child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             env=child_env, cwd=directory)
    status = 1
    stalled = False
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            while True:
                remaining = progress.remaining(clock())
                if remaining == 0:
                    status = 124
                    stalled = True
                    kind = 'test progress' if is_test else 'output'
                    print(f'gate stall {label}: no {kind} for {STALL_INTERVALS * progress.expected:.3f}s ({phase})',
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
                if is_test or is_build or is_unittest:
                    pending += data
                    lines = pending.split(b'\n')
                    pending = lines.pop()
                    for line in lines:
                        if is_unittest:
                            if line.startswith((b'FAIL: ', b'ERROR: ')):
                                failed_tests.add(line.split(b': ', 1)[1].decode(errors='replace'))
                            continue
                        try:
                            event = json.loads(line)
                        except (ValueError, UnicodeDecodeError):
                            continue
                        if not isinstance(event, dict):
                            continue
                        action = event.get('Action')
                        if action == 'build-fail' and isinstance(event.get('ImportPath'), str):
                            failed_builds.add(event['ImportPath'])
                        if action == 'fail' and isinstance(event.get('FailedBuild'), str):
                            failed_builds.add(event['FailedBuild'])
                        if action == 'output' and isinstance(event.get('Output'), str):
                            output = event['Output'].strip().removeprefix('test2json: ')
                            if output.startswith(('panic: test timed out after ', '*** Test killed')):
                                termination = output.removeprefix('panic: ')
                            elif re.fullmatch(r'signal: [a-zA-Z0-9 -]+', output):
                                termination = output
                        if not is_test:
                            continue
                        # Diagnostic output (including reconnect warnings) is
                        # not evidence that a test advanced. Only test2json
                        # lifecycle transitions renew the learned watchdog.
                        if action not in ('start', 'run', 'pause', 'cont', 'pass', 'fail', 'skip'):
                            continue
                        now = clock()
                        if action == 'start':
                            if package_started is not None:
                                continue
                            package_started = now
                            phase = 'startup'
                            progress.expected = startup_limit / STALL_INTERVALS
                        name = event.get('Test')
                        if phase == 'startup' and (isinstance(name, str)
                                                  or action in ('pass', 'fail', 'skip')):
                            startup_gap = now - package_started
                            phase = 'test'
                            progress.expected = expected
                        if isinstance(name, str):
                            named_test_started = True
                            if action == 'run':
                                active_tests.add(name)
                            elif action in ('pass', 'fail', 'skip'):
                                active_tests.discard(name)
                                if action == 'fail':
                                    failed_tests.add(name)
                        progress.output(now)
                if not is_test:
                    progress.output(clock())
                sys.stdout.buffer.write(data)
                sys.stdout.buffer.flush()
    except (KeyboardInterrupt, SystemExit) as error:
        status = error.code if isinstance(error, SystemExit) else 130
    finally:
        child.stdout.close()
        command_status = status
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
        if command_status:
            if stalled:
                reason = f'watchdog during {phase if is_test else "command output"} (exit 124)'
            elif termination:
                reason = f'{termination} ({receipts.exit_reason(command_status)})'
            elif failed_builds:
                reason = 'build failure in ' + ', '.join(sorted(failed_builds))
            elif is_build and command_status <= 128:
                reason = f'build failure ({receipts.exit_reason(command_status)})'
            else:
                reason = receipts.exit_reason(command_status)
            tests = failed_tests | (active_tests if not failed_tests or termination or stalled else set())
            if not tests and is_test and not failed_builds and not named_test_started:
                reason += ' before first test'
            receipts.failure(label, reason, tests)
        if status == 125 and command_status != 125:
            receipts.failure(label, 'package cleanup unconfirmed (exit 125)')
        # A successful no-test package can exit without a named lifecycle.
        if status == 0 and is_test and startup_gap is None and package_started is not None:
            startup_gap = clock() - package_started
        elapsed = clock() - progress.started
        record = dict(label=label, cpu=cpu_count(), elapsed_seconds=elapsed,
                      shard_slots=os.environ.get('GATE_TEST_SLOTS'),
                      longest_gap_seconds=max(progress.longest_gap, clock() - progress.last),
                      expected_seconds=expected, exit=status,
                      startup_gap_seconds=startup_gap,
                      startup_limit_seconds=startup_limit if is_test else None,
                      first_launch=first_launch if compiled else None,
                      stopped_phase=phase if stalled else None,
                      progress_kind='test lifecycle' if is_test else 'output',
                      active_tests=sorted(active_tests))
        # Every run is evidence, including failures, but censored/failed runs
        # must not teach a shorter timeout or ratchet a hang into the baseline.
        with report.open('a') as output:
            output.write(json.dumps(record) + '\n')
        if status == 0 and elapsed > 0:
            if first_launch and startup_gap is not None:
                startup_samples = [*startup_samples, startup_gap][-STALL_INTERVALS:]
            learned = dict(count=(history['count'] if history else 0) + 1,
                           expected_seconds=elapsed, startup_samples=startup_samples,
                           image_sha256=image_sha)
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
        try:
            status = run(label, command, Path(os.environ['GATE_TIMING_CACHE']),
                         Path(os.environ['GATE_TIMING_REPORT']))
        except (OSError, ValueError) as error:
            receipts.failure(label, f'watchdog error ({type(error).__name__}; exit 1)')
            raise
        except (KeyboardInterrupt, SystemExit) as error:
            status = error.code if isinstance(error, SystemExit) else 130
            receipts.failure(label, receipts.exit_reason(status))
        sys.exit(status)
