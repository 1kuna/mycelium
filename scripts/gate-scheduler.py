#!/usr/bin/env python3
"""Admit packages in selection order as any watchdog finishes custody cleanup."""
from pathlib import Path
import importlib.util
import hashlib
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time


_receipt_spec = importlib.util.spec_from_file_location('gate_receipt', Path(__file__).with_name('gate-receipt.py'))
receipts = importlib.util.module_from_spec(_receipt_spec)
_receipt_spec.loader.exec_module(receipts)


def merge_status(status, result):
    # Signal death cannot establish that the watchdog completed fixture cleanup.
    # The whole-run supervisor must settle that custody before any slot reuse.
    if result < 0 or result == 125 or status == 125:
        return 125
    if result == 124 or status == 124:
        return 124
    return status if result == 0 else 1


def run(entries, cores, command, environment=None):
    pending = {}
    completed = queue.Queue()
    status = 0
    label, stage = 'scheduler', 'schedule packages'

    def wait(child):
        child.wait()
        completed.put(child)

    def reap():
        nonlocal status
        # Scan every child before each admission: an already-exited custody
        # refusal must win even if another wait thread notified us first.
        for child, (label, started) in list(pending.items()):
            result = child.poll()
            if result is not None:
                status = merge_status(status, result)
                del pending[child]
                if result < 0:
                    receipts.failure(label, 'watchdog killed by ' + receipts.exit_reason(result))
                print(f'{label}: {int(time.monotonic() - started)}s', flush=True)

    try:
        for entry in entries:
            while True:
                reap()
                if status == 125:
                    break
                if len(pending) < cores:
                    break
                # OS child waits wake this queue, including signal death. No
                # child-written notification, inherited output pipe or polling
                # interval can strand a slot when a watchdog is killed.
                completed.get()
            if status == 125:
                break
            label = entry.split('\t', 1)[0] if isinstance(entry, str) else entry[0]
            stage = 'prepare package'
            label, argv = command(entry)
            # Command preparation (quick's go list) may take time.
            reap()
            if status == 125:
                break
            stage = 'start watchdog'
            child = subprocess.Popen(argv, env=environment)
            pending[child] = (label, time.monotonic())
            threading.Thread(target=wait, args=(child,), daemon=True).start()
        while pending and status != 125:
            reap()
            if pending and status != 125:
                completed.get()
    except (KeyboardInterrupt, SystemExit) as error:
        status = error.code if isinstance(error, SystemExit) else 130
    except Exception as error:
        print(f'gate scheduler: {error}', file=sys.stderr, flush=True)
        reason = (receipts.exit_reason(error.returncode)
                  if isinstance(error, subprocess.CalledProcessError) else type(error).__name__)
        receipts.failure(label, f'{stage} failed ({reason})')
        status = merge_status(status, 1)
    finally:
        # Watchdogs handle TERM with their existing package custody cleanup.
        # The whole-run gate-budget supervisor remains the final fixture fence.
        terminated = set()
        for child in pending:
            if child.poll() is None:
                child.terminate()
                terminated.add(child)
        for child, (label, _) in pending.items():
            result = child.wait()
            if result < 0 and (result != -signal.SIGTERM or child not in terminated):
                receipts.failure(label, 'watchdog killed by ' + receipts.exit_reason(result))
            if result < 0 or result in (124, 125):
                status = merge_status(status, result)
    return status


def python_client_command(names):
    """The Python client suite (#1224): the whole suite in full, named tests in quick."""
    client = Path(__file__).resolve().parent.parent / 'clients/python'
    label = 'python-client' + (':' + names if names else '')
    target = names.split() if names else ['discover', '-s', str(client / 'tests')]
    # gate.sh exposes only Python 3.11 (the client needs 3.10+); the system python3 may be older.
    return label, [sys.executable, str(Path(__file__).with_name('gate-progress.py')), label,
                   'env', f'PYTHONPATH={client / "src"}{os.pathsep}{client / "tests"}',
                   'python3.11', '-m', 'unittest', *target]


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def compile_packages(entries, folder):
    """Link each selected test package once; execution never uses Go's result cache."""
    folder = Path(folder).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    binaries = {}
    for entry in entries:
        package = entry.split('\t', 1)[0]
        if package.startswith('@') or package in binaries:
            continue
        # Resolve the package cwd that `go test` ordinarily chooses. Relative
        # fixture paths and TestMain's package-directory hygiene depend on it.
        try:
            info = json.loads(subprocess.check_output(
                ['go', 'list', '-race', '-json', package], text=True))
        except subprocess.CalledProcessError as error:
            receipts.failure(package, f'build preparation failed ({receipts.exit_reason(error.returncode)})')
            raise
        if not info.get('TestGoFiles') and not info.get('XTestGoFiles'):
            continue  # Preserve `go test`'s ordinary no-test package result.
        image = folder / (hashlib.sha256(package.encode()).hexdigest() + '.test')
        label = 'compile-test:' + package
        try:
            subprocess.run([sys.executable, str(Path(__file__).with_name('gate-progress.py')),
                            label, 'go', 'test', '-c', '-race', '-vet=off', '-p', '1',
                            '-json', '-o', str(image), info['ImportPath']], check=True)
        except subprocess.CalledProcessError as error:
            if error.returncode < 0:
                receipts.failure(label, 'watchdog killed by ' + receipts.exit_reason(error.returncode))
            raise
        binaries[package] = (str(image), info['Dir'])
    return binaries


def main():
    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(signum, interrupted)
    mode, cores, selection = sys.argv[1:]
    entries = Path(selection).read_text().splitlines()
    slots = int(cores)
    environment = None
    binaries = {}
    if mode == 'full' and (folder := os.environ.get('GATE_TEST_BINARY_DIR')):
        # Full reuses the images for inventory and every shard. Quick has no
        # repeated shards, so retain its overlapping Go build/test execution.
        try:
            binaries = compile_packages(entries, folder)
        except subprocess.CalledProcessError as error:
            return merge_status(0, error.returncode)
    if mode == 'full':
        spec = importlib.util.spec_from_file_location('gate_shards', Path(__file__).with_name('gate-shards.py'))
        shards = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(shards)
        entries = shards.plan(entries, slots, binaries) if binaries else shards.plan(entries, slots)
        # Cross-release proof owns a normal slot and the same watchdog/custody
        # as the tests. Start it early so it overlaps rather than extends them.
        # The Python client suite (one shared myce dev start) also starts early.
        entries.sort(key=lambda entry: (entry.split('\t', 1)[0] != '@upgrade-compat',
                                        entry.split('\t', 1)[0] != '@python-client'))
        Path(selection).with_suffix('.shards.json').write_text(json.dumps(entries, indent=2) + '\n')
        slots = min(9, slots)
        # Limit aggregate CPU pressure instead of giving every process the
        # entire host. Go compilation within a shard also inherits this budget.
        environment = dict(os.environ, GOMAXPROCS='1',
                           GOFLAGS=(os.environ.get('GOFLAGS', '') + ' -p=1').strip(),
                           GATE_TEST_SLOTS=str(slots))

    def command(entry):
        package, names = entry.split('\t', 1)
        if package == '@upgrade-compat':
            return 'upgrade-compat', [sys.executable, str(Path(__file__).with_name('gate-progress.py')),
                                     'upgrade-compat', sys.executable,
                                     str(Path(__file__).with_name('upgrade-compat.py')), 'run']
        if package == '@python-client':
            return python_client_command(names)
        binary = binaries.get(package)
        if mode != 'full':
            package = subprocess.check_output(['go', 'list', './' + package.removeprefix('./')],
                                              text=True).strip()
        print(f'{mode} {package}: {names}', flush=True)
        label = f'test:{package}:{names}'
        if binary:
            image, directory = binary
            return label, [sys.executable, str(Path(__file__).with_name('gate-progress.py')),
                           label, '--test-binary', package, image, directory,
                           '-test.parallel=' + ('1' if mode == 'full' else cores),
                           '-test.count=1', '-test.run=' + names]
        return label, [sys.executable, str(Path(__file__).with_name('gate-progress.py')),
                       label, 'go', 'test', '-race', '-vet=off', '-json', '-p', '1',
                       '-parallel', '1' if mode == 'full' else cores,
                       '-count=1', package, '-run', names]

    return run(entries, slots, command, environment)


if __name__ == '__main__':
    sys.exit(main())
