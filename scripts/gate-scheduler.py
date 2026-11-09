#!/usr/bin/env python3
"""Admit packages in selection order as any watchdog finishes custody cleanup."""
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading
import time


def merge_status(status, result):
    # Signal death cannot establish that the watchdog completed fixture cleanup.
    # The whole-run supervisor must settle that custody before any slot reuse.
    if result < 0 or result == 125 or status == 125:
        return 125
    if result == 124 or status == 124:
        return 124
    return status if result == 0 else 1


def run(entries, cores, command):
    pending = {}
    completed = queue.Queue()
    status = 0

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
            label, argv = command(entry)
            # Command preparation (quick's go list) may take time.
            reap()
            if status == 125:
                break
            child = subprocess.Popen(argv)
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
        status = merge_status(status, 1)
    finally:
        # Watchdogs handle TERM with their existing package custody cleanup.
        # The whole-run gate-budget supervisor remains the final fixture fence.
        for child in pending:
            if child.poll() is None:
                child.terminate()
        for child in pending:
            result = child.wait()
            if result < 0 or result in (124, 125):
                status = merge_status(status, result)
    return status


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def main():
    for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(signum, interrupted)
    mode, cores, selection = sys.argv[1:]

    def command(entry):
        package, names = entry.split('\t', 1)
        if mode != 'full':
            package = subprocess.check_output(['go', 'list', './' + package.removeprefix('./')],
                                              text=True).strip()
        print(f'{mode} {package}: {names}', flush=True)
        label = f'test:{package}:{names}'
        return label, [sys.executable, str(Path(__file__).with_name('gate-progress.py')),
                       label, 'go', 'test', '-race', '-vet=off', '-json', '-p', '1',
                       '-parallel', cores, '-count=1', package, '-run', names]

    return run(Path(selection).read_text().splitlines(), int(cores), command)


if __name__ == '__main__':
    sys.exit(main())
