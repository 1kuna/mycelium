#!/usr/bin/env python3
"""Guest custody from channel readiness through positively verified fixture exit."""
import fcntl
import importlib.util
import os
from pathlib import Path
import signal
import select
import subprocess
import sys
import threading
import time


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def main():
    identity, cores, *command = sys.argv[1:]
    root = Path.cwd()
    reports = root / '.reports'
    reports.mkdir(exist_ok=True)
    cancelled = threading.Event()
    current = None
    # PID/nonce are published before cache readiness or any payload starts.
    (reports / 'supervisor.pid').write_text(str(os.getpid()))

    def stop(*_):
        cancelled.set()
        if current is not None and current.poll() is None:
            try:
                current.terminate()
            except ProcessLookupError:
                pass
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop)
    # No payload or cache operation before the host has established custody.
    if not os.read(sys.stdin.fileno(), 1):
        return 143
    reader_done = threading.Event()

    def channel():
        try:
            while not reader_done.is_set():
                ready, _, _ = select.select([sys.stdin.fileno()], [], [], .2)
                if ready and not os.read(sys.stdin.fileno(), 4096):
                    stop()
                    return
        finally:
            if not reader_done.is_set():
                stop()  # EOF, including host supervisor/transport death, revokes work.
    reader = threading.Thread(target=channel, daemon=True)
    reader.start()
    cache_root = Path.home() / '.cache/mycelium-gates'
    cache_root.mkdir(parents=True, exist_ok=True)
    active = cache_root / 'active'
    active.mkdir(exist_ok=True)
    marker = active / identity
    receipt = reports / 'cleanup.json'
    receipts = module('gate-receipt')
    env = dict(os.environ, GOCACHE=str(cache_root / 'go-build'),
               GOMODCACHE=str(cache_root / 'go-mod'), GOMAXPROCS=cores, GATE_TARGET='native',
               GATE_CLEANUP_ID=identity, GATE_CLEANUP_RECEIPT=str(receipt))
    for path in (env['GOCACHE'], env['GOMODCACHE']):
        Path(path).mkdir(exist_ok=True)
    with (cache_root / 'admission.lock').open('a') as admission:
        while not cancelled.is_set():
            try:
                fcntl.flock(admission, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                cancelled.wait(.1)
        if not cancelled.is_set():
            # A durable marker outlives the guardian, budget and all descriptors.
            # No new run may evict after an unconfirmed supervisor death.
            if not any(active.iterdir()):
                module('gate-cache').trim_cache(Path(env['GOCACHE']))
            if not cancelled.is_set():
                marker.write_text(identity + '\n')
        fcntl.flock(admission, fcntl.LOCK_UN)
    if cancelled.is_set():
        receipts.publish(receipt, identity, stopped=True, status=143)
        marker.unlink(missing_ok=True)
        return 143
    current = subprocess.Popen([sys.executable, 'scripts/gate-budget.py',
                                'scripts/gate-command.sh', *command], env=env,
                               stdin=subprocess.DEVNULL)
    # Close the race where EOF/TERM arrived between the check and Popen.
    if cancelled.is_set():
        stop()
    status = current.wait()
    reader_done.set()
    reader.join()
    if receipts.confirmed(receipt, identity):
        marker.unlink(missing_ok=True)
    else:
        return 125
    return status if status >= 0 else 128 - status


if __name__ == '__main__':
    sys.exit(main())
