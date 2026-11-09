#!/usr/bin/env python3
"""Run identified Darwin/Linux source under host-wide admission and disk guard."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import uuid

REPO = Path(__file__).resolve().parent.parent


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


capacity, receipts, sources = (module('gate-' + name) for name in ('capacity', 'receipt', 'source'))
trim_cache = module('gate-cache').trim_cache
HELPERS = ('gate-budget.py', 'gate-handles.py', 'gate-command.sh', 'gate-leg.sh',
           'gate-cache.py', 'gate-measure.py', 'gate-memory.py', 'gate-guest.py', 'gate-receipt.py')


def execute(path):
    """Child orchestration; parent measures host disk across every phase."""
    manifest = json.loads(path.read_text())
    repo, folder = Path(manifest['repo']), path.parent
    source = folder / 'source'
    archive = folder / 'source.tar'
    source.mkdir()
    identity = sources.freeze(repo, archive, manifest.get('ref'))
    sources.extract(archive, source)
    predecessor_files = sources.predecessors(repo, source) if manifest['mode'] == 'full' and not manifest['command'] else []
    # Both platforms receive the same current supervisor instrumentation,
    # separately identified from the selected product/test revision.
    import hashlib
    digest = hashlib.sha256()
    for helper in HELPERS:
        data = Path(__file__).with_name(helper).read_bytes()
        digest.update(helper.encode() + b'\0' + data)
        (source / 'scripts' / helper).write_bytes(data)
    identity['instrumentation_sha256'] = digest.hexdigest()
    runtime = folder / 'runtime.tar'
    sources.runtime_archive(archive, runtime, source, HELPERS)
    info = folder / 'source-identity.json'
    info.write_text(json.dumps(identity) + '\n')
    cache = Path(manifest['cache'])
    env = dict(os.environ, GOCACHE=str(cache), GOMAXPROCS=str(manifest['cores']),
               GATE_TARGET='native', GIT_CEILING_DIRECTORIES=str(source.parent))
    for key in list(env):
        if key.startswith('MYCE_'):
            del env[key]
    report = Path(manifest['report'])
    selection = folder / 'selection.tsv'
    if manifest['mode'] == 'quick' and not manifest['command']:
        if manifest.get('selection'):
            shutil.copyfile(manifest['selection'], selection)
        else:
            with selection.open('w') as output:
                subprocess.run([sys.executable, str(repo / 'scripts/gate-related.py'),
                                manifest.get('base') or 'origin/main'], cwd=repo,
                               stdout=output, check=True)
        # A concurrent edit cannot change the source selected for either leg.
        verify = folder / 'verify.tar'
        checked = sources.freeze(repo, verify, manifest.get('ref'))
        verify.unlink()
        if checked != {k: v for k, v in identity.items() if k != 'instrumentation_sha256'}:
            raise RuntimeError('source changed during quick selection; no test started')
    started_receipts = []
    current = None
    interrupted = []

    def interrupt(signum, frame):
        interrupted.append(signum)
        if current is not None and current.poll() is None:
            current.send_signal(signum)
    handlers = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        mode = manifest['mode']
        status = 0
        if manifest['platform'] != 'linux':
            native_id = uuid.uuid4().hex
            native_receipt = folder / 'native-cleanup.json'
            native_env = dict(env, GATE_CLEANUP_ID=native_id, GATE_CLEANUP_RECEIPT=str(native_receipt))
            mode = manifest['mode']
            payload = manifest['command'] or ['bash', 'scripts/gate-leg.sh', mode]
            if not manifest['command'] and mode == 'quick':
                payload += ['--selection', str(selection)]
            native = [sys.executable, str(REPO / 'scripts/gate-measure.py'), '--report', str(report),
                      '--run-id', manifest['identity'],
                      '--source-info', str(info), '--mode', mode, '--phase', 'native', '--',
                      sys.executable, 'scripts/gate-budget.py', 'scripts/gate-command.sh', *payload]
            started_receipts.append((native_receipt, native_id))
            current = subprocess.Popen(native, cwd=source, env=native_env)
            status = current.wait()
            if not receipts.confirmed(native_receipt, native_id) or status == 125 or status >= 128 or status < 0 or interrupted:
                return 125
            if manifest['platform'] == 'native':
                return status
        linux_id = uuid.uuid4().hex
        linux_receipt = folder / 'linux-cleanup.json'
        linux = [sys.executable, str(REPO / 'scripts/gate-linux.py'), '--archive', str(runtime),
                 '--report', str(report), '--cleanup-receipt', str(linux_receipt),
                 '--cleanup-id', linux_id, '--cores', str(manifest['cores']),
                 '--input', str(info) + '=source-identity.json']
        for predecessor in predecessor_files:
            linux += ['--input', str(predecessor) + '=.gate-predecessors/' + predecessor.name]
        if mode == 'quick' and not manifest['command']:
            linux += ['--input', str(selection) + '=gate-selection.tsv']
        linux_payload = manifest['command'] or ['bash', 'scripts/gate-leg.sh', mode]
        if mode == 'quick' and not manifest['command']:
            linux_payload += ['--selection', 'gate-selection.tsv']
        linux += ['--', 'python3', 'scripts/gate-measure.py', '--source-info', 'source-identity.json',
                  '--run-id', manifest['identity'],
                  '--mode', mode, '--phase', 'linux', '--', *linux_payload]
        started_receipts.append((linux_receipt, linux_id))
        current = subprocess.Popen(linux, cwd=source, env=env)
        result = current.wait()
        if not receipts.confirmed(linux_receipt, linux_id) or result == 125 or interrupted:
            return 125
        return status or result
    finally:
        # Positive receipts, not exit-code guesses, authorize capacity reuse.
        complete = (bool(started_receipts) and
                    all(receipts.confirmed(p, key) for p, key in started_receipts) and
                    current is not None and current.poll() is not None)
        receipts.publish(folder / 'overall-cleanup.json', manifest['identity'],
                         stopped=complete, status=0 if complete else 125)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', nargs='?', choices=('quick', 'full'), default='quick')
    parser.add_argument('base', nargs='?')
    parser.add_argument('--selection', type=Path)
    parser.add_argument('--repo', type=Path, default=REPO)
    parser.add_argument('--ref')
    parser.add_argument('--platform', choices=('native', 'linux', 'both'))
    parser.add_argument('--priority', choices=('normal', 'release', 'calibration'), default=os.environ.get('GATE_PRIORITY', 'normal'))
    parser.add_argument('--cores', type=int, default=4)
    parser.add_argument('--after-pid', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--command', nargs=argparse.REMAINDER, default=[])
    parser.add_argument('--execute', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.execute:
        return execute(args.execute)
    if args.after_pid and args.priority != 'calibration':
        parser.error('--after-pid is only for the reserved calibration full')
    repo = args.repo.resolve()
    slot, folder, child = None, None, None
    interrupted = []
    def interrupt(signum, frame):
        interrupted.append(signum)
        if child is not None and child.poll() is None:
            child.send_signal(signum)
        else:
            raise SystemExit(128 + signum)
    handlers = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        slot = capacity.acquire(repo, mode=args.mode, priority=args.priority, cores=args.cores,
                                wait=True, after_pid=args.after_pid,
                                platform=args.platform or ('native' if args.command else 'both'))
        cache = Path.home() / 'Library/Caches/mycelium/gates/go-build'
        cache.mkdir(parents=True, exist_ok=True)
        identity = uuid.uuid4().hex
        folder = repo / '.reports/local-gates' / identity
        folder.mkdir(parents=True)
        report = repo / '.reports/gate-local.jsonl'
        manifest = dict(repo=str(repo), mode=args.mode, base=args.base, ref=args.ref,
                        selection=str(args.selection.resolve()) if args.selection else None,
                        command=args.command, cores=args.cores, identity=identity,
                        platform=args.platform or ('native' if args.command else 'both'),
                        cache=str(cache), report=str(report))
        path = folder / 'manifest.json'
        path.write_text(json.dumps(manifest) + '\n')
        with capacity.admission_lock(capacity.root(repo)):
            legacy = [p for d in capacity.legacy_directories(repo) for p in capacity.occupied_lanes(d)]
            others = [p for p in capacity.occupied_lanes(capacity.root(repo)) if p != slot.directory]
            if not others and not legacy and not capacity.native_full_custody():
                trim_cache(cache)
        env = dict(os.environ, GOCACHE=str(cache))
        command = [sys.executable, str(REPO / 'scripts/gate-measure.py'), '--report', str(report),
                   '--run-id', identity,
                   '--source-info', str(folder / 'source-identity.json'), '--mode', args.mode,
                   '--phase', 'host', '--disk-root', str(Path.home()), '--',
                   sys.executable, str(__file__), '--execute', str(path)]
        child = subprocess.Popen(command, cwd=repo, env=env)
        status = child.wait()
        confirmed = receipts.confirmed(folder / 'overall-cleanup.json', identity)
        if confirmed and status == 0:
            shutil.rmtree(folder / 'source')
            (folder / 'source.tar').unlink()
            (folder / 'runtime.tar').unlink()
        return status if confirmed else 125
    finally:
        if slot is not None:
            slot.release(stopped=bool(folder) and receipts.confirmed(folder / 'overall-cleanup.json', identity))
        if folder:
            print('local gate receipts:', folder, flush=True)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print('STOP local gate:', error, file=sys.stderr)
        sys.exit(125)
