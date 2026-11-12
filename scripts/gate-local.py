#!/usr/bin/env python3
"""Run identified Darwin/Linux source under host-wide admission and disk guard."""
import argparse
import importlib.util
import json
import os
import queue
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import traceback
import uuid

REPO = Path(__file__).resolve().parent.parent


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


capacity, receipts, sources = (module('gate-' + name) for name in ('capacity', 'receipt', 'source'))
trim_cache = module('gate-cache').trim_cache
# Workers' direct go test runs fill the default cache, which nothing else trims (#1224).
trim_default_cache = module('gate-default-cache').trim_default_cache
HELPERS = ('gate-budget.py', 'gate-capacity.py', 'gate-handles.py', 'gate-command.sh', 'gate-leg.sh',
           'gate-cache.py', 'gate-measure.py', 'gate-memory.py', 'gate-guest.py', 'gate-receipt.py')


def prepare(path):
    """Synchronous preparation starts no native or guest payload."""
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
    owner = manifest.get('capacity_owner', {})
    identity['admission'] = dict(cores=manifest['cores'], exclusive=owner.get('exclusive', False),
        calibration=owner.get('calibration', False),
        host_cores=owner.get('cores', manifest['cores']),
        cpu_share_per_leg=owner.get('cpu_share_per_leg'),
        automatic_slots=owner.get('automatic_slots', False),
        concurrent_platforms=manifest['platform'] == 'both' and not manifest.get('sequential_platforms', False),
        sequential_platforms=manifest.get('sequential_platforms', False),
        configuration_sha256=owner.get('admission_config_sha256'),
        fleet_worker_cpu_reserve_cores=owner.get('fleet_worker_cpu_reserve_cores'),
        workstation_cpu_reserve_cores=owner.get('workstation_cpu_reserve_cores'))
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
        if checked != {k: v for k, v in identity.items() if k not in ('instrumentation_sha256', 'admission')}:
            raise RuntimeError('source changed during quick selection; no test started')
    return manifest, repo, folder, source, identity, env, report, selection, runtime, info, predecessor_files


def execute(path):
    """Child orchestration; parent measures host disk across every phase."""
    try:
        (manifest, repo, folder, source, identity, env, report, selection,
         runtime, info, predecessor_files) = prepare(path)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError):
        # All preparation commands use run/check_output, which reap on these
        # failures. No payload/transport has been launched. Signal death and
        # unknown exceptions still leave no positive cleanup receipt.
        manifest = json.loads(path.read_text())
        receipts.publish(path.parent / 'overall-cleanup.json', manifest['identity'],
                         stopped=True, status=125, phase='preparation', no_payload_started=True)
        raise
    started_receipts = []
    children = []
    completed = queue.Queue()
    interrupted = []

    def interrupt(signum, frame):
        interrupted.append(signum)
        for child in children:
            if child.poll() is None:
                child.send_signal(signum)

    def invalid_exit(result):
        return result == 125 or result < 0 or result >= 128

    def launch(command, receipt, nonce, **kwargs):
        # A completed refusal must stop new work, even before its wait-thread
        # notification. Ordinary test failures still collect the other leg.
        if interrupted or any(invalid_exit(child.poll()) for child in children
                              if child.poll() is not None):
            return None
        started_receipts.append((receipt, nonce))
        child = subprocess.Popen(command, **kwargs)
        children.append(child)
        # TERM may arrive inside Popen before this child is registered.
        if interrupted and child.poll() is None:
            child.send_signal(interrupted[-1])
        def wait():
            completed.put((child, child.wait(), receipt, nonce))
        threading.Thread(target=wait, daemon=True).start()
        return child

    handlers = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        mode = manifest['mode']
        status = 0
        collected = 0
        if manifest['platform'] != 'linux':
            native_id = uuid.uuid4().hex
            native_receipt = folder / 'native-cleanup.json'
            native_env = dict(env, GATE_CLEANUP_ID=native_id, GATE_CLEANUP_RECEIPT=str(native_receipt),
                              GATE_EXECUTION_CUSTODY=str(folder / 'native-custody.json'))
            if manifest.get('capacity_owner', {}).get('admission_protocol') == 2:
                native_env.update(GATE_LOCAL_SLOT=manifest['capacity_slot'],
                                  GATE_LOCAL_SLOT_ID=manifest['capacity_owner']['run_id'])
                marker = manifest['capacity_owner'].get('run_marker')
                if marker:
                    native_env.update(GATE_CUSTODY_ID=marker, GATE_SCRATCH=manifest['capacity_owner']['scratch'])
            mode = manifest['mode']
            payload = manifest['command'] or ['bash', 'scripts/gate-leg.sh', mode]
            if not manifest['command'] and mode == 'quick':
                payload += ['--selection', str(selection)]
            native = [sys.executable, str(source / 'scripts/gate-measure.py'), '--report', str(report),
                      '--run-id', manifest['identity'],
                      '--source-info', str(info), '--mode', mode, '--phase', 'native', '--',
                      sys.executable, 'scripts/gate-budget.py', 'scripts/gate-command.sh', *payload]
            if launch(native, native_receipt, native_id, cwd=source, env=native_env) is None:
                return 125
        if (mode == 'quick' and manifest['platform'] == 'both' and
                manifest.get('sequential_platforms', False)):
            child, result, receipt, nonce = completed.get()
            collected += 1
            if invalid_exit(result) or interrupted or not receipts.confirmed(receipt, nonce):
                return 125
            status = status or result
        if manifest['platform'] != 'native':
            linux_id = uuid.uuid4().hex
            linux_receipt = folder / 'linux-cleanup.json'
            receipts.publish(folder / 'linux-custody.json', linux_id, stopped=False, status=125,
                             vm=env.get('GATE_VM', 'mycelium-gates'),
                             source_info=json.loads(info.read_text()))
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
            if launch(linux, linux_receipt, linux_id, cwd=source, env=env) is None:
                return 125
        for _ in children[collected:]:
            child, result, receipt, nonce = completed.get()
            if (invalid_exit(result) or interrupted or
                    not receipts.confirmed(receipt, nonce)):
                return 125
            status = status or result
        return status
    finally:
        # Positive receipts authorize source deletion, never admission occupancy.
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            child.wait()
        complete = (bool(started_receipts) and
                    all(receipts.confirmed(p, key) for p, key in started_receipts) and
                    all(child.poll() is not None for child in children))
        receipts.publish(folder / 'overall-cleanup.json', manifest['identity'],
                         stopped=complete, status=0 if complete else 125)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def main():
    # Admission itself invokes OS/Git tools. Strip inherited fleet authority
    # before even those first subprocesses, not just before the test payload.
    for key in list(os.environ):
        if key.startswith('MYCE_'):
            del os.environ[key]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', nargs='?', choices=('quick', 'full'), default='quick')
    parser.add_argument('base', nargs='?')
    parser.add_argument('--selection', type=Path)
    parser.add_argument('--repo', type=Path, default=REPO)
    parser.add_argument('--ref')
    parser.add_argument('--platform', choices=('native', 'linux', 'both'))
    parser.add_argument('--priority', choices=('normal', 'release', 'calibration'), default=os.environ.get('GATE_PRIORITY', 'normal'),
                        help='all requests use FIFO; release full drains and holds local admission; calibration qualifies isolated measurements')
    parser.add_argument('--cores', type=int,
                        help='test slots per platform; full defaults to measured eight-slot memory capacity, otherwise four')
    parser.add_argument('--exclusive', action='store_true',
                        help='wait for all local gate work; reserve it for isolated timing')
    parser.add_argument('--after-pid', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--command', nargs=argparse.REMAINDER, default=[])
    parser.add_argument('--execute', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--reconcile', type=Path, nargs='+', metavar='RECEIPT',
                        help='compatibility command; admission now reclaims dead holders automatically')
    args = parser.parse_args()
    if (args.mode == 'quick' or args.command) and args.priority == 'release':
        args.priority = 'normal'
    if args.cores is None and (args.mode != 'full' or args.command):
        args.cores = 4
    if args.execute:
        return execute(args.execute)
    if args.reconcile:
        result = module('gate-reconcile').reconcile(args.reconcile, args.repo.resolve())
        print('local gate reconciliation:', json.dumps(result), flush=True)
        return 0
    if args.after_pid and args.priority != 'calibration':
        parser.error('--after-pid is only for the reserved calibration full')
    repo = args.repo.resolve()
    slot, folder, child, cache = None, None, None, None
    interrupted = []
    def interrupt(signum, frame):
        interrupted.append(signum)
        if child is not None and child.poll() is None:
            child.send_signal(signum)
        else:
            raise SystemExit(128 + signum)
    handlers = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        identity = uuid.uuid4().hex
        planned_folder = repo / '.reports/local-gates' / identity
        cache = Path.home() / 'Library/Caches/mycelium/gates/go-build'
        cache.mkdir(parents=True, exist_ok=True)
        # Reclaim rebuildable objects before admission checks disk headroom.
        trim_cache(cache)
        trim_default_cache(capacity.ENTRY_FLOOR, repo, skip=(cache,))
        slot = capacity.acquire(repo, mode=args.mode, priority=args.priority, cores=args.cores,
                                wait=True, after_pid=args.after_pid,
                                exclusive=args.exclusive, source_root=planned_folder / 'source',
                                platform=args.platform or ('native' if args.command else 'both'))
        args.cores = slot.owner.get('per_leg_cores', args.cores or 4)
        folder = planned_folder
        folder.mkdir(parents=True)
        report = repo / '.reports/gate-local.jsonl'
        manifest = dict(repo=str(repo), mode=args.mode, base=args.base, ref=args.ref,
                        selection=str(args.selection.resolve()) if args.selection else None,
                        command=args.command, cores=args.cores, identity=identity,
                        platform=args.platform or ('native' if args.command else 'both'),
                        sequential_platforms=bool(slot.owner.get('sequential_platforms', False)),
                        cache=str(cache), report=str(report),
                        capacity_slot=str(slot.directory), capacity_owner=slot.owner,
                        local_worker_usage_at_entry=slot.owner.get('local_worker_usage_at_entry'))
        path = folder / 'manifest.json'
        path.write_text(json.dumps(manifest) + '\n')
        env = dict(os.environ, GOCACHE=str(cache))
        if slot.owner.get('run_marker'):
            env.update(GATE_CUSTODY_ID=slot.owner['run_marker'], GATE_SCRATCH=slot.owner['scratch'])
        command = [sys.executable, str(REPO / 'scripts/gate-measure.py'), '--report', str(report),
                   '--run-id', identity,
                   '--source-info', str(folder / 'source-identity.json'), '--mode', args.mode,
                   '--phase', 'host', '--disk-root', str(Path.home()), '--',
                   sys.executable, str(__file__), '--execute', str(path)]
        child = subprocess.Popen(command, cwd=repo, env=env)
        status = child.wait()
        confirmed = receipts.confirmed(folder / 'overall-cleanup.json', identity)
        if confirmed:
            # A terminal failure is as rebuildable as a successful source tree.
            try:
                if (folder / 'source').exists():
                    shutil.rmtree(folder / 'source')
                (folder / 'source.tar').unlink(missing_ok=True)
                (folder / 'runtime.tar').unlink(missing_ok=True)
            except Exception as error:
                (folder / 'finalize-error.json').write_text(json.dumps(dict(status=125,
                    error=str(error), traceback=traceback.format_exc())) + '\n')
                print('gate: source cleanup failed: ' + str(error), file=sys.stderr)
                return 125
        return status if confirmed else 125
    finally:
        if slot is not None:
            slot.release(stopped=bool(folder) and receipts.confirmed(folder / 'overall-cleanup.json', identity))
            capacity._cache.trim_backups(slot.directory.parent / 'selfheal-backups')
        if cache is not None:
            trim_cache(cache)
            trim_default_cache(capacity.ENTRY_FLOOR, repo, skip=(cache,))
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
