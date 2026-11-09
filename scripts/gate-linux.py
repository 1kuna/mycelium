#!/usr/bin/env python3
"""Run frozen source directly in a local VM; require positive guest cleanup."""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import tempfile
import uuid


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


receipts, sources = module('gate-receipt'), module('gate-source')


def shell(vm, *args, **kwargs):
    return subprocess.run(['limactl', 'shell', '--workdir=/', vm, '--', *args],
                          check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--ref')
    parser.add_argument('--archive', type=Path, help='use an already frozen source archive')
    parser.add_argument('--input', action='append', default=[])
    parser.add_argument('--report', type=Path)
    parser.add_argument('--cleanup-receipt', type=Path)
    parser.add_argument('--cleanup-id', default=uuid.uuid4().hex)
    parser.add_argument('--cores', type=int, default=4)
    parser.add_argument('--vm', default=os.environ.get('GATE_VM', 'mycelium-gates'))
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('command required after --')
    subprocess.run(['limactl', 'start', '--tty=false', args.vm], check=True)
    remote = '/tmp/mycelium-gate-' + args.cleanup_id
    receipt = remote + '/.reports/cleanup.json'
    with tempfile.TemporaryDirectory(prefix='gate-transport-') as folder:
        archive = Path(folder) / 'source.tar'
        if args.archive:
            import shutil
            shutil.copyfile(args.archive, archive)
        else:
            sources.freeze(args.repo, archive, args.ref)
        with tarfile.open(archive, 'a') as tar:
            # Older product revisions receive current instrumentation only.
            for helper in ('gate-cache.py', 'gate-command.sh', 'gate-measure.py',
                           'gate-budget.py', 'gate-handles.py', 'gate-leg.sh', 'gate-memory.py'):
                tar.add(Path(__file__).with_name(helper), arcname='scripts/' + helper)
            for item in args.input:
                source, destination = item.split('=', 1)
                if Path(destination).is_absolute() or '..' in Path(destination).parts:
                    parser.error('input must remain inside source snapshot')
                tar.add(source, arcname=destination, recursive=False)
        with archive.open('rb') as stream:
            shell(args.vm, 'bash', '-c', 'set -eu; mkdir -m 700 "$1"; tar -xf - -C "$1"',
                  'gate-stage', remote, stdin=stream)
    script = r'''
set -euo pipefail
export PATH="$HOME/.local/bin:/usr/local/go/bin:$PATH"
export GOCACHE="$HOME/.cache/mycelium-gates/go-build"
export GOMODCACHE="$HOME/.cache/mycelium-gates/go-mod"
export GOMAXPROCS="$2" GATE_TARGET=native
export GATE_CLEANUP_ID="$3" GATE_CLEANUP_RECEIPT="$1/.reports/cleanup.json"
cd "$1"
shift 3
mkdir -p "$GOCACHE" "$GOMODCACHE" .reports
exec 9>"$HOME/.cache/mycelium-gates/cache-use.lock"
if flock -n -x 9; then python3 scripts/gate-cache.py "$GOCACHE"; fi
flock -s 9
echo $$ > .reports/supervisor.pid
exec python3 scripts/gate-budget.py scripts/gate-command.sh "$@"
'''
    child = subprocess.Popen(['limactl', 'shell', '--workdir=/', args.vm, '--',
                              'env', 'GATE_CLEANUP_ID=' + args.cleanup_id,
                              'bash', '-c', script, 'gate-linux', remote,
                              str(args.cores), args.cleanup_id, *command])
    interrupted = []

    def interrupt(signum, frame):
        interrupted.append(signum)
        # Never kill an unverified PID: the nonce must match the exact guest
        # supervisor's own environment, not its children or an old PID reuse.
        stop = r'''
import os, pathlib, signal, sys
root, identity = pathlib.Path(sys.argv[1]), sys.argv[2]
pid = int((root / '.reports/supervisor.pid').read_text())
environment = pathlib.Path('/proc/%d/environ' % pid).read_bytes().split(b'\0')
assert ('GATE_CLEANUP_ID=' + identity).encode() in environment, 'guest identity changed'
os.kill(pid, signal.SIGTERM)
'''
        try:
            shell(args.vm, 'python3', '-c', stop, remote, args.cleanup_id, timeout=15)
        except (OSError, subprocess.SubprocessError) as error:
            print('STOP: guest stop unconfirmed; capacity retained:', error, file=sys.stderr)
            child.terminate()  # Only our local transport; never proof of guest exit.
    old = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        status = child.wait()
        if status < 0 or status in (125, 255):
            return 125
        result = shell(args.vm, 'cat', receipt, capture_output=True, text=True, timeout=15)
        row = json.loads(result.stdout)
        if (row.get('identity') != args.cleanup_id or row.get('stopped') is not True
                or row.get('status') != status):
            return 125
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            with args.report.open('ab') as output:
                fcntl.flock(output, fcntl.LOCK_EX)
                shell(args.vm, 'cat', remote + '/.reports/gate-local.jsonl', stdout=output, timeout=15)
        if status == 0:
            shell(args.vm, 'rm', '-rf', '--', remote, timeout=15)
        else:
            print('Linux source retained for diagnosis:', remote, file=sys.stderr)
        if args.cleanup_receipt:
            receipts.publish(args.cleanup_receipt, args.cleanup_id, stopped=True, status=status)
        return 128 + interrupted[0] if interrupted else status
    finally:
        for sig, handler in old.items():
            signal.signal(sig, handler)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print('STOP Linux gate: transport/cleanup unconfirmed; source retained:', error, file=sys.stderr)
        sys.exit(125)
