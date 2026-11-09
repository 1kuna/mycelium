#!/usr/bin/env python3
"""Run a source snapshot directly in the local Lima VM, without fleet authority."""
import argparse
import fcntl
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import tempfile
import uuid


def shell(vm, *args, **kwargs):
    return subprocess.run(['limactl', 'shell', '--workdir=/', vm, '--', *args],
                          check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument('--ref', help='committed source instead of current tracked/unignored files')
    parser.add_argument('--input', action='append', default=[], help='extra FILE=ARCHIVE_PATH')
    parser.add_argument('--report', type=Path, help='append the guest measurement to this host file')
    parser.add_argument('--vm', default=os.environ.get('GATE_VM', 'mycelium-gates'))
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command[:1] == ['--']:
        command = command[1:]
    if not command:
        parser.error('a command is required after --')
    # No HOME/NAS mounts: compilation uses the VM filesystem and its persistent caches.
    subprocess.run(['limactl', 'start', '--tty=false', args.vm], check=True)
    run_id = uuid.uuid4().hex
    remote = '/tmp/mycelium-gate-' + run_id
    with tempfile.TemporaryFile() as archive:
        if args.ref:
            subprocess.run(['git', '-C', str(args.repo), 'archive', args.ref],
                           stdout=archive, check=True)
        else:
            names = subprocess.check_output(['git', '-C', str(args.repo), 'ls-files',
                                             '-z', '--cached', '--others', '--exclude-standard'])
            with tarfile.open(fileobj=archive, mode='w') as tar:
                for name in sorted(set(os.fsdecode(n) for n in names.split(b'\0') if n)):
                    path = args.repo / name
                    if path.is_file() or path.is_symlink():
                        tar.add(path, arcname=name, recursive=False)
        if True:  # Add instrumentation even for an older committed source snapshot.
            archive.seek(0)
            with tarfile.open(fileobj=archive, mode='a') as tar:
                for helper in ('gate-cache.py', 'gate-command.sh', 'gate-measure.py', 'gate-budget.py', 'gate-handles.py'):
                    tar.add(Path(__file__).with_name(helper), arcname='scripts/' + helper)
                for item in args.input:
                    source, destination = item.split('=', 1)
                    if Path(destination).is_absolute() or '..' in Path(destination).parts:
                        parser.error('input destination must remain inside the source snapshot')
                    tar.add(source, arcname=destination, recursive=False)
        archive.seek(0)
        shell(args.vm, 'bash', '-c', 'mkdir -m 700 "$1"; tar -xf - -C "$1"',
              'gate-stage', remote, stdin=archive)
    script = r'''
set -euo pipefail
export PATH="$HOME/.local/bin:/usr/local/go/bin:$PATH"
export GOCACHE="$HOME/.cache/mycelium-gates/go-build"
export GOMODCACHE="$HOME/.cache/mycelium-gates/go-mod"
export GOMAXPROCS="${GATE_CORES:-4}" GATE_TARGET=native
cd "$1"
shift
mkdir -p "$GOCACHE" "$GOMODCACHE"
# An inherited shared lock covers every command using the guest caches.
# Only an idle VM can evict build objects; another running owner is preserved.
exec 9>"$HOME/.cache/mycelium-gates/cache-use.lock"
if flock -n -x 9; then python3 scripts/gate-cache.py "$GOCACHE"; fi
flock -s 9
# Preparation may fetch dependencies; gate.sh itself remains offline.
python3 -c 'import shutil; assert shutil.disk_usage(".").free >= 45*1024**3, "STOP guest below 45 GiB entry floor"'
go mod download
if [[ "$*" == *'gate.sh full'* ]]; then python3 scripts/upgrade-compat.py prepare; fi
exec python3 scripts/gate-budget.py scripts/gate-command.sh "$@"
'''
    # gate-budget owns all fixtures. Never erase a workspace on SSH/custody failure.
    try:
        result = subprocess.run(['limactl', 'shell', '--workdir=/', args.vm, '--',
                                 'bash', '-c', script, 'gate-linux', remote, *command])
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            with args.report.open('ab') as output:
                fcntl.flock(output, fcntl.LOCK_EX)
                shell(args.vm, 'cat', remote + '/.reports/gate-local.jsonl', stdout=output)
        if result.returncode == 0:
            shell(args.vm, 'rm', '-rf', '--', remote)
        else:
            print('Linux source retained for diagnosis:', remote, file=sys.stderr)
        return result.returncode
    except BaseException:
        print('Linux source retained; confirm guest process exit before cleanup:', remote,
              file=sys.stderr)
        raise


if __name__ == '__main__':
    sys.exit(main())
