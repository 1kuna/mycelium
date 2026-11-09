#!/usr/bin/env python3
"""Transport one frozen runtime into the local VM under cancellation custody."""
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
import threading
import uuid


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


receipts, sources = module('gate-receipt'), module('gate-source')


class Cancelled(Exception):
    pass


class Transport:
    def __init__(self, vm, remote, identity):
        self.vm, self.remote, self.identity = vm, remote, identity
        self.current = None
        self.cancelled = False
        self.guest = False

    def shell_command(self, *args):
        return ['limactl', 'shell', '--workdir=/', self.vm, '--', *args]

    def stop(self, signum, frame):
        self.cancelled = True
        if self.current is not None and self.current.stdin:
            self.current.stdin.close()  # Guest guardian treats EOF as cancellation.
        if self.guest:
            # Guardian PID exists before cache waits. If not yet ready, its EOF
            # reader observes closed input before allowing a payload to start.
            return
        if self.current is not None and self.current.poll() is None:
            try:
                os.killpg(self.current.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def run(self, command, *, heartbeat=False, check=True, **kwargs):
        if self.cancelled:
            raise Cancelled()
        self.current = subprocess.Popen(command, start_new_session=True,
            stdin=subprocess.PIPE if heartbeat else kwargs.pop('stdin', None), **kwargs)
        child = self.current
        if self.cancelled:
            # TERM can arrive between the precheck and assignment by Popen.
            # Revoke this new transport too, before waiting or allowing writes.
            self.stop(signal.SIGTERM, None)
        sender_done = threading.Event()
        def sender():
            try:
                while not sender_done.is_set() and not self.cancelled:
                    child.stdin.write(b'.')
                    child.stdin.flush()
                    sender_done.wait(.5)
            except (OSError, ValueError):
                pass
            finally:
                try:
                    child.stdin.close()
                except (OSError, ValueError):
                    pass
        if heartbeat:
            threading.Thread(target=sender, daemon=True).start()
        try:
            try:
                status = child.wait()
            except BaseException:
                os.killpg(child.pid, signal.SIGTERM)
                child.wait()
                raise
        finally:
            sender_done.set()
            self.current = None
        if self.cancelled:
            raise Cancelled()
        if check and status:
            raise subprocess.CalledProcessError(status, command)
        return status

    def shell(self, *args, **kwargs):
        return self.run(self.shell_command(*args), **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True,
                        help='runtime archive with already frozen instrumentation')
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
    remote = '/tmp/mycelium-gate-' + args.cleanup_id
    transport = Transport(args.vm, remote, args.cleanup_id)
    handlers = {sig: signal.signal(sig, transport.stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        transport.run(['limactl', 'start', '--tty=false', args.vm])
        with tempfile.TemporaryDirectory(prefix='gate-transport-') as folder:
            archive = Path(folder) / 'runtime.tar'
            import shutil
            shutil.copyfile(args.archive, archive)
            with tarfile.open(archive, 'a') as tar:
                for item in args.input:
                    source, destination = item.split('=', 1)
                    if Path(destination).is_absolute() or '..' in Path(destination).parts:
                        parser.error('input must remain inside source snapshot')
                    tar.add(source, arcname=destination, recursive=False)
            with archive.open('rb') as stream:
                transport.shell('bash', '-c', 'set -eu; mkdir -m 700 "$1"; tar -xf - -C "$1"',
                                'gate-stage', remote, stdin=stream)
        transport.guest = True
        launch = transport.shell_command('env', 'GATE_CLEANUP_ID=' + args.cleanup_id,
            'bash', '-c', 'export PATH="$HOME/.local/bin:/usr/local/go/bin:$PATH"; cd "$1"; '
            'shift; exec python3 scripts/gate-guest.py "$@"', 'gate-linux', remote,
            args.cleanup_id, str(args.cores), *command)
        status = transport.run(launch, heartbeat=True, check=False)
        transport.guest = False
        if status < 0 or status in (125, 255):
            return 125
        with tempfile.TemporaryFile() as stream:
            transport.shell('cat', remote + '/.reports/cleanup.json', stdout=stream)
            stream.seek(0)
            row = json.load(stream)
        if (row.get('identity') != args.cleanup_id or row.get('stopped') is not True
                or row.get('status') != status):
            return 125
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            with args.report.open('ab') as output:
                fcntl.flock(output, fcntl.LOCK_EX)
                transport.shell('cat', remote + '/.reports/gate-local.jsonl', stdout=output)
        if status == 0:
            transport.shell('rm', '-rf', '--', remote)
        else:
            print('Linux source retained for diagnosis:', remote, file=sys.stderr)
        if args.cleanup_receipt:
            receipts.publish(args.cleanup_receipt, args.cleanup_id, stopped=True, status=status)
        return status
    except Cancelled:
        # No receipt: cancellation is safe to retry only after explicit cleanup
        # reconciliation. In-flight guest work revokes itself on channel EOF.
        return 125
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print('STOP Linux gate: transport/cleanup unconfirmed; source retained:', error, file=sys.stderr)
        sys.exit(125)
