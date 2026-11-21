#!/usr/bin/env python3
"""Affirmative, attempt-specific cleanup receipts; absence is never proof."""
import json
import os
from pathlib import Path
import signal
import sys
import uuid


def publish(path, identity, *, stopped, status, **extra):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + '.' + uuid.uuid4().hex)
    pending.write_text(json.dumps(dict(schema=1, identity=identity,
                                      stopped=stopped, status=status, **extra)) + '\n')
    pending.replace(path)


def confirmed(path, identity):
    try:
        row = json.loads(Path(path).read_text())
        return row.get('schema') == 1 and row.get('identity') == identity and row.get('stopped') is True
    except (OSError, ValueError, TypeError):
        return False


def exit_reason(status):
    if status < 0 or status > 128:
        number = -status if status < 0 else status - 128
        try:
            return 'signal ' + signal.Signals(number).name
        except ValueError:
            pass
    return f'exit {status}'


def failure(label, reason, tests=()):
    if label.startswith('test:'):
        label = label.split(':', 2)[1]
    message = f'{label}: {reason}'
    if tests:
        message += ' (' + ', '.join(sorted(tests)) + ')'
    print('gate failure ' + message, file=sys.stderr, flush=True)
    if path := os.environ.get('GATE_FAILURE_REPORT'):
        # A single append per failure keeps parallel watchdog records intact.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, (message + '\n').encode())
        finally:
            os.close(fd)
