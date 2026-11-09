#!/usr/bin/env python3
"""Affirmative, attempt-specific cleanup receipts; absence is never proof."""
import json
from pathlib import Path
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
