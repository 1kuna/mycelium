#!/usr/bin/env python3
"""Bound gate-owned Go objects; preserve modules and timing evidence."""
import re
import json
import importlib.util
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time


# One day's debugging window, eight failed runs and 2 GiB of rebuildable scratch.
# Real verdicts and diagnostics stay in their original receipt directories.
BACKUP_AGE = 24 * 60 * 60
BACKUP_COUNT = 8
BACKUP_BYTES = 2 * 1024 ** 3


def scratch_idle(owner, path):
    """Scratch liveness affects collection, never admission or the verdict."""
    spec = importlib.util.spec_from_file_location('gate_budget', Path(__file__).with_name('gate-budget.py'))
    budget = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(budget)
    if owner.get('source_root'):
        journal = Path(owner['source_root']).parent / 'native-custody.json'
        if journal.exists():
            for row in json.loads(journal.read_text()).get('supervisors', []):
                if budget.process_start(row['pid']) == row['process_identity']:
                    return False
    marker = owner.get('marker') or 'GATE_RUN_ID=' + owner.get('run_marker', '')
    if budget.owned_processes(marker, str(path), {}):
        return False
    return not budget.owned_processes(marker, str(path), {}, inspect_handles=True, complete_handles=True,
                                      born_after=owner.get('process_identity', owner.get('holder_start')))


def collect_scratch(owner, archive):
    """Retry deferred scratch on later admissions without holding a slot."""
    path = Path(owner['scratch'])
    validate_scratch(path)
    if path.exists():
        if not scratch_idle(owner, path):
            raise RuntimeError('live fixture uses scratch; preserved in place')
        backup_scratch(str(path), archive)
    (archive / 'scratch-pending.json').unlink(missing_ok=True)


def validate_scratch(path):
    if (not path.is_absolute() or path.parent.name != '.gate' or
            not path.name.startswith('run.') or path.resolve() != path or path.is_symlink()):
        raise ValueError('not private gate scratch')


def backup_scratch(scratch, archive):
    """Rename private scratch on its own filesystem; never traverse a link."""
    path = Path(scratch)
    validate_scratch(path)
    if not path.exists():
        return None
    destination = path.parent / 'selfheal-backups' / archive.name
    destination.parent.mkdir(exist_ok=True)
    if destination.parent.is_symlink():
        raise ValueError('linked scratch backup directory')
    # Publish the destination first, so interruption after rename is recoverable.
    (archive / 'scratch.json').write_text(json.dumps(dict(original=str(path), backup=str(destination))) + '\n')
    path.rename(destination)
    return destination


def trim_backups(directory, *, now=None, limit=BACKUP_BYTES, count=BACKUP_COUNT, age=BACKUP_AGE):
    try:
        _trim_backups(directory, now=now, limit=limit, count=count, age=age)
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        print('WARNING gate: backup collection failed: ' + str(error), file=sys.stderr)


def _trim_backups(directory, *, now, limit, count, age):
    """Expire only archived, rebuildable scratch; serialize with its publisher."""
    import fcntl
    if directory.is_symlink():
        raise ValueError('linked custody backup directory')
    directory.mkdir(parents=True, exist_ok=True)
    with (directory.parent / 'local-go-admission.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        now = time.time() if now is None else now
        rows = []
        for archive in directory.iterdir():
            if archive.is_symlink() or not archive.is_dir():
                continue
            scratch = None
            size = 0
            pending = archive / 'scratch-pending.json'
            if pending.exists():
                try:
                    collect_scratch(json.loads(pending.read_text()), archive)
                except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError):
                    continue  # Protected/unknown scratch is not disposable output.
            info = archive / 'scratch.json'
            if info.exists():
                row = json.loads(info.read_text())
                scratch = Path(row['backup'])
                original = Path(row['original'])
                if (original.parent.name != '.gate' or not original.name.startswith('run.') or
                        scratch != original.parent / 'selfheal-backups' / archive.name or
                        scratch.is_symlink() or scratch.resolve() != scratch):
                    continue
                if scratch.exists():
                    for root, dirs, files in os.walk(scratch, followlinks=False):
                        size += sum((Path(root) / name).lstat().st_size for name in files)
            rows.append((archive.stat().st_mtime, size, archive, scratch))
        total = sum(row[1] for row in rows)
        remaining = len(rows)
        for modified, size, archive, scratch in sorted(rows):
            if now - modified < age and total <= limit and remaining <= count:
                continue
            if scratch is not None and scratch.exists():
                try:
                    owner = json.loads((archive / 'recovery.json').read_text())['owner']
                    if not scratch_idle(owner, scratch):
                        continue
                except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError):
                    continue
                def writable(function, name, error):
                    os.chmod(os.path.dirname(name), 0o700)
                    function(name)
                shutil.rmtree(scratch, onerror=writable)
            shutil.rmtree(archive)
            total -= size
            remaining -= 1


def trim_cache(cache: Path, limit: int = 12 * 1024 ** 3) -> int:
    """Remove oldest rebuildable Go objects, including during concurrent gates.

    Keep timing/compatibility records and module downloads. Go transparently
    rebuilds evicted objects. Never scan or trim another project's normal cache.
    Concurrent writers can grow it again; gate entry and exit enforce the limit
    on the objects in each scan.
    """
    if limit < 0:
        raise ValueError('cache limit must be nonnegative')
    if cache.is_symlink():
        raise ValueError('refusing symlinked gate cache')
    files = []
    for folder in cache.glob('[0-9a-f][0-9a-f]'):
        if folder.is_symlink() or not folder.is_dir():
            continue
        for path in folder.iterdir():
            if not re.fullmatch('[0-9a-f]{64}-[ad]', path.name):
                continue
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue  # Go or another trimmer already removed it.
            if stat.S_ISREG(info.st_mode):
                files.append((info.st_mtime, info.st_size, path))
    total = sum(size for _, size, _ in files)
    for _, size, path in sorted(files):
        if total <= limit:
            break
        path.unlink(missing_ok=True)
        total -= size
    return total


if __name__ == "__main__":
    print("gate build cache retained bytes:", trim_cache(Path(sys.argv[1])))
