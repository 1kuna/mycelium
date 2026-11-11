#!/usr/bin/env python3
"""Bound the user's default Go build cache so direct `go test` runs cannot starve gates.

usage: scripts/gate-default-cache.py [--dry-run]

Workers' direct `go test` runs use the default GOCACHE, which nothing else trims.
On 2026-11-11 it grew to 51 GB (about 10 GB/h) and pushed the host below the
local gate's disk entry floor, blocking every gate. The local gate calls
trim_default_cache at entry and exit, beside its own 12 GiB trim_cache.

Budget: keep free disk at least the gate entry floor plus one hour of this
cache's measured writes, so an hour of ordinary work cannot push the next gate
under the floor. Only the shortfall is evicted, oldest first; with enough free
disk nothing is removed. Go refreshes an entry's mtime on use at most once per
hour (cmd/go/internal/cache mtimeInterval), so entries younger than that interval
plus five minutes are never removed: anything used in the last five minutes is
among them. Go rebuilds an evicted entry on its next miss.
"""
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import time

GIB = 1024 ** 3
GO_MTIME_INTERVAL = 60 * 60
IN_USE = GO_MTIME_INTERVAL + 5 * 60
ENTRY = re.compile('[0-9a-f]{64}-[ad]')


def entries(cache):
    found = []
    for folder in cache.glob('[0-9a-f][0-9a-f]'):
        if folder.is_symlink() or not folder.is_dir():
            continue
        for path in folder.iterdir():
            if not ENTRY.fullmatch(path.name):
                continue
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue  # Go or another trimmer removed it.
            if stat.S_ISREG(info.st_mode):
                found.append((info.st_mtime, info.st_size, path))
    return found


def plan(found, free, floor, now):
    """Return (budget, oldest-first removals); never selects in-use entries."""
    total = sum(size for _, size, _ in found)
    growth = sum(size for modified, size, _ in found if now - modified < GO_MTIME_INTERVAL)
    shortfall = max(0, floor + growth - free)
    budget = max(0, total - shortfall)
    removals, retained = [], total
    for modified, size, path in sorted(found):
        if retained <= budget or now - modified < IN_USE:
            break  # Sorted by age: every later entry is newer still.
        removals.append((size, path))
        retained -= size
    return budget, removals


def default_cache():
    """Go's GOCACHE resolution: environment, `go env -w` file, then the user cache dir."""
    home = Path.home()
    darwin = sys.platform == 'darwin'
    value = os.environ.get('GOCACHE')
    if value is None:
        config = os.environ.get('GOENV') or str(
            (home / 'Library/Application Support' if darwin else
             Path(os.environ.get('XDG_CONFIG_HOME') or home / '.config')) / 'go/env')
        if config != 'off' and Path(config).is_file():
            for line in Path(config).read_text().splitlines():
                if line.startswith('GOCACHE='):
                    value = line.split('=', 1)[1].strip()
    if value is None:
        base = home / 'Library/Caches' if darwin else Path(os.environ.get('XDG_CACHE_HOME') or home / '.cache')
        value = str(base / 'go-build')
    return Path(value) if value and value != 'off' else None


def trim_default_cache(floor, disk_root, *, skip=(), dry_run=False, now=None, cache=None):
    """Trim the default cache toward its derived budget; warn, never fail a gate."""
    try:
        cache = cache or default_cache()
        if cache is None or not cache.is_dir() or cache.is_symlink():
            return None
        if any(cache.resolve() == Path(other).resolve() for other in skip):
            return None  # The gate's own cache has its own fixed trim.
        if cache.stat().st_dev != Path(disk_root).stat().st_dev:
            return None  # Evicting another filesystem cannot restore the floor.
        free = shutil.disk_usage(disk_root).free
        found = entries(cache)
        budget, removals = plan(found, free, floor, time.time() if now is None else now)
        removed = 0
        for size, path in removals:
            if not dry_run:
                path.unlink(missing_ok=True)
            removed += size
        if removals:
            total = sum(size for _, size, _ in found)
            verb = 'would trim' if dry_run else 'trimmed'
            print(f'gate: default Go cache {verb} {removed / GIB:.1f} of {total / GIB:.1f} GiB '
                  f'(budget {budget / GIB:.1f} GiB; free {free / GIB:.1f} GiB, floor {floor / GIB:.0f} GiB)',
                  file=sys.stderr, flush=True)
        return budget, removed
    except (OSError, ValueError) as error:
        print('WARNING gate: default Go cache trim skipped: ' + str(error), file=sys.stderr, flush=True)
        return None


if __name__ == '__main__':
    import importlib.util
    spec = importlib.util.spec_from_file_location('capacity', Path(__file__).with_name('gate-capacity.py'))
    capacity = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(capacity)
    result = trim_default_cache(capacity.ENTRY_FLOOR, Path.home(), dry_run='--dry-run' in sys.argv)
    print('default Go cache: ' + ('skipped' if result is None else
          f'budget {result[0] / GIB:.1f} GiB, removed {result[1] / GIB:.1f} GiB'))
