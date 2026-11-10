#!/usr/bin/env python3
"""Bound gate-owned Go objects; preserve modules and timing evidence."""
import re
from pathlib import Path
import stat
import sys


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
