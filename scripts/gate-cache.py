#!/usr/bin/env python3
"""Evict only idle gate-owned Go objects; preserve modules and timing evidence."""
import re
from pathlib import Path
import sys


def trim_cache(cache, limit=12 * 1024 ** 3):
    """Only gate-owned Go build objects; caller verifies idle under admission lock.

    Keep timing/compatibility records and module downloads. Go transparently
    rebuilds evicted objects. Never scan or trim another project's normal cache.
    """
    files = []
    for folder in cache.glob('[0-9a-f][0-9a-f]'):
        for path in folder.iterdir():
            if re.fullmatch('[0-9a-f]{64}-[ad]', path.name) and not path.is_symlink():
                stat = path.stat()
                files.append((stat.st_mtime, stat.st_size, path))
    total = sum(size for _, size, _ in files)
    for _, size, path in sorted(files):
        if total <= limit:
            break
        path.unlink()
        total -= size
    return total


if __name__ == "__main__":
    print("gate build cache retained bytes:", trim_cache(Path(sys.argv[1])))
