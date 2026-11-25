#!/usr/bin/env python3
"""Trim Go's shared build cache to a size cap, least recently used first.

Parallel worktrees and race builds can grow GOCACHE by tens of GB in hours,
and on a host with local snapshots deleted bytes stay pinned for hours. Go
refreshes an entry's mtime when it is used (at most hourly), so mtime order
approximates recency. Entries newer than the grace period are never removed,
so a concurrent build keeps what it just wrote.
"""
import os
import sys
import time


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: trim-go-cache.py GOCACHE", file=sys.stderr)
        return 2
    root = sys.argv[1]
    cap = int(float(os.environ.get("GATE_GOCACHE_CAP_GB", "15")) * (1 << 30))
    grace = float(os.environ.get("GATE_GOCACHE_GRACE_SECONDS", "600"))
    # Go creates the 256 two-hex shard directories 00..ff. Only those shards are
    # trimmed; other directories under GOCACHE (tool state) are left alone.
    if not all(os.path.isdir(os.path.join(root, s)) for s in ("00", "7f", "ff")):
        return 0  # Not a Go build cache; touch nothing.
    entries, total = [], 0
    for name in os.listdir(root):
        sub = os.path.join(root, name)
        if len(name) != 2 or not os.path.isdir(sub):
            continue
        with os.scandir(sub) as it:
            for e in it:
                try:
                    st = e.stat(follow_symlinks=False)
                except OSError:
                    continue
                if e.is_file(follow_symlinks=False):
                    entries.append((st.st_mtime, st.st_size, e.path))
                    total += st.st_size
    if total <= cap:
        return 0
    cutoff = time.time() - grace
    entries.sort()
    freed = 0
    for mtime, size, path in entries:
        if total - freed <= cap or mtime > cutoff:
            break
        try:
            os.remove(path)
            freed += size
        except OSError:
            pass
    print(f"go cache trimmed {freed >> 20} MiB; {(total - freed) >> 20} MiB remain (cap {cap >> 20} MiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
