#!/usr/bin/env python3
"""Owned synthetic DB only: scatter state pages and evict that file's cache.

No host cache purge, production access, VACUUM, root or artificial I/O delay.
"""
import ctypes
import hashlib
import json
import mmap
import os
import sqlite3
import sys
import time

assert sys.platform == "linux" and os.environ.get("MYCE_JOB_ID")
assert not os.environ.get("MYCE_TOKEN")
action, path = sys.argv[1:]


def resident(fd):
    length = os.fstat(fd).st_size
    with mmap.mmap(fd, length, access=mmap.ACCESS_COPY) as mapping:
        buffer = (ctypes.c_char * length).from_buffer(mapping)
        pages = (length + mmap.PAGESIZE - 1) // mmap.PAGESIZE
        flags = (ctypes.c_ubyte * pages)()
        libc = ctypes.CDLL(None, use_errno=True)
        code = libc.mincore(ctypes.c_void_p(ctypes.addressof(buffer)), ctypes.c_size_t(length), flags)
        del buffer
        assert code == 0, ctypes.get_errno()
        return sum(value & 1 for value in flags), pages


if action == "evict":
    fd = os.open(path, os.O_RDWR)
    try:
        os.fdatasync(fd)
        before = resident(fd)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        after = resident(fd)
        print(json.dumps({"resident_before": before, "resident_after": after}))
        assert after[0] < after[1] // 100, "owned file cache did not become cold"
    finally:
        os.close(fd)
    raise SystemExit(0)

db = sqlite3.connect(path)
assert db.execute("SELECT count(*) FROM queue_state").fetchone()[0] > 12000
started = time.monotonic()
if action == "fragmented":
    # Existing state stays intact until the copy transaction commits. Interleave
    # shuffled state batches with disposable blocks throughout the old freelist.
    # Dropping the blocks leaves holes between live state pages, unlike merely
    # appending/deleting one large table at the end of the file.
    db.execute("PRAGMA secure_delete=OFF")
    db.execute("ALTER TABLE queue_state RENAME TO physical_saved_state")
    db.execute("CREATE TABLE queue_state(key TEXT PRIMARY KEY,data BLOB NOT NULL)")
    db.execute("CREATE TABLE physical_spacing(data BLOB)")
    rows = db.execute("SELECT key,data FROM physical_saved_state").fetchall()
    rows.sort(key=lambda row: hashlib.sha256(row[0].encode()).digest())
    free_bytes = db.execute("PRAGMA freelist_count").fetchone()[0] * 4096
    payload = sum(len(data) + len(key) for key, data in rows)
    batches = (len(rows) + 31) // 32
    spacing = max(4096, int((free_bytes - 2 * payload) * .85 / batches))
    for start in range(0, len(rows), 32):
        db.executemany("INSERT INTO queue_state VALUES(?,?)", rows[start:start + 32])
        db.execute("INSERT INTO physical_spacing VALUES(zeroblob(?))", (spacing,))
        if start % 8192 == 0:
            db.commit()
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert db.execute("SELECT count(*) FROM (SELECT * FROM physical_saved_state EXCEPT SELECT * FROM queue_state)").fetchone()[0] == 0
    db.execute("DROP TABLE physical_saved_state")
    db.execute("DROP TABLE physical_spacing")
    db.commit()
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
else:
    assert action == "compact"

pages = list(db.execute("SELECT pageno,pagetype FROM dbstat WHERE name='queue_state' ORDER BY path"))
jumps = [abs(a[0] - b[0]) for a, b in zip(pages, pages[1:])]
print(json.dumps({"layout": action, "prepare_seconds": time.monotonic() - started,
                  "query_plan": list(db.execute("EXPLAIN QUERY PLAN SELECT key,data FROM queue_state")),
                  "page_count": db.execute("PRAGMA page_count").fetchone()[0],
                  "freelist_count": db.execute("PRAGMA freelist_count").fetchone()[0],
                  "state_pages": len(pages), "state_min_page": min(p[0] for p in pages),
                  "state_max_page": max(p[0] for p in pages),
                  "nonadjacent_page_steps": sum(step != 1 for step in jumps),
                  "mean_page_step": sum(jumps) / len(jumps)}))
db.close()
