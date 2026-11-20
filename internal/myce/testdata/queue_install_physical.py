#!/usr/bin/env python3
"""Owned synthetic DB only: scatter state pages and evict that file's cache.

No host cache purge, production access, VACUUM, root or artificial I/O delay.
"""
import ctypes
import hashlib
import json
import mmap
import os
import random
import re
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
    # A monotonically increasing rowid still visits recently allocated pages in
    # order even if keys were shuffled. Random rowids force B-tree splits and
    # nonlocal traversal. Interleave regenerated synthetic logs, state and holes
    # over the whole existing file, not just its tail freelist.
    rows = db.execute("SELECT key,data FROM queue_state").fetchall()
    jobs = [row[0] for row in db.execute("SELECT DISTINCT job FROM logs ORDER BY job")]
    assert len(jobs) == 4103 and all(re.fullmatch(r"retained-[0-9]+", job) for job in jobs)
    log_schema = db.execute("SELECT sql FROM sqlite_master WHERE name='logs'").fetchone()[0]
    log_rows, log_bytes = db.execute("SELECT count(*),sum(length(text)) FROM logs").fetchone()
    assert (log_rows, log_bytes) == (655178, 3440339990)
    width, extra = divmod(log_bytes, log_rows)
    text = ("synthetic-volume-log " * (width // 21 + 2))[:width + 1]
    def digest(items):
        result = hashlib.sha256()
        for key, data in sorted(items):
            key = key.encode()
            for part in (key, data):
                result.update(len(part).to_bytes(8, 'big'))
                result.update(part)
        return result.hexdigest()
    expected = digest(rows)
    page_size = db.execute("PRAGMA page_size").fetchone()[0]
    free_bytes = db.execute("PRAGMA freelist_count").fetchone()[0] * page_size
    db.execute("PRAGMA secure_delete=OFF")
    db.execute("DROP TABLE queue_state")
    db.execute("DROP TABLE logs")
    db.execute("CREATE TABLE queue_state(key TEXT PRIMARY KEY,data BLOB NOT NULL)")
    db.execute(log_schema)
    db.execute("CREATE TABLE physical_spacing(data BLOB)")
    db.execute("CREATE TEMP TABLE physical_jobs(rank INTEGER PRIMARY KEY,job TEXT)")
    db.executemany("INSERT INTO physical_jobs VALUES(?,?)", enumerate(jobs))
    rows.sort(key=lambda row: hashlib.sha256(row[0].encode()).digest())
    rowids = list(range(1, len(rows) + 1))
    random.Random(1742).shuffle(rowids)
    batch_size = 8
    batches = (len(rows) + batch_size - 1) // batch_size
    # Leave room for less densely packed state leaves produced by random splits.
    spacing = max(page_size, (free_bytes - 128 * 1024**2) // batches)
    log_start = 0
    for batch, start in enumerate(range(0, len(rows), batch_size)):
        db.executemany("INSERT INTO queue_state(rowid,key,data) VALUES(?,?,?)",
                       [(rowids[i], *rows[i]) for i in range(start, min(start + batch_size, len(rows)))])
        log_end = log_rows * (batch + 1) // batches
        db.execute("WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i+1<?) "
                   "INSERT INTO logs(seq,job,attempt,text) "
                   "SELECT n.i+1,j.job,1,substr(?,1,?+(n.i<?)) FROM n JOIN physical_jobs j "
                   "ON j.rank=CASE WHEN n.i<10037 THEN 0 ELSE 1+(n.i-10037)%4102 END",
                   (log_start, log_end, text, width, extra))
        log_start = log_end
        db.execute("INSERT INTO physical_spacing VALUES(zeroblob(?))", (spacing,))
        if batch % 128 == 0:
            db.commit()
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert digest(db.execute("SELECT key,data FROM queue_state")) == expected
    assert db.execute("SELECT count(*),sum(length(text)) FROM logs").fetchone() == (log_rows, log_bytes)
    db.execute("DROP TABLE physical_spacing")
    db.execute("CREATE INDEX logs_job ON logs(job,seq)")
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
                  "state_pages": len(pages), "page_types": {kind: sum(p[1] == kind for p in pages) for kind in ("internal", "leaf", "overflow")}, "state_min_page": min(p[0] for p in pages),
                  "state_max_page": max(p[0] for p in pages),
                  "nonadjacent_page_steps": sum(step != 1 for step in jumps),
                  "mean_page_step": sum(jumps) / len(jumps)}))
db.close()
