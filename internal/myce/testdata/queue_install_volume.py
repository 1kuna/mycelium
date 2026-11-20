#!/usr/bin/env python3
"""Seed measured aggregate volumes into a new, synthetic fleet-test database.

No production data or inputs. Run only from TestQueueInstalledCutover after its
writer closes. Chunked commits/checkpoints keep the temporary WAL bounded.
"""
import base64
import json
import math
import os
import re
import sqlite3
import statistics
import sys
import time

assert os.environ.get("MYCE_TEST_DATABASE_VOLUME") == "1"
assert os.environ.get("MYCE_JOB_ID") and sys.platform == "linux"
assert not os.environ.get("MYCE_TOKEN")
db = sqlite3.connect(sys.argv[1])
assert db.execute("SELECT data FROM queue WHERE id=1").fetchone()[0] in (
    b'"myce-normalized-state-v3"', '"myce-normalized-state-v3"'
)


def decode(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).decode()


job_sizes = {}
families = {}
for key, size in db.execute("SELECT key,length(data) FROM queue_state"):
    logical = decode(key.split("/")[1]) if key.startswith("fields/") else key
    family, _, identity = logical.partition("/")
    if family == "jobs":
        assert re.fullmatch(r"(?:retained|queued|running)-[0-9]+", identity)
        job_sizes[identity] = job_sizes.get(identity, 0) + size
    if family == "machines":
        assert re.fullmatch(r"worker-[0-9]+", identity)
    record = families.setdefault(family, {"rows": 0, "bytes": 0})
    record["rows"] += 1
    record["bytes"] += size
assert len(job_sizes) == 12192
assert db.execute("SELECT count(*) FROM logs").fetchone()[0] == 0
fresh = sorted(j for j in job_sizes if j.startswith("retained-") and int(j.split("-")[1]) % 2)
assert len(fresh) >= 4103
db.execute("CREATE TEMP TABLE volume_jobs(rank INTEGER PRIMARY KEY, job TEXT)")
db.executemany("INSERT INTO volume_jobs VALUES(?,?)", enumerate(fresh[:4103]))
db.commit()


def chunks(total, statement, args=(), batch=8192):
    for start in range(0, total, batch):
        stop = min(total, start + batch)
        db.execute(
            "WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i+1<?) " + statement,
            (start, stop, *args),
        )
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()


log_rows, log_bytes = 655178, 3440339990
width, extra = divmod(log_bytes, log_rows)
text = ("synthetic-volume-log " * (width // 21 + 2))[:width + 1]
assert len(text) == width + 1
chunks(log_rows, "INSERT INTO logs(job,attempt,text) "
       "SELECT j.job,1,substr(?,1,?+(n.i<?)) FROM n JOIN volume_jobs j "
       "ON j.rank=CASE WHEN n.i<10037 THEN 0 ELSE 1+(n.i-10037)%4102 END",
       (text, width, extra))
chunks(653585, "INSERT INTO log_offsets(job,attempt,offset) "
       "SELECT logs.job,logs.attempt,logs.seq FROM n JOIN logs ON logs.seq=n.i+1")
minute = int(time.time()) // 60 * 60
# Replace only this newly generated fixture's usage with the aggregate target.
db.execute("DELETE FROM usage_samples")
# Historical rows need not refer to still-retained jobs. Seven distinct machine
# keys avoid primary-key collisions per minute; keep the real usage schema.
chunks(369176, "INSERT INTO usage_samples "
       "SELECT ?-(i/7)*60,60,'synthetic-usage-'||(i%7),'','','','',0,0,0,0,0 FROM n",
       (minute,))

chunks(1557, "INSERT INTO environment_build_logs(machine,session,key,offset,text) "
       "SELECT 'worker-0','synthetic-history','volume-'||i,i,substr(?,1,?+(i<?)) FROM n",
       ("e" * (7837066 // 1557 + 1), 7837066 // 1557, 7837066 % 1557))
chunks(3473, "INSERT INTO job_summaries(id,finished,summary,learned) "
       "SELECT 'synthetic-summary-'||i,?, ?, '{}' FROM n",
       ((minute - 15 * 86400) * 1000000000, json.dumps({"state": "succeeded", "project": "synthetic", "name": "volume"})))

# Allocate and release real SQLite pages; a sparse extension of the file would
# not reproduce a freelist. No VACUUM, host cache purge or shared state changes.
db.execute("PRAGMA secure_delete=OFF")
db.execute("CREATE TABLE volume_free_pages(data BLOB)")
target_free = 955022
existing_free = db.execute("PRAGMA freelist_count").fetchone()[0]
to_add = math.ceil(target_free / 16) if existing_free < target_free else 0
chunks(to_add, "INSERT INTO volume_free_pages SELECT zeroblob(65436) FROM n", batch=256)
db.execute("DROP TABLE volume_free_pages")
db.commit()
db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()

sizes = sorted(job_sizes.values())
counts = {table: db.execute("SELECT count(*) FROM " + table).fetchone()[0]
          for table in ("queue_state", "logs", "log_offsets", "usage_samples", "job_summaries", "environment_build_logs")}
actual_logs = db.execute("SELECT count(*),sum(length(text)),count(distinct job) FROM logs").fetchone()
assert actual_logs == (log_rows, log_bytes, 4103), actual_logs
assert counts["usage_samples"] == 369176, counts
print(json.dumps({"tables": counts, "log_bytes": actual_logs[1], "log_jobs": actual_logs[2],
                  "page_size": db.execute("PRAGMA page_size").fetchone()[0],
                  "page_count": db.execute("PRAGMA page_count").fetchone()[0],
                  "freelist_count": db.execute("PRAGMA freelist_count").fetchone()[0],
                  "queue_state_families": families,
                  "job_bytes": {"n": len(sizes), "sum": sum(sizes), "p50": statistics.median(sizes),
                                "p95": sizes[int(len(sizes) * .95)], "max": max(sizes)}}, sort_keys=True))
db.close()
