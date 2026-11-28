#!/usr/bin/env python3
"""Run the full core race inventory in isolated, concurrent test processes."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import subprocess
import sys
from threading import Lock
import time
from zlib import crc32


PACKAGE = "mycelium/internal/myce"
WORKERS = 4
CHUNKS = 4 * WORKERS
TIMEOUT_SECONDS = 3600
# These assert a physical mutex hold bound and uninterrupted lease renewal.
# The two-core AMD64 gate showed that concurrent race shards can obscure both.
SERIAL_TESTS = {
    "TestFileSubmissionBatchMutexHold",
    "TestSharedQueueMemberHungStorePromotion",
}
# Start the longest measured roots first. A process that finishes can pick up
# another group while a slow root is still running.
LONG_TESTS = (
    "TestNativeManagedInputBindings",
    "TestLaunchGateDecisionEquivalence",
    "TestCheckinWaitingPlacementEquivalence",
    "TestSmallOutputPublicationThroughput",
    "TestQueueHistoryBurst",
    "TestQueueSQLiteIdleSessionSurvivesUncleanRestart",
    "TestQueueSQLiteCustodySurvivesUncleanRestart",
    "TestSharedQueuePostgresChangeLogStaysBounded",
)


def main(binary):
    started = time.monotonic()
    deadline = started + TIMEOUT_SECONDS
    package_dir = Path(__file__).resolve().parents[1] / "internal/myce"
    inventory = subprocess.check_output([binary, "-test.list=."], cwd=package_dir, text=True)
    roots = [name for name in inventory.splitlines() if re.fullmatch(r"(?:Test|Example|Fuzz)\w*", name)]
    if not roots or len(roots) != len(set(roots)):
        raise ValueError("invalid compiled core test inventory")
    serial = [name for name in roots if name in SERIAL_TESTS]
    parallel_roots = set(roots) - SERIAL_TESTS
    early = [[name] for name in LONG_TESTS if name in parallel_roots]
    chunks = [[] for _ in range(CHUNKS)]
    for name in roots:
        if name in parallel_roots and name not in LONG_TESTS:
            chunks[crc32(name.encode()) % CHUNKS].append(name)
    groups = early + [chunk for chunk in chunks if chunk]

    output_lock = Lock()

    def run(group):
        remaining_ms = int((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            with output_lock:
                print(f"{PACKAGE} timed out before starting {len(group)} roots", file=sys.stderr)
            return 1
        pattern = "^(" + "|".join(map(re.escape, group)) + ")$"
        cmd = ["go", "tool", "test2json", "-t", "-p", PACKAGE, binary,
               "-test.v=test2json", f"-test.timeout={remaining_ms}ms", "-test.count=1", "-test.parallel=1",
               "-test.run=" + pattern]
        with subprocess.Popen(cmd, cwd=package_dir, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT) as child:
            for line in child.stdout:
                with output_lock:
                    sys.stdout.buffer.write(line)
                    sys.stdout.buffer.flush()
            return child.wait()

    statuses = [run(serial)] if serial else []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        statuses.extend(pool.map(run, groups))
    status = int(any(statuses))
    print(f"{PACKAGE} full: {len(roots)} roots, exit={status}, wall={time.monotonic() - started:.3f}s")
    return status


if __name__ == "__main__":
    sys.exit(main(str(Path(sys.argv[1]).resolve())))
