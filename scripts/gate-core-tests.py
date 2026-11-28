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
SHARDS = 4
# These assert a physical mutex hold bound and uninterrupted lease renewal.
# The two-core AMD64 gate showed that concurrent race shards can obscure both.
SERIAL_TESTS = {
    "TestFileSubmissionBatchMutexHold",
    "TestSharedQueueMemberHungStorePromotion",
}


def main(binary):
    started = time.monotonic()
    package_dir = Path(__file__).resolve().parents[1] / "internal/myce"
    inventory = subprocess.check_output([binary, "-test.list=."], cwd=package_dir, text=True)
    roots = [name for name in inventory.splitlines() if re.fullmatch(r"(?:Test|Example|Fuzz)\w*", name)]
    if not roots or len(roots) != len(set(roots)):
        raise ValueError("invalid compiled core test inventory")
    serial = []
    groups = [[] for _ in range(SHARDS)]
    for name in roots:
        if name in SERIAL_TESTS:
            serial.append(name)
        else:
            groups[crc32(name.encode()) % SHARDS].append(name)

    output_lock = Lock()

    def run(group):
        pattern = "^(" + "|".join(map(re.escape, group)) + ")$"
        cmd = ["go", "tool", "test2json", "-t", "-p", PACKAGE, binary,
               "-test.v=test2json", "-test.timeout=1h", "-test.count=1", "-test.parallel=1",
               "-test.run=" + pattern]
        with subprocess.Popen(cmd, cwd=package_dir, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT) as child:
            for line in child.stdout:
                with output_lock:
                    sys.stdout.buffer.write(line)
                    sys.stdout.buffer.flush()
            return child.wait()

    statuses = [run(serial)] if serial else []
    with ThreadPoolExecutor(max_workers=SHARDS) as pool:
        statuses.extend(pool.map(run, (group for group in groups if group)))
    status = int(any(statuses))
    print(f"{PACKAGE} full: {len(roots)} roots, exit={status}, wall={time.monotonic() - started:.3f}s")
    return status


if __name__ == "__main__":
    sys.exit(main(str(Path(sys.argv[1]).resolve())))
