#!/usr/bin/env python3
"""Small opt-in native interference canary; never saturates the shared host.

Measure foreground 5ms wake lateness and a fixed CPU burst, first idle, then
with two background CPU commands using the same taskpolicy/nice as Mycelium.
This measures interference at that declared concurrency, not a macOS CPU cap.
No services, profile changes, memory stress, or dependencies are involved.
"""

import argparse
import json
import math
import os
import platform
import signal
import subprocess
import time


def sample(seconds):
    wakes, bursts = [], []
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        due = time.perf_counter() + .005
        time.sleep(.005)
        wakes.append(max(0, time.perf_counter() - due) * 1000)
        begin = time.perf_counter()
        sum(i * i for i in range(10000))
        bursts.append((time.perf_counter() - begin) * 1000)
    def stats(values):
        values.sort()
        return {"samples": len(values), "p50_ms": round(values[math.ceil(.5 * len(values)) - 1], 3),
                "p99_ms": round(values[math.ceil(.99 * len(values)) - 1], 3),
                "max_ms": round(values[-1], 3)}
    return {"wake_delay": stats(wakes), "cpu_burst": stats(bursts)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=3)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 10:
        parser.error("use 1..10 seconds per phase")
    cpus = os.cpu_count() or 1
    if cpus < 4:
        parser.error("requires four logical CPUs to leave owner capacity")
    burners = []
    command = ["/usr/bin/nice", "-n", "10", "/bin/sh", "-c", "while :; do :; done"]
    if platform.system() == "Darwin":
        command = ["/usr/sbin/taskpolicy", "-b", *command]
    try:
        baseline = sample(args.seconds)
        for _ in range(2):
            burners.append(subprocess.Popen(command, start_new_session=True,
                                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                             stderr=subprocess.DEVNULL))
        time.sleep(.1)
        if any(p.poll() is not None for p in burners):
            raise RuntimeError("CPU command did not remain running")
        loaded = sample(args.seconds)
        if any(p.poll() is not None for p in burners):
            raise RuntimeError("CPU command exited during measurement")
    finally:
        for proc in burners:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    print(json.dumps({"os": platform.system(), "logical_cpus": cpus,
                      "background_commands": 2, "seconds_per_phase": args.seconds,
                      "foreground_nice": os.getpriority(os.PRIO_PROCESS, 0),
                      "cpu_time_cap_qualified": False, "baseline": baseline,
                      "background_load": loaded}, indent=2))


if __name__ == "__main__":
    main()
