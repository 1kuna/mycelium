#!/usr/bin/env python3
"""Bounded macOS dev-runner comparison: nice-10 versus background + nice-10.

Runs two CPU-bound children for one second per phase through an isolated
`myce dev --gpu cpu`. Never contacts the fleet or edits installed profiles.
Each payload self-expires after three seconds, limiting load if the probe fails.
This light canary measures interference, not saturation or a hard CPU quota.
"""

import argparse
import json
import math
import os
import platform
import re
import select
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen


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
    parser.add_argument("--myce", default="./myce", help="built CLI path")
    args = parser.parse_args()
    if platform.system() != "Darwin" or (os.cpu_count() or 1) < 4:
        parser.error("requires macOS with at least four logical CPUs")
    binary = str(Path(args.myce).resolve())
    scratch_root = Path(__file__).resolve().parents[1] / ".gate"
    scratch_root.mkdir(exist_ok=True)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("MYCE_", "GH_", "GITHUB_"))}
    with tempfile.TemporaryDirectory(prefix="background-probe-", dir=scratch_root) as root:
        env["HOME"] = root
        with open(Path(root) / "stderr", "w+") as stderr:
            dev = subprocess.Popen([binary, "dev", "--listen", "127.0.0.1:0",
                                    "--db", str(Path(root) / "state" / "queue.db"),
                                    "--checkpoints", str(Path(root) / "checkpoints"),
                                    "--workers", "1", "--gpu", "cpu", "--host-id", "dev-host"],
                                   cwd=root, env=env, stdout=subprocess.PIPE,
                                   stderr=stderr, text=True)
            try:
                if not select.select([dev.stdout], [], [], 15)[0]:
                    raise RuntimeError("dev startup did not report its address")
                url = re.search(r"http://127\.0\.0\.1:\d+", dev.stdout.readline()).group()

                def api(path, data=None):
                    request = Request(url + path, headers={"Authorization": "Bearer dev-token",
                                                          "Content-Type": "application/json"},
                                      data=None if data is None else json.dumps(data).encode())
                    with urlopen(request, timeout=3) as response:
                        body = response.read()
                        return json.loads(body) if body else None

                def await_value(read, ready):
                    # Bound fixture startup/release waits; no CPU load is added here.
                    end = time.monotonic() + 15
                    while time.monotonic() < end:
                        value = read()
                        if ready(value):
                            return value
                        time.sleep(.02)
                    raise RuntimeError("dev state did not become ready")

                host = "/api/hosts/dev-host/maintenance"
                await_value(lambda: api(host), lambda s: s.get("external_memory") and
                            len(s.get("workers", [])) == 1 and s["workers"][0]["fresh"])
                results = {}
                # A supervisor and two CPU children exercise real fork/exec inheritance.
                burn = "import time; end=time.monotonic()+3\nwhile time.monotonic()<end: pass"
                payload = ("import os,subprocess,sys\n"
                           "os.setpriority(os.PRIO_PROCESS,0,max(10,os.getpriority(os.PRIO_PROCESS,0)))\n"
                           f"children=[subprocess.Popen([sys.executable,'-c',{burn!r}]) for _ in range(2)]\n"
                           "print('probe-ready',flush=True)\n"
                           "for p in children: p.wait()")
                for background in (False, True):
                    if background:
                        api(host, {"action": "drain"})
                        await_value(lambda: api(host), lambda s: s["quiescent"])
                        api(host, {"action": "budget", "cpu_budget_cores": 2, "memory_budget_gb": 2})
                        api(host, {"action": "profile", "shared_host": {"interactive_reserve_gb": .125}})
                        api(host, {"action": "resume"})
                    job = api("/api/jobs", {"name": "background-latency-probe", "project": "dev-probe",
                                         "priority": "experiment", "needs": {"cpu_cores": 2, "memory_gb": .05},
                                         "run": {"command": [sys.executable, "-c", payload]}})
                    path = "/api/jobs/" + job["id"]
                    try:
                        await_value(lambda: api(path + "/logs"),
                                    lambda logs: any("probe-ready" in entry["text"] for entry in logs))
                        results["background" if background else "nice_only"] = sample(1)
                        if api(path)["state"] != "running":
                            raise RuntimeError("CPU job did not remain running during probe")
                    finally:
                        api(path + "/cancel", {})
                        await_value(lambda: api(host), lambda s: not s.get("attempts") and
                                    not s.get("pending_releases") and not s.get("pending_cleanup"))
                print(json.dumps({"os": "Darwin", "logical_cpus": os.cpu_count(),
                                  "runner": "myce dev", "cpu_children": 2, "seconds_per_phase": 1,
                                  "foreground_nice": os.getpriority(os.PRIO_PROCESS, 0),
                                  "cpu_time_cap_qualified": False, **results}, indent=2))
            finally:
                dev.send_signal(signal.SIGINT)
                try:
                    dev.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    dev.kill()
                    dev.wait()
                    raise RuntimeError("dev failed to join; inspect cleanup before retrying")
                dev.stdout.close()
                if dev.returncode:
                    stderr.seek(0)
                    raise RuntimeError("dev exited unsuccessfully: " + stderr.read().replace(root, "[probe-root]"))


if __name__ == "__main__":
    main()
