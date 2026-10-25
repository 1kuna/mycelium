#!/usr/bin/env python3
"""Paired #175 qualification, using slice 4's three parallel seeds/runtime.

Each seed runs the same fixed scenarios and the existing seven-day mixed load.
Synthetic assumptions and complete per-piece timing/coverage remain in JSON.
The homogeneous no-overlap control is explicitly excluded from speed claims.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--binary", default="./myce")
parser.add_argument("--out", default=".reports/175/sim")
args = parser.parse_args()
folder = Path(args.out)
folder.mkdir(parents=True, exist_ok=True)
binary = str(Path(args.binary).resolve())
head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
paths = [Path(binary), Path("examples/sim/fleet.yaml"), Path("examples/sim/declared-fan-out/workload.yaml")]
manifest = {"source": head, "seeds": [1, 2, 3], "runtime": {"GOMAXPROCS": "1", "GOGC": "400"},
            "sha256": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}, "started_at": time.time()}
(folder / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
# Preserve original mixed-load arrivals/policy/curves exactly.
# The CLI constructs fixed scenario assumptions; this harness uses only stdlib.
env = dict(os.environ, **manifest["runtime"])
scenarios = ["homogeneous", "heterogeneous-tail", "separate-stage", "shared-stage", "mixed-production"]

def seed_run(seed):
    rows = []
    for scenario in scenarios:
        result = folder / f"{scenario}-seed{seed}.json"
        log = folder / f"{scenario}-seed{seed}.log"
        with log.open("w") as output:
            status = subprocess.call([binary, "sim", "--pipeline-scenario", scenario, "--seed", str(seed), "--json", str(result)],
                                     env=env, stdout=output, stderr=subprocess.STDOUT)
        row = {"scenario": scenario, "seed": seed, "exit_code": status}
        if result.exists():
            r = json.loads(result.read_text())
            a, b = r["gather_then_stage"], r["per_piece"]
            row.update({k: r[k] for k in ["gate_passed", "complete_output_coverage", "earlier_downstream_start", "mean_makespan_gain_seconds", "worst_pipeline_regression_seconds", "production_p95_delta_seconds", "worst_producer_gather_slowdown_seconds"]})
            row.update({"production_p95_baseline": a["production_wait"]["p95_seconds"], "production_p95_per_piece": b["production_wait"]["p95_seconds"],
                        "failures_baseline": a["failed"], "failures_per_piece": b["failed"], "unfinished_baseline": a["unfinished"], "unfinished_per_piece": b["unfinished"]})
            # Equal timings are expected when all pieces accept jointly.
            control = scenario == "homogeneous"
            row["passed"] = status == 0 and (r["gate_passed"] if not control else r["complete_output_coverage"] and r["mean_makespan_gain_seconds"] == 0 and r["production_p95_delta_seconds"] == 0 and a["failed"] == b["failed"] == a["unfinished"] == b["unfinished"] == 0)
        else:
            row["passed"] = False
        rows.append(row)
    return rows

with ThreadPoolExecutor(max_workers=3) as executor:
    rows = [row for result in executor.map(seed_run, [1, 2, 3]) for row in result]
summary = {"source": head, "rows": rows, "passed": all(r["passed"] for r in rows), "wall_seconds": time.time() - manifest["started_at"]}
(folder / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
sys.exit(0 if summary["passed"] else 1)
