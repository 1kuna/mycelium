#!/usr/bin/env python3
"""Run and report the release performance gate inside one worker assignment."""

import argparse
from decimal import Decimal
from pathlib import Path
import re
import subprocess
import tempfile


CHECKIN = "BenchmarkStatusReadCheckinContention"
CHECKIN_LIMIT_MS = 500
OTHER_BENCHMARKS = (
    "BenchmarkCheckinRetainedHistoryContention",
    "BenchmarkDevFanOutBurst62",
    "BenchmarkDevProductionStart",
)
PUBLIC_REPO = "https://github.com/1kuna/mycelium.git"


def logged_run(log, label, args, cwd):
    print(f"=== {label} ===", file=log, flush=True)
    result = subprocess.run(args, cwd=cwd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, errors="replace")
    print(result.stdout, end="", file=log)
    print(f"exit={result.returncode}", file=log, flush=True)
    return result


def prepare_baseline(log, revision, source):
    source.mkdir()
    for args in (
        ["git", "init", "-q"],
        ["git", "fetch", "--depth=1", "--no-tags", PUBLIC_REPO, revision],
        ["git", "checkout", "-q", "--detach", "FETCH_HEAD"],
    ):
        if logged_run(log, "deployed baseline source", args, source).returncode:
            raise RuntimeError("could not fetch or check out the deployed baseline")
    actual = logged_run(log, "deployed baseline identity", ["git", "rev-parse", "HEAD"], source)
    if actual.returncode or actual.stdout.strip() != revision:
        raise RuntimeError("deployed baseline source identity mismatch")


def build(log, label, source, binary):
    result = logged_run(log, label, ["go", "test", "-c", "-o", str(binary), "./internal/myce"], source)
    if result.returncode:
        raise RuntimeError(f"{label} failed (exit {result.returncode})")


def bench(log, label, source, binary, names):
    pattern = "^(" + "|".join(names) + ")$"
    return logged_run(log, label, [str(binary), "-test.run=^$", f"-test.bench={pattern}",
                                  "-test.benchtime=1x", "-test.count=1",
                                  "-test.timeout=30m", "-test.v"], source / "internal/myce")


def benchmark_row(name, output):
    return re.search(rf"^{name}(?:-\d+)?\s+1\s+\S", output, re.MULTILINE)


def duration_ms(value):
    # Go durations can combine units, e.g. 1m1.2s, and preserve nanoseconds.
    units = {"ns": Decimal("0.000001"), "µs": Decimal("0.001"), "ms": 1,
             "s": 1000, "m": 60000, "h": 3600000}
    pattern = r"(\d+(?:\.\d+)?)(ns|µs|ms|s|m|h)"
    if not re.fullmatch(rf"(?:{pattern})+", value):
        raise ValueError(f"invalid check-in p99 duration {value!r}")
    return sum(Decimal(amount) * units[unit] for amount, unit in re.findall(pattern, value))


def checkin_ms(result):
    # Go omits the benchmark metric row on failure. The fixture logs the exact
    # duration on both outcomes, including at the 500 ms boundary.
    output = result.stdout
    measurement = r"status_read_contention_test\.go:\d+: 100 check-ins with 62 concurrent observation readers: p99=(\S+) max=\S+"
    values = re.findall(rf"^\s+{measurement}$", output, re.MULTILINE)
    if len(values) != 1:
        raise ValueError(f"expected one check-in p99 measurement (exit {result.returncode})")
    value, = values
    ms = duration_ms(value)
    if result.returncode == 0 and output.rstrip().endswith("\nPASS") and benchmark_row(CHECKIN, output):
        return ms

    # A baseline miss is usable. Only accept the benchmark's explicit latency
    # error and reject other fixture, process or cleanup failures.
    failure = rf"status_read_contention_test\.go:\d+: check-in p99 {re.escape(value)} exceeds half the 1s heartbeat interval"
    bound_only = rf"^--- FAIL: {CHECKIN}\n[ \t]+{measurement}\n[ \t]+{failure}\nFAIL\s*\Z"
    if result.returncode == 1 and ms >= CHECKIN_LIMIT_MS and re.search(bound_only, output, re.MULTILINE):
        return ms
    raise ValueError(f"check-in benchmark errored (exit {result.returncode}); see release-perf.log")


def checkin_leg(log, label, source, binary):
    try:
        value = checkin_ms(bench(log, label, source, binary, (CHECKIN,)))
    except (OSError, ValueError) as exc:
        return None, f"ERROR ({exc})"
    return value, f"{value:f} ms"


def run_gate(report, log, candidate, scratch, revision):
    if not re.fullmatch("[0-9a-f]{40}", revision):
        print("Paired check-in FAIL (missing or invalid deployed baseline; supply its full commit SHA)", file=report)
        return False
    print(f"Deployed baseline: {revision}", file=report)
    baseline = scratch / "baseline"
    baseline_binary = scratch / "baseline.test"
    candidate_binary = scratch / "candidate.test"
    try:
        prepare_baseline(log, revision, baseline)
        build(log, "deployed baseline build", baseline, baseline_binary)
        build(log, "candidate build", candidate, candidate_binary)
    except (OSError, RuntimeError) as exc:
        print(f"Paired check-in FAIL (setup failed: {exc}); see release-perf.log", file=report)
        return False

    standalone, standalone_detail = checkin_leg(
        log, "candidate standalone (informational)", candidate, candidate_binary)
    if standalone is not None:
        status = "PASS" if standalone < CHECKIN_LIMIT_MS else "FAIL"
        standalone_detail = f"{status} ({standalone_detail})"
    print(f"Standalone check-in (informational): {standalone_detail}", file=report)

    deployed, deployed_detail = checkin_leg(log, "paired deployed baseline", baseline, baseline_binary)
    paired, paired_detail = checkin_leg(log, "paired candidate", candidate, candidate_binary)
    paired_pass = deployed is not None and paired is not None and paired < CHECKIN_LIMIT_MS
    print(f"Paired check-in {'PASS' if paired_pass else 'FAIL'} (candidate < {CHECKIN_LIMIT_MS} ms): "
          f"candidate {paired_detail}, deployed baseline {deployed_detail}", file=report)

    # These remain the candidate's standalone benchmarks, with their Go
    # assertions (including the production start bound) deciding the result.
    result = bench(log, "candidate release benchmarks", candidate, candidate_binary, OTHER_BENCHMARKS)
    others_pass = result.returncode == 0
    for name in OTHER_BENCHMARKS:
        passed = benchmark_row(name, result.stdout) and not re.search(rf"^--- FAIL: {name}\b", result.stdout, re.MULTILINE)
        print(f"{name} {'PASS' if passed else 'FAIL'}", file=report)
        others_pass = others_pass and bool(passed)
    if result.returncode:
        print(f"Candidate release benchmarks exited {result.returncode}; see release-perf.log", file=report)
    return paired_pass and others_pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="", help="full commit SHA of the deployed release")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="release-perf-") as scratch, \
            open("release-perf.txt", "w") as report, open("release-perf.log", "w") as log:
        try:
            passed = run_gate(report, log, Path.cwd(), Path(scratch), args.baseline)
        except OSError as exc:
            print(f"Release performance gate errored: {exc}", file=report)
            passed = False
        print("PASS" if passed else "FAIL", file=report)
    print(Path("release-perf.txt").read_text(), end="")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
