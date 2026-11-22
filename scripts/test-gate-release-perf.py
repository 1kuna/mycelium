#!/usr/bin/env python3
"""Exercise release verdicts without running the timing workloads."""

from importlib.util import module_from_spec, spec_from_file_location
from io import StringIO
from pathlib import Path
from subprocess import CompletedProcess
import unittest
from unittest.mock import patch


spec = spec_from_file_location("gate_release_perf", Path(__file__).with_name("gate-release-perf.py"))
gate = module_from_spec(spec)
spec.loader.exec_module(gate)


def checkin(duration, bound_failed=False, extra_error=""):
    name = gate.CHECKIN
    measurement = ("    status_read_contention_test.go:145: 100 check-ins with 62 "
                   f"concurrent observation readers: p99={duration} max=650ms\n")
    if bound_failed:
        diagnostic = ("    status_read_contention_test.go:31: "
                      f"check-in p99 {duration} exceeds half the 1s heartbeat interval\n")
        output = f"=== RUN   {name}\n--- FAIL: {name}\n{measurement}{extra_error}{diagnostic}FAIL\n"
        return CompletedProcess([], 1, stdout=output)
    output = f"=== RUN   {name}\n{name}-4\t1\t1000 ns/op\n--- BENCH: {name}-4\n{measurement}PASS\n"
    return CompletedProcess([], 0, stdout=output)


def other_benchmarks(production_failed=False):
    retained, fan_out, production = gate.OTHER_BENCHMARKS
    output = f"{retained}-4\t1\t1000 ns/op\n{fan_out}-4\t1\t1000 ns/op\n"
    if production_failed:
        output += f"--- FAIL: {production}\n    prod_test.go:166: production latency 15s exceeds 15s target\nFAIL\n"
        return CompletedProcess([], 1, stdout=output)
    return CompletedProcess([], 0, stdout=output + f"{production}-4\t1\t1000 ns/op\nPASS\n")


class ReleasePerfTests(unittest.TestCase):
    def evaluate(self, results, revision="a" * 40):
        report, log = StringIO(), StringIO()
        with patch.object(gate, "prepare_baseline"), patch.object(gate, "build"), \
                patch.object(gate, "bench", side_effect=results) as bench:
            passed = gate.run_gate(report, log, Path("candidate"), Path("scratch"), revision)
        return passed, report.getvalue(), bench.call_args_list

    def test_paired_candidate_passes_even_when_standalone_and_baseline_miss(self):
        passed, report, calls = self.evaluate([
            checkin("604.494ms", bound_failed=True),
            checkin("550.332ms", bound_failed=True),
            checkin("499.999999ms"),
            other_benchmarks(),
        ])
        self.assertTrue(passed)
        self.assertIn("Standalone check-in (informational): FAIL (604.494 ms)", report)
        self.assertIn("Paired check-in PASS (candidate < 500 ms): "
                      "candidate 499.999999 ms, deployed baseline 550.332 ms", report)
        self.assertEqual([(call.args[2], call.args[-1]) for call in calls], [
            (Path("candidate"), (gate.CHECKIN,)),
            (Path("scratch/baseline"), (gate.CHECKIN,)),
            (Path("candidate"), (gate.CHECKIN,)),
            (Path("candidate"), gate.OTHER_BENCHMARKS),
        ])

    def test_paired_candidate_fails_at_the_bound_even_when_standalone_passes(self):
        passed, report, _ = self.evaluate([
            checkin("252.452ms"), checkin("383.342ms"), checkin("500ms", bound_failed=True),
            other_benchmarks(),
        ])
        self.assertFalse(passed)
        self.assertIn("Paired check-in FAIL (candidate < 500 ms): "
                      "candidate 500 ms, deployed baseline 383.342 ms", report)

    def test_missing_deployed_source_id_fails_before_running_benchmarks(self):
        passed, report, calls = self.evaluate([], revision="")
        self.assertFalse(passed)
        self.assertIn("missing or invalid deployed baseline", report)
        self.assertEqual(calls, [])

    def test_missing_paired_candidate_measurement_fails_even_on_zero_exit(self):
        passed, report, _ = self.evaluate([
            checkin("252.452ms"), checkin("383.342ms"), CompletedProcess([], 0, stdout="PASS\n"),
            other_benchmarks(),
        ])
        self.assertFalse(passed)
        self.assertIn("candidate ERROR (expected one check-in p99 measurement", report)

    def test_paired_baseline_workload_error_cannot_count_as_a_latency_miss(self):
        baseline = checkin("550.332ms", bound_failed=True, extra_error=(
            "    status_read_contention_test.go:141: observation load interrupted the production sentinel\n"))
        passed, report, _ = self.evaluate([
            checkin("252.452ms"), baseline, checkin("267.802ms"), other_benchmarks(),
        ])
        self.assertFalse(passed)
        self.assertIn("deployed baseline ERROR (check-in benchmark errored (exit 1)", report)

    def test_production_start_still_binds(self):
        passed, report, _ = self.evaluate([
            checkin("252.452ms"), checkin("383.342ms"), checkin("267.802ms"),
            other_benchmarks(production_failed=True),
        ])
        self.assertFalse(passed)
        self.assertIn("Paired check-in PASS", report)
        self.assertIn("BenchmarkDevProductionStart FAIL", report)


if __name__ == "__main__":
    unittest.main()
