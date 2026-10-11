from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from pressure import Checker, digest, drm_owners, emit, replay, validate_save


def event(kind: str, at: float = 0, **fields: object) -> dict:
    return dict(kind=kind, at=at, boot="test-boot", **fields)


def beginning(c: Checker, resumed: int = 0, attempt: int = 1) -> None:
    if attempt == 1:
        c.feed(event("submit", job="j", priority="production", units=2))
    c.feed(event("start", job="j", attempt=attempt, resumed=resumed, units=2,
                 contract="c", resumable=True, card="1"))


class EvidenceTests(unittest.TestCase):
    @unittest.skipUnless(Path('/proc/self/fdinfo').is_dir(), 'Linux procfs required')
    def test_drm_collector_does_not_race_its_own_directory_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / str(os.getpid())).symlink_to(Path('/proc') / str(os.getpid()))
            self.assertEqual(drm_owners(['0000:07:00.0'], root), {'0000:07:00.0': []})

    def test_drm_numeric_identity_and_multiple_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, pci = Path(tmp), "0000:07:00.0"
            def process(pid: int, group: int, start: int) -> None:
                path = root / str(pid)
                (path / "fdinfo").mkdir(parents=True)
                (path / "fd").mkdir()
                (path / "fd/9").symlink_to("/dev/dri/renderD129")
                fields = ["S", "1", str(group)] + ["0"] * 16 + [str(start)]
                (path / "stat").write_bytes(str(pid).encode() + b" (spaced ) non-utf8 \xff) " + " ".join(fields).encode())
                (path / "fdinfo/9").write_text("drm-pdev:\t" + pci + "\n")
            process(100, 100, 10)
            process(101, 100, 11)
            self.assertEqual(len(drm_owners([pci], root)[pci]), 1)
            process(200, 200, 20)
            self.assertEqual(len(drm_owners([pci], root)[pci]), 2)
            (root / "100/stat").write_text("malformed")
            with self.assertRaises((IndexError, ValueError)):
                drm_owners([pci], root)

    def test_complete_resume(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("commit", job="j", attempt=1, unit=1))
        beginning(c, 1, 2)
        c.feed(event("commit", job="j", attempt=2, unit=2))
        c.feed(event("finish", job="j", attempt=2, unit=2, complete=True))
        c.feed(event("status", jobs=[dict(id="j", state="succeeded", attempt=2)]))
        self.assertEqual(c.finish()["violations"], [])

    def test_saved_progress_lost_and_duplicate(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("commit", job="j", attempt=1, unit=1))
        beginning(c, 0, 2)
        c.feed(event("commit", job="j", attempt=2, unit=1))
        self.assertTrue({"saved_units_lost", "completed_unit_duplicated"} <= c.errors)

    def test_bound_remains_during_outage(self) -> None:
        c = Checker(30)
        c.feed(event("submit", job="j", priority="production", units=2))
        c.feed(event("outage", 31))
        self.assertIn("production_start_bound_exceeded", c.errors)

    def test_late_start_fails(self) -> None:
        c = Checker(30)
        c.feed(event("submit", job="j", priority="production", units=2))
        c.feed(event("start", 31, job="j", attempt=1, resumed=0, units=2, resumable=True))
        self.assertIn("production_start_bound_exceeded", c.errors)

    def test_double_queue_owner(self) -> None:
        c = Checker(30)
        for jid in ("a", "b"):
            c.feed(event("submit", job=jid, priority="experiment", units=2))
        c.feed(event("status", jobs=[dict(id=j, state="running", machine="same", attempt=1) for j in ("a", "b")]))
        self.assertIn("queue_double_owner", c.errors)

    def test_physical_unknown_overlap_and_stale(self) -> None:
        c = Checker(30, True)
        c.feed(event("physical", valid=True, owners={"1": ["a", "b"]}))
        c.feed(event("tick", 4))
        c.feed(event("physical", 5, valid=False, owners={}))
        self.assertTrue({"physical_double_owner", "physical_ownership_unknown", "physical_evidence_missing_or_stale"} <= c.errors)

    def test_physical_scope_and_shape(self) -> None:
        c = Checker(30, True, ("0000:07:00.0",))
        c.feed(event("physical", valid=True, owners={"0000:03:00.0": []}))
        self.assertIn("physical_scope_missing", c.errors)
        for owners in ({}, {"0000:07:00.0": "owner"}):
            with self.assertRaises(ValueError):
                c.feed(event("physical", valid=True, owners=owners))

    def test_terminal_regression_and_incomplete_success(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("status", jobs=[dict(id="j", state="succeeded", attempt=1)]))
        c.feed(event("status", jobs=[dict(id="j", state="queued", attempt=2)]))
        self.assertTrue({"terminal_regressed", "success_without_complete_output"} <= c.errors)

    def test_missing_job(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("status", jobs=[]))
        self.assertIn("submitted_job_missing", c.errors)
        self.assertIn("job_not_terminal", c.finish()["violations"])

    def test_clock_change_is_hold(self) -> None:
        c = Checker(30)
        c.feed(event("submit", job="j", priority="production", units=2))
        c.feed(dict(kind="tick", at=1, boot="new-boot"))
        self.assertTrue(any(e.startswith("clock_domain_changed") for e in c.errors))

    def test_memory_and_corrupt_timestamps_fail_closed(self) -> None:
        c = Checker(30)
        c.feed(event("memory", valid=True, available_gib=3, minimum_available_gib=4))
        c.feed(event("memory", valid=False))
        self.assertTrue({"host_memory_headroom_exceeded", "host_memory_unknown"} <= c.errors)
        with self.assertRaises(ValueError):
            c.feed(event("tick", float("nan")))

    def test_refuse_stop_needs_external_kill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cp = root / "checkpoint"
            cp.mkdir()
            env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_")}
            env.update(MYCE_JOB_ID="j", MYCE_ATTEMPT="1", MYCE_CHECKPOINT_DIR=str(cp))
            proc = subprocess.Popen([sys.executable, str(Path(__file__).with_name("pressure.py")), "job",
                "--events", str(root / "events.jsonl"), "--units", "1000", "--unit-seconds", ".04",
                "--host-mib", "1", "--resumable", "--refuse-stop"], env=env, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                while not (cp / "checkpoint.json").exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue((cp / "checkpoint.json").exists())
                proc.send_signal(signal.SIGTERM)
                time.sleep(.1)
                self.assertIsNone(proc.poll())
            finally:
                proc.kill(); proc.wait(timeout=5)
                proc.stderr.close()

    def test_checkpoint_corruption_and_changed_contract(self) -> None:
        saved = dict(job="j", contract="c", unit=1, digest=digest(["j", "c", 1]))
        self.assertEqual(validate_save(saved, "j", "c", 2), 1)
        for changed in (dict(saved, unit=2), dict(saved, job="x"), dict(saved, contract="d")):
            with self.assertRaises(ValueError):
                validate_save(changed, "j", "c", 2)

    def test_malformed_and_streamed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            emit(path, "tick")
            c = Checker(30)
            offset = replay(path, c)
            self.assertEqual(replay(path, c, offset), offset)
            with path.open("a") as out:
                out.write("{broken}\n")
            with self.assertRaises(ValueError):
                replay(path, c, offset)


class CommandTests(unittest.TestCase):
    def command(self, root: Path, attempt: int, *extra: str) -> subprocess.Popen:
        cp = root / "checkpoint"
        cp.mkdir(exist_ok=True)
        env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_")}
        env.update(MYCE_JOB_ID="j", MYCE_ATTEMPT=str(attempt), MYCE_CHECKPOINT_DIR=str(cp))
        return subprocess.Popen([sys.executable, str(Path(__file__).with_name("pressure.py")), "job",
            "--events", str(root / "events.jsonl"), "--units", "50", "--unit-seconds", ".04",
            "--host-mib", "1", "--resumable", *extra], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def wait_checkpoint(self, root: Path) -> Path:
        cp = root / "checkpoint/checkpoint.json"
        deadline = time.monotonic() + 5
        while not cp.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(cp.exists())
        return cp

    def test_actual_sigterm_save_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc = self.command(root, 1)
            try:
                cp = self.wait_checkpoint(root)
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=5), 0)
                n = json.loads(cp.read_text())["unit"]
                self.assertGreater(n, 0)
                second = self.command(root, 2)
                try:
                    self.assertEqual(second.wait(timeout=5), 0)
                finally:
                    if second.poll() is None:
                        second.kill(); second.wait()
                    second.stderr.close()
                records = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
                resumed = [r for r in records if r["kind"] == "start" and r["attempt"] == 2][0]
                self.assertEqual(resumed["resumed"], n)
                self.assertEqual(json.loads(cp.read_text())["unit"], 50)
            finally:
                if proc.poll() is None:
                    proc.kill(); proc.wait()
                proc.stderr.close()

    def test_second_owner_refused_before_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self.command(root, 1)
            try:
                self.wait_checkpoint(root)
                second = self.command(root, 2)
                try:
                    self.assertEqual(second.wait(timeout=5), 91)
                finally:
                    if second.poll() is None:
                        second.kill(); second.wait()
                    second.stderr.close()
            finally:
                first.send_signal(signal.SIGTERM)
                first.wait(timeout=5)
                first.stderr.close()


if __name__ == "__main__":
    unittest.main()
