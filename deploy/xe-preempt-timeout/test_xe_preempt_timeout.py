"""Laptop integration tests: all helper invocations use temporary fake sysfs."""
from pathlib import Path
import argparse
import os
import runpy
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


HELPER = Path(__file__).with_name("myce-xe-preempt-timeout")


class TimeoutTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = self.root / "config"
        self.config.write_text("PREEMPT_TIMEOUT_US=1280000\n")
        self.state = self.root / "state"
        self.target = self.card("card0", "0000:01:00.0")

    def card(self, name, pci, driver="xe", initial=640000):
        device = self.root / "devices" / pci
        device.mkdir(parents=True)
        drivers = self.root / "drivers" / driver
        drivers.mkdir(parents=True, exist_ok=True)
        (device / "driver").symlink_to(drivers)
        card = self.root / "class/drm" / name
        card.mkdir(parents=True)
        (card / "device").symlink_to(device)
        engine = device / "tile0/gt0/engines/ccs"
        engine.mkdir(parents=True)
        for key, value in {"preempt_timeout_us": initial,
                           "preempt_timeout_min": 1,
                           "preempt_timeout_max": 10000000,
                           "job_timeout_ms": 5000,
                           "timeslice_duration_us": 1000}.items():
            (engine / key).write_text(f"{value}\n")
        return engine / "preempt_timeout_us"

    def invoke(self, action="apply", *extra, success=True):
        result = subprocess.run([sys.executable, str(HELPER), action,
                                 "--sysfs-root", str(self.root),
                                 "--config", str(self.config),
                                 "--state-dir", str(self.state), *extra],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0 if success else 1, result.stderr)
        return result.stderr

    def test_config_parsing_and_unset(self):
        for content in ("# default\n", "PREEMPT_TIMEOUT_US= # unset\n"):
            self.config.write_text(content)
            self.invoke()
            self.assertEqual(self.target.read_text(), "640000\n")
            self.assertFalse(self.state.exists())
        for content in ("PREEMPT_TIMEOUT_US=-1", "PREEMPT_TIMEOUT_US=1.5",
                        "PREEMPT_TIMEOUT_US=$(id)", "OTHER=1",
                        "PREEMPT_TIMEOUT_US=1\nPREEMPT_TIMEOUT_US=2"):
            self.config.write_text(content)
            self.invoke(success=False)
            self.assertEqual(self.target.read_text(), "640000\n")
        self.config.write_text(" # comment\n PREEMPT_TIMEOUT_US = 1280000 # trial\n")
        self.invoke()
        self.assertEqual(self.target.read_text(), "1280000\n")

    def test_range_refusal_before_any_write(self):
        other = self.card("card1", "0000:02:00.0")
        for value in (0, 10000001):
            self.config.write_text(f"PREEMPT_TIMEOUT_US={value}\n")
            self.invoke(success=False)
        self.config.write_text("PREEMPT_TIMEOUT_US=1280000\n")
        other.with_name("preempt_timeout_max").write_text("1000000\n")
        self.invoke(success=False)
        self.assertEqual(self.target.read_text(), "640000\n")
        self.assertEqual(other.read_text(), "640000\n")
        other.with_name("preempt_timeout_max").unlink()
        self.invoke(success=False)

    def test_idempotence_restore_and_reboot(self):
        other = self.card("card1", "0000:02:00.0", initial=700000)
        ignored = self.card("card2", "0000:03:00.0", driver="i915")
        self.assertIn("wrote 1280000, read back 1280000", self.invoke())
        before = self.target.stat().st_mtime_ns
        saved = (self.state / "prior.json").read_bytes()
        self.assertIn("no write", self.invoke())
        self.assertEqual(before, self.target.stat().st_mtime_ns)
        self.invoke("verify")
        # Simulate reboot resetting the attribute, then a coldplug callback.
        self.target.write_text("640000\n")
        (self.root / "class/drm/card0").rename(self.root / "class/drm/card7")
        self.invoke("apply", "--card", "card7")
        self.assertEqual(saved, (self.state / "prior.json").read_bytes())
        self.invoke("restore")
        self.invoke("restore")
        self.assertEqual(self.target.read_text(), "640000\n")

        self.assertEqual(other.read_text(), "700000\n")
        self.assertEqual(ignored.read_text(), "640000\n")
        for target in (self.target, other, ignored):
            self.assertEqual(target.with_name("job_timeout_ms").read_text(), "5000\n")
            self.assertEqual(target.with_name("timeslice_duration_us").read_text(), "1000\n")

    def test_live_range_endpoints_are_inclusive(self):
        for value in (1, 10000000):
            self.config.write_text(f"PREEMPT_TIMEOUT_US={value}\n")
            self.invoke()
            self.invoke("verify")
            self.assertEqual(self.target.read_text(), f"{value}\n")

    def test_worker_reports_unapplied_policy_without_failure(self):
        self.assertIn("expected 1280000, read 640000", self.invoke("verify"))
        self.assertIn("verification: mismatch, status=1, continuing", self.invoke("verify"))
        self.assertIn("verification: child failure, status=1, continuing", self.invoke("startup"))
        self.assertIn("continuing worker startup", self.invoke("verify"))
        self.invoke()
        self.invoke("verify")
        self.target.write_text("640000\n")
        self.assertIn("report-only", self.invoke("verify"))
        self.target.with_name("preempt_timeout_min").unlink()
        self.assertIn("WARNING", self.invoke("verify"))
        self.config.write_text("PREEMPT_TIMEOUT_US=invalid\n")
        self.assertIn("continuing worker startup", self.invoke("startup"))

    def test_startup_settle_is_conditional_bounded_and_report_only(self):
        helper = runpy.run_path(str(HELPER))
        args = argparse.Namespace(action="startup-check", config=self.config,
                                  sysfs_root=self.root, card=None, state_dir=self.state)
        with mock.patch.object(helper["subprocess"], "run") as settle, \
                mock.patch.object(helper["syslog"], "syslog") as journal:
            self.config.write_text("PREEMPT_TIMEOUT_US=\n")
            helper["run"](args)
            settle.assert_not_called()
            self.config.write_text("PREEMPT_TIMEOUT_US=1280000\n")
            driver = self.root / "class/drm/card0/device/driver"
            driver.unlink()
            helper["run"](args)
            settle.assert_not_called()
            driver.symlink_to(self.root / "drivers/xe")
            for outcome in (subprocess.CompletedProcess([], 0),
                            subprocess.CompletedProcess([], 1),
                            subprocess.TimeoutExpired("udevadm", 5),
                            FileNotFoundError("udevadm missing")):
                settle.reset_mock()
                settle.side_effect = outcome if isinstance(outcome, Exception) else None
                settle.return_value = outcome
                helper["run"](args)
                settle.assert_called_once_with(
                    ["/usr/bin/udevadm", "settle", "--timeout=5"], timeout=5, check=False)
            messages = [call.args[1] for call in journal.call_args_list]
            self.assertTrue(any("expected 1280000, read 640000" in m for m in messages))
            self.assertTrue(any("verification: mismatch, status=1, continuing" in m for m in messages))
            self.assertTrue(any("udev settle failed" in m for m in messages))
            self.assertTrue(any("udev settle exited 1" in m for m in messages))
        self.assertEqual(self.target.read_text(), "640000\n")
        self.assertFalse(self.state.exists())

    def test_whole_startup_bounds_stalled_attribute_read(self):
        # A FIFO with no writer blocks Path.read_text just like a stalled sysfs
        # read. Use the real supervisor/deadlines, without mocking read or wait.
        self.target.unlink()
        os.mkfifo(self.target)
        started = time.monotonic()
        result = subprocess.run(
            [sys.executable, str(HELPER), "startup", "--config", str(self.config),
             "--sysfs-root", str(self.root)], capture_output=True, text=True, timeout=15)
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("verification: timeout, status=124, continuing", result.stderr)
        self.assertGreaterEqual(elapsed, 10)
        # One second allows interpreter startup and test-host scheduling jitter.
        self.assertLess(elapsed, 13)
        self.assertFalse(self.state.exists())

    def test_no_xe_and_invalid_card(self):
        self.invoke("apply", "--card", "../card0", success=False)
        (self.root / "class/drm/card0/device/driver").unlink()
        self.assertIn("no xe", self.invoke())
        self.invoke("verify")
        self.invoke("restore")
        self.assertEqual(self.target.read_text(), "640000\n")


if __name__ == "__main__":
    unittest.main()
