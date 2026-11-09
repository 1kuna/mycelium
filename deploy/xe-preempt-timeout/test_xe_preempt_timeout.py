"""Laptop integration tests: all helper invocations use temporary fake sysfs."""
from pathlib import Path
import argparse
import fcntl
import os
import runpy
import shlex
import shutil
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
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stderr)
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

    def test_verification_returns_and_logs_mismatch_status(self):
        self.assertIn("expected 1280000, read 640000", self.invoke("verify", success=False))
        self.assertIn("verification: mismatch, status=1", self.invoke("verify", success=False))
        helper = runpy.run_path(str(HELPER))
        args = argparse.Namespace(action="verify", config=self.config,
                                  sysfs_root=self.root, card=None)
        with mock.patch.object(helper["syslog"], "syslog") as journal:
            self.assertEqual(helper["run"](args), 1)
            self.assertTrue(any("verification: mismatch, status=1" in call.args[1]
                                for call in journal.call_args_list))
        self.invoke()
        self.invoke("verify")
        self.target.with_name("preempt_timeout_min").unlink()
        self.assertIn("verification: error, status=2", self.invoke("verify", success=False))

    def test_udev_application_does_not_wait_for_lock(self):
        self.state.mkdir()
        with (self.state / "lock").open("a") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            started = time.monotonic()
            result = subprocess.run(
                [sys.executable, str(HELPER), "apply", "--nonblocking-lock",
                 "--config", str(self.config), "--sysfs-root", str(self.root),
                 "--state-dir", str(self.state)], capture_output=True, text=True, timeout=3)
            self.assertEqual(result.returncode, 75, result.stderr)
            self.assertLess(time.monotonic() - started, 3)
            self.assertIn("busy, status=75; no wait", result.stderr)
        self.assertEqual(self.target.read_text(), "640000\n")
        self.invoke()
        self.invoke("verify")

    def test_bounded_udev_run_terminates_stalled_attribute_read(self):
        timeout = shutil.which("timeout") or shutil.which("gtimeout")
        if not timeout:
            self.skipTest("GNU timeout unavailable; run this test on Linux or with coreutils")
        rule = HELPER.with_name("70-myce-xe-preempt-timeout.rules").read_text()
        command = shlex.split(rule.split('RUN+="', 1)[1].strip().removesuffix('"'))
        command[0] = timeout
        index = command.index("/usr/local/libexec/myce-xe-preempt-timeout")
        command[index:index + 1] = [sys.executable, str(HELPER)]
        command[command.index("%k")] = "card0"
        command.extend(["--config", str(self.config), "--sysfs-root", str(self.root),
                        "--state-dir", str(self.state)])
        # Exercise the actual RUN command using a FIFO that never supplies data.
        self.target.unlink()
        os.mkfifo(self.target)
        started = time.monotonic()
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        elapsed = time.monotonic() - started
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertIn("sending signal TERM", result.stderr)
        self.assertGreaterEqual(elapsed, 10)
        self.assertLess(elapsed, 13)  # Includes one second scheduling tolerance.
        self.assertFalse((self.state / "prior.json").exists())

    def test_no_xe_and_invalid_card(self):
        self.invoke("apply", "--card", "../card0", success=False)
        (self.root / "class/drm/card0/device/driver").unlink()
        self.assertIn("no xe", self.invoke())
        self.invoke("verify")
        self.invoke("restore")
        self.assertEqual(self.target.read_text(), "640000\n")


if __name__ == "__main__":
    unittest.main()
