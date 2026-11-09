"""Laptop integration tests: all helper invocations use temporary fake sysfs."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


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

    def test_worker_refuses_unapplied_policy(self):
        self.assertIn("worker must not start", self.invoke("verify", success=False))
        self.invoke()
        self.invoke("verify")
        self.target.write_text("640000\n")
        self.invoke("verify", success=False)

    def test_no_xe_and_invalid_card(self):
        self.invoke("apply", "--card", "../card0", success=False)
        (self.root / "class/drm/card0/device/driver").unlink()
        self.assertIn("no xe", self.invoke())
        self.invoke("verify")
        self.invoke("restore")
        self.assertEqual(self.target.read_text(), "640000\n")


if __name__ == "__main__":
    unittest.main()
