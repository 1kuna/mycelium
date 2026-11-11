#!/usr/bin/env python3
"""Regression checks for observed compatibility-supervisor review failures."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

MODULE_PATH = Path(__file__).with_name('upgrade-compat.py')
spec = importlib.util.spec_from_file_location('upgrade_compat', MODULE_PATH)
compat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compat)


class SupervisorTests(unittest.TestCase):
    def test_inventory_restart_only_accepts_one_exact_disk_budget_retry(self):
        fence = dict(session="same-session", attempt_protocol="keyed-attempt-v1", hash="same-inventory")
        refused = dict(fence, status=400, error=json.dumps(dict(code="request_invalid", error='json: unknown field "disk_budget"')))
        accepted = dict(fence, status=200)
        compat.validate_inventory_receipts([refused, accepted], True)
        for rows, allow in [([refused, accepted], False), ([accepted, refused], True), ([refused, refused], True),
                            ([dict(refused, error=json.dumps(dict(code="request_invalid", error='unknown field "attempts"')))], True)]:
            with self.assertRaises(RuntimeError):
                compat.validate_inventory_receipts(rows, allow)
        with self.assertRaises(AssertionError):
            compat.validate_inventory_receipts([refused, dict(accepted, session="different")], True)

    def test_sigterm_reaps_detached_command_before_scratch_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pidfile = root / 'child-pid'
            child_code = ('import os, pathlib, time; '
                          f'p=pathlib.Path({str(pidfile)!r}); staging=p.with_suffix(".tmp"); '
                          'staging.write_text(str(os.getpid())); staging.replace(p); time.sleep(60)')
            supervisor_code = (
                'import importlib.util, sys; '
                f's=importlib.util.spec_from_file_location("compat", {str(MODULE_PATH)!r}); '
                'm=importlib.util.module_from_spec(s); s.loader.exec_module(m); '
                'm.install_signal_handlers(); '
                f'm.command([sys.executable, "-c", {child_code!r}])')
            supervisor = subprocess.Popen([sys.executable, '-c', supervisor_code],
                                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            child_pid = None
            try:
                deadline = time.monotonic() + 5
                while not pidfile.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(pidfile.exists(), 'dummy command did not start')
                child_pid = int(pidfile.read_text())
                supervisor.send_signal(signal.SIGTERM)
                supervisor.communicate(timeout=5)
                self.assertNotEqual(supervisor.returncode, 0)
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)
            finally:
                if supervisor.poll() is None:
                    supervisor.kill()
                    supervisor.communicate(timeout=5)
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_successful_worker_does_not_hide_queue_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            executable = root / 'fixture'
            executable.write_text(
                '#!/usr/bin/env python3\n'
                'import os, signal, sys\n'
                'if os.environ["UPGRADE_MODE"] == "worker":\n'
                '    sys.exit(0)\n'
                'signal.signal(signal.SIGTERM, lambda *args: sys.exit(66))\n'
                'print("queue-ready", flush=True)\n'
                'signal.pause()\n')
            executable.chmod(0o700)
            with self.assertRaisesRegex(RuntimeError, 'queue fixture exited 66'):
                with compat.process(executable, 'queue', root / 'queue'):
                    # Waiting for its log ensures the termination handler is ready.
                    deadline = time.monotonic() + 5
                    while 'queue-ready' not in (root / 'queue/process.log').read_text():
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.01)
                    with compat.process(executable, 'worker', root / 'worker') as worker:
                        self.assertEqual(worker.wait(timeout=5), 0)


if __name__ == '__main__':
    unittest.main()
