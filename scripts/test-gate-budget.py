#!/usr/bin/env python3
"""Exercise real shell cleanup on TERM and the quick budget timeout path."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class GateBudgetTests(unittest.TestCase):
    def test_termination_summary_matches_status(self):
        scripts = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "scripts").mkdir()
            (root / "bin").mkdir()
            (root / "scripts/gate.sh").write_bytes((scripts / "gate.sh").read_bytes())
            (root / "scripts/gate-related.py").write_text("")
            go = root / "bin/go"
            go.write_text(f'''#!{sys.executable}
import signal, sys
from pathlib import Path
if sys.argv[1] == "env":
    print({str(root)!r})
else:
    Path({str(root / 'ready')!r}).touch()
    signal.pause()
''')
            go.chmod(0o755)
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"])
            env.pop("MYCE_GATE_BUDGET", None)
            # Setup does not need real Go, uv, Git state, or a full gate run.
            for name, body in {"git": "exit 0", "uv": f"echo {sys.executable}"}.items():
                stub = root / "bin" / name
                stub.write_text("#!/bin/sh\n" + body + "\n")
                stub.chmod(0o755)
            for budgeted in (False, True):
                with self.subTest(budgeted=budgeted):
                    ready = root / "ready"
                    ready.unlink(missing_ok=True)
                    if budgeted:
                        # Trigger the production timeout branch once the fake build
                        # is ready, without changing the production 175/5s budget.
                        launcher = '''import pathlib, runpy, subprocess, sys, time
from unittest.mock import patch
original = subprocess.Popen.wait
def wait(child, timeout=None):
    if timeout == 175:
        deadline = time.monotonic() + 10
        while not pathlib.Path(ready).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("fake build did not start")
            time.sleep(.01)
        raise subprocess.TimeoutExpired(child.args, timeout)
    return original(child, timeout)
script, ready, gate = sys.argv[1:]
sys.argv = [script, gate, "quick", "HEAD"]
with patch.object(subprocess.Popen, "wait", wait):
    runpy.run_path(script, run_name="__main__")
'''
                        command = [sys.executable, "-c", launcher, str(scripts / "gate-budget.py"), str(ready), str(root / "scripts/gate.sh")]
                        expected = 124
                    else:
                        command = ["bash", str(root / "scripts/gate.sh"), "full"]
                        expected = 143
                    child = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
                    try:
                        if not budgeted:
                            deadline = time.monotonic() + 10
                            while not ready.exists():
                                if time.monotonic() >= deadline:
                                    self.fail("fake build did not start")
                                time.sleep(.01)
                            os.killpg(child.pid, signal.SIGTERM)
                        output, _ = child.communicate(timeout=15)
                        self.assertEqual(child.returncode, expected, output)
                        self.assertNotIn("exit=0", output)
                        mode = "quick" if budgeted else "full"
                        self.assertIn(f"gate {mode}: exit={expected}", output)
                        if budgeted:
                            self.assertIn("timed out; run full", output)
                    finally:
                        if child.poll() is None:
                            os.killpg(child.pid, signal.SIGKILL)
                            child.communicate()


if __name__ == "__main__":
    unittest.main()
