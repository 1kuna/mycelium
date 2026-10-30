#!/usr/bin/env python3
"""Exercise real shell cleanup on TERM and the quick budget timeout path."""
import os
import contextlib
import importlib.util
import io
from pathlib import Path
import signal
import shlex
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('budget', Path(__file__).with_name('gate-budget.py'))
budget = importlib.util.module_from_spec(spec)
spec.loader.exec_module(budget)


class GateBudgetTests(unittest.TestCase):
    def test_stuck_handle_probe_retains_scratch_and_reports(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'scripts').mkdir()
            gate = root / 'scripts/gate.sh'
            gate.write_text('exit 0\n')
            probe = root / 'lsof'
            probe.write_text(f'#!{sys.executable}\nimport os, pathlib, signal\n'
                             f'pathlib.Path({str(root / "probe-pid")!r}).write_text(str(os.getpid()))\n'
                             'signal.pause()\n')
            probe.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'])
            output = io.StringIO()
            started = time.monotonic()
            real_kill = os.kill
            with (patch.object(sys, 'argv', ['gate-budget.py', str(gate)]),
                  patch.object(sys, 'platform', 'darwin'),
                  patch.dict(os.environ, env),
                  patch.object(os, 'kill', wraps=real_kill) as kill,
                  contextlib.redirect_stdout(output), contextlib.redirect_stderr(output)):
                self.assertEqual(budget.main(), 1)
            self.assertLess(time.monotonic() - started, 10)
            self.assertIn('fixture inventory failed; scratch retained', output.getvalue())
            self.assertIn('gate quick: exit=1', output.getvalue())
            self.assertEqual(len(list((root / '.gate').iterdir())), 1)
            self.assertEqual(kill.call_count, 1)  # Only the stuck probe.
            self.assertEqual(kill.call_args.args,
                             (int((root / 'probe-pid').read_text()), signal.SIGKILL))

    def test_uninterruptible_probe_has_bounded_reap(self):
        probe = unittest.mock.Mock()
        probe.wait.side_effect = subprocess.TimeoutExpired('lsof', 5)
        with (patch.object(sys, 'platform', 'darwin'),
              patch.object(subprocess, 'Popen', return_value=probe) as launch):
            with self.assertRaises(subprocess.TimeoutExpired):
                budget.scratch_processes('/scratch', {123, 456})
        self.assertEqual(probe.wait.call_args_list,
                         [unittest.mock.call(timeout=5), unittest.mock.call(timeout=.2)])
        probe.kill.assert_called_once_with()
        command = launch.call_args.args[0]
        self.assertIn('-O', command)

    def test_quick_timeout_uses_remaining_budget(self):
        script = Path(__file__).with_name('gate.sh').read_text()
        start = script.index('  quick_timeout() {')
        helper = script[start:script.index('  while IFS=', start)]
        # Use the supervisor interpreter, including when PATH resolves another Python.
        helper = helper.replace('python3 -', shlex.quote(sys.executable) + ' -')
        helper = helper.replace('import time', 'import time; time.monotonic = lambda: 100')
        env = dict(os.environ, GATE_TEST_DEADLINE='130')
        run = subprocess.run(['bash', '-c', helper + '\nquick_timeout'],
                             env=env, capture_output=True, text=True, check=True)
        self.assertEqual(run.stdout.strip(), '30.000s')
        env['GATE_TEST_DEADLINE'] = '99'
        run = subprocess.run(['bash', '-c', helper + '\nquick_timeout'],
                             env=env, capture_output=True, text=True)
        self.assertNotEqual(run.returncode, 0)
        self.assertIn('exhausted', run.stderr)

    def test_success_cleans_fixture(self):
        scripts = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'scripts').mkdir()
            gate = root / 'scripts/gate.sh'
            gate.write_text('cd "$GATE_SCRATCH"\nsleep 60 &\nexit 0\n')
            other = subprocess.Popen(['sleep', '60'], start_new_session=True)
            try:
                run = subprocess.run([sys.executable, str(scripts / 'gate-budget.py'), str(gate)],
                                     capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertIn('gate quick: exit=0', run.stdout)
                self.assertEqual(list((root / '.gate').iterdir()), [])
                self.assertIsNone(other.poll())
            finally:
                other.kill()
                other.wait()

    def test_permission_error_preserves_timeout(self):
        with tempfile.TemporaryDirectory() as folder:
            gate = Path(folder) / 'scripts' / 'gate.sh'
            child = unittest.mock.Mock()
            child.wait.side_effect = subprocess.TimeoutExpired('fixture', 175)
            output = io.StringIO()
            with (patch.object(sys, 'argv', ['gate-budget.py', str(gate)]),
                  patch.object(subprocess, 'Popen', return_value=child),
                  patch.object(budget, 'owned_processes', side_effect=[{12345}, set(), set(), set(), set()]),
                  patch.object(os, 'kill', side_effect=PermissionError('fixture denied')),
                  contextlib.redirect_stdout(output), contextlib.redirect_stderr(output)):
                self.assertEqual(budget.main(), 124)
            self.assertIn('cannot signal owned fixture', output.getvalue())
            self.assertIn('gate quick: exit=124', output.getvalue())

    def test_scrubbed_orphan_stops_before_scratch_removal(self):
        scripts = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'scripts').mkdir()
            gate = root / 'scripts/gate.sh'
            gate.write_text('exec "$1" "$2"\n')
            fixture = root / 'fixture.py'
            fixture.write_text('''import os, pathlib, signal, subprocess, sys
root = pathlib.Path(__file__).parent
if len(sys.argv) == 1:
    scratch = os.environ['GATE_SCRATCH']
    subprocess.Popen([sys.executable, __file__, 'child'], env={}, cwd=scratch, start_new_session=True)
    sys.exit(0)  # Reparent before timeout cleanup; no ancestry or env remains.
else:
    scratch = pathlib.Path.cwd()
    def term(*_):
        (root / 'saved').write_text(str(scratch.exists()))
        os.chdir('/')  # Escalation must remember ownership after handles change.
    signal.signal(signal.SIGTERM, term)
    (root / 'ready').write_text(str(os.getpid()))
while True: signal.pause()
''')
            launcher = '''import pathlib, runpy, subprocess, sys, time
from unittest.mock import patch
original = subprocess.Popen.wait
def wait(child, timeout=None):
    if timeout is not None and child.args[0] == 'bash':
        deadline = time.monotonic() + 10
        while not pathlib.Path(ready).exists():
            if time.monotonic() >= deadline: raise RuntimeError('fixture not ready')
            time.sleep(.01)
        raise subprocess.TimeoutExpired(child.args, timeout)
    return original(child, timeout)
script, gate, fixture, ready = sys.argv[1:]
sys.argv = [script, gate, sys.executable, fixture]
with patch.object(subprocess.Popen, 'wait', wait):
    runpy.run_path(script, run_name='__main__')
'''
            # Same UID and executable, different run: it must survive cleanup.
            other = subprocess.Popen([sys.executable, '-c', 'import signal; signal.pause()'], start_new_session=True)
            try:
                run = subprocess.run([sys.executable, '-c', launcher, str(scripts / 'gate-budget.py'),
                                      str(gate), str(fixture), str(root / 'ready')],
                                     capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, 124, run.stdout + run.stderr)
                self.assertEqual((root / 'saved').read_text(), 'True')
                self.assertIsNone(other.poll())
                self.assertEqual(list((root / '.gate').iterdir()), [])
                pid = int((root / 'ready').read_text())
                probe = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True)
                self.assertTrue(not probe.stdout.strip() or probe.stdout.strip().startswith('Z'), probe.stdout)
            finally:
                other.kill()
                other.wait()
                if (root / 'ready').exists():
                    try:
                        os.kill(int((root / 'ready').read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

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
                        # is ready, without changing the production budget.
                        launcher = '''import pathlib, runpy, subprocess, sys, time
from unittest.mock import patch
original = subprocess.Popen.wait
def wait(child, timeout=None):
    if timeout is not None and child.args[0] == 'bash':
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
