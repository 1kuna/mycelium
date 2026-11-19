#!/usr/bin/env python3
"""Real fake-child checks for package admission and typed watchdog results."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


def scheduler_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location('scheduler_module', SCHEDULER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SCHEDULER = Path(__file__).with_name('gate-scheduler.py')
HARNESS = '''
import importlib.util, json, signal, subprocess, sys, time
from pathlib import Path
spec = importlib.util.spec_from_file_location('scheduler', sys.argv[1])
scheduler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scheduler)
for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
    signal.signal(sig, scheduler.interrupted)
root = Path(sys.argv[2])
entries = json.loads((root / 'entries').read_text())
def command(entry):
    with (root / 'admitted').open('a') as out:
        out.write(entry[0] + '\\n')
    if entry[1] in ('prepare-error', 'spawn-error'):
        deadline = time.monotonic() + 5
        while not (root / 'oldest-started').exists():
            if time.monotonic() > deadline:
                raise AssertionError('active watchdog did not start')
            time.sleep(.01)
        if entry[1] == 'prepare-error':
            raise subprocess.CalledProcessError(7, ['preparation-fixture'])
        return entry[0], [str(root / 'missing-command')]
    return entry[0], [sys.executable, str(root / 'child.py'), str(root), *entry]
sys.exit(scheduler.run(entries, 2, command))
'''
CHILD = '''
import json, os, signal, sys, time
from pathlib import Path
root, name, behavior = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
def event(kind):
    fd = os.open(root / 'events', os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.write(fd, (json.dumps([name, kind]) + '\\n').encode())
    os.close(fd)
def stop(sig, frame):
    event('stopped')
    if behavior == 'cleanup-killed':
        os.kill(os.getpid(), signal.SIGKILL)
    if behavior.startswith('cleanup-'):
        sys.exit(int(behavior.removeprefix('cleanup-')))
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
event('start')
(root / (name + '-started')).touch()
if behavior == 'blocked' or behavior.startswith('cleanup-'):
    while not (root / 'release').exists():
        time.sleep(.01)
elif behavior == 'release':
    (root / 'release').touch()
elif behavior in ('125', 'killed-custody'):
    while not (root / 'oldest-started').exists():
        time.sleep(.01)
    if behavior == 'killed-custody':
        os.kill(os.getpid(), signal.SIGKILL)
elif behavior == 'killed':
    os.kill(os.getpid(), signal.SIGKILL)
event('finish')
sys.exit(int(behavior) if behavior.isdigit() else 0)
'''


class SchedulerTests(unittest.TestCase):
    def test_links_each_package_once_and_retains_no_test_packages(self):
        module = scheduler_module()
        entries = ['pkg/tests\t^TestA$', 'pkg/tests\t^TestB$', 'pkg/empty\t.*', '@python-client\t']
        with tempfile.TemporaryDirectory() as folder:
            def info(command, **kwargs):
                self.assertIn('-race', command)
                package = command[-1]
                return json.dumps(dict(Dir='/package', ImportPath=package,
                                       TestGoFiles=['a_test.go'] if package == 'pkg/tests' else []))
            with (patch.object(module.subprocess, 'check_output', side_effect=info),
                  patch.object(module.subprocess, 'run') as compile):
                binaries = module.compile_packages(entries, folder)
            self.assertEqual(list(binaries), ['pkg/tests'])
            self.assertEqual(binaries['pkg/tests'][1], '/package')
            command = compile.call_args.args[0]
            self.assertEqual(compile.call_count, 1)
            self.assertEqual(command[3:10], ['go', 'test', '-c', '-race', '-vet=off', '-p', '1'])
            self.assertIn(binaries['pkg/tests'][0], command)
            self.assertTrue(compile.call_args.kwargs['check'])

    def test_compiler_custody_refusal_starts_no_tests(self):
        module = scheduler_module()
        with tempfile.TemporaryDirectory() as folder:
            selection = Path(folder) / 'selection'
            selection.write_text('pkg/tests\t.*\n')
            with (patch.dict(os.environ, GATE_TEST_BINARY_DIR=folder),
                  patch.object(sys, 'argv', ['scheduler', 'full', '2', str(selection)]),
                  patch.object(module, 'compile_packages', side_effect=subprocess.CalledProcessError(125, ['compile'])),
                  patch.object(module, 'run') as run):
                self.assertEqual(module.main(), 125)
            run.assert_not_called()

    @unittest.skipUnless(os.environ.get('GATE_TEST_BINARY_INTEGRATION') == '1',
                         'full gate; targeted: GATE_TEST_BINARY_INTEGRATION=1 python3 scripts/test-gate-scheduler.py')
    def test_compiled_binary_runs_in_package_with_examples_fuzz_failures_and_race(self):
        # Exercise the Go tool boundary, not just argument construction. Keep
        # this tiny module outside the repository's recursive package inventory.
        module = scheduler_module()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'go.mod').write_text('module fixture.invalid/gate\n\ngo 1.25\n')
            (root / 'fixture.txt').write_text('package cwd')
            no_race = root / 'no_race'
            no_race.mkdir()
            (no_race / 'gate.go').write_text('package no_race\n')
            (no_race / 'gate_test.go').write_text('//go:build !race\n\npackage no_race\n'
                'import "testing"\nfunc TestNoRace(t *testing.T) { t.Fatal("excluded under race") }\n')
            (root / 'gate_test.go').write_text('''package gate
import ("fmt"; "os"; "testing")
func TestPass(t *testing.T) { t.Run("child", func(t *testing.T) {
 if _, err := os.ReadFile("fixture.txt"); err != nil { t.Fatal(err) }
}) }
func TestFail(t *testing.T) { t.Fatal("expected fixture failure") }
func TestExitZero(t *testing.T) { os.Exit(0) }
func TestRace(t *testing.T) {
 value := 0; done := make(chan struct{})
 go func() { for i:=0;i<1000;i++ { value++ }; close(done) }()
 for i:=0;i<1000;i++ { value++ }; <-done
}
func FuzzSeeds(f *testing.F) { f.Add("seed"); f.Fuzz(func(t *testing.T, s string) {}) }
func Example() { fmt.Println("example"); /* Output: example */ }
''')
            env = dict(os.environ, GOMAXPROCS='1', GORACE='atexit_sleep_ms=0',
                       GATE_TIMING_CACHE=str(root / 'cache'), GATE_TIMING_REPORT=str(root / 'report'))
            # compile_packages resolves cwd via go list; run it as a small
            # subprocess so this unittest never changes its own process cwd.
            build = '''import importlib.util, sys
spec=importlib.util.spec_from_file_location('scheduler', sys.argv[1])
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
images=m.compile_packages(['fixture.invalid/gate\\t.*', 'fixture.invalid/gate/no_race\\t.*'], sys.argv[2])
assert set(images) == {'fixture.invalid/gate'}, images
'''
            compiled = subprocess.run([sys.executable, '-c', build, str(SCHEDULER.resolve()), str(root / 'bin')],
                                      cwd=root, env=env, capture_output=True, text=True, timeout=60)
            self.assertEqual(compiled.returncode, 0, compiled.stdout + compiled.stderr)
            image = next((root / 'bin').glob('*.test'))
            progress = SCHEDULER.with_name('gate-progress.py').resolve()
            for expression, expected in [('^(TestPass|Example|FuzzSeeds)$', 0),
                                         ('^TestFail$', 1), ('^TestExitZero$', 1), ('^TestRace$', 1)]:
                with self.subTest(expression=expression):
                    result = subprocess.run([sys.executable, str(progress), expression, '--test-binary',
                        'fixture.invalid/gate', str(image), str(root), '-test.count=1',
                        '-test.parallel=1', '-test.run=' + expression], env=env,
                        capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                    events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
                    if expected == 0:
                        passed = {e.get('Test') for e in events if e.get('Action') == 'pass'}
                        self.assertTrue({'TestPass', 'TestPass/child', 'FuzzSeeds', 'FuzzSeeds/seed#0', 'Example'} <= passed)
                    else:
                        self.assertTrue(any(e['Action'] == 'fail' for e in events), result.stdout)
                    if expression == '^TestRace$':
                        self.assertIn('DATA RACE', result.stdout)

    def test_python_client_command_runs_suite_or_named_tests(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('scheduler_module', SCHEDULER)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        label, command = module.python_client_command('')
        self.assertEqual(label, 'python-client')
        self.assertEqual(command[3:5], ['env', command[4]])
        self.assertTrue(command[4].startswith('PYTHONPATH='))
        self.assertEqual(command[5:9], ['python3.11', '-m', 'unittest', 'discover'])
        self.assertTrue(command[-1].endswith('clients/python/tests'))
        name = 'test_client.DevTests.test_consumer_skill_examples'
        label, command = module.python_client_command(name)
        self.assertEqual((label, command[-1]), ('python-client:' + name, name))

    def launch(self, root, entries):
        (root / 'child.py').write_text(CHILD)
        (root / 'entries').write_text(json.dumps(entries))
        return subprocess.Popen([sys.executable, '-c', HARNESS, str(SCHEDULER), str(root)],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)

    def finish(self, child):
        try:
            output, _ = child.communicate(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            output, _ = child.communicate()
            self.fail('scheduler did not finish: ' + output)
        return child.returncode, output

    def test_completed_second_slot_admits_next_before_oldest_exits(self):
        # The old FIFO waits forever: only the third package releases oldest.
        entries = [('oldest', 'blocked'), ('bridge', '0'), ('core', 'release'), ('tail', '0')]
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            status, output = self.finish(self.launch(root, entries))
            self.assertEqual(status, 0, output)
            self.assertEqual((root / 'admitted').read_text().splitlines(), [x[0] for x in entries])
            events = [json.loads(line) for line in (root / 'events').read_text().splitlines()]
            self.assertLess(events.index(['core', 'start']), events.index(['oldest', 'finish']))
            active = set()
            for name, kind in events:
                if kind == 'start':
                    self.assertNotIn(name, active)
                    active.add(name)
                    self.assertLessEqual(len(active), 2)
                else:
                    active.remove(name)
            self.assertFalse(active)

    def test_actual_failure_and_killed_watchdog_cannot_be_masked(self):
        for behavior in ('7', 'killed'):
            with self.subTest(behavior=behavior), tempfile.TemporaryDirectory() as folder:
                status, output = self.finish(self.launch(Path(folder), [('failure', behavior), ('ok', '0')]))
                self.assertEqual(status, 125 if behavior == 'killed' else 1, output)

    def test_stall_dominates_ordinary_failure_in_final_drain(self):
        with tempfile.TemporaryDirectory() as folder:
            status, output = self.finish(self.launch(Path(folder), [('failure', '7'), ('stall', '124')]))
            self.assertEqual(status, 124, output)

    def test_custody_refusal_stops_before_refill_and_unwinds_sibling(self):
        for behavior in ('125', 'killed-custody'):
            with self.subTest(behavior=behavior), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                status, output = self.finish(self.launch(root, [('oldest', 'blocked'), ('custody', behavior),
                                                               ('forbidden', 'release')]))
                self.assertEqual(status, 125, output)
                self.assertEqual((root / 'admitted').read_text().splitlines(), ['oldest', 'custody'])
                self.assertIn(['oldest', 'stopped'], [json.loads(x) for x in (root / 'events').read_text().splitlines()])

    def test_signal_unwinds_active_watchdog(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            child = self.launch(root, [('oldest', 'blocked')])
            deadline = time.monotonic() + 5
            while not (root / 'events').exists() and time.monotonic() < deadline:
                time.sleep(.01)
            child.send_signal(signal.SIGTERM)
            status, output = self.finish(child)
            self.assertEqual(status, 143, output)
            self.assertIn(['oldest', 'stopped'], [json.loads(x) for x in (root / 'events').read_text().splitlines()])

    def test_preparation_and_spawn_failures_preserve_cleanup_status(self):
        for failure in ('prepare-error', 'spawn-error'):
            for cleanup, expected in (('0', 1), ('124', 124), ('125', 125), ('killed', 125)):
                with self.subTest(failure=failure, cleanup=cleanup), tempfile.TemporaryDirectory() as folder:
                    root = Path(folder)
                    status, output = self.finish(self.launch(root, [('oldest', 'cleanup-' + cleanup),
                                                                   ('failure', failure), ('forbidden', 'release')]))
                    self.assertEqual(status, expected, output)
                    self.assertEqual((root / 'admitted').read_text().splitlines(), ['oldest', 'failure'])
                    self.assertIn(['oldest', 'stopped'], [json.loads(x) for x in (root / 'events').read_text().splitlines()])


if __name__ == '__main__':
    unittest.main()
