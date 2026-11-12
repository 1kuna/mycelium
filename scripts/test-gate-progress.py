#!/usr/bin/env python3
"""Fake-time regressions for host/selection progress supervision."""
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('progress', Path(__file__).with_name('gate-progress.py'))
progress = importlib.util.module_from_spec(spec)
spec.loader.exec_module(progress)


class ProgressTests(unittest.TestCase):
    def execute(self, root, times, expected=None, command=None, status=0, start=True):
        cache, report = root / 'cache', root / 'report'
        now = [0.0]
        child = Mock()
        child.wait.return_value = status
        child.stdout.fileno.return_value = 123
        if expected is not None:
            cache.mkdir(exist_ok=True)
            identity = progress.history_identity('package:selection')
            key = progress.hashlib.sha256(json.dumps(identity).encode()).hexdigest()
            (cache / (key + '.json')).write_text(json.dumps(dict(count=1, expected_seconds=expected)))
        selector = Mock()
        selector.__enter__ = Mock(return_value=selector)
        selector.__exit__ = Mock(return_value=False)
        events = iter(([(0, b'{"Action":"start"}\n')] if start else []) + times)
        chunks = []

        def select(timeout):
            at, data = next(events)
            now[0] = at
            chunks.append(data)
            return [True] if data is not None else []

        selector.select.side_effect = select
        output = Mock(buffer=io.BytesIO())
        with (patch.object(progress.subprocess, 'Popen', return_value=child) as launch,
              patch.object(progress.selectors, 'DefaultSelector', return_value=selector),
              patch.object(progress.os, 'read', side_effect=lambda *_: chunks.pop(0)),
              patch.object(progress.custody, 'stop_fixtures', return_value=True),
              patch.object(sys, 'stdout', output)):
            code = progress.run('package:selection', command or ['go', 'test', './pkg'],
                                cache, report, clock=lambda: now[0])
        self.launched_environment = launch.call_args.kwargs.get('env')
        return code, launch.call_args.args[0], json.loads(report.read_text().splitlines()[-1])

    def test_slow_progressing_run_outlives_old_wall_budget(self):
        with tempfile.TemporaryDirectory() as folder:
            code, command, record = self.execute(Path(folder),
                [(100, b'{"Action":"run","Test":"TestA"}\n'),
                 (200, b'{"Action":"pass","Test":"TestA"}\n'),
                 (300, b'{"Action":"run","Test":"TestB"}\n'), (400, b'')], expected=100)
        self.assertEqual(code, 0)
        self.assertEqual(record['elapsed_seconds'], 400)
        self.assertIn('-timeout=0', command)

    def test_reconnect_output_cannot_keep_a_stuck_test_alive(self):
        with tempfile.TemporaryDirectory() as folder:
            code, _, record = self.execute(Path(folder), [
                (0, b'{"Action":"run","Test":"TestBlockedCompletion"}\n'),
                (10, b'{"Action":"output","Test":"TestBlockedCompletion","Output":"reconnect"}\n'),
                (20, b'{"Action":"output","Output":"reconnect"}\n'),
                (30, b'{"Action":"output","Output":"reconnect"}\n')], expected=10)
        self.assertEqual(code, 124)
        self.assertEqual(record['elapsed_seconds'], 30)
        self.assertEqual(record['active_tests'], ['TestBlockedCompletion'])
        self.assertEqual(record['longest_gap_seconds'], 30)

    def test_split_json_progress_is_recognized_after_compilation(self):
        with tempfile.TemporaryDirectory() as folder:
            code, _, record = self.execute(Path(folder), [
                (100, b'{"Action":"sta'),
                (110, b'rt"}\n{"Action":"run","Test":"TestA"}\n'),
                (130, b'{"Action":"pass","Test":"TestA"}\n'),
                (150, b'')], expected=10, start=False)
        self.assertEqual(code, 0)
        self.assertEqual(record['active_tests'], [])

    def test_cold_run_keeps_go_backstop_and_learns(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            code, command, _ = self.execute(root, [(20, b'')])
            self.assertEqual(code, 0)
            self.assertIn('-timeout=1h', command)
            code, command, _ = self.execute(root, [(60, None)])
            self.assertEqual(code, 124)
            self.assertIn('-timeout=0', command)
            learned = json.loads(next((root / 'cache').glob('*.json')).read_text())
            self.assertEqual(learned['count'], 1)  # A hang never lengthens the baseline.

    def test_cpu_contract_limits_parallelism(self):
        with patch.dict(os.environ, GOMAXPROCS='1'):
            self.assertEqual(progress.cpu_count(), 1)

    def test_shard_concurrency_does_not_reuse_another_profiles_deadline(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for slots, timeout in [('4', '-timeout=1h'), ('8', '-timeout=1h'), ('4', '-timeout=0')]:
                with patch.dict(os.environ, GOMAXPROCS='1', GATE_TEST_SLOTS=slots):
                    code, command, record = self.execute(root, [(20, b'')])
                self.assertEqual(code, 0)
                self.assertIn(timeout, command)
                self.assertEqual(record['shard_slots'], slots)
            self.assertEqual(len(list((root / 'cache').glob('*.json'))), 2)

    def test_shared_images_retain_private_executable_custody_and_exact_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / 'shared-image'
            source.write_bytes(b'fixture executable bytes')
            source.chmod(0o755)
            remove = progress.custody.remove_scratch
            inspected = []
            def inspect_and_remove(scratch):
                for key in ('GATE_TEST_CLI_BINARY',):
                    private = Path(scratch) / 'tmp' / key.lower()
                    self.assertEqual(private.read_bytes(), source.read_bytes())
                    self.assertTrue(os.access(private, os.X_OK))
                    self.assertNotEqual(private.stat().st_ino, source.stat().st_ino)
                    inspected.append((key, str(private)))
                remove(scratch)
            with (patch.dict(os.environ, GATE_TEST_CLI_BINARY=str(source)),
                  patch.object(progress.custody, 'remove_scratch', side_effect=inspect_and_remove)):
                code, _, _ = self.execute(root, [(20, b'')])
            self.assertEqual(code, 0)
            self.assertEqual(len(inspected), 1)
            for key, private in inspected:
                self.assertEqual(self.launched_environment[key], private)
                self.assertFalse(Path(private).exists())
            self.assertEqual(source.read_bytes(), b'fixture executable bytes')

    def test_nested_execution_quota_limits_parallelism(self):
        files = {'/proc/self/cgroup': '0::/worker/job',
                 '/sys/fs/cgroup/worker/cpu.max': '150000 100000'}

        def read(path):
            if str(path) not in files:
                raise FileNotFoundError(path)
            return files[str(path)]

        with (patch.object(Path, 'read_text', read),
              patch.object(progress.os, 'sched_getaffinity', return_value=set(range(8)), create=True),
              patch.dict(os.environ, GOMAXPROCS='4')):
            self.assertEqual(progress.cpu_count(), 1)

    def test_stalled_child_exits_before_slot_is_released(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            tool = root / 'go'
            tool.write_text('#!' + sys.executable + '\n'
                            'import json, os, pathlib, time\n'
                            'pathlib.Path(os.environ["FIXTURE_PID"]).write_text(str(os.getpid()))\n'
                            'print(json.dumps({"Action": "start"}), flush=True)\n'
                            'print(json.dumps({"Action": "run", "Test": "TestChattyHang"}), flush=True)\n'
                            'while True:\n'
                            ' print(json.dumps({"Action": "output", "Output": "reconnect"}), flush=True)\n'
                            ' time.sleep(.01)\n')
            tool.chmod(0o755)
            cache = root / 'cache'
            cache.mkdir()
            identity = progress.history_identity('hung')
            key = progress.hashlib.sha256(json.dumps(identity).encode()).hexdigest()
            (cache / (key + '.json')).write_text(json.dumps(dict(count=1, expected_seconds=.05)))
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                       FIXTURE_PID=str(root / 'pid'), GATE_TIMING_CACHE=str(cache),
                       GATE_TIMING_REPORT=str(root / 'report'))
            peer = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
            try:
                run = subprocess.run([sys.executable, str(Path(progress.__file__)), 'hung', 'go', 'test'],
                                     env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, 124, run.stdout + run.stderr)
                self.assertIn('gate stalled test: TestChattyHang', run.stderr)
                pid = int((root / 'pid').read_text())
                state = subprocess.run(['ps', '-p', str(pid), '-o', 'stat='], capture_output=True, text=True)
                self.assertTrue(not state.stdout.strip() or state.stdout.strip().startswith('Z'), state.stdout)
                self.assertIsNone(peer.poll())
            finally:
                peer.kill()
                peer.wait()

    def test_real_command_records_output_and_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, GATE_TIMING_CACHE=str(root / 'cache'),
                       GATE_TIMING_REPORT=str(root / 'report'))
            run = subprocess.run([sys.executable, str(Path(progress.__file__)), 'fixture',
                                  sys.executable, '-c', 'print("progress"); raise SystemExit(7)'],
                                 env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(run.returncode, 7, run.stderr)
            self.assertIn('progress', run.stdout)
            self.assertEqual(json.loads((root / 'report').read_text())['exit'], 7)
            self.assertEqual(list((root / 'cache').iterdir()), [])


if __name__ == '__main__':
    unittest.main()
