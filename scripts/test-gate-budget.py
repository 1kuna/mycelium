#!/usr/bin/env python3
"""Exercise real shell cleanup on TERM and the quick budget timeout path."""
import os
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import shlex
import shutil
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
    def test_complete_handle_inventory_keeps_new_fixtures_and_excludes_older_helpers(self):
        uid = os.getuid()
        table = (f'10 1 {uid} S Tue Nov 10 10:00:00 2026 (sd-pam)\n'
                 f'20 1 {uid} S Tue Nov 10 12:00:00 2026 fixture\n')
        with (patch.object(budget.subprocess, 'check_output', return_value=table),
              patch.object(budget, 'scratch_processes', return_value={20}) as handles):
            owned = budget.owned_processes('GATE_RUN_ID=fixture', '/scratch', {},
                inspect_handles=True, complete_handles=True, born_after='Tue Nov 10 11:00:00 2026')
            self.assertEqual(owned, {20})
            self.assertEqual(handles.call_args.args[1], {20})
            self.assertTrue(handles.call_args.kwargs['all_handles'])

    def setUp(self):
        home = tempfile.TemporaryDirectory()
        self.addCleanup(home.cleanup)
        mock = patch.object(budget.Path, 'home', return_value=Path(home.name).resolve())
        mock.start()
        self.addCleanup(mock.stop)
        # Path.home() mocks stay in this process; fixture subprocesses must
        # also use the private coordinator and never inherit a real gate lane.
        fixture_environment = {name: value for name, value in os.environ.items()
                               if not name.startswith(('GATE_', 'MYCE_', 'XDG_'))}
        fixture_environment.update(HOME=home.name, USERPROFILE=home.name,
                                   XDG_CONFIG_HOME=home.name + '/.config',
                                   XDG_CACHE_HOME=home.name + '/.cache',
                                   XDG_DATA_HOME=home.name + '/.local/share')
        environment = patch.dict(os.environ, fixture_environment, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def test_native_only_full_obeys_resource_group_but_fleet_remains_background(self):
        with tempfile.TemporaryDirectory() as folder:
            control = budget.Path.home() / '.cache/mycelium-gates/control'
            control.mkdir(parents=True)
            group = control / 'local-go-resource-group.custody'
            group.write_text(json.dumps(dict(admission_protocol=2, resource_group='exclusive')))
            actual = budget.subprocess.check_output
            def output(args, **kwargs):
                return folder if args[0] == 'go' else actual(args, **kwargs)
            with patch.object(budget.subprocess, 'check_output', side_effect=output):
                with patch.object(budget.time, 'sleep', side_effect=RuntimeError('blocked')):
                    with self.assertRaisesRegex(RuntimeError, 'blocked'):
                        budget.acquire_full_slot()
                self.assertFalse(list(Path(folder).glob('*.custody')))
                fleet = budget.Path.home() / '.local/share/mycelium/worker/state/job-files/job/source'
                fleet.mkdir(parents=True)
                with patch.object(budget.Path, 'cwd', return_value=fleet):
                    slot = budget.acquire_full_slot()
                    budget.release_full_slot(slot, False)
                    self.assertTrue(all(p.exists() for p in budget.full_custody_paths(slot)))
                    budget.release_full_slot(slot, True)
                self.assertTrue(group.exists())
                group.unlink()
                # Reverse order: native-only/custom-cache work publishes a
                # host-wide reservation before a resource group can enter.
                slot = budget.acquire_full_slot()
                try:
                    paths = budget.full_custody_paths(slot)
                    self.assertEqual(len(paths), 2)
                    self.assertTrue(all(path.exists() for path in paths))
                    spec = importlib.util.spec_from_file_location('capacity', Path(__file__).with_name('gate-capacity.py'))
                    capacity = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(capacity)
                    self.assertTrue(capacity.native_full_custody())
                    budget.release_full_slot(slot, False)
                    self.assertFalse(any(path.exists() for path in paths))
                finally:
                    slot.close()
                self.assertFalse(any(control.glob('local-go-native-full-*.custody')))

    def test_host_resource_admission_replaces_legacy_count_only_for_owned_source_and_ancestor(self):
        with tempfile.TemporaryDirectory() as folder:
            home = Path(folder).resolve()
            control = home / '.cache/mycelium-gates/control'
            control.mkdir(parents=True)
            lane = control / 'local-go-slot-3'
            lane.mkdir()
            owner = dict(admission_protocol=2, resource_group='group', run_id='run',
                         admission_config_sha256='f' * 64,
                         source_root=str(home), pid=os.getpid())
            (lane / 'owner.json').write_text(json.dumps(owner))
            (control / 'local-go-resource-group.custody').write_text(json.dumps(
                dict(admission_protocol=2, resource_group='group', admission_config_sha256='f' * 64)))
            for name in ('local-go-slot', 'local-go-slot-2'):
                (control / name).mkdir()
                (control / name / 'owner.json').write_text(json.dumps(
                    dict(protocol_guard=True, resource_group='group')))
            with (patch.object(budget.Path, 'home', return_value=home),
                  patch.object(budget.Path, 'cwd', return_value=home),
                  patch.dict(os.environ, GATE_LOCAL_SLOT=str(lane), GATE_LOCAL_SLOT_ID='run')):
                self.assertTrue(budget.local_resource_admission())
                owner['pid'] = 999999999
                (lane / 'owner.json').write_text(json.dumps(owner))
                with self.assertRaisesRegex(RuntimeError, 'ancestor'):
                    budget.local_resource_admission()
                owner['pid'] = os.getpid()
                owner['source_root'] = '/another-source'
                (lane / 'owner.json').write_text(json.dumps(owner))
                with self.assertRaisesRegex(RuntimeError, 'identity'):
                    budget.local_resource_admission()

    @unittest.skipUnless(sys.platform == 'darwin', 'native measured admission')
    def test_measured_identity_launches_and_cleans_payload_without_signalling_supervisor(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            source = root / 'source'
            (source / 'scripts').mkdir(parents=True)
            control = root / '.cache/mycelium-gates/control'
            lane = control / 'local-go-slot-3'
            lane.mkdir(parents=True)
            marker = 'a' * 32
            scratch = source / '.gate' / ('run.' + marker)
            owner = dict(admission_protocol=2, resource_group='b' * 32,
                         admission_config_sha256='c' * 64, run_id='d' * 32,
                         source_root=str(source), pid=os.getpid(), run_marker=marker,
                         process_identity='fixture-owner', scratch=str(scratch))
            (lane / 'owner.json').write_text(json.dumps(owner))
            (control / 'local-go-resource-group.custody').write_text(json.dumps(
                dict(admission_protocol=2, resource_group='b' * 32, admission_config_sha256='c' * 64)))
            for name in ('local-go-slot', 'local-go-slot-2'):
                (control / name).mkdir()
                (control / name / 'owner.json').write_text(json.dumps(
                    dict(protocol_guard=True, resource_group='b' * 32)))
            gate = source / 'scripts/payload.sh'
            gate.write_text('set -eu\n'
                'test -z "${GATE_EXECUTION_CUSTODY:-}"\n'
                'test -z "${GATE_CUSTODY_ID:-}"\n'
                f'test "$GATE_RUN_ID" = {marker}\n'
                f'echo ran > {shlex.quote(str(root / "ran"))}\n')
            receipt, journal = root / 'cleanup.json', root / 'native-custody.json'
            env = dict(os.environ, HOME=str(root), GATE_LOCAL_SLOT=str(lane),
                       GATE_LOCAL_SLOT_ID='d' * 32, GATE_CUSTODY_ID=marker,
                       GATE_CLEANUP_ID='e' * 32, GATE_CLEANUP_RECEIPT=str(receipt),
                       GATE_EXECUTION_CUSTODY=str(journal))
            result = subprocess.run([sys.executable, budget.__file__, str(gate)], cwd=source,
                                    env=env, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((root / 'ran').exists())
            self.assertTrue(json.loads(receipt.read_text())['stopped'])
            custody = json.loads(journal.read_text())
            self.assertEqual(custody['run_marker'], marker)
            self.assertEqual(custody['scratch'], str(scratch))
            self.assertTrue(custody['supervisors'][0]['process_identity'])
            self.assertFalse(scratch.exists())
            self.assertEqual(json.loads((lane / 'owner.json').read_text()), owner)

    def test_abandoned_slots_reconcile_but_live_fixtures_keep_custody(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = Path(folder)
            # Reproduce the incident: both flocks are free but both cancelled
            # runs left complete legacy records. Keep one orphan fixture alive
            # after its supervisor exits, outside its original ancestry.
            scratches = [cache / 'run.first', cache / 'run.second']
            markers = ['GATE_RUN_ID=' + digit * 32 for digit in ('a', 'b')]
            for i in range(2):
                scratches[i].mkdir()
                (cache / f'mycelium-full-gate-{i}.custody').write_text(
                    json.dumps(dict(marker=markers[i], scratch=str(scratches[i]))))
            fixture = subprocess.Popen(
                [sys.executable, '-c', 'import signal; signal.pause()'],
                env=dict(os.environ, GATE_RUN_ID=markers[1].split('=')[1]),
                start_new_session=True)
            real_check_output = subprocess.check_output

            def inventory(args, **kwargs):
                return folder if args[0] == 'go' else '' if args == ['ps', '-axo', 'pid=,command='] else real_check_output(args, **kwargs)

            try:
                with patch.object(budget.subprocess, 'check_output', side_effect=inventory):
                    first = budget.acquire_full_slot()
                    try:
                        # Real ps/OS-handle inventory reclaimed exactly one.
                        self.assertEqual(Path(first.name).name, 'mycelium-full-gate-0.lock')
                        saved = json.loads((cache / 'mycelium-full-gate-0.custody').read_text())
                        self.assertEqual(saved['holder_pid'], os.getpid())
                        self.assertEqual(saved['holder_start'], budget.process_start(os.getpid()))
                        self.assertEqual(len(list((cache / 'selfheal-backups').iterdir())), 1)
                        with (cache / 'mycelium-full-gate-1.lock').open('a') as held:
                            budget.fcntl.flock(held, budget.fcntl.LOCK_EX | budget.fcntl.LOCK_NB)
                            self.assertFalse(budget.reconcile_full_custody(held))
                        self.assertIsNone(fixture.poll())  # Recovery never kills fixtures.
                        fixture.terminate()
                        fixture.wait(timeout=5)
                        second = budget.acquire_full_slot()
                        budget.release_full_slot(second, True)
                        self.assertEqual(len(list((cache / 'selfheal-backups').iterdir())), 2)
                        self.assertTrue(all(p.exists() for p in scratches))
                    finally:
                        budget.release_full_slot(first, True)
            finally:
                if fixture.poll() is None:
                    fixture.kill()
                fixture.wait(timeout=5)

    def test_holder_birth_fences_reuse_and_uncertain_inventory(self):
        with tempfile.TemporaryDirectory() as folder:
            slot = (Path(folder) / 'mycelium-full-gate-0.lock').open('a')
            self.addCleanup(slot.close)
            path = Path(slot.name).with_suffix('.custody')
            record = dict(holder_pid=123, holder_start='old birth', phase='running',
                          marker='GATE_RUN_ID=' + 'c' * 32, scratch=folder + '/run.old')
            path.write_text(json.dumps(record))
            with (patch.object(budget, 'process_start', return_value='old birth'),
                  patch.object(budget, 'owned_processes') as fixtures):
                self.assertFalse(budget.reconcile_full_custody(slot))
                fixtures.assert_not_called()
            with (patch.object(budget, 'process_start', return_value='reused PID birth'),
                  patch.object(budget, 'owned_processes', side_effect=PermissionError('unknown'))):
                self.assertTrue(budget.reconcile_full_custody(slot))
                self.assertFalse(path.exists())
                path.write_text(json.dumps(record))
            with (patch.object(budget, 'process_start', return_value='reused PID birth'),
                  patch.object(budget, 'owned_processes', return_value=set())):
                self.assertTrue(budget.reconcile_full_custody(slot))
            # A supervisor killed before scratch/child creation leaves a complete
            # reservation which is reclaimable without inventing a run marker.
            path.write_text(json.dumps(dict(holder_pid=123, holder_start='old birth', phase='reserved')))
            with patch.object(budget, 'process_start', return_value=None):
                self.assertTrue(budget.reconcile_full_custody(slot))
            path.write_text('')
            self.assertFalse(budget.reconcile_full_custody(slot))

    def test_linux_full_does_not_wait_on_mac_suite_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            gate = Path(folder) / 'scripts/gate.sh'
            child = unittest.mock.Mock()
            child.wait.return_value = 0
            with (patch.object(sys, 'platform', 'linux'),
                  patch.object(sys, 'argv', ['gate-budget.py', str(gate), 'full']),
                  patch.object(budget, 'acquire_full_slot') as slot,
                  patch.object(budget.subprocess, 'Popen', return_value=child),
                  patch.object(budget, 'stop_fixtures', return_value=True),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(budget.main(), 0)
            slot.assert_not_called()

    def test_full_slots_count_gate_owners_not_package_commands(self):
        with tempfile.TemporaryDirectory() as folder:
            # New full packages carry -run expressions; owner locks, not the
            # number or arguments of their package processes, bound full suites.
            processes = '123 go test -race -run .* mycelium/pkg\n'
            with (patch.object(budget, 'process_start', return_value='owner start'),
                  patch.object(budget.subprocess, 'check_output', side_effect=lambda args, **_: folder if args[0] == 'go' else processes)):
                first = budget.acquire_full_slot()
                second = budget.acquire_full_slot()
                try:
                    with patch.object(budget.time, 'sleep', side_effect=RuntimeError('blocked')):
                        with self.assertRaisesRegex(RuntimeError, 'blocked'):
                            budget.acquire_full_slot()
                    budget.release_full_slot(first, True)
                    replacement = budget.acquire_full_slot()
                    budget.release_full_slot(replacement, True)
                finally:
                    first.close()
                    budget.release_full_slot(second, True)

    def test_legacy_and_higher_occupied_slot_block_third_suite(self):
        with tempfile.TemporaryDirectory() as folder:
            higher = (Path(folder) / 'mycelium-full-gate-1.lock').open('a')
            budget.fcntl.flock(higher, budget.fcntl.LOCK_EX | budget.fcntl.LOCK_NB)
            legacy = '123 go test -race ./...\n'
            try:
                with (patch.object(budget, 'process_start', return_value='owner start'),
                      patch.object(budget.subprocess, 'check_output', side_effect=lambda args, **_: folder if args[0] == 'go' else legacy),
                      patch.object(budget.time, 'sleep', side_effect=RuntimeError('blocked'))):
                    with self.assertRaisesRegex(RuntimeError, 'blocked'):
                        budget.acquire_full_slot()
            finally:
                higher.close()
            with (patch.object(budget, 'process_start', return_value='owner start'),
                  patch.object(budget.subprocess, 'check_output', side_effect=lambda args, **_: folder if args[0] == 'go' else legacy)):
                admitted = budget.acquire_full_slot()
                budget.release_full_slot(admitted, True)

    def test_unconfirmed_cleanup_releases_admission_and_retains_verdict(self):
        with tempfile.TemporaryDirectory() as folder:
            gate = Path(folder) / 'scripts/gate.sh'
            processes = '123 go test -race -run .* mycelium/pkg\n'
            child = unittest.mock.Mock()
            child.wait.return_value = 0
            with (patch.object(budget, 'process_start', return_value='owner start'),
                  patch.object(budget.subprocess, 'check_output', side_effect=lambda args, **_: folder if args[0] == 'go' else processes)):
                first = budget.acquire_full_slot()
                peer = budget.acquire_full_slot()
                try:
                    with (patch.object(sys, 'platform', 'darwin'),
                          patch.object(sys, 'argv', ['gate-budget.py', str(gate), 'full']),
                          patch.object(budget, 'acquire_full_slot', return_value=first),
                          patch.object(budget.subprocess, 'Popen', return_value=child),
                          patch.object(budget, 'stop_fixtures', return_value=False),
                          contextlib.redirect_stdout(io.StringIO()),
                          contextlib.redirect_stderr(io.StringIO())):
                        self.assertEqual(budget.main(), 125)
                    self.assertTrue(first.closed)
                    replacement = budget.acquire_full_slot()
                    budget.release_full_slot(replacement, True)
                finally:
                    first.close()
                    budget.release_full_slot(peer, True)

    def test_failed_cleanup_releases_slot_and_preserves_diagnostic(self):
        for failure in ('scratch',):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as folder:
                root = Path(folder).resolve()
                gate = root / 'scripts/gate.sh'
                child = unittest.mock.Mock()
                child.wait.return_value = 0
                receipt = root / 'native-cleanup.json'
                with (patch.object(budget, 'process_start', return_value='owner start'),
                      patch.object(budget.subprocess, 'check_output', side_effect=lambda args, **_: folder if args[0] == 'go' else ''),
                      patch.object(budget.os, 'uname') as uname):
                    uname.return_value.sysname = 'Darwin'
                    slot = budget.acquire_full_slot()
                    with (patch.object(sys, 'platform', 'darwin'),
                          patch.object(sys, 'argv', ['gate-budget.py', str(gate), 'full']),
                          patch.object(budget, 'acquire_full_slot', return_value=slot),
                          patch.object(budget.subprocess, 'Popen', return_value=child),
                          patch.object(budget, 'remove_scratch', side_effect=PermissionError('scratch removal failed') if failure == 'scratch' else None),
                          patch.object(budget, 'stop_fixtures', return_value=True),
                          patch.dict(os.environ, GATE_CLEANUP_RECEIPT=str(receipt), GATE_CLEANUP_ID='cleanup-failure')):
                        self.assertEqual(budget.main(), 125)
                    verdict = json.loads(receipt.read_text())
                    self.assertFalse(verdict['stopped'])
                    self.assertEqual(verdict['status'], 125)
                    diagnostic = next((root / '.reports/gate-runtime').glob('*/cleanup-error.json'))
                    self.assertIn('scratch removal failed', diagnostic.read_text())
                    self.assertTrue(list((root / '.gate').iterdir()))
                    replacement = budget.acquire_full_slot()
                    budget.release_full_slot(replacement, True)

    def test_quick_overlaps_packages_and_collects_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            scripts = root / 'scripts'
            scripts.mkdir()
            shutil.copyfile(Path(__file__).with_name('gate.sh'), scripts / 'gate.sh')
            for name in ('gate-scheduler.py', 'test-gate-scheduler.py'):
                shutil.copyfile(Path(__file__).with_name(name), scripts / name)
            shutil.copyfile(Path(__file__).with_name('gate-progress.py'), scripts / 'gate-progress.py')
            shutil.copyfile(Path(__file__).with_name('gate-budget.py'), scripts / 'gate-budget.py')
            shutil.copyfile(Path(__file__).with_name('gate-handles.py'), scripts / 'gate-handles.py')
            for name in ('gate-capacity.py', 'gate-cache.py', 'gate-memory.py'):
                shutil.copyfile(Path(__file__).with_name(name), scripts / name)
            shutil.copyfile(Path(__file__).with_name('test-gate-progress.py'), scripts / 'test-gate-progress.py')
            (scripts / 'gate-related.py').write_text('print("a\\t^TestA$")\nprint("b\\t^TestB$")\n')
            bin_dir = root / 'bin'
            bin_dir.mkdir()
            git = bin_dir / 'git'
            git.write_text('#!/bin/sh\nexit 0\n')
            git.chmod(0o755)
            uv = bin_dir / 'uv'
            uv.write_text('#!/bin/sh\necho ' + shlex.quote(sys.executable) + '\n')
            uv.chmod(0o755)
            go = bin_dir / 'go'
            go.write_text(f'''#!{sys.executable}
import os, pathlib, sys, time
args = sys.argv[1:]
root = pathlib.Path(os.environ['TEST_MARKERS'])
if args[0] == 'env':
    print(root)
elif args[0] == 'build':
    output = pathlib.Path(args[args.index('-o') + 1])
    output.write_text('#!/bin/sh\\nexit 0\\n')
    output.chmod(0o755)
elif args[0] == 'list':
    print(args[1].removeprefix('./'))
elif args[0] == 'test':
    package = next(arg for arg in args if arg in ('a', 'b'))
    other = 'b' if package == 'a' else 'a'
    (root / (package + '-started')).touch()
    deadline = time.monotonic() + 5
    while not (root / (other + '-started')).exists():
        if time.monotonic() > deadline:
            sys.exit(2)
        time.sleep(.01)
    (root / (package + '-finished')).touch()
    sys.exit(1 if package == 'a' else 0)
''')
            go.chmod(0o755)
            scratch = root / 'scratch'
            scratch.mkdir()
            env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH'],
                       MYCE_GATE_BUDGET='1', GATE_SCRATCH=str(scratch),
                       GOMAXPROCS='2', TEST_MARKERS=str(root))
            run = subprocess.run(['bash', str(scripts / 'gate.sh'), 'quick', 'base'],
                                 env=env, capture_output=True, text=True, timeout=15)
            self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertTrue((root / 'a-finished').exists(), run.stdout + run.stderr)
            self.assertTrue((root / 'b-finished').exists(), run.stdout + run.stderr)

    def test_hung_network_mount_does_not_block_owned_scratch(self):
        if sys.platform != 'darwin':
            self.skipTest('native macOS process API')
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'scripts').mkdir()
            gate = root / 'scripts/gate.sh'
            gate.write_text('cd "$GATE_SCRATCH"\nsleep 60 &\nexit 0\n')
            # Simulate tools stuck while examining an unrelated network mount.
            # The native inventory must not need either tool to find fixtures.
            for name in ('mount', 'lsof'):
                tool = root / name
                tool.write_text(f'#!{sys.executable}\nimport pathlib, signal\n'
                                f'pathlib.Path({str(root / "network-touched")!r}).touch()\n'
                                'signal.pause()\n')
                tool.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'])
            run = subprocess.run([sys.executable, str(Path(budget.__file__)), str(gate)],
                                 env=env, capture_output=True, text=True, timeout=15)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertEqual(list((root / '.gate').iterdir()), [])
            self.assertFalse((root / 'network-touched').exists())

    @unittest.skipUnless(sys.platform == 'darwin', 'native macOS process API')
    def test_native_executable_and_standard_handle_ownership(self):
        with tempfile.TemporaryDirectory() as folder:
            scratch = Path(folder).resolve()
            executable = scratch / 'sleep'
            shutil.copyfile('/bin/sleep', executable)
            executable.chmod(0o755)
            # A copied Apple platform binary cannot execute with its original
            # platform signature; use an ad-hoc signature for this local fixture.
            subprocess.run(['codesign', '--force', '--sign', '-', str(executable)],
                           capture_output=True, check=True)
            with (scratch / 'stdout').open('w') as output:
                executable_child = subprocess.Popen([str(executable), '60'], cwd='/')
                handle_child = subprocess.Popen(['/bin/sleep', '60'], cwd='/', stdout=output)
                peer = subprocess.Popen(['/bin/sleep', '60'], cwd='/')
                children = [executable_child, handle_child, peer]
                try:
                    self.assertEqual(budget.scratch_processes(str(scratch),
                                                             {p.pid for p in children}),
                                     {executable_child.pid, handle_child.pid})
                finally:
                    for child in children:
                        child.kill()
                        child.wait()

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
            real_scratch_processes = budget.scratch_processes

            def macos_handles(*args, **kwargs):
                # Simulate only the native probe, leaving ps flags native to
                # the host running this cross-platform regression.
                with patch.object(sys, 'platform', 'darwin'):
                    return real_scratch_processes(*args, **kwargs)

            with (patch.object(sys, 'argv', ['gate-budget.py', str(gate)]),
                  patch.object(budget, 'scratch_processes', side_effect=macos_handles),
                  patch.object(budget, 'MACOS_HANDLE_PROBE', [str(probe)]),
                  patch.dict(os.environ, env),
                  patch.object(os, 'kill', wraps=real_kill) as kill,
                  contextlib.redirect_stdout(output), contextlib.redirect_stderr(output)):
                self.assertEqual(budget.main(), 125)
            self.assertLess(time.monotonic() - started, 10)
            self.assertIn('fixture inventory failed; scratch retained', output.getvalue())
            self.assertIn('gate quick: exit=125', output.getvalue())
            self.assertEqual(len(list((root / '.gate').iterdir())), 1)
            self.assertEqual(kill.call_count, 1)  # Only the stuck probe.
            self.assertEqual(kill.call_args.args,
                             (int((root / 'probe-pid').read_text()), signal.SIGKILL))

    def test_uninterruptible_probe_has_bounded_reap(self):
        for complete, timeout in ((False, 5), (True, 30)):
            with self.subTest(complete=complete):
                probe = unittest.mock.Mock()
                probe.wait.side_effect = subprocess.TimeoutExpired('inventory', timeout)
                with (patch.object(sys, 'platform', 'darwin'),
                      patch.object(subprocess, 'Popen', return_value=probe) as launch):
                    with self.assertRaises(subprocess.TimeoutExpired):
                        budget.scratch_processes('/scratch', {123, 456}, all_handles=complete)
                self.assertEqual(probe.wait.call_args_list,
                                 [unittest.mock.call(timeout=timeout), unittest.mock.call(timeout=.2)])
                probe.kill.assert_called_once_with()
                command = launch.call_args.args[0]
                self.assertEqual(command[:2], budget.MACOS_HANDLE_PROBE)

    def test_success_cleans_fixture(self):
        scripts = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'scripts').mkdir()
            gate = root / 'scripts/gate.sh'
            gate.write_text('test -z "${GATE_CLEANUP_RECEIPT+x}" || exit 9\n'
                            'test -z "${GATE_CLEANUP_ID+x}" || exit 9\n'
                            'cd "$GATE_SCRATCH"\nsleep 60 &\nexit 0\n')
            receipt = root / 'cleanup.json'
            env = dict(os.environ, GATE_CLEANUP_RECEIPT=str(receipt), GATE_CLEANUP_ID='test-nonce')
            other = subprocess.Popen(['sleep', '60'], start_new_session=True)
            try:
                run = subprocess.run([sys.executable, str(scripts / 'gate-budget.py'), str(gate)],
                                     capture_output=True, text=True, timeout=15, env=env)
                self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
                self.assertEqual(json.loads(receipt.read_text()),
                    dict(schema=1, identity='test-nonce', stopped=True, status=0))
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
            child.wait.return_value = 124
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
    if child.args[0] == 'bash':
        deadline = time.monotonic() + 10
        while not pathlib.Path(ready).exists():
            if time.monotonic() >= deadline: raise RuntimeError('fixture not ready')
            time.sleep(.01)
        return 124
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
            for name in ("gate-scheduler.py", "test-gate-scheduler.py"):
                shutil.copyfile(scripts / name, root / "scripts" / name)
            shutil.copyfile(scripts / "gate-progress.py", root / "scripts/gate-progress.py")
            shutil.copyfile(scripts / "gate-budget.py", root / "scripts/gate-budget.py")
            shutil.copyfile(scripts / "gate-handles.py", root / "scripts/gate-handles.py")
            for name in ("gate-capacity.py", "gate-cache.py", "gate-memory.py"):
                shutil.copyfile(scripts / name, root / "scripts" / name)
            shutil.copyfile(scripts / "test-gate-progress.py", root / "scripts/test-gate-progress.py")
            # This fixture exercises shell/budget termination, not nested
            # regression suites. Their real child-process timing consumes the
            # readiness/cleanup deadline before the fake build is reached.
            # The suites run independently in the gate and validation batch.
            for name in ("test-gate-progress.py", "test-gate-scheduler.py"):
                (root / "scripts" / name).write_text('# isolated termination fixture\n')
            (root / "scripts/gate-related.py").write_text("")
            go = root / "bin/go"
            go.write_text(f'''#!{sys.executable}
import os, signal, sys, time
from pathlib import Path
if sys.argv[1] == "env":
    print({str(root)!r})
else:
    Path({str(root / 'ready')!r}).write_text(str(os.getpid()))
    signal.signal(signal.SIGTERM, lambda *_: time.sleep(.3))
    signal.pause()
''')
            go.chmod(0o755)
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
                       MYCE_PROCESS_SESSION="1")
            env.pop("MYCE_GATE_BUDGET", None)
            env["GATE_SCRATCH"] = str(root / "scratch")
            (root / "scratch").mkdir()
            # Setup does not need real Go, uv, Git state, or a full gate run.
            for name, body in {"git": "exit 0", "uv": f"echo {sys.executable}"}.items():
                stub = root / "bin" / name
                stub.write_text("#!/bin/sh\n" + body + "\n")
                stub.chmod(0o755)
            for budgeted, repeated in ((False, False), (False, True), (True, False)):
                with self.subTest(budgeted=budgeted, repeated=repeated):
                    ready = root / "ready"
                    ready.unlink(missing_ok=True)
                    receipt = root / 'cleanup.json'
                    receipt.unlink(missing_ok=True)
                    env.update(GATE_CLEANUP_RECEIPT=str(receipt), GATE_CLEANUP_ID='termination-fixture')
                    if budgeted:
                        # Trigger the production timeout branch once the fake build
                        # is ready, without changing the production budget.
                        launcher = '''import pathlib, runpy, subprocess, sys, time
from unittest.mock import patch
original = subprocess.Popen.wait
def wait(child, timeout=None):
    if child.args[0] == 'bash':
        deadline = time.monotonic() + 10
        while not pathlib.Path(ready).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("fake build did not start")
            time.sleep(.01)
        return 124
    return original(child, timeout)
script, ready, gate = sys.argv[1:]
sys.argv = [script, gate, "quick", "HEAD"]
with patch.object(subprocess.Popen, "wait", wait):
    runpy.run_path(script, run_name="__main__")
'''
                        command = [sys.executable, "-c", launcher, str(scripts / "gate-budget.py"), str(ready), str(root / "scripts/gate.sh")]
                        expected = 124
                    else:
                        command = [sys.executable, str(scripts / "gate-budget.py"), str(root / "scripts/gate.sh"), "full"]
                        expected = 143
                    child = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
                    try:
                        if not budgeted:
                            deadline = time.monotonic() + 10
                            while not ready.exists():
                                if time.monotonic() >= deadline:
                                    self.fail("fake build did not start")
                                time.sleep(.01)
                            child.terminate()
                            if repeated:
                                time.sleep(.05)  # Independent host/native guard observations.
                                child.terminate()
                        output, _ = child.communicate(timeout=15)
                        self.assertEqual(child.returncode, expected, output)
                        self.assertNotIn("exit=0", output)
                        mode = "quick" if budgeted else "full"
                        self.assertIn(f"gate {mode}: exit={expected}", output)
                        self.assertEqual(json.loads(receipt.read_text()),
                                         dict(schema=1, identity='termination-fixture',
                                              stopped=True, status=expected))
                        if budgeted:
                            self.assertIn("exit=124", output)
                    finally:
                        # Every fixture shares our explicitly owned session.
                        # A deliberately broken cleanup must not strand a shell
                        # that starts the next fake command after its Go exits.
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        child.communicate()
                        if ready.exists():
                            fixture_pid = int(ready.read_text())
                            probe = subprocess.run(['ps', '-p', str(fixture_pid), '-o', 'args='],
                                                   capture_output=True, text=True)
                            if str(go) in probe.stdout:
                                os.kill(fixture_pid, signal.SIGKILL)


if __name__ == "__main__":
    unittest.main()
