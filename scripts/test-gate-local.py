#!/usr/bin/env python3
"""Protect admission, disk refusal, and eviction boundaries on shared hosts."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import sys
import tarfile
import signal
import threading
import shutil
import time
import unittest
from unittest.mock import patch, Mock


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


capacity = load('gate-capacity')
local = load('gate-local')
linux = load('gate-linux')
measurement = load('gate-measure')


class LocalGateTests(unittest.TestCase):
    def setUp(self):
        for name, value in [('legacy_directories', []), ('native_full_custody', False), ('check_pressure', None)]:
            mock = patch.object(capacity, name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)

    def test_legacy_owners_are_preserved_and_new_owner_releases_only_its_lane(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            legacy = root / 'local-go-slot'
            legacy.mkdir()
            (legacy / 'owner').write_text('another owner')
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                slot = capacity.acquire(root)
                self.assertEqual(slot.directory.name, 'local-go-slot-2')
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root)
                slot.release()
            self.assertEqual((legacy / 'owner').read_text(), 'another owner')

    def test_full_is_serial_until_measured_and_custody_failure_holds_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                first = capacity.acquire(root, mode='full')
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full')
                with contextlib.redirect_stdout(io.StringIO()):
                    first.release(stopped=False)
                self.assertTrue(first.directory.exists())
                self.assertTrue(first.full.exists())
                first.release()

    def test_release_waiter_gets_next_lane_before_normal_work(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            waiter = root / 'local-go-release-wait-other'
            waiter.touch()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                with self.assertRaisesRegex(RuntimeError, 'reserved for release'):
                    capacity.acquire(root)
                release = capacity.acquire(root, priority='release')
                release.release()
            self.assertTrue(waiter.exists())

    def test_calibration_gets_next_full_lane_ahead_of_release_full(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'local-go-calibration-wait-owner').touch()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                with self.assertRaisesRegex(RuntimeError, 'reserved'):
                    capacity.acquire(root, mode='full', priority='release')
                # The full reservation does not block native release assembly.
                build = capacity.acquire(root, priority='release')
                build.release()
                full = capacity.acquire(root, mode='full', priority='calibration')
                full.release()

    def test_disk_refusal_starts_nothing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            disk = type('Disk', (), {'free': capacity.ENTRY_FLOOR - 1})()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity.shutil, 'disk_usage', return_value=disk)):
                with self.assertRaisesRegex(RuntimeError, 'entry floor'):
                    capacity.acquire(root)
            self.assertFalse(any(root.glob('local-go-slot*')))

    def test_full_waiters_are_fifo_and_killed_queue_ticket_does_not_hold_execution_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            script = """import importlib.util,pathlib,sys
p,folder,label=sys.argv[1:]
s=importlib.util.spec_from_file_location('cap',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
r=pathlib.Path(folder)
m.root=lambda _:r;m.legacy_directories=lambda _:[];m.native_full_custody=lambda:False
m.available_memory=lambda:64*m.GIB;m.check_pressure=lambda:None
slot=m.acquire(r,mode='full',wait=True)
with (r/'order').open('a') as out:out.write(label+'\\n')
slot.release()
"""
            children = []
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                held = capacity.acquire(root, mode='full')
                try:
                    for label in ('first', 'second', 'killed'):
                        child = subprocess.Popen([sys.executable, '-c', script, capacity.__file__,
                            str(root), label], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        children.append(child)
                        deadline = time.monotonic() + 5
                        while len(list(root.glob('local-go-wait-*.json'))) < len(children):
                            if time.monotonic() > deadline:
                                self.fail('waiter did not register')
                            time.sleep(.01)
                    children[-1].kill(); children[-1].wait()
                    with capacity.admission_lock(root):
                        self.assertEqual(len(capacity.full_waiters(root)), 2)
                    self.assertTrue(held.directory.exists())  # Never clear execution custody.
                    held.release()
                    for child in children[:-1]:
                        output, error = child.communicate(timeout=8)
                        self.assertEqual(child.returncode, 0, error)
                        self.assertIn(b'full queue position', output)
                    self.assertEqual((root / 'order').read_text().splitlines(), ['first', 'second'])
                finally:
                    for child in children:
                        if child.poll() is None: child.kill(); child.wait()
                        child.stdout.close(); child.stderr.close()

    def test_ordinary_full_cli_waits_by_default_and_term_cleans_queue(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            script = """import importlib.util,pathlib,sys
p,folder=sys.argv[1:]
s=importlib.util.spec_from_file_location('local',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
r=pathlib.Path(folder);c=m.capacity
c.root=lambda _:r;c.legacy_directories=lambda _:[];c.native_full_custody=lambda:False
c.available_memory=lambda:64*c.GIB;c.check_pressure=lambda:None
sys.argv=[p,'full','--platform','native','--repo',str(r)]
sys.exit(m.main())
"""
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                held = capacity.acquire(root, mode='full')
                child = subprocess.Popen([sys.executable, '-c', script, local.__file__, str(root)],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 5
                    while not list(root.glob('local-go-wait-*.json')):
                        if time.monotonic() > deadline: self.fail('ordinary full did not queue')
                        time.sleep(.01)
                    self.assertIsNone(child.poll())
                    child.terminate()
                    child.communicate(timeout=5)
                    self.assertEqual(child.returncode, 143)
                    self.assertFalse(list(root.glob('local-go-wait-*.json')))
                    self.assertTrue(held.directory.exists())
                finally:
                    if child.poll() is None: child.kill(); child.wait()
                    held.release()

    def test_measured_native_and_linux_memory_replace_initial_guard(self):
        config = dict(memory_measurement=dict(native_peak_bytes=4919132160,
            vm_peak_bytes=315785216, headroom_bytes=5835636736,
            linux_provisional_bytes=6 * capacity.GIB, linux_peak_bytes=None,
            evidence=['completed native receipt and host observation sidecar']))
        self.assertEqual(capacity.memory_requirement(config, 'native'), 10754768896)
        self.assertEqual(capacity.memory_requirement(config, 'both'), 12593872896)
        config['memory_measurement']['linux_peak_bytes'] = 7 * capacity.GIB
        self.assertEqual(capacity.memory_requirement(config, 'linux'),
                         7 * capacity.GIB + 315785216 + 5835636736)

    def test_trim_removes_only_old_rebuildable_go_objects(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            bucket = root / 'ab'
            bucket.mkdir()
            old = bucket / ('a' * 64 + '-d')
            new = bucket / ('b' * 64 + '-a')
            old.write_bytes(b'old cache')
            new.write_bytes(b'new')
            os.utime(old, (1, 1))
            evidence = root / 'myce-upgrade-compat'
            evidence.mkdir()
            (evidence / 'source.tar').write_text('retained')
            sentinel = bucket / 'unrelated'
            sentinel.write_text('keep')
            self.assertEqual(local.trim_cache(root, limit=3), 3)
            self.assertFalse(old.exists())
            self.assertTrue(new.exists())
            self.assertEqual(sentinel.read_text(), 'keep')
            self.assertEqual((evidence / 'source.tar').read_text(), 'retained')


    def test_legacy_unknown_owner_blocks_full_even_with_free_second_lane(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            legacy = root / 'old'
            (legacy / 'local-go-slot').mkdir(parents=True)
            (legacy / 'local-go-slot/owner').write_text('legacy full or unknown')
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'legacy_directories', return_value=[legacy]),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full')
                self.assertTrue((legacy / 'local-go-slot/owner').exists())

    def test_ordinary_clones_share_full_admission_and_cache_use(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            a, b = root / 'a', root / 'b'
            a.mkdir(); b.mkdir()
            # Real root depends on HOME, never on a clone's Git identity.
            self.assertEqual(capacity.root(a), capacity.root(b))
            with (patch.object(capacity, 'root', return_value=root / 'control'),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                first = capacity.acquire(a, mode='full')
                self.assertIn(first.directory, capacity.occupied_lanes(capacity.root(b)))
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(b, mode='full')
                first.release()

    def test_source_archive_identifies_dirty_bytes_and_freezes_both_legs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            repo = root / 'repo'
            repo.mkdir()
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            source = repo / 'product.txt'
            source.write_text('committed')
            (repo / 'go.mod').write_text('module fixture\n')
            (repo / 'internal/myce').mkdir(parents=True)
            (repo / 'internal/myce/source.go').write_text('package myce\n')
            subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
            subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Fixture', '-c',
                            'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture'], check=True)
            source.write_text('dirty')
            archive = root / 'source.tar'
            identity = local.sources.freeze(repo, archive)
            self.assertTrue(identity['source_dirty'])
            source.write_text('later edit')
            for name in ('native', 'linux'):
                destination = root / name
                destination.mkdir()
                local.sources.extract(archive, destination)
                self.assertEqual((destination / 'product.txt').read_text(), 'dirty')
            self.assertNotEqual(identity['source_archive_sha256'],
                                local.sources.freeze(repo, root / 'later.tar')['source_archive_sha256'])
            frozen = root / 'native'
            (frozen / 'scripts').mkdir()
            (frozen / 'scripts/upgrade-compat.py').write_text('\n'.join(
                key + ' = ' + repr(identity['source_sha']) for key in
                ('PREVIOUS', 'INVENTORY_BASELINE', 'CADENCE_BASELINE')))
            # Reproduce the empty nested archive incident: cwd is below Git root.
            previous = Path.cwd()
            try:
                os.chdir(frozen)
                archives = local.sources.predecessors(repo, frozen)
            finally:
                os.chdir(previous)
            self.assertEqual(len(archives), 3)
            with tarfile.open(archives[0]) as tar:
                self.assertIn('go.mod', tar.getnames())
                self.assertIn('internal/myce/source.go', tar.getnames())

    def test_unknown_native_exit_never_starts_linux_or_confirms_capacity(self):
        for code in (-9, 255):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                repo = root / 'repo'
                (repo / 'scripts').mkdir(parents=True)
                path = root / 'manifest.json'
                path.write_text(json.dumps(dict(repo=str(repo), cache=str(root / 'cache'), cores=4,
                    report=str(root / 'report'), mode='quick', command=['true'], platform='both', identity='run')))
                child = Mock()
                child.wait.return_value = code
                child.poll.return_value = code
                with (patch.object(local.sources, 'freeze', return_value={'source_sha': 'test'}),
                      patch.object(local.sources, 'extract', side_effect=lambda a, d: (d / 'scripts').mkdir()),
                      patch.object(local.sources, 'runtime_archive'),
                      patch.object(local.subprocess, 'Popen', return_value=child) as launch):
                    self.assertEqual(local.execute(path), 125)
                self.assertEqual(launch.call_count, 1)
                self.assertFalse(local.receipts.confirmed(root / 'overall-cleanup.json', 'run'))

    def test_linux_transport_loss_cannot_publish_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            archive = root / 'source.tar'
            with tarfile.open(archive, 'w'): pass
            receipt = root / 'cleanup.json'
            with (patch.object(sys, 'argv', ['gate-linux.py', '--archive', str(archive),
                    '--cleanup-receipt', str(receipt), '--cleanup-id', 'nonce', '--', 'true']),
                  patch.object(linux.Transport, 'run', side_effect=[0, 0, 255]) as launch):
                self.assertEqual(linux.main(), 125)
            self.assertFalse(receipt.exists())
            self.assertEqual(launch.call_count, 3)  # start, stage, run; no success inference.

    def test_linux_staging_has_cancellation_custody_before_first_side_effect(self):
        with tempfile.TemporaryDirectory() as folder:
            archive = Path(folder) / 'source.tar'
            with tarfile.open(archive, 'w'): pass
            calls = []
            def run(transport, command, **kwargs):
                calls.append(command)
                handler = signal.getsignal(signal.SIGTERM)
                self.assertEqual(handler.__self__, transport)
                if len(calls) == 2:
                    handler(signal.SIGTERM, None)
                    raise linux.Cancelled()
                return 0
            with (patch.object(sys, 'argv', ['gate-linux.py', '--archive', str(archive), '--', 'true']),
                  patch.object(linux.Transport, 'run', run)):
                self.assertEqual(linux.main(), 125)
            self.assertEqual(len(calls), 2)  # Never launches a guest after cancelled staging.
        transport = linux.Transport('vm', 'remote', 'nonce')
        timer = threading.Timer(.2, lambda: transport.stop(signal.SIGTERM, None))
        timer.start()
        try:
            with self.assertRaises(linux.Cancelled):
                transport.run([sys.executable, '-c', 'import time; time.sleep(60)'])
            self.assertIsNone(transport.current)  # Owned local child was reaped.
        finally:
            timer.join()

    def test_linux_receives_frozen_helpers_even_after_live_helper_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            frozen = root / 'native'
            (frozen / 'scripts').mkdir(parents=True)
            helper = frozen / 'scripts/frozen.py'
            helper.write_text('frozen bytes')
            archive = root / 'source.tar'
            with tarfile.open(archive, 'w'): pass
            runtime = root / 'runtime.tar'
            local.sources.runtime_archive(archive, runtime, frozen, ['frozen.py'])
            helper.write_text('later live bytes')
            def run(transport, command, **kwargs):
                if 'stdin' in kwargs:
                    with tarfile.open(fileobj=kwargs['stdin']) as tar:
                        self.assertEqual(tar.extractfile('scripts/frozen.py').read(), b'frozen bytes')
                return 255 if kwargs.get('heartbeat') else 0
            with (patch.object(sys, 'argv', ['gate-linux.py', '--archive', str(runtime), '--', 'true']),
                  patch.object(linux.Transport, 'run', run)):
                self.assertEqual(linux.main(), 125)

    def test_guest_eof_prevents_payload_and_unknown_cache_marker_prevents_eviction(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            scripts = root / 'scripts'
            scripts.mkdir()
            for name in ('gate-guest.py', 'gate-receipt.py'):
                shutil.copyfile(Path(__file__).with_name(name), scripts / name)
            (scripts / 'gate-cache.py').write_text('def trim_cache(path): raise RuntimeError("evicted active cache")\n')
            (scripts / 'gate-budget.py').write_text(
                'import json,os,pathlib\npathlib.Path("payload").touch()\n'
                'pathlib.Path(os.environ["GATE_CLEANUP_RECEIPT"]).write_text(json.dumps(dict('
                'schema=1,identity=os.environ["GATE_CLEANUP_ID"],stopped=True,status=0)))\n')
            active = root / '.cache/mycelium-gates/active'
            active.mkdir(parents=True)
            (active / 'dead-supervisor').write_text('unknown old cache user')
            command = [sys.executable, str(scripts / 'gate-guest.py'), 'nonce', '4', 'true']
            env = dict(os.environ, HOME=str(root))
            first = subprocess.run(command, input=b'', cwd=root, env=env, capture_output=True, timeout=5)
            self.assertEqual(first.returncode, 143, first.stderr)
            self.assertFalse((root / 'payload').exists())
            child = subprocess.Popen(command, stdin=subprocess.PIPE, cwd=root, env=env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                child.stdin.write(b'.'); child.stdin.flush()
                self.assertEqual(child.wait(timeout=5), 0, child.stderr.read())
            finally:
                child.stdin.close()
                if child.poll() is None:
                    child.kill(); child.wait()
                child.stdout.close(); child.stderr.close()
            self.assertTrue((root / 'payload').exists())
            self.assertTrue((active / 'dead-supervisor').exists())
            self.assertFalse((active / 'nonce').exists())

    def test_cancellation_between_legs_and_during_launch_handoff_revokes_linux(self):
        for timing in ('between', 'handoff'):
            with self.subTest(timing=timing), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                path = root / 'manifest.json'
                path.write_text(json.dumps(dict(repo=str(root), cache=str(root / 'cache'), cores=4,
                    report=str(root / 'report'), mode='full', command=[], platform='both', identity='run')))
                calls, children, ids = [], [], []
                def nonce():
                    ids.append(1)
                    if timing == 'between' and len(ids) == 2:
                        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                    return Mock(hex='nonce-' + str(len(ids)))
                def launch(command, **kwargs):
                    calls.append(command)
                    child = Mock()
                    children.append(child)
                    if len(calls) == 1:
                        env = kwargs['env']
                        local.receipts.publish(env['GATE_CLEANUP_RECEIPT'], env['GATE_CLEANUP_ID'],
                                               stopped=True, status=0)
                        child.wait.return_value = child.poll.return_value = 0
                    else:
                        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                        child.poll.return_value = None
                        def wait():
                            child.poll.return_value = 143
                            local.receipts.publish(root / 'linux-cleanup.json', 'nonce-2', stopped=True, status=143)
                            return 143
                        child.wait.side_effect = wait
                    return child
                with (patch.object(local.sources, 'freeze', return_value={'source_sha': 'test'}),
                      patch.object(local.sources, 'extract', side_effect=lambda a, d: (d / 'scripts').mkdir()),
                      patch.object(local.sources, 'runtime_archive'),
                      patch.object(local.sources, 'predecessors', return_value=[]),
                      patch.object(local, 'uuid', Mock(uuid4=nonce)),
                      patch.object(local.subprocess, 'Popen', side_effect=launch)):
                    self.assertEqual(local.execute(path), 125)
                self.assertEqual(len(calls), 1 if timing == 'between' else 2)
                if timing == 'handoff':
                    children[-1].send_signal.assert_called_once_with(signal.SIGTERM)
                self.assertTrue(local.receipts.confirmed(root / 'overall-cleanup.json', 'run'))

    def test_dead_priority_reservations_do_not_survive_their_waiters(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            identity = capacity.process_identity
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB),
                  patch.object(capacity, 'process_identity', side_effect=lambda pid: None if pid == 999999 else identity(pid))):
                held = capacity.acquire(root, mode='full')
                row = dict(pid=999999, process_identity='old process', sequence=0, priority='release')
                for priority in ('release', 'calibration'):
                    (root / ('local-go-' + priority + '-wait-dead')).write_text(json.dumps(row))
                (root / 'local-go-wait-dead.json').write_text(json.dumps(row))
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full')
                self.assertTrue(held.directory.exists())
                self.assertTrue(held.full.exists())
                held.release()
                next_full = capacity.acquire(root, mode='full')
                next_full.release()
            self.assertFalse(list(root.glob('local-go-*-wait-dead')))

    def test_native_receipt_cannot_hide_a_missing_linux_cleanup_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = root / 'manifest.json'
            path.write_text(json.dumps(dict(repo=str(root), cache=str(root / 'cache'), cores=4,
                report=str(root / 'report'), mode='full', command=[], platform='both', identity='run')))
            calls = []
            def launch(command, **kwargs):
                calls.append(command)
                child = Mock()
                if len(calls) == 1:
                    self.assertEqual(Path(command[1]), root / 'source/scripts/gate-measure.py')
                    env = kwargs['env']
                    local.receipts.publish(env['GATE_CLEANUP_RECEIPT'], env['GATE_CLEANUP_ID'],
                                           stopped=True, status=0)
                    child.wait.return_value = child.poll.return_value = 0
                else:
                    child.wait.return_value = child.poll.return_value = 255
                return child
            with (patch.object(local.sources, 'freeze', return_value={'source_sha': 'test'}),
                  patch.object(local.sources, 'extract', side_effect=lambda a, d: (d / 'scripts').mkdir()),
                      patch.object(local.sources, 'runtime_archive'),
                  patch.object(local.sources, 'predecessors', return_value=[]),
                  patch.object(local.subprocess, 'Popen', side_effect=launch)):
                self.assertEqual(local.execute(path), 125)
            self.assertEqual(len(calls), 2)
            self.assertFalse(local.receipts.confirmed(root / 'overall-cleanup.json', 'run'))

    def test_host_disk_guard_stops_linux_preparation_despite_guest_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            report = root / 'metrics.jsonl'
            high = type('Disk', (), {'free': 46 * capacity.GIB})()
            low = type('Disk', (), {'free': 41 * capacity.GIB})()
            with (patch.object(measurement.subprocess, 'check_output', return_value=str(root)),
                  patch.object(measurement, 'cache_bytes', return_value=0),
                  patch.object(measurement.shutil, 'disk_usage', side_effect=[high, low, low]),
                  patch.object(measurement.memory, 'snapshot', return_value=None),
                  patch.object(measurement, 'tree_rss') as rss):
                status = measurement.measure([sys.executable, '-c', 'import time; time.sleep(10)'],
                    report, 'full', 'test', phase='host', disk_root=root)
            self.assertEqual(status, 125)
            rss.assert_not_called()  # The host floor check precedes guest/probe work.
            row = json.loads(report.read_text())
            self.assertEqual(row['phase'], 'host')
            self.assertLess(row['min_free_bytes'], capacity.STOP_FLOOR)

    def test_release_command_uses_priority_admission_and_host_supervision(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            slot = Mock()
            slot.directory = root / 'lane'
            output = root / 'release-binary'
            def supervised(command, **kwargs):
                manifest_path = Path(command[-1])
                manifest = json.loads(manifest_path.read_text())
                self.assertEqual(manifest['command'], ['go', 'build', '-o', str(output)])
                self.assertIn('--disk-root', command)
                local.receipts.publish(manifest_path.parent / 'overall-cleanup.json',
                    manifest['identity'], stopped=True, status=0)
                child = Mock()
                child.wait.return_value = 0
                return child
            with (patch.object(sys, 'argv', ['gate-local.py', '--repo', str(root), '--priority',
                    'release', '--command', 'go', 'build', '-o', str(output)]),
                  patch.object(local.capacity, 'acquire', return_value=slot) as admission,
                  patch.object(local.capacity, 'root', return_value=root / 'control'),
                  patch.object(local.capacity, 'legacy_directories', return_value=[]),
                  patch.object(local.capacity, 'native_full_custody', return_value=False),
                  patch.object(local, 'trim_cache'), patch.object(local.shutil, 'rmtree'),
                  patch.object(Path, 'unlink'),
                  patch.object(local.subprocess, 'Popen', side_effect=supervised)):
                self.assertEqual(local.main(), 0)
            self.assertEqual(admission.call_args.kwargs['priority'], 'release')
            self.assertTrue(admission.call_args.kwargs['wait'])
            slot.release.assert_called_once_with(stopped=True)

    def test_memory_guard_stops_owned_work_on_pressure_or_new_swapouts(self):
        for current in (dict(pressure_level=2, swapouts=0),
                        dict(pressure_level=1, swapouts=1)):
            with self.subTest(current=current), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                base = dict(pressure_level=1, swapouts=0)
                with (patch.object(measurement.subprocess, 'check_output', return_value=str(root)),
                      patch.object(measurement, 'cache_bytes', return_value=0),
                      patch.object(measurement.memory, 'snapshot', side_effect=[base, current])):
                    result = measurement.measure([sys.executable, '-c', 'import time; time.sleep(10)'],
                        root / 'report', 'full', 'test', phase='host')
                self.assertEqual(result, 125)
                self.assertTrue(json.loads((root / 'report').read_text())['refusal'])


if __name__ == '__main__':
    unittest.main()
