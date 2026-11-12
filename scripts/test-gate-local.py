#!/usr/bin/env python3
"""Protect admission, disk refusal, and eviction boundaries on shared hosts."""
import contextlib
import hashlib
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


def qualified_config(root):
    native, host, vm, headroom = 7 * capacity.GIB, capacity.GIB // 8, 10 * capacity.GIB, 5 * capacity.GIB
    evidence = root / 'resource-measurements.jsonl'
    evidence.write_text(''.join(json.dumps(row) + '\n' for row in [
        dict(mode='full', status=1, phase='native', max_tree_rss_sum_bytes=native),
        dict(mode='full', status=0, phase='host', run_id='linux-measurement',
             max_tree_rss_sum_bytes=host, samples=[dict(host_memory=dict(all_vz_rss_sum_bytes=vm))]),
        dict(mode='full', status=0, phase='linux', run_id='linux-measurement')]))
    config = dict(workstation_cpu_reserve_cores=6, full_slots=1,
        resource_admission=dict(schema=1, measured_cores=4, vm_cores=8, vm_memory_bytes=24 * capacity.GIB),
        memory_measurement=dict(native_peak_bytes=native, linux_peak_bytes=host,
            vm_peak_bytes=vm, headroom_bytes=headroom,
            evidence=[dict(path=str(evidence), sha256=hashlib.sha256(evidence.read_bytes()).hexdigest())]))
    (root / 'gate-capacity.json').write_text(json.dumps(config))
    return config


class LocalGateTests(unittest.TestCase):
    def test_killed_holder_is_reclaimed_with_live_peer_and_waiter(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            qualified_config(root)
            script = str(Path(__file__).with_name('gate-capacity.py').resolve())
            launcher = '''import importlib.util,json,pathlib,signal,sys
s=importlib.util.spec_from_file_location('capacity',sys.argv[1]); c=importlib.util.module_from_spec(s);s.loader.exec_module(c)
r=pathlib.Path(sys.argv[2]);c.root=lambda _:r;c.legacy_directories=lambda _:[]
c._memory.evidence_directory=lambda:r/'evidence'
c.local_worker_usage=lambda:None;c.native_full_custody=lambda:False;c.check_pressure=lambda:None
c.available_memory=lambda:64*c.GIB;c.total_memory=lambda:64*c.GIB;c.os.cpu_count=lambda:18
slot=c.acquire(r,mode='full',source_root=r/'dead'/'source')
p=pathlib.Path(slot.owner['scratch']);p.mkdir(parents=True);(p/'fixture').write_text('rebuildable')
print(json.dumps(slot.owner),flush=True);signal.pause()
'''
            child = subprocess.Popen([sys.executable, '-c', launcher, script, str(root)],
                                     stdout=subprocess.PIPE, text=True)
            try:
                dead = json.loads(child.stdout.readline())
                with (patch.object(capacity, 'root', return_value=root),
                      patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB),
                      # Admission/recovery bookkeeping is independent of a
                      # host-wide handle scan, which may correctly defer scratch
                      # collection (e.g. a same-second non-dumpable PAM helper).
                      patch.object(capacity._cache, 'scratch_idle', return_value=True)):
                    peer = capacity.acquire(root, mode='full')
                    waiter = root / 'local-go-release-wait-peer'
                    waiter.write_text(json.dumps(dict(pid=os.getpid(),
                        process_identity=capacity.process_identity(os.getpid()),
                        priority='normal', sequence=0)))
                    child.kill()
                    child.wait(timeout=5)
                    next_gate = capacity.acquire(root)
                    try:
                        self.assertTrue(peer.directory.exists())
                        self.assertFalse(waiter.exists())  # Retired priority label, not its live process.
                        self.assertNotEqual(peer.owner['run_id'], next_gate.owner['run_id'])
                        self.assertFalse(Path(dead['scratch']).exists())
                        archives = list((root / 'selfheal-backups').iterdir())
                        self.assertEqual(len(archives), 1)
                        saved = json.loads((archives[0] / 'recovery.json').read_text())
                        self.assertEqual(saved['owner'], dead)
                        moved = Path(json.loads((archives[0] / 'scratch.json').read_text())['backup'])
                        self.assertEqual((moved / 'fixture').read_text(), 'rebuildable')
                    finally:
                        next_gate.release()
                        peer.release()
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=5)
                child.stdout.close()

    def test_failed_scratch_inventory_defers_collection_without_holding_admission(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                slot = capacity.acquire(root, source_root=root / 'source')
                scratch = Path(slot.owner['scratch'])
                scratch.mkdir(parents=True)
                (scratch / 'fixture').write_text('rebuildable')
                with patch.object(capacity._cache, 'scratch_idle', side_effect=PermissionError('fixture inventory denied')):
                    slot.release(stopped=False)
                    next_gate = capacity.acquire(root)
                    next_gate.release()
                self.assertFalse(slot.directory.exists())
                self.assertTrue(scratch.exists())
                archives = list((root / 'selfheal-backups').iterdir())
                self.assertEqual(len(archives), 1)
                self.assertTrue((archives[0] / 'scratch-pending.json').exists())
                self.assertIn('fixture inventory denied', (archives[0] / 'scratch-retained.txt').read_text())
                with patch.object(capacity._cache, 'scratch_idle', return_value=True):
                    next_gate = capacity.acquire(root)
                    next_gate.release()
                self.assertFalse(scratch.exists())
                moved = Path(json.loads((archives[0] / 'scratch.json').read_text())['backup'])
                self.assertEqual((moved / 'fixture').read_text(), 'rebuildable')

    def test_reused_pid_reclaims_but_unknown_or_live_identity_does_not(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            for index, birth in enumerate(('old-start', 'current-start', None)):
                lane = root / f'local-go-slot-{index + 1}'
                lane.mkdir()
                owner = dict(pid=12345)
                if birth:
                    owner['process_identity'] = birth
                (lane / 'owner.json').write_text(json.dumps(owner))
            with capacity.admission_lock(root), patch.object(capacity, 'process_identity', return_value='current-start'):
                capacity.reclaim_dead(root)
            self.assertFalse((root / 'local-go-slot-1').exists())
            self.assertTrue((root / 'local-go-slot-2').exists())
            self.assertTrue((root / 'local-go-slot-3').exists())

    def test_live_mapped_fixture_protects_scratch_without_holding_admission(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                slot = capacity.acquire(root, source_root=root / 'source')
                scratch = Path(slot.owner['scratch'])
                scratch.mkdir(parents=True)
                (scratch / 'held').write_bytes(b'x' * 4096)
                fixture = subprocess.Popen([sys.executable, '-c',
                    'import mmap,os,signal,sys; f=open(sys.argv[1],"rb"); '
                    'm=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ); f.close(); '
                    'os.environ.clear(); print("ready",flush=True); signal.pause()',
                    str(scratch / 'held')], cwd='/', stdout=subprocess.PIPE, text=True)
                try:
                    self.assertEqual(fixture.stdout.readline().strip(), 'ready')
                    slot.release(stopped=False)
                    self.assertFalse(slot.directory.exists())
                    self.assertTrue(scratch.exists())
                    next_gate = capacity.acquire(root)
                    next_gate.release()
                    self.assertIsNone(fixture.poll())
                    fixture.terminate()
                    fixture.wait(timeout=5)
                    next_gate = capacity.acquire(root)
                    next_gate.release()
                    self.assertFalse(scratch.exists())
                finally:
                    if fixture.poll() is None:
                        fixture.kill()
                    fixture.wait(timeout=5)
                    fixture.stdout.close()

    def test_scratch_backups_expire_by_age_count_and_bytes_preserving_verdicts(self):
        cache = load('gate-cache')
        for bound in ('age', 'count', 'bytes'):
            with self.subTest(bound=bound), tempfile.TemporaryDirectory() as folder:
                root = Path(folder).resolve()
                backups = root / 'control/selfheal-backups'
                scratch = root / 'source/.gate/run.dead'
                scratch.mkdir(parents=True)
                (scratch / 'fixture').write_bytes(b'0123456789')
                verdict = root / 'source/cleanup-error.json'
                verdict.write_text('real failure')
                archive = backups / 'backup'
                archive.mkdir(parents=True)
                (archive / 'recovery.json').write_text(json.dumps(dict(owner={})))
                moved = cache.backup_scratch(str(scratch), archive)
                options = dict(age=100, count=8, limit=100)
                options.update(dict(age=0) if bound == 'age' else
                               dict(count=0) if bound == 'count' else dict(limit=1))
                with patch.object(cache, 'scratch_idle', return_value=True):
                    cache.trim_backups(backups, **options)
                self.assertFalse(archive.exists())
                self.assertFalse(moved.exists())
                self.assertEqual(verdict.read_text(), 'real failure')

    def setUp(self):
        fakehome = tempfile.TemporaryDirectory()
        self.addCleanup(fakehome.cleanup)
        fakebin = Path(fakehome.name)
        for memory in (capacity._memory, measurement.memory):
            retained = patch.object(memory, 'evidence_directory', return_value=fakebin / 'evidence')
            retained.start()
            self.addCleanup(retained.stop)
        for name, value in [('legacy_directories', []), ('native_full_custody', False),
                            ('check_pressure', None), ('local_worker_usage', None),
                            ('total_memory', 64 * capacity.GIB)]:
            mock = patch.object(capacity, name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        mock = patch.object(capacity.os, 'cpu_count', return_value=18)
        mock.start()
        self.addCleanup(mock.stop)

    def test_calibration_survives_source_worktree_removal(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                first = capacity.acquire(root, mode='full')
                first.release()
                (root / 'resource-measurements.jsonl').unlink()
                second = capacity.acquire(root, mode='full')
                try:
                    self.assertEqual(second.owner['admission_protocol'], 2)
                    self.assertEqual(capacity._memory.retained_calibration(config), config)
                finally:
                    second.release()

    def test_missing_calibration_falls_back_without_losing_fifo_or_live_custody(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            config['quick_memory_measurement'] = dict(peak_bytes=1, evidence=[])
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            (root / 'resource-measurements.jsonl').unlink()
            group = root / 'local-go-resource-group.custody'
            group.write_text(json.dumps(dict(resource_group='live owner custody')))
            lane = root / 'local-go-slot-3'
            lane.mkdir()
            owner = lane / 'owner.json'
            owner.write_text(json.dumps(dict(pid=os.getpid(), process_identity=capacity.process_identity(os.getpid()))))
            waits = []

            def cleanup(_):
                tickets = capacity.full_waiters(root)
                self.assertEqual(len(tickets), 1)
                waits.append(tickets[0])
                self.assertEqual(json.loads(group.read_text())['resource_group'], 'live owner custody')
                self.assertTrue(owner.exists())
                owner.unlink()
                lane.rmdir()
                group.unlink()  # Simulate its owner's confirmed cleanup.

            warning = io.StringIO()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB),
                  patch.object(capacity, 'time', Mock(sleep=cleanup)),
                  contextlib.redirect_stderr(warning)):
                run = capacity.acquire(root, mode='full', wait=True)
                try:
                    self.assertNotIn('admission_protocol', run.owner)
                    self.assertEqual(run.owner['memory_requirement_at_entry_bytes'], 22 * capacity.GIB)
                    self.assertEqual(len(waits), 1)
                finally:
                    run.release()
                # A dead/empty former group must also self-reclaim on fallback.
                group.write_text(json.dumps(dict(resource_group='empty former custody')))
                run = capacity.acquire(root, mode='full')
                run.release()
                self.assertFalse(group.exists())
            self.assertIn('evidence is missing', warning.getvalue())
            self.assertEqual(json.loads((root / 'gate-capacity.json').read_text()), config)

    def test_retained_calibration_digest_mismatch_still_refuses(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            (root / 'resource-measurements.jsonl').write_text('changed evidence')
            with self.assertRaisesRegex(ValueError, 'digest changed'):
                capacity._memory.retained_calibration(config)

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

    def test_full_is_serial_until_measured_and_cleanup_failure_releases_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                first = capacity.acquire(root, mode='full')
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full')
                with contextlib.redirect_stdout(io.StringIO()):
                    first.release(stopped=False)
                self.assertFalse(first.directory.exists())
                self.assertFalse(first.full.exists())
                replacement = capacity.acquire(root, mode='full')
                replacement.release()

    def test_measured_fulls_share_resources_and_old_clients_see_both_lanes_reserved(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            qualified_config(root)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                runs = [capacity.acquire(root, mode='full') for _ in range(3)]
                try:
                    # Three fulls fit 18 CPUs with six reserved and no worker
                    # demand; the old fixed full/slot counts no longer apply.
                    self.assertEqual(len(capacity.execution_lanes(root)), 3)
                    self.assertTrue(all(run.directory.name not in ('local-go-slot', 'local-go-slot-2') for run in runs))
                    # Frozen old clients count both these directories as occupied.
                    for name in ('local-go-slot', 'local-go-slot-2'):
                        self.assertTrue((root / name / 'owner.json').exists())
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode='full')
                    runs[0].release(stopped=False)
                    self.assertFalse(runs[0].directory.exists())
                    self.assertTrue((root / 'local-go-slot').exists())
                    config_path = root / 'gate-capacity.json'
                    original = config_path.read_text()
                    config = json.loads(original)
                    del config['resource_admission']
                    config_path.write_text(json.dumps(config))
                    with self.assertRaisesRegex(RuntimeError, 'qualification removed'):
                        capacity.acquire(root)
                    config_path.write_text(original)
                finally:
                    for run in runs:
                        if run.directory.exists(): run.release()
                self.assertFalse(capacity.occupied_lanes(root))
                self.assertFalse((root / 'local-go-resource-group.custody').exists())
                orphan = root / 'local-go-full-0.custody'
                orphan.write_text('unknown former full owner')
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full')
                self.assertEqual(orphan.read_text(), 'unknown former full owner')

    def test_resource_admission_reserves_future_memory_growth_and_vm_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            qualified_config(root)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=18 * capacity.GIB)):
                first = capacity.acquire(root, mode='full')
                try:
                    # Current free RAM covers the new run alone but not the
                    # still-growing first run's reserved peak as well.
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode='full')
                finally: first.release()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                runs = [capacity.acquire(root, mode='full', platform='linux') for _ in range(2)]
                try:
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode='full', platform='linux', cores=1)
                    with self.assertRaisesRegex(RuntimeError, 'measured local gate budget'):
                        capacity.acquire(root, cores=5)
                finally:
                    for run in runs: run.release()

    def test_exclusive_calibration_can_measure_an_unqualified_core_count(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                for platform, exclusive, priority in [('native', False, 'calibration'),
                        ('both', True, 'calibration'), ('native', True, 'normal')]:
                    with self.subTest(platform=platform, exclusive=exclusive, priority=priority):
                        with self.assertRaisesRegex(RuntimeError, 'measured local gate budget'):
                            capacity.acquire(root, mode='full', platform=platform, cores=8,
                                             exclusive=exclusive, priority=priority)
                run = capacity.acquire(root, mode='full', platform='native', cores=8,
                                       exclusive=True, priority='calibration')
                try:
                    self.assertTrue(run.owner['calibration'])
                    self.assertEqual(run.owner['peak_bytes'], 14 * capacity.GIB)
                    self.assertEqual(run.owner['memory_requirement_at_entry_bytes'], 19 * capacity.GIB)
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode='quick', cores=1)
                finally: run.release()
                run = capacity.acquire(root, mode='full', platform='linux', cores=8,
                                       exclusive=True, priority='calibration')
                try:
                    self.assertTrue(run.owner['calibration'])
                    self.assertEqual(run.owner['memory_requirement_at_entry_bytes'],
                                     2 * (10 * capacity.GIB + capacity.GIB // 8) + 5 * capacity.GIB)
                finally: run.release()
            config['resource_admission']['native_measured_cores'] = 8
            with self.assertRaisesRegex(ValueError, 'exclusive calibration'):
                capacity.resource_qualification(config)
            evidence = root / 'resource-measurements.jsonl'
            with evidence.open('a') as stream:
                stream.write(json.dumps(dict(mode='full', status=0, phase='native',
                    max_tree_rss_sum_bytes=7 * capacity.GIB,
                    admission=dict(cores=8, exclusive=True))) + '\n')
            config['memory_measurement']['evidence'][0]['sha256'] = hashlib.sha256(evidence.read_bytes()).hexdigest()
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                run = capacity.acquire(root, mode='full', platform='native', cores=8)
                try: self.assertFalse(run.owner['calibration'])
                finally: run.release()
                with self.assertRaisesRegex(RuntimeError, 'measured local gate budget'):
                    capacity.acquire(root, mode='full', platform='linux', cores=8)

    def test_full_defaults_follow_measured_memory_and_keep_the_cpu_share(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            evidence = root / 'eight-slots.jsonl'
            legacy = root / 'resource-measurements.jsonl'
            with legacy.open('a') as stream:
                stream.write(json.dumps(dict(mode='full', status=1, phase='native',
                    admission=dict(cores=8), max_tree_rss_sum_bytes=12 * capacity.GIB)) + '\n')
            config['memory_measurement']['native_peak_bytes'] = 12 * capacity.GIB
            config['memory_measurement']['evidence'][0]['sha256'] = hashlib.sha256(legacy.read_bytes()).hexdigest()
            observations = [
                dict(mode='full', phase='native', status=1, admission=dict(cores=8),
                     max_tree_rss_sum_bytes=10 * capacity.GIB),
                dict(mode='full', phase='linux', status=0, admission=dict(cores=8), run_id='eight'),
                dict(mode='full', phase='host', status=0, admission=dict(cores=8), run_id='eight',
                     max_tree_rss_sum_bytes=capacity.GIB // 8,
                     samples=[dict(host_memory=dict(all_vz_rss_sum_bytes=12 * capacity.GIB))])]
            evidence.write_text(''.join(json.dumps(r) + '\n' for r in observations))
            config['full_slot_measurement'] = dict(slots=8, evidence=[dict(path=str(evidence),
                sha256=hashlib.sha256(evidence.read_bytes()).hexdigest())])
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=28 * capacity.GIB)):
                for available, explicit, expected in [(28, None, 8), (25, None, 4), (28, 4, 4)]:
                    with self.subTest(available=available, explicit=explicit):
                        with patch.object(capacity, 'available_memory', return_value=available * capacity.GIB):
                            run = capacity.acquire(root, mode='full', cores=explicit, platform='both')
                        try:
                            self.assertEqual(run.owner['per_leg_cores'], expected)
                            self.assertEqual(run.owner['cores'], 8)
                            self.assertEqual(run.owner['cpu_share_per_leg'], 4)
                        finally: run.release()
                run = capacity.acquire(root, mode='quick', platform='both')
                try: self.assertEqual(run.owner['per_leg_cores'], 4)
                finally: run.release()
                with (patch.object(capacity, 'available_memory', side_effect=[21 * capacity.GIB, 28 * capacity.GIB]),
                      patch.object(capacity.time, 'sleep'), contextlib.redirect_stdout(io.StringIO())):
                    run = capacity.acquire(root, mode='full', cores=None, platform='both', wait=True)
                try: self.assertEqual(run.owner['per_leg_cores'], 8)
                finally: run.release()
                config['resource_admission']['vm_cores'] = 4
                (root / 'gate-capacity.json').write_text(json.dumps(config))
                run = capacity.acquire(root, mode='full', cores=None, platform='both')
                try: self.assertEqual(run.owner['per_leg_cores'], 4)
                finally: run.release()
            # A native-only observation cannot qualify a larger Linux slot set.
            evidence.write_text(json.dumps(observations[0]) + '\n')
            config['full_slot_measurement']['evidence'][0]['sha256'] = hashlib.sha256(evidence.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError, 'native and Linux/VM'):
                capacity.full_slot_profile(config)

    def test_both_reserves_two_host_budgets_but_one_guest_budget(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                both = capacity.acquire(root, mode='full', platform='both', cores=4)
                try:
                    self.assertEqual(both.owner['cores'], 8)
                    self.assertEqual(both.owner['per_leg_cores'], 4)
                    self.assertEqual(both.owner['peak_bytes'],
                        capacity.memory_requirement(config, 'both') - 5 * capacity.GIB)
                    self.assertEqual(both.owner['vm_peak_bytes'],
                        capacity.memory_requirement(config, 'linux') - 5 * capacity.GIB)
                    # Four more guest cores fit the 8-vCPU VM and 18-core host;
                    # native's CPU and RAM must not be charged to the guest.
                    guest = capacity.acquire(root, mode='full', platform='linux', cores=4)
                    try:
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, mode='quick', cores=1)
                    finally: guest.release()
                finally: both.release()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=22 * capacity.GIB)):
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full', platform='both', cores=4)

    def test_paired_quick_reserves_max_leg_only_when_combined_memory_does_not_fit(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            pair = capacity.memory_requirement(config, 'both', 'quick')
            largest = max(capacity.memory_requirement(config, leg, 'quick')
                          for leg in ('native', 'linux'))
            for available, sequential in ((pair, False), (pair - 1, True), (largest, True)):
                with self.subTest(available=available), \
                     patch.object(capacity, 'root', return_value=root), \
                     patch.object(capacity, 'available_memory', return_value=available):
                    slot = capacity.acquire(root, mode='quick', platform='both')
                    try:
                        self.assertEqual(slot.owner['sequential_platforms'], sequential)
                        self.assertEqual(slot.owner['cores'], 4 if sequential else 8)
                        self.assertEqual(slot.owner['memory_requirement_at_entry_bytes'],
                                         largest if sequential else pair)
                        self.assertEqual(slot.owner['vm_peak_bytes'],
                            capacity.memory_requirement(config, 'linux', 'quick') - 5 * capacity.GIB)
                    finally: slot.release()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=largest)):
                pair_slot = capacity.acquire(root, mode='quick', platform='both')
                guest = capacity.acquire(root, mode='quick', platform='linux')
                try:
                    # The native-first pair already reserves its later guest
                    # share: a third guest must not overbook the VM meanwhile.
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode='quick', platform='linux', cores=1)
                finally:
                    guest.release()
                    pair_slot.release()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=largest - 1)):
                # Native alone fits here; a paired quick must not start it
                # unless the later Linux leg also fits the reserved envelope.
                for mode in ('quick', 'full'):
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode=mode, platform='both')
                self.assertFalse(capacity.execution_lanes(root))

    def test_memory_wait_allows_one_fitting_backfill_then_protects_head(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            available = 20 * capacity.GIB
            config['fleet_worker_cpu_reserve_cores'] = 4
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            ticket_bytes, waits = None, 0
            output = io.StringIO()
            def wait(_):
                nonlocal available, ticket_bytes, waits
                waits += 1
                tickets = capacity.full_waiters(root)
                self.assertEqual(len(tickets), 1)
                if ticket_bytes is None: ticket_bytes = tickets[0].read_bytes()
                self.assertEqual(tickets[0].read_bytes(), ticket_bytes)
                self.assertEqual(len(capacity.execution_lanes(root)), 1)
                if waits == 1:
                    # The paired head is memory-blocked, but native fits.
                    fitting = capacity.acquire(root, mode='full', platform='native')
                    try:
                        self.assertEqual(json.loads(tickets[0].read_text())['memory_backfill_used'],
                                         fitting.owner['run_id'])
                    finally: fitting.release()
                    # Even after that run exits, a stream of fitting arrivals
                    # cannot prevent memory accumulating for the older head.
                    for mode in ('quick', 'full', 'quick'):
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, mode=mode, platform='native', cores=1)
                    ticket_bytes = tickets[0].read_bytes()
                    available = 21 * capacity.GIB
                if waits == 2:
                    reviewer.release()
                    available = int(26.8 * capacity.GIB)
                if waits > 2: self.fail('memory waiter did not enter after capacity freed')
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', side_effect=lambda: available),
                  patch.object(capacity, 'time', Mock(sleep=wait)),
                  contextlib.redirect_stdout(output)):
                reviewer = capacity.acquire(root, mode='quick', platform='native')
                full = capacity.acquire(root, mode='full', platform='both', wait=True)
                try:
                    self.assertEqual(waits, 2)
                    self.assertIn('FIFO queue position 1; waiting for memory:', output.getvalue())
                    self.assertIn('pressure normal', output.getvalue())
                    self.assertEqual(full.owner['memory_requirement_at_entry_bytes'],
                        capacity.memory_requirement(config, 'both'))
                    self.assertEqual(full.owner['available_memory_at_entry_bytes'], available)
                    self.assertEqual(capacity.full_waiters(root), [])
                finally: full.release()

    def test_memory_backfill_preserves_fitting_unknown_and_exclusive_predecessors(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            digest = capacity.admission_config_digest(config)
            row = dict(pid=os.getpid(), process_identity=capacity.process_identity(os.getpid()),
                admission_protocol=3, priority='calibration', exclusive=True,
                requested_priority='normal', requested_exclusive=False, mode='quick',
                sequence=0, minimum_memory_bytes=24 * capacity.GIB,
                memory_config_sha256=digest)
            head = root / 'local-go-wait-head.json'
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=20 * capacity.GIB)):
                for update in (dict(minimum_memory_bytes=12 * capacity.GIB),
                               dict(minimum_memory_bytes=None),
                               dict(memory_config_sha256='old-config'),
                               dict(requested_exclusive=True)):
                    with self.subTest(update=update):
                        head.write_text(json.dumps(dict(row, **update)))
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, mode='quick', platform='native')
                        self.assertNotIn('memory_backfill_used', json.loads(head.read_text()))
                # An exclusive drain behind the blocked head also keeps its
                # place; a later native request must not jump over it.
                head.write_text(json.dumps(row))
                drain = root / 'local-go-wait-drain.json'
                drain.write_text(json.dumps(dict(row, sequence=1, requested_exclusive=True)))
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='quick', platform='native')
                self.assertNotIn('memory_backfill_used', json.loads(head.read_text()))

    def test_impossible_single_run_memory_requirement_refuses_without_waiting(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'total_memory', return_value=16 * capacity.GIB),
                  patch.object(capacity, 'time', Mock()) as clock):
                with self.assertRaisesRegex(RuntimeError, 'entry requirement exceeds.*host physical memory'):
                    capacity.acquire(root, mode='full', wait=True)
                clock.sleep.assert_not_called()
                self.assertFalse(capacity.occupied_lanes(root))
                self.assertEqual(capacity.full_waiters(root), [])

    def test_memory_wait_still_stops_on_pressure_or_disk_danger(self):
        for danger in ('pressure', 'disk'):
            with self.subTest(danger=danger), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                waiting = False
                def wait(_):
                    nonlocal waiting
                    waiting = True
                def pressure():
                    if waiting and danger == 'pressure':
                        raise RuntimeError('host memory pressure is not normal')
                def disk(_):
                    return type('Disk', (), dict(free=capacity.ENTRY_FLOOR +
                        (-1 if waiting and danger == 'disk' else capacity.GIB)))()
                with (patch.object(capacity, 'root', return_value=root),
                      patch.object(capacity, 'available_memory', return_value=20 * capacity.GIB),
                      patch.object(capacity, 'check_pressure', side_effect=pressure),
                      patch.object(capacity.shutil, 'disk_usage', side_effect=disk),
                      patch.object(capacity, 'time', Mock(sleep=wait))):
                    with self.assertRaisesRegex(RuntimeError, 'pressure is not normal|disk below'):
                        capacity.acquire(root, mode='full', wait=True)
                    self.assertTrue(waiting)
                    self.assertFalse(capacity.occupied_lanes(root))
                    self.assertEqual(capacity.full_waiters(root), [])

    def test_exclusive_waiter_drains_local_work_and_blocks_new_quick_runs(self):
        for declaration in ('exclusive', 'release'):
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                qualified_config(root)
                script = '''import importlib.util,pathlib,signal,sys,time
p,folder,declaration=sys.argv[1:];s=importlib.util.spec_from_file_location('c',p);c=importlib.util.module_from_spec(s);s.loader.exec_module(c)
r=pathlib.Path(folder);c.root=lambda _:r;c.legacy_directories=lambda _:[];c.native_full_custody=lambda:False
c.available_memory=lambda:64*c.GIB;c.check_pressure=lambda:None;c.local_worker_usage=lambda:None;c.os.cpu_count=lambda:18
def stop(*_): raise SystemExit(143)
signal.signal(signal.SIGTERM,stop)
slot=c.acquire(r,mode='full',exclusive=declaration=='exclusive',
 priority='release' if declaration=='release' else 'normal',wait=True)
try:
 (r/'exclusive-ready').write_text('ready');time.sleep(30)
finally: slot.release()
'''
                with (patch.object(capacity, 'root', return_value=root),
                      patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                    held = capacity.acquire(root, mode='full')
                    child = subprocess.Popen([sys.executable, '-c', script, capacity.__file__, str(root), declaration],
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    try:
                        deadline = time.monotonic() + 5
                        while not list(root.glob('local-go-wait-*.json')):
                            if time.monotonic() > deadline: self.fail('exclusive did not queue')
                            time.sleep(.01)
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root)
                        self.assertFalse((root / 'exclusive-ready').exists())
                        # Release labels cannot jump an earlier exclusive waiter.
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, priority='release')
                        held.release()
                        deadline = time.monotonic() + 5
                        while not (root / 'exclusive-ready').exists():
                            if time.monotonic() > deadline: self.fail('exclusive did not acquire')
                            time.sleep(.01)
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root)
                        with patch.object(capacity, 'available_memory', return_value=20 * capacity.GIB):
                            # Sequential fallback cannot bypass release custody.
                            with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                                capacity.acquire(root, mode='quick', platform='both')
                        child.terminate()
                        out, err = child.communicate(timeout=5)
                        self.assertEqual(child.returncode, 143, err)
                        self.assertIn(b'queue position', out)
                        self.assertFalse(capacity.occupied_lanes(root))
                    finally:
                        if held.directory.exists(): held.release()
                        if child.poll() is None: child.kill(); child.wait()
                        child.stdout.close(); child.stderr.close()

    def test_concurrent_capacity_requires_digest_bound_complete_measurements(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            self.assertIsNotNone(capacity.resource_qualification(config))
            config['memory_measurement']['native_peak_bytes'] += 1
            with self.assertRaisesRegex(ValueError, 'native resource peak'):
                capacity.resource_qualification(config)
            config = qualified_config(root)
            (root / 'resource-measurements.jsonl').write_text('{}\n')
            with self.assertRaisesRegex(ValueError, 'digest changed'):
                capacity.resource_qualification(config)

    def test_active_group_binds_admission_configuration_until_verified_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=15 * capacity.GIB)):
                first = capacity.acquire(root, mode='full')
                original = json.dumps(config)
                try:
                    with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                        capacity.acquire(root, mode='full')
                    config['fleet_worker_cpu_reserve_cores'] = 4
                    (root / 'gate-capacity.json').write_text(json.dumps(config))
                    existing_group = (root / 'local-go-resource-group.custody').read_bytes()
                    with self.assertRaisesRegex(RuntimeError, 'configuration changed') as refused:
                        capacity.acquire(root, mode='full', wait=True)
                    self.assertIn('no work started', str(refused.exception))
                    self.assertNotIn('retained', str(refused.exception))
                    self.assertEqual(capacity.execution_lanes(root), [first.directory])
                    self.assertEqual((root / 'local-go-resource-group.custody').read_bytes(), existing_group)
                    self.assertFalse(list(root.glob('local-go-wait-*.json')))
                    del config['fleet_worker_cpu_reserve_cores']
                    config['memory_measurement']['headroom_bytes'] = 1
                    (root / 'gate-capacity.json').write_text(json.dumps(config))
                    with self.assertRaisesRegex(RuntimeError, 'configuration changed'):
                        capacity.acquire(root, mode='full')
                    self.assertTrue(first.directory.exists())
                    first.release(stopped=False)
                    replacement = capacity.acquire(root)
                    replacement.release()
                finally:
                    (root / 'gate-capacity.json').write_text(original)
                    if first.directory.exists():
                        first.release()
                config['memory_measurement']['headroom_bytes'] = 1
                (root / 'gate-capacity.json').write_text(json.dumps(config))
                replacement = capacity.acquire(root, mode='full')
                replacement.release()

    def test_queued_exclusive_behind_older_full_still_drains_new_quick_work(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            qualified_config(root)
            record = dict(pid=os.getpid(), process_identity=capacity.process_identity(os.getpid()),
                          priority='normal', sequence=1, exclusive=False)
            older = root / 'local-go-wait-older.json'
            older.write_text(json.dumps(record))
            exclusive = root / 'local-go-wait-exclusive.json'
            exclusive.write_text(json.dumps(dict(record, sequence=2, exclusive=True)))
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                self.assertEqual(capacity.full_waiters(root), [older, exclusive])
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root)
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, priority='release')
                self.assertTrue(older.exists())
                self.assertTrue(exclusive.exists())

    def test_zombie_waiter_is_dead_but_execution_custody_remains(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            child = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(.2)'])
            try:
                identity = subprocess.check_output(['ps', '-p', str(child.pid), '-o', 'lstart='], text=True).strip()
                deadline = time.monotonic() + 5
                while True:
                    state = subprocess.check_output(['ps', '-p', str(child.pid), '-o', 'stat='], text=True).strip()
                    if state.startswith('Z'): break
                    if time.monotonic() > deadline: self.fail('own waiter did not become zombie')
                    time.sleep(.01)
                self.assertIsNone(child.returncode)  # Deliberately not reaped yet.
                record = dict(pid=child.pid, process_identity=identity, priority='release', sequence=1)
                ticket = root / 'local-go-wait-zombie.json'
                ticket.write_text(json.dumps(record))
                reservation = root / 'local-go-release-wait-zombie'
                reservation.write_text(json.dumps(record))
                custody = root / 'local-go-full-unknown.custody'
                custody.write_text(json.dumps(record))
                self.assertEqual(capacity.full_waiters(root), [])
                self.assertTrue(custody.exists())
            finally: child.wait(timeout=5)

    def test_later_release_stream_cannot_starve_a_queued_normal_quick(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            launcher = """import importlib.util,pathlib,sys,time,types
s=importlib.util.spec_from_file_location('c',sys.argv[1]);c=importlib.util.module_from_spec(s);s.loader.exec_module(c)
r=pathlib.Path(sys.argv[2]);c.root=lambda _:r;c.legacy_directories=lambda _:[]
c._memory.evidence_directory=lambda:r/'evidence';c.native_full_custody=lambda:False
c.available_memory=lambda:64*c.GIB;c.total_memory=lambda:64*c.GIB;c.check_pressure=lambda:None
c.local_worker_usage=lambda:None;c.os.cpu_count=lambda:18
original_sleep=time.sleep
def wait(_):
 (r/'queued').touch()
 while not (r/'allow').exists(): original_sleep(.01)
c.time=types.SimpleNamespace(sleep=wait)
slot=c.acquire(r,mode='quick',wait=True)
(r/'normal-admitted').touch()
slot.release()
"""
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                first, second = capacity.acquire(root), capacity.acquire(root)
                child = subprocess.Popen([sys.executable, '-c', launcher, capacity.__file__, str(root)],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 5
                    while not (root / 'queued').exists():
                        if time.monotonic() > deadline: self.fail('normal quick did not queue')
                        time.sleep(.01)
                    first.release()  # A lane now fits every following request.
                    # Pre-FIFO quick callers had no ticket of their own and
                    # used this rank/exclusive predicate to honor a drain.
                    # They must not bypass the new FIFO during an upgrade.
                    rank = {'calibration': 0, 'release': 1, 'normal': 2}
                    waiting = [json.loads(path.read_text()) for path in capacity.full_waiters(root)]
                    for priority in rank:
                        self.assertTrue(any(row.get('exclusive') and
                            rank[priority] >= rank[row['priority']] for row in waiting))
                    self.assertEqual(waiting[0]['requested_priority'], 'normal')
                    self.assertFalse(waiting[0]['requested_exclusive'])
                    # Migrate older full tickets too: a frozen caller sorts
                    # priority before sequence, which must agree with FIFO.
                    older = root / 'local-go-wait-older.json'
                    older.write_text(json.dumps(dict(pid=os.getpid(),
                        process_identity=capacity.process_identity(os.getpid()),
                        priority='normal', sequence=0, exclusive=False)))
                    try:
                        queue = capacity.full_waiters(root)
                        legacy = sorted(queue, key=lambda path: (
                            rank[json.loads(path.read_text())['priority']],
                            json.loads(path.read_text())['sequence']))
                        self.assertEqual(queue, legacy)
                        self.assertEqual(queue[0], older)
                    finally:
                        older.unlink()
                    for mode, priority in [('quick', 'release'), ('full', 'release'),
                                           ('full', 'calibration')] * 3:
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, mode=mode, priority=priority)
                    self.assertEqual(len(capacity.full_waiters(root)), 1)
                    (root / 'allow').touch()
                    output, error = child.communicate(timeout=5)
                    self.assertEqual(child.returncode, 0, error)
                    self.assertIn(b'FIFO queue position 1', output)
                    self.assertTrue((root / 'normal-admitted').exists())
                    release = capacity.acquire(root, priority='release')
                    self.assertEqual(release.owner['priority'], 'normal')
                    release.release()
                finally:
                    for slot in (first, second):
                        if slot.directory.exists(): slot.release()
                    if child.poll() is None: child.kill()
                    child.wait(timeout=5)
                    child.stdout.close(); child.stderr.close()

    def test_legacy_calibration_label_is_not_an_execution_reservation(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'local-go-calibration-wait-owner').touch()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                full = capacity.acquire(root, mode='full', priority='release')
                try:
                    self.assertEqual(full.owner['priority'], 'release')
                    self.assertTrue(full.owner['exclusive'])
                    for mode in ('quick', 'full'):
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, mode=mode)
                finally: full.release()
                build = capacity.acquire(root, priority='release')
                self.assertEqual(build.owner['priority'], 'normal')
                build.release()

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
m.available_memory=lambda:64*m.GIB;m.check_pressure=lambda:None;m.local_worker_usage=lambda:None
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
                        self.assertIn(b'FIFO queue position', output)
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
        self.assertEqual(capacity.memory_requirement(config, 'linux'), 12593872896)
        self.assertEqual(capacity.memory_requirement(config, 'both'), 12593872896 + 4919132160)
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


    def test_trim_preserves_symlinks_and_handles_concurrent_eviction(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cache, outside = root / 'cache', root / 'outside'
            (cache / 'ab').mkdir(parents=True)
            outside.mkdir()
            unique = outside / ('a' * 64 + '-d')
            unique.write_text('unique evidence')
            (cache / 'cd').symlink_to(outside, target_is_directory=True)
            (cache / 'ab' / ('b' * 64 + '-d')).symlink_to(unique)
            (cache / 'ab' / ('c' * 64 + '-a')).mkdir()
            obj = cache / 'ab' / ('d' * 64 + '-d')
            obj.write_text('rebuildable')
            original = Path.unlink
            def racing_unlink(path, *args, **kwargs):
                if path == obj:
                    original(path)  # Concurrent Go cleanup removes it first.
                return original(path, *args, **kwargs)
            with patch.object(Path, 'unlink', racing_unlink):
                self.assertEqual(local.trim_cache(cache, limit=0), 0)
            self.assertEqual(unique.read_text(), 'unique evidence')
            self.assertTrue((cache / 'ab' / ('b' * 64 + '-d')).is_symlink())
            with self.assertRaises(ValueError):
                local.trim_cache(cache, limit=-1)

    def test_entry_exit_trim_and_terminal_source_cleanup(self):
        for status, confirmed, cleanup_fails in ((0, True, False), (1, True, False),
                                                (125, False, False), (125, True, True)):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                slot = Mock(directory=root / 'lane', owner={})
                run_folder = None
                events = []
                def trim(cache):
                    events.append('trim')
                def admit(*args, **kwargs):
                    self.assertEqual(events, ['trim'])
                    return slot
                def supervised(command, **kwargs):
                    nonlocal run_folder
                    manifest_path = Path(command[-1])
                    manifest = json.loads(manifest_path.read_text())
                    run_folder = manifest_path.parent
                    (run_folder / 'source').mkdir()
                    (run_folder / 'source' / 'file.go').write_text('snapshot')
                    for name in ('source.tar', 'runtime.tar'):
                        (run_folder / name).write_text('rebuildable')
                    if confirmed:
                        local.receipts.publish(run_folder / 'overall-cleanup.json',
                            manifest['identity'], stopped=True, status=status)
                    events.append('payload')
                    return Mock(wait=Mock(return_value=status))
                with (patch.object(sys, 'argv', ['gate-local.py', '--repo', str(root), '--command', 'true']),
                      patch.object(Path, 'home', return_value=root),
                      patch.object(local.capacity, 'acquire', side_effect=admit),
                      patch.object(local.capacity, 'execution_lanes', return_value=[root / 'other']),
                      patch.object(local.capacity, 'native_full_custody', return_value=True),
                      patch.object(local, 'trim_cache', side_effect=trim),
                      patch.object(local.shutil, 'rmtree', wraps=shutil.rmtree,
                                   side_effect=PermissionError('source cleanup failed') if cleanup_fails else None),
                      patch.object(local.subprocess, 'Popen', side_effect=supervised)):
                    self.assertEqual(local.main(), status)
                self.assertEqual(events, ['trim', 'payload', 'trim'])
                for name in ('source', 'source.tar', 'runtime.tar'):
                    self.assertEqual((run_folder / name).exists(), not confirmed or cleanup_fails)
                if cleanup_fails:
                    self.assertIn('source cleanup failed', (run_folder / 'finalize-error.json').read_text())
                self.assertTrue((run_folder / 'manifest.json').exists())
                slot.release.assert_called_once_with(stopped=confirmed)


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

    def test_source_identity_pins_moving_refs_and_verifies_clean_snapshot_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            repo = root / 'repo'
            subprocess.run(['git', 'init', '-q', str(repo)], check=True)
            product = repo / 'product.txt'
            def commit(data):
                product.write_text(data)
                subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
                subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Fixture', '-c',
                    'user.email=fixture@example.invalid', 'commit', '-qm', data], check=True)
                return subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
            old = commit('old')
            new = commit('new')
            self.assertFalse(local.sources.freeze(repo, root / 'clean.tar')['source_dirty'])
            product.chmod(0o755)
            self.assertTrue(local.sources.freeze(repo, root / 'executable.tar')['source_dirty'])
            product.chmod(0o644)
            subprocess.run(['git', '-C', str(repo), 'update-ref', 'refs/heads/moving', old], check=True)
            original_run = subprocess.run
            def move_after_archive(args, **kwargs):
                result = original_run(args, **kwargs)
                if 'archive' in args:
                    original_run(['git', '-C', str(repo), 'update-ref', 'refs/heads/moving', new], check=True)
                return result
            archive = root / 'moving.tar'
            with patch.object(local.sources.subprocess, 'run', side_effect=move_after_archive):
                identity = local.sources.freeze(repo, archive, 'moving')
            self.assertEqual(identity['source_sha'], old)
            with tarfile.open(archive) as tar:
                self.assertEqual(tar.extractfile('product.txt').read(), b'old')
            original_output = subprocess.check_output
            reads = 0
            def edit_after_names(args, **kwargs):
                nonlocal reads
                result = original_output(args, **kwargs)
                if 'ls-files' in args and '--cached' in args:
                    reads += 1
                    product.write_text('edit after clean check')
                return result
            with patch.object(local.sources.subprocess, 'check_output', side_effect=edit_after_names):
                identity = local.sources.freeze(repo, root / 'dirty.tar')
            self.assertTrue(identity['source_dirty'])
            self.assertNotEqual(reads, 0)
            reads = 0
            def edit_during_verification(args, **kwargs):
                nonlocal reads
                result = original_output(args, **kwargs)
                if 'ls-files' in args and '--cached' in args:
                    reads += 1
                    if reads == 2: product.write_text('intervening edit')
                return result
            with patch.object(local.sources.subprocess, 'check_output', side_effect=edit_during_verification):
                with self.assertRaisesRegex(RuntimeError, 'source changed'):
                    local.sources.freeze(repo, root / 'unstable.tar')

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
                ('PREVIOUS',)))
            # Reproduce the empty nested archive incident: cwd is below Git root.
            previous = Path.cwd()
            try:
                os.chdir(frozen)
                archives = local.sources.predecessors(repo, frozen)
            finally:
                os.chdir(previous)
            self.assertEqual(len(archives), 1)
            with tarfile.open(archives[0]) as tar:
                self.assertIn('go.mod', tar.getnames())
                self.assertIn('internal/myce/source.go', tar.getnames())

    def test_preparation_failures_confirm_no_payload_and_allow_capacity_release(self):
        for phase in ('source', 'selection'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                path = root / 'manifest.json'
                path.write_text(json.dumps(dict(repo=str(root), cache=str(root / 'cache'), cores=4,
                    report=str(root / 'report'), mode='quick', command=[], platform='both', identity='run')))
                failure = (RuntimeError('source refused') if phase == 'source' else
                           subprocess.CalledProcessError(2, ['gate-related.py']))
                with (patch.object(local.sources, 'freeze', side_effect=failure if phase == 'source' else None,
                                   return_value={'source_sha': 'test'}),
                      patch.object(local.sources, 'extract', side_effect=lambda a, d: (d / 'scripts').mkdir()),
                      patch.object(local.sources, 'runtime_archive'),
                      patch.object(local.subprocess, 'run', side_effect=failure),
                      patch.object(local.subprocess, 'Popen') as launch):
                    with self.assertRaises(type(failure)):
                        local.execute(path)
                launch.assert_not_called()
                self.assertTrue(local.receipts.confirmed(root / 'overall-cleanup.json', 'run'))
                receipt = json.loads((root / 'overall-cleanup.json').read_text())
                self.assertTrue(receipt['no_payload_started'])
                self.assertEqual(receipt['status'], 125)
                self.assertFalse((root / 'native-cleanup.json').exists())
                self.assertFalse((root / 'linux-cleanup.json').exists())

    def test_both_legs_overlap_or_run_sequentially_and_collect_test_failure(self):
        # Pipes prove overlap; sequential mode must observe confirmed native
        # cleanup before guest launch. Neither proof uses elapsed-time limits.
        cases = [(mode, sequential, result) for mode, sequential in
                 (('full', False), ('quick', False), ('quick', True)) for result in (0, 1)]
        for mode, sequential, result in cases:
            with self.subTest(mode=mode, sequential=sequential, result=result), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                path = root / 'manifest.json'
                path.write_text(json.dumps(dict(repo=str(root), cache=str(root / 'cache'), cores=2,
                    report=str(root / 'report'), mode=mode, command=[], platform='both', identity='run',
                    sequential_platforms=sequential)))
                original = subprocess.Popen
                children, writers = [], []
                def launch(command, **kwargs):
                    if children:
                        if sequential:
                            self.assertIsNotNone(children[0].poll(), 'native still running at guest launch')
                            self.assertTrue(json.loads((root / 'native-cleanup.json').read_text())['stopped'])
                        else:
                            self.assertIsNone(children[0].poll(), 'native finished before guest launch')
                        receipt = root / 'linux-cleanup.json'
                        nonce = command[command.index('--cleanup-id') + 1]
                        code = 0
                    else:
                        receipt = kwargs['env']['GATE_CLEANUP_RECEIPT']
                        nonce = kwargs['env']['GATE_CLEANUP_ID']
                        code = result
                    reader, writer = os.pipe()
                    writers.append(writer)
                    overlap = ('' if sequential else
                        'assert select.select([fd],[],[],5)[0], "peer was not launched"; '
                        'assert os.read(fd,1)==b"x"; ')
                    worker = ('import json,os,pathlib,select,sys; '
                        'fd=int(sys.argv[1]); ' + overlap +
                        'pathlib.Path(sys.argv[2]).write_text(json.dumps(dict(schema=1,'
                        'identity=sys.argv[3],stopped=True,status=int(sys.argv[4])))); '
                        'sys.exit(int(sys.argv[4]))')
                    try:
                        child = original([sys.executable, '-c', worker, str(reader), str(receipt), nonce, str(code)],
                                         pass_fds=(reader,))
                    finally: os.close(reader)
                    children.append(child)
                    if len(children) == 2 and not sequential:
                        for fd in writers: os.write(fd, b'x')
                    return child
                def freeze(repo, archive, ref=None):
                    archive.touch()
                    return {'source_sha': 'test'}
                try:
                    with (patch.object(local.sources, 'freeze', side_effect=freeze),
                          patch.object(local.sources, 'extract', side_effect=lambda a, d: (d / 'scripts').mkdir()),
                          patch.object(local.sources, 'runtime_archive'),
                          patch.object(local.sources, 'predecessors', return_value=[]),
                          patch.object(local.subprocess, 'run'),
                          patch.object(local.subprocess, 'Popen', side_effect=launch)):
                        self.assertEqual(local.execute(path), result)
                    self.assertEqual(len(children), 2)
                    self.assertTrue(all(child.poll() is not None for child in children))
                    self.assertTrue(local.receipts.confirmed(root / 'overall-cleanup.json', 'run'))
                finally:
                    for fd in writers: os.close(fd)
                    for child in children:
                        if child.poll() is None: child.kill(); child.wait()

    def test_unknown_native_exit_never_starts_linux_or_confirms_capacity(self):
        for code, sequential in ((-9, False), (-9, True), (255, False), (255, True), (125, True), (0, True)):
            with self.subTest(code=code, sequential=sequential), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                repo = root / 'repo'
                (repo / 'scripts').mkdir(parents=True)
                path = root / 'manifest.json'
                path.write_text(json.dumps(dict(repo=str(repo), cache=str(root / 'cache'), cores=4,
                    report=str(root / 'report'), mode='quick', command=['true'], platform='both', identity='run',
                    sequential_platforms=sequential)))
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
        transport = linux.Transport('vm', 'remote', 'nonce')
        original = subprocess.Popen
        launched = []
        def cancelled_handoff(*args, **kwargs):
            transport.stop(signal.SIGTERM, None)
            child = original(*args, **kwargs)
            launched.append(child)
            return child
        with patch.object(linux.subprocess, 'Popen', side_effect=cancelled_handoff):
            with self.assertRaises(linux.Cancelled):
                transport.run([sys.executable, '-c', 'import time; time.sleep(60)'])
        self.assertIsNotNone(launched[0].poll())
        self.assertIsNone(transport.current)

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
        for timing in ('between', 'handoff', 'both-running'):
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
                        if timing == 'both-running':
                            stopped = threading.Event()
                            child.poll.return_value = None
                            child.send_signal.side_effect = lambda sig: stopped.set()
                            def native_wait():
                                self.assertTrue(stopped.wait(5), 'native cancellation was not forwarded')
                                child.poll.return_value = 143
                                return 143
                            child.wait.side_effect = native_wait
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
                if timing in ('handoff', 'both-running'):
                    children[-1].send_signal.assert_called_once_with(signal.SIGTERM)
                if timing == 'both-running':
                    children[0].send_signal.assert_called_once_with(signal.SIGTERM)
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

    def test_command_cannot_claim_release_priority_and_keeps_host_supervision(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            slot = Mock()
            slot.directory = root / 'lane'
            slot.owner = {}
            output = root / 'release-binary'
            def supervised(command, **kwargs):
                self.assertFalse('MYCE_TOKEN' in kwargs['env'], 'gate inherited forbidden authority variable')
                manifest_path = Path(command[-1])
                manifest = json.loads(manifest_path.read_text())
                self.assertEqual(manifest['command'], ['go', 'build', '-o', str(output)])
                self.assertIn('--disk-root', command)
                local.receipts.publish(manifest_path.parent / 'overall-cleanup.json',
                    manifest['identity'], stopped=True, status=0)
                child = Mock()
                child.wait.return_value = 0
                return child
            with (patch.object(sys, 'argv', ['gate-local.py', 'full', '--repo', str(root), '--priority',
                    'release', '--command', 'go', 'build', '-o', str(output)]),
                  patch.dict(os.environ, MYCE_TOKEN='nonsecret-fixture'),
                  patch.object(local.capacity, 'acquire', side_effect=lambda *a, **kw:
                      (self.assertFalse('MYCE_TOKEN' in os.environ, 'admission inherited forbidden authority variable'), slot)[1]) as admission,
                  patch.object(local.capacity, 'root', return_value=root / 'control'),
                  patch.object(local.capacity, 'legacy_directories', return_value=[]),
                  patch.object(local.capacity, 'native_full_custody', return_value=False),
                  patch.object(local, 'trim_cache'), patch.object(local.shutil, 'rmtree'),
                  patch.object(Path, 'unlink'),
                  patch.object(local.subprocess, 'Popen', side_effect=supervised)):
                self.assertEqual(local.main(), 0)
            self.assertEqual(admission.call_args.kwargs['priority'], 'normal')
            self.assertTrue(admission.call_args.kwargs['wait'])
            slot.release.assert_called_once_with(stopped=True)

    def test_memory_guard_stops_owned_work_on_pressure_or_new_swapouts(self):
        for current in (dict(pressure_level=4, swapouts=0),
                        dict(pressure_level=1, swapouts=1)):
            with self.subTest(current=current), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                base = dict(pressure_level=1, swapouts=0)
                with (patch.object(measurement.subprocess, 'check_output', return_value=str(root)),
                      patch.object(measurement, 'cache_bytes', return_value=0),
                      patch.object(measurement.memory, 'idle_baseline', return_value=None),
                      patch.object(measurement.memory, 'snapshot', side_effect=[base, current])):
                    result = measurement.measure([sys.executable, '-c', 'import time; time.sleep(10)'],
                        root / 'report', 'full', 'test', phase='host')
                self.assertEqual(result, 125)
                self.assertTrue(json.loads((root / 'report').read_text())['refusal'])

    def test_measured_swap_guard_allows_one_batch_but_stops_continuation(self):
        baseline = dict(schema=1, window_seconds=2, evidence=['idle samples'],
                        peak_swapout_bytes_per_second=0,
                        peak_swap_use_growth_bytes_per_second=0)
        def sample(seconds, pages=28, used=0, working=100):
            return dict(monotonic_seconds=seconds, pressure_level=1, swapouts=pages,
                        page_bytes=16384, swap_used_bytes=used, all_vz_rss_sum_bytes=working)
        guard = measurement.memory.SwapGuard(sample(0), baseline)
        # Reproduces the actual isolated 384 KiB counter batch under normal pressure.
        self.assertIsNone(guard.check(sample(2, 52, 393216, 200)))
        self.assertIsNone(guard.check(sample(4, 52, 393216, 200)))
        self.assertIsNone(guard.check(sample(6, 52, 393216, 200)))
        self.assertEqual(guard.excess_windows, 0)
        for kind in ('rate', 'working-set'):
            with self.subTest(kind=kind):
                guard = measurement.memory.SwapGuard(sample(0), baseline)
                self.assertIsNone(guard.check(sample(2, 52 if kind == 'rate' else 28, 10, 200)))
                reason = guard.check(sample(4, 76 if kind == 'rate' else 28, 20, 300))
                self.assertIn('swapout rate' if kind == 'rate' else 'working set', reason)
        # Measured background rates are allowed without accumulating excess.
        baseline = dict(baseline, peak_swapout_bytes_per_second=24 * 16384 / 2,
                        peak_swap_use_growth_bytes_per_second=10)
        guard = measurement.memory.SwapGuard(sample(0), baseline)
        for index in range(1, 4):
            self.assertIsNone(guard.check(sample(index * 2, 28 + index * 24, index * 10, 200)))
        current = sample(6.1); current['pressure_level'] = 4
        self.assertIn('pressure', guard.check(current))  # Never wait for a rate window.

    def test_warning_pressure_without_swap_growth_is_allowed(self):
        before = dict(pressure_level=1, swapouts=52, swap_used_bytes=100)
        warning = dict(before, pressure_level=2)
        self.assertIsNone(measurement.memory.refusal(before, warning))
        self.assertIn('swapping', measurement.memory.refusal(before, dict(warning, swapouts=53)))
        self.assertIn('pressure', measurement.memory.refusal(before, dict(warning, pressure_level=4)))

    def test_missing_memory_observation_does_not_stop_payload(self):
        for initial_failure in (True, False):
            with self.subTest(initial_failure=initial_failure), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                timeout = subprocess.TimeoutExpired(['ps'], 5)
                before = dict(pressure_level=1, swapouts=52)
                observations = timeout if initial_failure else [before, timeout]
                with (patch.object(measurement.subprocess, 'check_output', return_value=str(root)),
                      patch.object(measurement, 'cache_bytes', return_value=0),
                      patch.object(measurement, 'tree_rss', return_value=1),
                      patch.object(measurement.memory, 'idle_baseline', return_value=None),
                      patch.object(measurement.memory, 'snapshot', side_effect=observations)):
                    self.assertEqual(measurement.measure([sys.executable, '-c', 'import time; time.sleep(.7)'],
                        root / 'metrics', 'quick', 'test', phase='host'), 0)
                row = json.loads((root / 'metrics').read_text())
                self.assertFalse(row['refusal'])
                self.assertTrue(any('memory_observation_warning' in sample for sample in row['samples']))
        guard = measurement.memory.SwapGuard(None)
        self.assertIsNone(guard.check(None))
        self.assertIsNone(guard.check(dict(pressure_level=2, swapouts=1)))
        self.assertIn('pressure', guard.check(dict(pressure_level=4, swapouts=1)))

    def test_quick_memory_uses_measured_selection_peak(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            evidence = root / 'quick.jsonl'
            evidence.write_text(json.dumps(dict(mode='quick', status=0, max_tree_rss_sum_bytes=capacity.GIB)) + '\n')
            config['quick_memory_measurement'] = dict(peak_bytes=capacity.GIB,
                evidence=[dict(path=str(evidence), sha256=hashlib.sha256(evidence.read_bytes()).hexdigest())])
            self.assertEqual(capacity.memory_requirement(config, 'native', 'quick'), 6 * capacity.GIB)
            self.assertEqual(capacity.memory_requirement(config, 'both', 'quick'), 17 * capacity.GIB)
            self.assertEqual(capacity.memory_requirement(config, 'native'), 12 * capacity.GIB)
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                full = capacity.acquire(root, mode='full', cores=4)
                try:
                    with patch.object(capacity, 'available_memory', return_value=6 * capacity.GIB):
                        quick = capacity.acquire(root, mode='quick', platform='native', cores=1)
                        self.assertEqual(quick.owner['peak_bytes'], capacity.GIB)
                        self.assertEqual(quick.owner['memory_requirement_at_entry_bytes'], 6 * capacity.GIB)
                        quick.release()
                finally: full.release()
            config['quick_memory_measurement']['peak_bytes'] += 1
            with self.assertRaisesRegex(RuntimeError, 'quick memory qualification'):
                capacity.memory_requirement(config, 'native', 'quick')

    def test_measured_sustained_swap_guard_terminates_owned_supervisor(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            baseline = dict(schema=1, window_seconds=2, evidence=['idle samples'],
                            peak_swapout_bytes_per_second=0,
                            peak_swap_use_growth_bytes_per_second=0)
            samples = [dict(monotonic_seconds=i * 2, pressure_level=1, swapouts=i,
                            page_bytes=16384, swap_used_bytes=i, all_vz_rss_sum_bytes=0)
                       for i in range(3)]
            with (patch.object(measurement.subprocess, 'check_output', return_value=str(root)),
                  patch.object(measurement, 'cache_bytes', return_value=0),
                  patch.object(measurement, 'tree_rss', return_value=1),
                  patch.object(measurement.memory, 'idle_baseline', return_value=baseline),
                  patch.object(measurement.memory, 'snapshot', side_effect=samples)):
                status = measurement.measure([sys.executable, '-c', 'import time; time.sleep(10)'],
                    root / 'metrics', 'full', 'test', phase='host')
            row = json.loads((root / 'metrics').read_text())
            self.assertEqual(status, 125)
            self.assertIn('sustained', row['refusal'][0])
            self.assertEqual(row['swap_guard']['baseline'], baseline)
            self.assertEqual(len(row['swap_guard']['windows']), 2)

    def test_static_fleet_cpu_reserve_is_counted_even_when_worker_is_idle_or_grows(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            config['fleet_worker_cpu_reserve_cores'] = 4
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            worker = dict(pid=661, cpu_cores=0, rss_sum_bytes=capacity.GIB)
            actual = subprocess.run
            def offline(args, **kwargs):
                self.assertNotEqual(Path(args[0]).name, 'myce', 'admission must not query the fleet')
                return actual(args, **kwargs)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB),
                  patch.object(capacity, 'local_worker_usage', return_value=worker),
                  patch.object(capacity.subprocess, 'run', side_effect=offline)):
                first = capacity.acquire(root, mode='full', cores=4)
                try:
                    worker['cpu_cores'] = 12  # Accepted drift only affects speed.
                    second = capacity.acquire(root, mode='full', cores=4)
                    try:
                        self.assertEqual(first.owner['fleet_worker_cpu_reserve_cores'], 4)
                        self.assertEqual(second.owner['fleet_worker_cpu_reserve_cores'], 4)
                        self.assertEqual(6 + 4 + first.owner['cores'] + second.owner['cores'], 18)
                        with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                            capacity.acquire(root, mode='full', cores=4)
                    finally: second.release()
                finally: first.release()

    def test_live_available_memory_admits_without_worker_ram_or_allowance_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            config = qualified_config(root)
            config['fleet_worker_cpu_reserve_cores'] = 4
            (root / 'gate-capacity.json').write_text(json.dumps(config))
            # Old high or malformed allowance caches have no role in admission.
            cache = root / 'worker-allowance.json'
            cache.write_text('obsolete cache: 60 GiB; intentionally not JSON')
            worker = dict(pid=661, cpu_cores=0, rss_sum_bytes=32 * capacity.GIB)
            actual = subprocess.run
            def offline(args, **kwargs):
                self.assertNotEqual(Path(args[0]).name, 'myce', 'admission must stay local')
                return actual(args, **kwargs)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=16 * capacity.GIB),
                  patch.object(capacity, 'local_worker_usage', return_value=worker),
                  patch.object(capacity.subprocess, 'run', side_effect=offline)):
                for platform in ('native', 'linux'):
                    slot = capacity.acquire(root, mode='full', platform=platform)
                    try:
                        self.assertEqual(slot.owner['memory_requirement_at_entry_bytes'],
                            capacity.memory_requirement(config, platform))
                        self.assertNotIn('worker_allowance_at_entry', slot.owner)
                    finally: slot.release()
                self.assertEqual(cache.read_text(), 'obsolete cache: 60 GiB; intentionally not JSON')

    def test_worker_usage_counts_only_installed_worker_process_tree(self):
        memory = measurement.memory
        with (patch.object(memory.sys, 'platform', 'darwin'),
              patch.object(memory.subprocess, 'check_output', side_effect=[
                  '661 0 local.mycelium.worker\n999 0 other.service\n',
                  '661 1 100 0.5\n800 661 200 190.0\n801 800 300 50.0\n999 1 900 300.0\n'])):
            usage = memory.local_worker_usage()
        self.assertEqual(usage['rss_sum_bytes'], 600 * 1024)
        self.assertEqual(usage['process_count'], 3)
        self.assertAlmostEqual(usage['cpu_cores'], 2.405)

    def test_fleet_custody_is_separate_but_still_prevents_cache_eviction(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            cache = root / 'Library/Caches/mycelium/gates/go-build'
            cache.mkdir(parents=True)
            path = cache / 'mycelium-full-gate-0.custody'
            scratch = root / '.local/share/mycelium/worker/state/job-files/job/1/.gate/run.x'
            original = json.dumps(dict(marker='fleet', scratch=str(scratch)))
            path.write_text(original)
            with (patch.object(Path, 'home', return_value=root),
                  patch.object(capacity.subprocess, 'check_output', return_value=str(cache))):
                # setUp mocks the public function, so exercise its real definition.
                real = load('gate-capacity')
                self.assertFalse(real.native_full_custody())
                self.assertTrue(real.native_full_custody(include_fleet=True))
                self.assertEqual(path.read_text(), original)
                path.write_text('{unknown')
                self.assertTrue(real.native_full_custody())
                # A dead local reserved holder has no scratch yet. Its cache
                # record must not survive forever after host custody is gone.
                path.write_text(json.dumps(dict(holder_pid=99999999,
                    holder_start='old-start', phase='reserved')))
                with patch.object(real, 'process_identity', return_value=None):
                    with path.with_suffix('.lock').open('a') as live_lock:
                        real.fcntl.flock(live_lock, real.fcntl.LOCK_EX)
                        self.assertTrue(real.native_full_custody())
                        self.assertTrue(path.exists())
                    self.assertFalse(real.native_full_custody())
                    self.assertFalse(path.exists())

    def test_nonpositive_requested_or_recorded_cpu_demand_cannot_bypass_ledger(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                for cores in (0, -1):
                    with self.subTest(cores=cores), self.assertRaisesRegex(ValueError, 'positive integer'):
                        capacity.acquire(root, cores=cores)
                config = root / 'gate-capacity.json'
                for reserve in (-1, True, 1.5, '4'):
                    config.write_text(json.dumps(dict(fleet_worker_cpu_reserve_cores=reserve)))
                    with self.subTest(reserve=reserve), self.assertRaisesRegex(ValueError, 'nonnegative integer'):
                        capacity.acquire(root)
                config.unlink()
                lane = root / 'local-go-slot'; lane.mkdir()
                owner = lane / 'owner.json'; owner.write_text(json.dumps(dict(cores=0)))
                with self.assertRaisesRegex(RuntimeError, 'invalid CPU demand'):
                    capacity.acquire(root, cores=4)
                self.assertTrue(owner.exists())

    def test_swap_baseline_requires_samples_digest_local_exclusion_and_background_jobs(self):
        import hashlib
        memory = measurement.memory
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            samples = root / 'samples.jsonl'; status = root / 'status.json'
            status.write_text(json.dumps(dict(jobs=[dict(id='fleet-job', machine='worker', state='running')])))
            rows = [dict(monotonic_seconds=i * 2, local_gate_owners=[], local_native_custody=False,
                         observation_only=True, memory=dict(pressure_level=1, page_bytes=16384,
                         swapouts=0, swap_used_bytes=0, local_worker_usage=dict(pid=661))) for i in range(3)]
            samples.write_text(''.join(json.dumps(row) + '\n' for row in rows))
            def evidence(path):
                return dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            baseline = dict(schema=1, evidence=evidence(samples), **memory.measured_rates(rows),
                            fleet_background=dict(machine_id='worker', worker_pids=[661],
                                job_ids=['fleet-job'], status_snapshots=[evidence(status)]))
            memory.verify_baseline(baseline)
            for change in ('fabricated', 'rates', 'digest', 'local', 'job'):
                with self.subTest(change=change):
                    candidate = json.loads(json.dumps(baseline))
                    if change == 'fabricated': candidate['evidence'] = ['truthy']
                    elif change == 'rates': candidate['peak_swapout_bytes_per_second'] = 1024**3
                    elif change == 'digest': candidate['evidence']['sha256'] = 'wrong'
                    elif change == 'job': candidate['fleet_background']['job_ids'] = ['unobserved-job']
                    else:
                        rows[0]['local_gate_owners'] = ['another-local-lane']
                        samples.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                        candidate['evidence'] = evidence(samples)
                    with self.assertRaises(ValueError): memory.verify_baseline(candidate)
                    samples.write_text(''.join(json.dumps(dict(row, local_gate_owners=[])) + '\n' for row in rows))


if __name__ == '__main__':
    unittest.main()
