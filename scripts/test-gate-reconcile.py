#!/usr/bin/env python3
"""Abandoned measured custody needs affirmative host and guest exit proof."""
import copy
import contextlib
import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('reconcile', Path(__file__).with_name('gate-reconcile.py'))
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)
REAL_HANDLES = recovery.budget.scratch_processes


def write(path, row):
    path.write_text(json.dumps(row))


def process(**changes):
    row = dict(parent=1, uid=os.getuid(), birth='old-start', markers=[], paths=[], supervisor=False)
    row.update(changes)
    return row


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.control = self.root / 'control'
        self.control.mkdir()
        self.group = dict(admission_protocol=2, resource_group='a' * 32,
                          admission_config_sha256='b' * 64)
        write(self.control / 'local-go-resource-group.custody', self.group)
        for name in ('local-go-slot', 'local-go-slot-2'):
            guard = self.control / name
            guard.mkdir()
            write(guard / 'owner.json', dict(protocol_guard=True, resource_group='a' * 32))
        self.folders = []
        self.runs = []
        self.host = {os.getpid(): process()}
        self.guest = dict(markers=[], stopped=True, cleanups=[])
        for obj, name, value in ((recovery.capacity, 'root', self.control),
                                 (recovery, 'host_inventory', self.host),
                                 (recovery, 'guest_inventory', self.guest),
                                 (recovery.budget, 'scratch_processes', set()),
                                 (recovery.runtime, 'source_cleanup', [dict(stopped=True)])):
            mock = patch.object(obj, name, return_value=value)
            setattr(self, 'mock_root' if name == 'root' else name, mock.start())
            self.addCleanup(mock.stop)

    def add_run(self, precise=True, platform='native', index=3):
        folder = self.root / (f'{index:032x}')
        (folder / 'source').mkdir(parents=True)
        slot = self.control / f'local-go-slot-{index}'
        slot.mkdir()
        owner = dict(pid=800000 + index, run_id=f'{index + 100:032x}', mode='full',
                     platform=platform, source_root=str(folder / 'source'), **self.group)
        if precise:
            owner.update(process_identity='old-start', run_marker=f'{index + 200:032x}',
                         scratch=str(folder / 'source/.gate' / f'run.{index + 200:032x}'))
            write(folder / 'native-custody.json', dict(schema=1, identity='c' * 32,
                  run_marker=owner['run_marker'], scratch=owner['scratch'],
                  supervisors=[dict(pid=900000 + index, process_identity='old-start')]))
        manifest = dict(identity=folder.name, mode=owner['mode'], platform=platform,
                        capacity_slot=str(slot), capacity_owner=owner)
        write(folder / 'manifest.json', manifest)
        write(slot / 'owner.json', owner)
        write(self.control / ('local-go-full-' + owner['run_id'] + '.custody'), owner)
        if platform == 'linux':
            write(folder / 'source-identity.json', dict(source_sha='d' * 40, source_dirty=False, source_tree_sha256='e' * 64,
                source_archive_sha256='f' * 64, instrumentation_sha256='a' * 64))
            if precise:
                write(folder / 'linux-custody.json', dict(schema=1, identity='e' * 32,
                    vm='test-vm', source_info=dict(source_sha='d' * 40, source_dirty=False, source_tree_sha256='e' * 64,
                source_archive_sha256='f' * 64, instrumentation_sha256='a' * 64)))
        self.folders.append(folder)
        self.runs.append(owner)
        return owner

    def run_recovery(self):
        return recovery.reconcile(self.folders, self.root)

    def assert_refused(self, reason):
        before = {p: p.read_bytes() for p in self.control.rglob('*') if p.is_file()}
        with self.assertRaisesRegex(RuntimeError, reason):
            self.run_recovery()
        for path, data in before.items():
            self.assertEqual(path.read_bytes(), data)
        self.assertFalse(list(self.control.glob('.reconciled-*')))
        decision = json.loads(next(self.control.glob('reconcile-*.json')).read_text())
        self.assertEqual(decision['status'], 'refused')
        self.assertIn('reason', decision)

    def test_precise_success_archives_all_bindings_and_keeps_scratch(self):
        owner = self.add_run(index=10)  # More than nine measured lanes are valid.
        scratch = Path(owner['scratch'])
        scratch.mkdir(parents=True)
        (scratch / 'retained').write_text('fixture evidence')
        result = self.run_recovery()
        archive = Path(result['archive'])
        self.assertEqual(result['status'], 'reconciled')
        self.assertEqual(len(list(archive.iterdir())), 6)
        self.assertEqual((scratch / 'retained').read_text(), 'fixture evidence')
        self.assertFalse(recovery.capacity.execution_lanes(self.control))
        self.scratch_processes.assert_called_with(str(self.folders[0] / 'source'), set(self.host), all_handles=True,
            protected_roots={owner['pid'], 900010}, evidence=[])

    def test_legacy_success_requires_global_quiet_and_guest_cleanup(self):
        self.add_run(False)
        self.add_run(False, 'linux', 4)
        result = self.run_recovery()
        self.assertEqual(result['status'], 'reconciled')
        self.assertTrue(all(not r['precise'] for r in result['evidence']['host_after'] if 'precise' in r))

    def test_live_holder_and_reused_legacy_pid_refuse(self):
        for precise, birth in ((True, 'old-start'), (False, 'reused-start')):
            with self.subTest(precise=precise):
                owner = self.add_run(precise, index=3 + len(self.folders))
                self.host[owner['pid']] = process(birth=birth)
                self.assert_refused('holder is still live')

    def test_precise_start_identity_does_not_override_live_pid(self):
        owner = self.add_run()
        self.host[owner['pid']] = process(birth='reused-start')
        self.assert_refused('holder is still live')

    def test_live_supervisor_marker_descendant_and_scratch_holder_refuse(self):
        owner = self.add_run()
        cases = [({900003: process()}, 'supervisor is still live'),
                 ({99: process(markers=[owner['run_marker']])}, 'global gate quiet'),
                 ({99: process(parent=owner['pid'])}, 'fixtures remain'),
                 ({99: process(paths=[owner['scratch'] + '/tmp'])}, 'fixtures remain')]
        for processes, reason in cases:
            with self.subTest(reason=reason):
                self.host_inventory.return_value = dict(self.host)
                self.host_inventory.return_value.update(processes)
                self.assert_refused(reason)
        self.host_inventory.return_value = self.host
        self.scratch_processes.return_value = {99}
        self.assert_refused('file handles remain')

    def test_legacy_any_gate_marker_or_supervisor_refuses(self):
        self.add_run(False, 'linux')
        for row in (process(markers=['unrelated-run']), process(supervisor=True)):
            self.host_inventory.return_value = {99: row}
            self.assert_refused('global gate quiet')

    def test_partial_identity_malformed_binding_and_unknown_inventory_refuse(self):
        self.add_run()
        manifest_path = self.folders[0] / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        changed = copy.deepcopy(manifest)
        del changed['capacity_owner']['process_identity']
        write(manifest_path, changed)
        self.assert_refused('binding is malformed')
        write(manifest_path, manifest)
        self.scratch_processes.side_effect = PermissionError('inventory unavailable')
        self.assert_refused('inventory unavailable')
        self.scratch_processes.side_effect = None
        self.host_inventory.side_effect = RuntimeError('host inventory incomplete')
        self.assert_refused('host inventory incomplete')

    def test_guest_failure_and_host_change_during_proof_refuse(self):
        owner = self.add_run()
        self.guest_inventory.side_effect = RuntimeError('guest cleanup unconfirmed')
        self.assert_refused('guest cleanup unconfirmed')
        self.guest_inventory.side_effect = None
        self.host_inventory.side_effect = [self.host, {99: process(markers=[owner['run_marker']])}]
        self.assert_refused('global gate quiet')

    def test_daemon_uncertainty_retains_all_bindings(self):
        self.add_run()
        error = RuntimeError('Docker daemon cleanup is unknown')
        error.runtime_evidence = dict(executable_observations=[dict(
            pid=23, guest_sudo_noninteractive=True, operation='readlink', status='failed')])
        self.source_cleanup.side_effect = error
        self.assert_refused('Docker daemon cleanup is unknown')
        decision = json.loads(next(self.control.glob('reconcile-*.json')).read_text())
        self.assertEqual(decision['evidence']['runtime_failure'], error.runtime_evidence)

    def test_guest_probe_requires_empty_markers_nonce_and_positive_cleanup(self):
        # Exercise the actual parser, not the reconcile-level guest stub.
        spec2 = importlib.util.spec_from_file_location('guest_parser', Path(recovery.__file__))
        parser = importlib.util.module_from_spec(spec2)
        spec2.loader.exec_module(parser)
        request = dict(run_id='a' * 32, nonce='b' * 32, source_info=dict(head='c' * 40))
        good = dict(challenge='d' * 32, stopped=True, markers=[], cleanups=[dict(
            run_id=request['run_id'], receipt=dict(schema=1, identity='b' * 32, stopped=True, status=143), docker=[dict(stopped=True)])])
        variants = [good, dict(good, markers=['active']), dict(good, challenge='wrong'),
                    dict(good, stopped=False), dict(good, cleanups=[])]
        for docker in (None, [], [dict(stopped=False)]):
            changed = copy.deepcopy(good)
            changed['cleanups'][0]['docker'] = docker
            variants.append(changed)
        for field, value in (('identity', 'f' * 32), ('stopped', False), ('status', None)):
            changed = copy.deepcopy(good)
            changed['cleanups'][0]['receipt'][field] = value
            variants.append(changed)
        for index, row in enumerate(variants):
            with patch.object(parser.uuid, 'uuid4') as nonce, patch.object(parser.subprocess, 'run') as run:
                nonce.return_value.hex = 'd' * 32
                run.return_value.stdout = json.dumps(row)
                run.return_value.returncode = 0
                if index == 0:
                    self.assertEqual(parser.guest_inventory('test-vm', [request]), good)
                else:
                    with self.assertRaises(RuntimeError):
                        parser.guest_inventory('test-vm', [request])

    def test_guest_probe_binds_retained_source_and_cleanup_nonce(self):
        home = self.root / 'guest-home'
        active = home / '.cache/mycelium-gates/active'
        active.mkdir(parents=True)
        # Map only the guest's absolute scratch prefix into this fixture.
        scratch = self.root / 'guest-tmp' / ('mycelium-gate-' + 'b' * 32)
        (scratch / '.reports').mkdir(parents=True)
        source = dict(head='c' * 40)
        write(scratch / 'source-identity.json', source)
        receipt = dict(schema=1, identity='b' * 32, stopped=True, status=143)
        write(scratch / '.reports/cleanup.json', receipt)
        request = dict(run_id='a' * 32, nonce='b' * 32, source_info=source)
        prefix = """def source_cleanup(*args, **kwargs): return [dict(stopped=True)]
def host_inventory(): return {}
def scratch_processes(*args, **kwargs): return set()
"""
        # Substitute only guest fixture roots; replacing pathlib.Path itself is
        # incompatible with its different construction paths on Python 3.9/3.12.
        probe = recovery.GUEST_PROBE.replace("pathlib.Path.home()", "pathlib.Path(sys.argv[5])")
        probe = probe.replace("pathlib.Path('/tmp/mycelium-gate-'+nonce)",
                              "pathlib.Path(sys.argv[4])/('mycelium-gate-'+nonce)")
        probe = probe.replace("pathlib.Path('/tmp').glob", "pathlib.Path(sys.argv[4]).glob")
        command = [sys.executable, '-c', prefix + probe,
                   json.dumps([request]), 'challenge', 'false', str(scratch.parent), str(home)]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['cleanups'][0]['receipt'], receipt)
        for field, value in (('identity', 'd' * 32), ('stopped', False)):
            write(scratch / '.reports/cleanup.json', dict(receipt, **{field: value}))
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
        write(scratch / '.reports/cleanup.json', receipt)
        write(scratch / 'source-identity.json', dict(head='different-source'))
        self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)

    def test_host_inventory_recognizes_invocations_without_shell_text_false_positive(self):
        spec2 = importlib.util.spec_from_file_location('host_parser', Path(recovery.__file__))
        parser = importlib.util.module_from_spec(spec2)
        spec2.loader.exec_module(parser)
        lines = [f'{os.getpid()} 1 501 S Mon Nov 9 12:00:00 2026 python3 scripts/gate-local.py',
                 '98 1 501 S Mon Nov 9 12:00:00 2026 /bin/sh -c echo gate-local.py',
                 '99 1 501 S Mon Nov 9 12:00:00 2026 Python -u scripts/gate-budget.py GATE_RUN_ID=marked',
                 '100 1 501 S Mon Nov 9 12:00:00 2026 Python scripts/gate-measure.py GATE_CUSTODY_ID=supervised']
        with patch.object(parser.subprocess, 'run') as run:
            run.return_value.stdout = '\n'.join(lines)
            rows = parser.host_inventory()
        self.assertFalse(rows[os.getpid()]['supervisor'])
        self.assertFalse(rows[98]['supervisor'])
        self.assertTrue(rows[99]['supervisor'])
        self.assertEqual(rows[99]['markers'], ['marked'])
        self.assertEqual(rows[100]['markers'], ['supervised'])
        with patch.object(parser.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, ['ps'])):
            with self.assertRaises(subprocess.CalledProcessError):
                parser.host_inventory()

    def test_native_handle_and_closed_descriptor_mapping_keep_custody(self):
        owner = self.add_run()
        root = Path(owner['scratch'])
        root.mkdir(parents=True)
        (root / 'held').write_bytes(b'x' * 4096)
        for mapped in (False, True):
            with self.subTest(mapped=mapped):
                script = 'import mmap,os,signal,sys; f=open(sys.argv[1], "rb"); '
                if mapped:
                    script += 'mapping=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ); f.close(); os.environ.clear(); '
                script += 'print("ready",flush=True); signal.pause()'
                child = subprocess.Popen([sys.executable, '-c', script, str(root / 'held')],
                                         cwd='/', stdout=subprocess.PIPE, text=True)
                try:
                    self.assertEqual(child.stdout.readline().strip(), 'ready')
                    self.assertEqual(REAL_HANDLES(str(root), {child.pid}, all_handles=True), {child.pid})
                    self.host[child.pid] = process()
                    self.host[700001] = process(uid=os.getuid() + 1)
                    self.scratch_processes.side_effect = REAL_HANDLES
                    self.assert_refused('file handles remain')
                    self.scratch_processes.assert_called_with(str(self.folders[0] / 'source'),
                                                             {os.getpid(), child.pid}, all_handles=True,
                        protected_roots={owner['pid'], 900003}, evidence=[])
                finally:
                    self.host.pop(child.pid, None)
                    child.kill()
                    child.wait()
                    child.stdout.close()

    def test_protected_process_exclusions_require_system_identity_and_safe_ancestry(self):
        spec = importlib.util.spec_from_file_location('handles', Path(recovery.__file__).with_name('gate-handles.py'))
        handles = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handles)
        base = '1 0 /sbin/launchd /sbin/launchd\n'
        cases = [('409 1 /usr/libexec/UserEventAgent /usr/libexec/UserEventAgent', None),
                 ('409 1 /opt/fixture/python /opt/fixture/python', 'not a protected'),
                 ('409 800 /usr/bin/fixture /usr/bin/fixture\n800 1 /opt/python /opt/python gate-budget.py', 'descends'),
                 ('409 1 /usr/libexec/fixture /usr/libexec/fixture GATE_RUN_ID=owned', 'marker or path'),
                 ('409 1 /usr/libexec/fixture /usr/libexec/fixture /retained/source/input', 'marker or path')]
        for line, reason in cases:
            def ps(args, **kwargs):
                return line.split(None, 3)[2] if 'comm=' in args else base + line
            with self.subTest(line=line), patch.object(handles.subprocess, 'check_output', side_effect=ps), \
                    patch.object(handles.c, 'CDLL'), \
                    patch.object(handles, 'sealed_residency', return_value=dict(mount_on='/', readonly=True)), \
                    patch.object(handles, 'inventory_pid', side_effect=PermissionError(errno.EPERM, 'restricted')):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    if reason:
                        with self.assertRaisesRegex(RuntimeError, reason):
                            handles.inventory('/retained/source', [409], True, {800})
                    else:
                        handles.inventory('/retained/source', [409], True, {800})
                        row = json.loads(output.getvalue()[1:])
                        self.assertEqual(row['pid'], 409)
                        self.assertEqual(row['ancestry'], [409, 1])
                        self.assertFalse(row['gate_markers_or_paths'])
        with patch.object(handles.subprocess, 'check_output', side_effect=PermissionError('ps unavailable')):
            with self.assertRaises(PermissionError):
                handles.protected_process(409, '/retained/source', set())

    def test_protected_executable_requires_readonly_root_not_data_volume(self):
        spec = importlib.util.spec_from_file_location('mounts', Path(recovery.__file__).with_name('gate-handles.py'))
        handles = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handles)
        with patch.object(handles.c, 'CDLL') as load:
            with self.assertRaisesRegex(RuntimeError, 'writable or auxiliary volume'):
                handles.sealed_residency('/System/Volumes/Data/fixture')
            load.assert_not_called()
            for mount, flags, accepts in ((b'/', 1, True), (b'/', 0, False),
                                           (b'/System/Volumes/Data', 1, False)):
                def observe(path, pointer):
                    info = handles.c.cast(pointer, handles.c.POINTER(handles.StatFS)).contents
                    info.mount_on, info.flags = mount, flags
                    return 0
                load.return_value.statfs64.side_effect = observe
                if accepts:
                    self.assertTrue(handles.sealed_residency('/usr/bin/fixture')['readonly'])
                else:
                    with self.assertRaisesRegex(RuntimeError, 'read-only system root'):
                        handles.sealed_residency('/usr/bin/fixture')


if __name__ == '__main__':
    unittest.main()
