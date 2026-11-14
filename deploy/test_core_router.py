"""Harmless fixtures for the actual operator-installed handler, on any laptop."""
import importlib.machinery
import importlib.util
import json
import os
import stat
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).with_name('myce-core-router')
loader = importlib.machinery.SourceFileLoader('router', str(SOURCE))
spec = importlib.util.spec_from_loader(loader.name, loader)
router = importlib.util.module_from_spec(spec)
loader.exec_module(router)


class CoreRouterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = self.root / 'cores'
        self.store.mkdir()
        (self.store / 'keep').mkdir()
        self.state = self.root / 'state'
        self.state.mkdir()
        self.identity = 'a' * 32
        self.group = '/service/attempt-' + self.identity
        self.cfg = {'workers': [{'cgroup': '/service', 'state_dir': str(self.state)}],
                    'budget_bytes': 1024 * 1024,
                    'previous_core_pattern': '|/usr/share/apport/apport -P%P'}
        self.record = {'identity': self.identity, 'cgroup': '/sys/fs/cgroup' + self.group,
                       'session': 'session', 'job': {'id': 'fixture', 'attempt': 2,
                       'session': 'session', 'spec': {'worker_data': 'ephemeral'}}}
        self.path = self.state / 'state-attempt-fixture.json'
        self.save()
        self.addCleanup(patch.stopall)
        patch.object(router, 'ROOT', str(self.store)).start()
        patch.object(router, 'membership', return_value=self.group + '/main').start()
        self.events = patch.object(router, 'event').start()

    def save(self):
        self.path.write_text(json.dumps(self.record))

    def test_ephemeral_stream_is_closed_unread_with_metadata_only(self):
        # A real open pipe, with no bytes or EOF: reading/draining would hang.
        code = '''import importlib.machinery, json, sys
r = importlib.machinery.SourceFileLoader('router', sys.argv[1]).load_module()
r.membership = lambda pid: sys.argv[3]
r.ROOT = sys.argv[4]
r.suppression_evidence = lambda *args: print(json.dumps(args))
sys.exit(r.route(json.loads(sys.argv[2]), ['-P42', '-s11']))
'''
        read, write = os.pipe()
        try:
            p = subprocess.Popen([sys.executable, '-c', code, str(SOURCE), json.dumps(self.cfg),
                                  self.group + '/main', str(self.store)], stdin=read,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                out, err = p.communicate(timeout=5)
            finally:
                if p.poll() is None:
                    p.kill()
                    p.wait()
            self.assertEqual(p.returncode, 0, err)
            self.assertEqual(json.loads(out), [42, '11', self.group + '/main', 'attempt-' + self.identity])
            self.assertEqual(list(self.store.iterdir()), [self.store / 'keep'])
        finally:
            os.close(read)
            os.close(write)

    def test_native_journal_metadata_contains_no_payload_fields(self):
        with patch.object(router.socket, 'socket') as socket, patch.object(router.time, 'time_ns', return_value=123000):
            router.suppression_evidence(42, '11', self.group, 'attempt-' + self.identity)
        message = socket.return_value.__enter__.return_value.send.call_args.args[0].decode()
        fields = dict(line.split('=', 1) for line in message.splitlines())
        self.assertEqual(fields, {'SYSLOG_IDENTIFIER': 'myce-core-router', 'MESSAGE': 'Ephemeral core discarded unread',
                                 'COREDUMP_PID': '42', 'COREDUMP_SIGNAL': '11', 'COREDUMP_TIMESTAMP': '123',
                                 'COREDUMP_CGROUP': self.group, 'MYCE_ATTEMPT': 'attempt-' + self.identity,
                                 'MYCE_CORE_CAPTURE': 'suppressed_ephemeral'})

    def test_ordinary_attempt_retains_bounded_capture(self):
        self.record['job']['spec'] = {}
        self.save()
        with patch.object(router.os, 'read', side_effect=[b'harmless-core', b'']):
            self.assertEqual(router.route(self.cfg, ['-P42', '-s11']), 0)
        cores = list(self.store.glob('core-*'))
        self.assertEqual(len(cores), 1)
        self.assertEqual(cores[0].read_bytes(), b'harmless-core')
        self.assertEqual(cores[0].stat().st_mode & 0o777, 0o600)
        # A full ordinary store still refuses before reading stdin.
        self.cfg['budget_bytes'] = 1
        with patch.object(router.os, 'read', side_effect=AssertionError('budget-full stream read')):
            self.assertEqual(router.route(self.cfg, ['-P42', '-s11']), 0)
        self.assertEqual(list(self.store.glob('core-*')), cores)

    def test_partial_ordinary_core_is_removed_on_overflow(self):
        with patch.object(router, 'allocated', side_effect=[0, 0, 1024 * 1024 + 1]), patch.object(router.os, 'read', side_effect=[b'first', b'second']):
            self.assertEqual(router.capture(self.cfg, 42), 0)
        self.assertEqual(list(self.store.glob('core-*')), [])

    def test_foreign_process_forwards_original_arguments_and_stdin(self):
        args = ['-P42', '-s11', '-F7', '--', '!bin!fixture']
        with patch.object(router, 'membership', return_value='/another-service'), patch.object(router.os, 'execv') as forward, patch.object(router.os, 'read', side_effect=AssertionError('stdin read')):
            router.route(self.cfg, args)
        forward.assert_called_once_with('/usr/share/apport/apport', ['/usr/share/apport/apport'] + args)

    def test_unknown_or_stale_attempt_never_captures_or_forwards(self):
        for key, value in [('identity', 'b' * 32), ('cgroup', '/sys/fs/cgroup/other'), ('session', 'stale')]:
            with self.subTest(key=key):
                saved = self.record[key]
                self.record[key] = value
                self.save()
                with patch.object(router, 'capture', side_effect=AssertionError('core captured')), patch.object(router.os, 'execv', side_effect=AssertionError('core forwarded')):
                    with self.assertRaises(ValueError):
                        router.route(self.cfg, ['-P42', '-s11'])
                self.record[key] = saved
        self.path.unlink()
        with self.assertRaises(ValueError):
            router.route(self.cfg, ['-P42', '-s11'])

    def test_qualification_requires_matching_state_and_proc_lifetime(self):
        from types import SimpleNamespace
        patch.object(router.os, 'stat', return_value=SimpleNamespace(st_mode=stat.S_IFSOCK)).start()
        patch.object(router.os.path, 'isdir', return_value=True).start()
        with patch.object(router.Path, 'read_text', return_value='10'), patch('builtins.print') as output:
            router.qualify(self.cfg, str(self.state))
            output.assert_called_once_with(router.PROTOCOL)
            with self.assertRaises(ValueError):
                router.qualify(self.cfg, str(self.root))
        with patch.object(router.Path, 'read_text', return_value='0'), self.assertRaises(ValueError):
            router.qualify(self.cfg, str(self.state))

    def test_config_failure_never_delegates(self):
        with patch.object(router, 'CONFIG', str(self.root / 'missing')), patch.object(router.os, 'execv', side_effect=AssertionError('core forwarded')):
            self.assertEqual(router.main(), 1)

    def test_pattern_activation_and_undo_fence_other_owners(self):
        self.cfg['previous_core_pattern'] = router.PATTERN.replace('/usr/local/libexec/myce-core-router', '/usr/share/apport/apport')
        for action, current, expected in [('--activate', self.cfg['previous_core_pattern'], router.PATTERN),
                                           ('--deactivate', router.PATTERN, self.cfg['previous_core_pattern'])]:
            with self.subTest(action=action), patch.object(router, 'configuration', return_value=self.cfg), patch.object(sys, 'argv', ['router', action]):
                if action == '--activate':
                    from unittest.mock import mock_open
                    fake = mock_open(read_data=current)
                    with patch('builtins.open', fake):
                        self.assertEqual(router.main(), 0)
                    fake().write.assert_called_once_with(expected + '\n')
                    with patch('builtins.open', mock_open(read_data='core.someone-else')), self.assertRaises(ValueError):
                        router.main()
                else:
                    with patch.object(router.Path, 'read_text', return_value=current), patch.object(router.Path, 'write_text') as write:
                        self.assertEqual(router.main(), 0)
                    write.assert_called_once_with(expected + '\n')
                    with patch.object(router.Path, 'read_text', return_value='core.someone-else'), self.assertRaises(ValueError):
                        router.main()


if __name__ == '__main__':
    unittest.main()
