#!/usr/bin/env python3
"""Daemon resources must not disappear with their test's process custody."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('runtime', Path(__file__).with_name('gate-runtime.py'))
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)
REAL_LAUNCHER_ENVIRONMENT = runtime.launcher_environment


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.marker = 'a' * 32
        self.endpoint = 'unix:///fixture/docker.sock'
        self.entry = dict(schema=1, marker=self.marker, labelled_fixtures=False,
                          endpoint=self.endpoint, configured=True,
                          reachable=True, daemon_identity='fixture-daemon')
        mock = patch.object(runtime.shutil, 'which', return_value='/fixture/docker')
        mock.start()
        self.addCleanup(mock.stop)
        self.resources = {}
        self.contexts = {'default': self.endpoint, 'desktop-linux': 'unix:///fixture/desktop.sock'}
        for name, value in [('launcher_environment', dict(checked=[1], unreadable=[])),
                            ('local_socket_paths', [])]:
            mock = patch.object(runtime, name, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)
        def call(binary, endpoint, arguments):
            if arguments[:2] == ['context', 'ls']:
                return '\n'.join(json.dumps(name) for name in self.contexts)
            if arguments[:2] == ['context', 'inspect']:
                name = arguments[2] if arguments[2] != '--format' else 'default'
                return json.dumps(self.contexts[name])
            if arguments[0] == 'info':
                return '"fixture-daemon"'
            kind = 'container' if arguments[0] == 'ps' else arguments[0]
            if '--filter' in arguments:
                label = arguments[-1]
                return self.resources.get((kind, label), '')
            return self.resources.get((kind, 'names'), '')
        mock = patch.object(runtime, 'docker_call', side_effect=call)
        self.call = mock.start()
        self.addCleanup(mock.stop)

    def test_positive_empty_runtime_inventory(self):
        proof = runtime.docker_inventory(self.marker, self.entry)
        self.assertEqual(proof[0]['resources'], dict(container=[], volume=[], network=[]))
        self.assertTrue(proof[0]['stopped'])

    def test_each_daemon_resource_retains_capacity(self):
        for kind in ('container', 'volume', 'network'):
            with self.subTest(kind=kind):
                self.resources = {(kind, 'label=myce.gate=' + self.marker): 'owned-resource\n'}
                with self.assertRaisesRegex(RuntimeError, 'fixture resources remain'):
                    runtime.docker_inventory(self.marker, self.entry)

    def test_unlabelled_legacy_fixture_and_other_compose_services(self):
        self.resources = {('container', 'names'): '"unrelated-compose-service"\n'}
        self.assertTrue(runtime.docker_inventory(self.marker, self.entry)[0]['stopped'])
        self.resources[('container', 'names')] += '"myce-model-view-old-fixture"\n'
        with self.assertRaisesRegex(RuntimeError, 'fixture resources remain'):
            runtime.docker_inventory(self.marker, self.entry)
        # New labelled sources inspect only their run; installed fleet work is
        # independent capacity and must not hold an unrelated gate reservation.
        self.resources[('container', 'label=myce.worker')] = 'another-runtime\n'
        self.assertTrue(runtime.docker_inventory(self.marker,
            dict(self.entry, labelled_fixtures=True))[0]['stopped'])

    def test_unreachable_or_failed_inventory_is_not_cleanup(self):
        self.call.side_effect = subprocess.CalledProcessError(1, ['docker'])
        with self.assertRaisesRegex(RuntimeError, 'unknown or unreachable'):
            runtime.docker_inventory(self.marker, self.entry)
        with patch.object(runtime, 'docker_snapshot', return_value=self.entry):
            with self.assertRaises(subprocess.CalledProcessError):
                runtime.docker_inventory(self.marker, self.entry)

    def test_no_runtime_requires_positive_prerequisite_absence(self):
        with (patch.object(runtime.shutil, 'which', return_value=None),
              patch.object(runtime.Path, 'exists', return_value=False)):
            absent = dict(self.entry, configured=False, reachable=False)
            self.assertTrue(runtime.docker_inventory(self.marker, absent)[0]['unavailable'])
            with self.assertRaisesRegex(RuntimeError, 'legacy Docker context inventory is unavailable'):
                runtime.docker_inventory(self.marker)
            with self.assertRaisesRegex(RuntimeError, 'unknown or unreachable'):
                runtime.docker_inventory(self.marker, self.entry)

    def test_journal_binding_and_daemon_identity_are_required(self):
        with self.assertRaisesRegex(RuntimeError, 'journal is malformed'):
            runtime.docker_inventory(self.marker, self.entry, [dict(marker='other', endpoint=self.endpoint)])
        with patch.object(runtime, 'docker_snapshot', return_value=dict(self.entry, daemon_identity='other')):
            with self.assertRaisesRegex(RuntimeError, 'identity changed'):
                runtime.docker_inventory(self.marker, self.entry)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            (path / 'docker-entry.json').write_text(json.dumps(self.entry))
            (path / 'docker-used.jsonl').write_text('not-json\n')
            with self.assertRaises(ValueError):
                runtime.runtime_cleanup(path, self.marker)

    def test_legacy_requires_all_contexts_reachable_and_deduplicates_daemons(self):
        proof = runtime.docker_inventory(self.marker)
        self.assertEqual(len(proof), 1)
        self.assertEqual(proof[0]['endpoints'], sorted(self.contexts.values()))
        self.assertTrue(proof[0]['stopped'])
        self.assertIn('legacy_discovery', proof[0])
        self.contexts['extra'] = 'tcp://unreachable.invalid:2375'
        actual = self.call.side_effect
        def failure(binary, endpoint, arguments):
            if endpoint == self.contexts['extra']:
                raise subprocess.CalledProcessError(1, ['docker'])
            return actual(binary, endpoint, arguments)
        self.call.side_effect = failure
        with self.assertRaisesRegex(RuntimeError, 'unknown or unreachable'):
            runtime.docker_inventory(self.marker)

    def test_new_snapshot_records_effective_context_endpoint(self):
        with patch.dict(os.environ, DOCKER_CONTEXT='desktop-linux', DOCKER_HOST='tcp://other.invalid:2375'):
            snapshot = runtime.docker_snapshot()
        self.assertEqual(snapshot['endpoint'], self.contexts['desktop-linux'])
        self.assertEqual(snapshot['context'], 'desktop-linux')
        self.assertEqual(snapshot['endpoint_source'], 'DOCKER_CONTEXT')
        with patch.dict(os.environ, DOCKER_CONTEXT='', DOCKER_HOST=self.endpoint):
            snapshot = runtime.docker_snapshot()
        self.assertEqual(snapshot['endpoint'], self.endpoint)
        self.assertEqual(snapshot['endpoint_source'], 'DOCKER_HOST')

    def test_readable_launch_override_refuses_and_unreadable_environment_is_recorded(self):
        lines = f'{os.getpid()} 1 {os.getuid()} python HOME=/fixture\n1 0 0 launchd'
        def ps(args, **kwargs):
            return lines if 'eww' in args else lines.replace(' HOME=/fixture', '')
        with patch.dict(os.environ, DOCKER_HOST='', DOCKER_CONTEXT=''), \
                patch.object(runtime.subprocess, 'check_output', side_effect=ps):
            proof = REAL_LAUNCHER_ENVIRONMENT()
            self.assertEqual(proof['checked'], [os.getpid()])
            altered = lines.replace('HOME=/fixture', 'HOME=/fixture DOCKER_HOST=tcp://fixture.invalid:2375')
            def overridden(args, **kwargs):
                return altered if 'eww' in args else lines.replace(' HOME=/fixture', '')
            with patch.object(runtime.subprocess, 'check_output', side_effect=overridden):
                with self.assertRaisesRegex(RuntimeError, 'contains Docker endpoint overrides'):
                    REAL_LAUNCHER_ENVIRONMENT()
            with patch.object(runtime.subprocess, 'check_output', side_effect=PermissionError('unreadable')):
                proof = REAL_LAUNCHER_ENVIRONMENT()
                self.assertIn('inventory_uncertainty', proof)
                self.assertIn('outside this host', proof['residual_risk'])


if __name__ == '__main__':
    unittest.main()
