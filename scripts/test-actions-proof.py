#!/usr/bin/env python3
"""Offline tree-proof and superseded-main coverage regressions."""
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('ci', Path(__file__).with_name('actions-full.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
proof = ci.proof


class FakeGitHub:
    def __init__(self):
        self.statuses = {}
        self.prs = {}
        self.trees = {}
        self.published = []

    def status(self, sha, tree):
        return self.statuses.get((sha, tree))

    def publish(self, sha, tree, url, state='success'):
        self.statuses[sha, tree] = {'state': state, 'target_url': url, 'source_sha': sha}
        self.published.append((sha, tree, state))

    def pages(self, path):
        return self.prs.get(path, [])

    def tree(self, sha):
        return self.trees[sha]


class ActionsProofTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.old = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.old)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        (self.root / 'source').write_text('initial')
        profiles = self.root / '.mycelium/profiles'
        profiles.mkdir(parents=True)
        for name in ('gate-full-darwin', 'gate-full'):
            (profiles / (name + '.yaml')).write_text('source:\n  git: HEAD\nsuite: original\n')
        self.commit('base')
        (self.root / 'scripts').mkdir()
        (self.root / 'scripts/actions-proof.py').write_text('protocol fixture')
        self.first = self.commit('activate')
        (self.root / 'source').write_text('different')
        (profiles / 'gate-full.yaml').write_text('source:\n  git: HEAD\nsuite: revised\n')
        self.second = self.commit('new source')
        self.third = self.commit('identical source')

    def git(self, *args):
        return subprocess.check_output(['git', *args], text=True, stderr=subprocess.DEVNULL).strip()

    def commit(self, message):
        self.git('add', '.')
        self.git('commit', '-q', '--allow-empty', '-m', message)
        return self.git('rev-parse', 'HEAD')

    def test_coalesced_history_proves_each_distinct_tree_and_reuses_on_rerun(self):
        self.assertEqual(proof.main_history(self.third), [self.first, self.second, self.third])
        api = FakeGitHub()
        executed = []
        def run(root, sha, scratch, owner):
            self.assertEqual(ci.active_sha(owner, ''), sha)
            for name in ('gate-full-darwin', 'gate-full'):
                self.assertEqual((root / f'.mycelium/profiles/{name}.yaml').read_text().strip(),
                                 self.git('show', f'{sha}:.mycelium/profiles/{name}.yaml'))
            executed.append(sha)
            return 0
        with patch.object(proof, 'GitHub', return_value=api), patch.object(ci, 'run', run), \
                patch.object(ci, 'cleanup', return_value=True), \
                patch.dict(os.environ, RUNNER_TEMP=str(self.root), GITHUB_RUN_ATTEMPT='1'), \
                redirect_stdout(io.StringIO()) as output:
            scratch = self.root / 'scratch'
            scratch.mkdir()
            self.assertEqual(ci.run_pending(self.root, self.third, scratch, '123:1'), 0)
            self.assertEqual(executed, [self.first, self.second])
            self.assertEqual(ci.run_pending(self.root, self.third, scratch, '123:1'), 0)
            self.assertEqual(executed, [self.first, self.second])
        self.assertIn('SKIP: identical tree', output.getvalue())
        self.assertEqual(len(api.statuses), 3)

    def test_pr_proof_requires_same_tree_and_success_and_base_repository(self):
        api = FakeGitHub()
        tree = self.git('rev-parse', self.second + '^{tree}')
        head = 'a' * 40
        pr = {'merged_at': '2026-11-09', 'base': {'ref': 'main', 'repo': {'full_name': proof.REPOSITORY}},
              'head': {'sha': head}}
        api.prs[f'commits/{self.second}/pulls'] = [pr]
        api.trees[head] = self.git('rev-parse', self.first + '^{tree}')
        api.publish(head, tree, 'https://github.com/1kuna/mycelium/pull/1')
        self.assertIsNone(proof.proof_for_commit(api, self.second, tree))
        api.trees[head] = tree
        self.assertEqual(proof.proof_for_commit(api, self.second, tree)['state'], 'success')
        api.statuses[head, tree]['state'] = 'pending'
        self.assertIsNone(proof.proof_for_commit(api, self.second, tree))
        api.statuses[head, tree]['state'] = 'success'
        pr['base']['repo']['full_name'] = 'other/repository'
        self.assertIsNone(proof.proof_for_commit(api, self.second, tree))

    def test_interrupted_backlog_cleans_active_commit_including_final_step(self):
        backlog = self.first
        def interrupt(root, sha, scratch, owner):
            ci.active_file(owner).write_text(json.dumps({'sha': backlog}))
            raise ci.SubmitterStopped()
        env = dict(GITHUB_SHA=self.third, GITHUB_RUN_ID='123', GITHUB_RUN_ATTEMPT='1',
                   RUNNER_TEMP=str(self.root))
        with patch.dict(os.environ, env), patch.object(ci.sys, 'argv', ['actions-full.py']), \
                patch.object(ci, 'run_pending', interrupt), patch.object(ci, 'cleanup', return_value=True) as cleanup, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.main(), 1)
            cleanup.assert_called_once_with(backlog, '123:1')
            cleanup.reset_mock()
            with patch.object(ci.sys, 'argv', ['actions-full.py', '--cleanup-only']):
                self.assertEqual(ci.main(), 0)
            cleanup.assert_called_once_with(backlog, '123:1')

    def test_failed_tree_is_not_success_or_automatically_retried(self):
        api = FakeGitHub()
        tree = self.git('rev-parse', self.first + '^{tree}')
        api.publish(self.first, tree, 'https://github.com/1kuna/mycelium/actions/runs/1', 'failure')
        with patch.object(proof, 'GitHub', return_value=api), patch.object(ci, 'run') as run, \
                patch.dict(os.environ, GITHUB_RUN_ATTEMPT='1'), redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.first, self.root, '123:1'), 1)
            run.assert_not_called()
        with patch.object(proof, 'GitHub', return_value=api), patch.object(ci, 'run', return_value=0) as run, \
                patch.object(ci, 'cleanup', return_value=True), \
                patch.dict(os.environ, RUNNER_TEMP=str(self.root), GITHUB_RUN_ATTEMPT='1'), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.third, self.root, '124:1'), 0)
            self.assertEqual([call.args[1] for call in run.call_args_list], [self.second])

    def test_only_clean_matching_full_receipts_authorize_publication(self):
        sha = self.third
        # Same canonical manifest used by the local gate source freezer.
        files = {name: (0o644, hashlib.sha256((self.root / name).read_bytes()).hexdigest())
                 for name in self.git('ls-files').splitlines()}
        identity = {'source_sha': sha, 'source_dirty': False,
                    'source_tree_sha256': hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(),
                    'instrumentation_sha256': 'a' * 64}
        folder = self.root / 'receipts'
        folder.mkdir()
        report = folder / 'report.jsonl'
        manifest = {'mode': 'full', 'platform': 'both', 'command': [], 'identity': 'fixture', 'report': str(report)}
        cleanup = {'schema': 1, 'identity': 'fixture', 'stopped': True, 'status': 0}
        rows = [dict(identity, schema=1, run_id='fixture', phase=phase, platform=platform,
                     mode='full', status=0, refusal=[]) for phase, platform in
                [('host', 'darwin/arm64'), ('native', 'darwin/arm64'), ('linux', 'linux/aarch64')]]
        def write():
            for name, value in [('manifest', manifest), ('source-identity', identity), ('overall-cleanup', cleanup)]:
                (folder / (name + '.json')).write_text(json.dumps(value))
            report.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        write()
        self.assertEqual(proof.local_proof(folder), (sha, self.git('rev-parse', sha + '^{tree}')))
        for field, invalid in [('mode', 'quick'), ('platform', 'native'), ('command', ['true'])]:
            before = manifest[field]
            manifest[field] = invalid
            write()
            with self.assertRaises(ValueError): proof.local_proof(folder)
            manifest[field] = before
        for field, invalid in [('source_dirty', True), ('source_tree_sha256', '0' * 64)]:
            before = identity[field]
            identity[field] = invalid
            write()
            with self.assertRaises(ValueError): proof.local_proof(folder)
            identity[field] = before
        for field, invalid in [('status', 1), ('source_sha', self.first), ('phase', 'native')]:
            before = rows[-1][field]
            rows[-1][field] = invalid
            write()
            with self.assertRaises(ValueError): proof.local_proof(folder)
            rows[-1][field] = before
        cleanup['stopped'] = False
        write()
        with self.assertRaises(ValueError): proof.local_proof(folder)

    def test_latest_status_cannot_be_overridden_by_old_success(self):
        api = object.__new__(proof.GitHub)
        sha, tree = 'a' * 40, 'b' * 40
        with patch.object(api, 'pages', return_value=[
            {'context': proof.CONTEXT, 'description': 'tree:' + tree, 'state': 'failure'},
            {'context': proof.CONTEXT, 'description': 'tree:' + tree, 'state': 'success'},
        ]):
            self.assertEqual(api.status(sha, tree)['state'], 'failure')
        with patch.object(api, 'request') as request:
            with self.assertRaises(ValueError): api.publish(sha, tree, 'https://example.invalid/private')
            request.assert_not_called()


if __name__ == '__main__':
    unittest.main()
