#!/usr/bin/env python3
"""Offline regressions for main event ordering and frozen submission identity."""
from contextlib import redirect_stdout
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

spec = importlib.util.spec_from_file_location('ci', Path(__file__).with_name('actions-full.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
proof = ci.proof


class FakeGitHub:
    def __init__(self, tip):
        self.tip = tip
        self.statuses = {}
        self.published = []
        self.counters = {}
        self.counter_states = {}

    def main_tip(self):
        return self.tip

    def status(self, sha, tree):
        return self.statuses.get((sha, tree))

    def attempts(self, sha, tree):
        return self.counters.get((sha, tree), (0, None))

    def record_attempt(self, sha, tree, count, owner, url, state='error'):
        self.counters[sha, tree] = count, owner
        self.counter_states[sha, tree] = state

    def publish(self, sha, tree, url, state='success'):
        self.statuses[sha, tree] = {'state': state, 'target_url': url, 'source_sha': sha}
        self.published.append((sha, tree, state))


class ActionsProofTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'repo'
        self.root.mkdir()
        self.old = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.old)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        (self.root / 'source').write_text('initial')
        profiles = self.root / '.mycelium/profiles'
        profiles.mkdir(parents=True)
        self.adapter = self.root / '.mycelium/adapter.yaml'
        self.adapter.write_text('adapter: initial\n')
        for name in ('gate-full-darwin', 'gate-full'):
            (profiles / (name + '.yaml')).write_text(
                'source:\n  git: HEAD\ninputs:\n  .mycelium/adapter.yaml: .mycelium/adapter.yaml\n')
        self.commit('base')
        (self.root / 'scripts').mkdir()
        (self.root / 'scripts/actions-proof.py').write_text('protocol fixture')
        self.first = self.commit('A activate')
        (self.root / 'source').write_text('B')
        self.adapter.write_text('adapter: B\n')
        self.second = self.commit('B source and input')
        (self.root / 'source').write_text('C')
        self.adapter.write_text('adapter: C\n')
        self.third = self.commit('C source and input')
        self.repeat = self.commit('identical C tree')
        remote = Path(self.temp.name) / 'remote.git'
        self.git('clone', '--bare', '--quiet', str(self.root), str(remote))
        self.git('remote', 'add', 'origin', str(remote))
        self.api = FakeGitHub(self.repeat)
        self.env = dict(RUNNER_TEMP=self.temp.name, GITHUB_RUN_ATTEMPT='1',
                        GITHUB_EVENT_NAME='push', MYCELIUM_RECOVERY_TREE='')
        self.scratch = Path(self.temp.name) / 'scratch'
        self.scratch.mkdir()

    def git(self, *args):
        return subprocess.check_output(['git', *args], text=True, stderr=subprocess.DEVNULL).strip()

    def commit(self, message):
        self.git('add', '.')
        self.git('commit', '-q', '--allow-empty', '-m', message)
        return self.git('rev-parse', 'HEAD')

    def test_coalesced_history_proves_each_distinct_tree_and_reuses_on_rerun(self):
        self.assertEqual(proof.main_history(self.repeat), [self.first, self.second, self.third, self.repeat])
        executed = []
        def run(root, sha, scratch, owner):
            self.assertEqual(ci.active_sha(owner, ''), sha)
            self.assertEqual((root / '.mycelium/adapter.yaml').read_text().strip(),
                             self.git('show', f'{sha}:.mycelium/adapter.yaml'))
            executed.append(sha)
            return 0
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', run), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()) as output:
            self.assertEqual(ci.run_pending(self.root, self.repeat, self.scratch, '123:1'), 0)
            self.assertEqual(executed, [self.first, self.second, self.third])
            self.assertEqual(ci.run_pending(self.root, self.repeat, self.scratch, '123:1'), 0)
            self.assertEqual(executed, [self.first, self.second, self.third])
        self.assertIn('SKIP: identical tree', output.getvalue())
        self.assertEqual(len(self.api.statuses), 4)

    def test_A_running_C_pending_then_delayed_B_covers_authoritative_C(self):
        # A completed while C entered the pending slot; delayed B replaces C.
        tree = self.git('rev-parse', self.first + '^{tree}')
        self.api.publish(self.first, tree, 'https://github.com/1kuna/mycelium/actions/runs/1')
        pending = self.third
        pending = self.second
        self.api.tip = self.third
        executed = []
        with patch.object(proof, 'GitHub', return_value=self.api), \
                patch.object(ci, 'run', side_effect=lambda root, sha, *args: executed.append(sha) or 0), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, pending, self.scratch, '123:1'), 0)
        self.assertEqual(executed, [self.second, self.third])
        self.assertIn((self.third, self.git('rev-parse', self.third + '^{tree}')), self.api.statuses)

    def test_tip_advance_during_batch_covers_every_new_tree_before_exit(self):
        self.api.tip = self.second
        executed = []
        def run(root, sha, *args):
            executed.append(sha)
            self.api.tip = self.third
            return 0
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', run), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.second, self.scratch, '123:1'), 0)
        self.assertEqual(executed, [self.first, self.second, self.third])

    def test_both_submissions_use_historical_adapter_and_mutation_cannot_publish(self):
        submitted = []
        def command(*args, cwd=None):
            output = ''
            if args[0] == 'run':
                sha, platform = args[-1].split(':')[1:3]
                self.assertNotEqual(cwd, self.root)
                self.assertEqual((cwd / '.mycelium/adapter.yaml').read_text().strip(),
                                 self.git('show', f'{sha}:.mycelium/adapter.yaml'))
                self.assertIn('git: ' + sha, Path(args[2]).read_text())
                submitted.append((sha, platform))
                output = json.dumps({'job_id': platform})
            elif args[0] == 'wait':
                output = 'darwin succeeded\nlinux succeeded\n'
            elif args[0] == 'result':
                path = Path(args[-1])
                path.mkdir()
                (path / 'gate-result.txt').write_text('gate full: exit=0\n')
            else:
                self.fail(args)
            return subprocess.CompletedProcess(args, 0, output, '')
        self.api.tip = self.second
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'command', command), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.second, self.scratch, '123:1'), 0)
        self.assertEqual(submitted, [(sha, platform) for sha in (self.first, self.second)
                                    for platform in ('darwin', 'linux')])
        self.api.statuses.clear()
        self.api.published.clear()
        def mutate(root, *args):
            (root / '.mycelium/adapter.yaml').write_text('different bytes after submission')
            return 0
        other = Path(self.temp.name) / 'mutation'
        other.mkdir()
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', mutate), \
                patch.dict(os.environ, self.env), redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.CalledProcessError):
                ci.run_pending(self.root, self.second, other, '124:1')
        self.assertEqual(self.api.published, [])

    def test_interrupted_backlog_cleans_active_commit_including_final_step(self):
        backlog = self.first
        def interrupt(root, sha, scratch, owner):
            ci.active_file(owner).write_text(json.dumps({'sha': backlog}))
            raise ci.SubmitterStopped()
        env = dict(self.env, GITHUB_SHA=self.third, GITHUB_RUN_ID='123')
        with patch.dict(os.environ, env), patch.object(ci.sys, 'argv', ['actions-full.py']), \
                patch.object(ci, 'run_pending', interrupt), patch.object(ci, 'cleanup', return_value=True) as cleanup, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.main(), 1)
            cleanup.assert_called_once_with(backlog, '123:1')
            cleanup.reset_mock()
            with patch.object(ci.sys, 'argv', ['actions-full.py', '--cleanup-only']):
                self.assertEqual(ci.main(), 0)
            cleanup.assert_called_once_with(backlog, '123:1')

    def test_unverified_failure_status_cannot_suppress_recovery(self):
        tree = self.git('rev-parse', self.first + '^{tree}')
        self.api.tip = self.third
        self.api.publish(self.first, tree, 'https://github.com/1kuna/mycelium/actions/runs/1', 'failure')
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', return_value=0) as run, \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.third, self.scratch, '124:1'), 0)
        self.assertEqual([call.args[1] for call in run.call_args_list], [self.first, self.second, self.third])

    def test_terminal_wrapper_failure_is_error_and_later_workflow_recovers_backlog(self):
        self.api.tip = self.first
        def failed_wrapper(*args, cwd=None):
            output = ''
            if args[0] == 'run':
                output = json.dumps({'job_id': args[-1].split(':')[2]})
            elif args[0] == 'wait':
                return subprocess.CompletedProcess(args, 1, 'darwin failed\nlinux failed\n', '')
            elif args[0] == 'status':
                output = json.dumps({'state': 'failed', 'attempt': 1})
            else:
                self.fail(args)
            return subprocess.CompletedProcess(args, 0, output, '')
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'command', failed_wrapper), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.first, self.scratch, '123:1'), 1)
        tree = self.git('rev-parse', self.first + '^{tree}')
        self.assertEqual(self.api.statuses[self.first, tree]['state'], 'error')
        self.api.tip = self.repeat
        later = Path(self.temp.name) / 'wrapper-recovery'
        later.mkdir()
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', return_value=0) as run, \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.repeat, later, '124:1'), 0)
        self.assertEqual([call.args[1] for call in run.call_args_list], [self.first, self.second, self.third])

    def test_incomplete_submissions_remain_recoverable_in_later_workflow(self):
        self.api.tip = self.first
        submitted = []
        def unavailable(*args, cwd=None):
            self.assertEqual(args[0], 'run')
            submitted.append(args[-1].split(':')[2])
            return subprocess.CompletedProcess(args, 69, '', 'PRIVATE infrastructure failure')
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'command', unavailable), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.first, self.scratch, '123:1'), 1)
        self.assertEqual(submitted, ['darwin', 'linux'])
        tree = self.git('rev-parse', self.first + '^{tree}')
        self.assertEqual(self.api.statuses[self.first, tree]['state'], 'error')
        self.api.tip = self.third
        later = Path(self.temp.name) / 'later'
        later.mkdir()
        recovered = []
        with patch.object(proof, 'GitHub', return_value=self.api), \
                patch.object(ci, 'run', side_effect=lambda root, sha, *args: recovered.append(sha) or 0), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.third, later, '124:1'), 0)
        self.assertEqual(recovered, [self.first, self.second, self.third])
        self.assertEqual(self.api.statuses[self.first, tree]['state'], 'success')

    def test_explicit_retry_runs_failed_identical_tree_once_per_batch(self):
        tree = self.git('rev-parse', self.third + '^{tree}')
        self.api.publish(self.third, tree, 'https://github.com/1kuna/mycelium/actions/runs/1', 'failure')
        retried = []
        def run(root, sha, *args):
            retried.append(sha)
            return int(sha in (self.third, self.repeat))
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', run), \
                patch.object(ci, 'cleanup', return_value=True), \
                patch.dict(os.environ, dict(self.env, GITHUB_RUN_ATTEMPT='2')), redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.repeat, self.scratch, '123:2'), 1)
        self.assertEqual(retried, [self.first, self.second, self.third])
        self.assertEqual(self.api.statuses[self.repeat, tree]['state'], 'error')

    def test_persistent_failures_get_only_one_automatic_recovery_across_workflows(self):
        submissions = []
        def unavailable(*args, cwd=None):
            self.assertEqual(args[0], 'run')
            submissions.append(args[-1].split(':')[1:3])
            return subprocess.CompletedProcess(args, 69, '', '')
        # Three distinct trees (plus an identical successor), initial attempt
        # followed by four independent workflows: only one extra attempt/tree.
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'command', unavailable), \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()) as output:
            for number in range(5):
                scratch = Path(self.temp.name) / f'batch-{number}'
                scratch.mkdir()
                self.assertEqual(ci.run_pending(self.root, self.repeat, scratch, f'{123 + number}:1'), 1)
        self.assertEqual(len(submissions), 12)  # 6 initial + 6 recovery platform submissions
        self.assertEqual(len(self.api.counters), 3)
        self.assertTrue(all(count == 2 for count, owner in self.api.counters.values()))
        self.assertTrue(all(row['state'] == 'error' for row in self.api.statuses.values()))
        self.assertIn('UNPROVEN:', output.getvalue())
        # Exhausted history must still fail while a newly landed tree gets proof.
        (self.root / 'source').write_text('D')
        new = self.commit('D new tree')
        self.git('push', '--quiet', 'origin', 'HEAD:refs/heads/main')
        self.api.tip = new
        later = Path(self.temp.name) / 'new-tree'
        later.mkdir()
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', return_value=0) as run, \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, self.env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, new, later, '128:1'), 1)
        self.assertEqual([call.args[1] for call in run.call_args_list], [new])
        self.assertEqual(self.api.statuses[new, self.git('rev-parse', new + '^{tree}')]['state'], 'success')

    def test_named_tree_dispatch_grants_one_attempt_without_resetting_other_budgets(self):
        trees = {sha: self.git('rev-parse', sha + '^{tree}')
                 for sha in (self.first, self.second, self.third)}
        for sha, tree in trees.items():
            self.api.counters[sha, tree] = 2, '122:1'
        env = dict(self.env, GITHUB_EVENT_NAME='workflow_dispatch',
                   MYCELIUM_RECOVERY_TREE=trees[self.third])
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', return_value=ci.INCOMPLETE) as run, \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.repeat, self.scratch, '123:1'), 1)
            # Re-entering the same dispatch cannot grant a second attempt.
            self.assertEqual(ci.run_pending(self.root, self.repeat, self.scratch, '123:1'), 1)
        self.assertEqual([call.args[1] for call in run.call_args_list], [self.third])
        self.assertEqual(self.api.counters[self.third, trees[self.third]], (3, '123:1'))
        self.assertEqual(self.api.counters[self.first, trees[self.first]], (2, '122:1'))
        later = Path(self.temp.name) / 'after-dispatch'
        later.mkdir()
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run') as run, \
                patch.dict(os.environ, self.env), redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.repeat, later, '124:1'), 1)
            run.assert_not_called()
        # A fresh explicit authorization may recover exactly the named tree.
        success = Path(self.temp.name) / 'authorized-success'
        success.mkdir()
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', return_value=0) as run, \
                patch.object(ci, 'cleanup', return_value=True), patch.dict(os.environ, env), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(ci.run_pending(self.root, self.repeat, success, '125:1'), 0)
        self.assertEqual([call.args[1] for call in run.call_args_list], [self.third])
        self.assertEqual(self.api.counters[self.third, trees[self.third]], (4, '125:1'))
        self.assertEqual(self.api.statuses[self.third, trees[self.third]]['state'], 'success')
        self.assertEqual(self.api.counter_states[self.third, trees[self.third]], 'success')
        self.assertEqual(self.api.statuses[self.repeat, trees[self.third]]['state'], 'success')
        self.assertEqual(self.api.statuses[self.first, trees[self.first]]['state'], 'error')

    def test_counter_is_reserved_before_interruption_or_uncertain_status_write(self):
        self.api.tip = self.first
        tree = self.git('rev-parse', self.first + '^{tree}')
        def interrupt(*args):
            self.assertEqual(self.api.counters[self.first, tree], (1, '123:1'))
            raise ci.SubmitterStopped()
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(ci, 'run', interrupt), \
                patch.dict(os.environ, self.env), redirect_stdout(io.StringIO()):
            with self.assertRaises(ci.SubmitterStopped):
                ci.run_pending(self.root, self.first, self.scratch, '123:1')
        # A reserve accepted upstream then reported as failed must not submit.
        other = Path(self.temp.name) / 'lost-status-reply'
        other.mkdir()
        reserve = self.api.record_attempt
        def lost_reply(*args):
            reserve(*args)
            raise OSError('synthetic lost status reply')
        with patch.object(proof, 'GitHub', return_value=self.api), patch.object(self.api, 'record_attempt', lost_reply), \
                patch.object(ci, 'run') as run, patch.dict(os.environ, self.env), redirect_stdout(io.StringIO()):
            with self.assertRaises(OSError):
                ci.run_pending(self.root, self.first, other, '124:1')
            run.assert_not_called()
        self.assertEqual(self.api.counters[self.first, tree], (2, '124:1'))

    def test_dispatch_requires_valid_landed_tree_and_fresh_authorization(self):
        cases = [
            dict(GITHUB_EVENT_NAME='push', MYCELIUM_RECOVERY_TREE='a' * 40),
            dict(GITHUB_EVENT_NAME='workflow_dispatch', MYCELIUM_RECOVERY_TREE=''),
            dict(GITHUB_EVENT_NAME='workflow_dispatch', MYCELIUM_RECOVERY_TREE='main'),
            dict(GITHUB_EVENT_NAME='workflow_dispatch', MYCELIUM_RECOVERY_TREE='a' * 40, GITHUB_RUN_ATTEMPT='2'),
            dict(GITHUB_EVENT_NAME='workflow_dispatch', MYCELIUM_RECOVERY_TREE='a' * 40),
        ]
        for settings in cases:
            with self.subTest(settings=settings), patch.object(proof, 'GitHub', return_value=self.api), \
                    patch.object(ci, 'run') as run, patch.dict(os.environ, dict(self.env, **settings)):
                with self.assertRaises(ValueError):
                    ci.run_pending(self.root, self.repeat, self.scratch, '123:1')
                run.assert_not_called()
        self.assertEqual(self.api.counters, {})

    def test_attempt_counter_uses_trusted_status_channel_and_rejects_malformed_records(self):
        api = object.__new__(proof.GitHub)
        sha, tree = 'a' * 40, 'b' * 40
        row = dict(context=proof.ATTEMPT_CONTEXT,
                   description=f'tree:{tree} attempts:2 owner:123:1', state='error')
        with patch.object(api, 'pages', return_value=[row]):
            self.assertEqual(api.attempts(sha, tree), (2, '123:1'))
        with patch.object(api, 'pages', return_value=[]):
            self.assertEqual(api.attempts(sha, tree), (0, None))
        for description in ('malformed', f'tree:{tree} attempts:0 owner:123:1',
                            f'tree:{sha} attempts:2 owner:123:1'):
            with patch.object(api, 'pages', return_value=[dict(row, description=description)]):
                with self.assertRaises(ValueError):
                    api.attempts(sha, tree)
        with patch.object(api, 'request') as request:
            url = 'https://github.com/1kuna/mycelium/actions/runs/123'
            api.record_attempt(sha, tree, 2, '123:1', url)
            self.assertEqual(request.call_args.args, (f'statuses/{sha}',
                dict(context=proof.ATTEMPT_CONTEXT, state='error',
                     description=row['description'], target_url=url)))
            api.record_attempt(sha, tree, 2, '123:1', url, state='success')
            self.assertEqual(request.call_args.args[1]['state'], 'success')
            self.assertEqual(request.call_args.args[1]['description'], row['description'])

    def test_old_local_context_is_ignored_and_latest_CI_status_is_authoritative(self):
        api = object.__new__(proof.GitHub)
        sha, tree = 'a' * 40, 'b' * 40
        old = {'context': 'mycelium/full-tree-v1', 'description': 'tree:' + tree, 'state': 'success'}
        ambiguous = dict(old, context='mycelium/main-full-tree-v2', state='failure')
        ambiguous_v3 = dict(ambiguous, context='mycelium/main-full-tree-v3')
        with patch.object(api, 'pages', return_value=[old, ambiguous, ambiguous_v3]):
            self.assertIsNone(api.status(sha, tree))
        with patch.object(api, 'pages', return_value=[old,
            {'context': proof.CONTEXT, 'description': 'tree:' + tree, 'state': 'failure'},
            {'context': proof.CONTEXT, 'description': 'tree:' + tree, 'state': 'success'},
        ]):
            self.assertEqual(api.status(sha, tree)['state'], 'failure')
        with patch.object(api, 'request') as request:
            with self.assertRaises(ValueError):
                api.publish(sha, tree, 'https://github.com/1kuna/mycelium/pull/1')
            with self.assertRaises(ValueError):
                api.publish(sha, tree, 'https://github.com/1kuna/mycelium/actions/runs/1', 'failure')
            request.assert_not_called()
        # No receipt verifier/publisher remains to accept a fabricated matching
        # instrumentation hash. PR-head reuse must be stacked on #1149 first.
        reply = subprocess.run([sys.executable, proof.__file__, 'synthetic-receipts',
                                '--evidence', 'https://github.com/1kuna/mycelium/pull/1'],
                               capture_output=True, text=True)
        self.assertNotEqual(reply.returncode, 0)
        self.assertIn('Local receipt publication is unavailable', reply.stderr)


if __name__ == '__main__':
    unittest.main()
