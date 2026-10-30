#!/usr/bin/env python3
"""No fleet, credentials, GitHub writes or full suites: synthetic CI lifecycle."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('main_ci', Path(__file__).with_name('main-ci.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
PROFILE = (Path(__file__).resolve().parents[1] / '.mycelium/profiles/gate-full.yaml').read_text()


class WatcherTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.remote = self.root / 'remote'
        self.remote.mkdir()
        self.git(self.remote, 'init', '-q', '-b', 'main')
        profile = self.remote / '.mycelium/profiles/gate-full.yaml'
        profile.parent.mkdir(parents=True)
        profile.write_text(PROFILE)
        self.first = self.commit('initial')
        self.repo = self.root / 'clone'
        self.git(self.root, 'clone', '-q', str(self.remote), str(self.repo))
        # Exercise real Git fetch against a local stand-in for the canonical URL.
        self.git(self.repo, 'config', f'url.{self.remote}.insteadOf',
                 'https://github.com/fixture/repo.git')
        self.state = self.root / 'state'
        self.state.mkdir()
        self.jobs = {}
        self.submissions = []
        self.statuses = []
        self.descriptions = {}
        self.fail_status = None
        self.real_command = ci.command
        self.addCleanup(patch.stopall)
        patch.object(ci, 'command', self.command).start()
        self.watcher = self.restart()

    def git(self, cwd, *args):
        return subprocess.run(['git', *args], cwd=cwd, check=True,
                              capture_output=True, text=True).stdout.strip()

    def commit(self, name):
        (self.remote / 'change').write_text(name)
        self.git(self.remote, 'add', '.')
        self.git(self.remote, '-c', 'user.name=Fixture', '-c',
                 'user.email=fixture@example.invalid', 'commit', '-qm', name)
        return self.git(self.remote, 'rev-parse', 'HEAD')

    def restart(self):
        return ci.Watcher(self.repo, self.state, 'fixture/repo', 'fake-myce')

    def command(self, argv, cwd, *, myce=False):
        if argv[0] == 'git':
            return self.real_command(argv, cwd, myce=myce)
        if argv[0] == 'gh':
            if '--method' not in argv:
                latest = {}
                sha = argv[-1].split('/')[-2]
                for commit, context, state in self.statuses:
                    if commit == sha:
                        latest[context] = {'context': context, 'state': state,
                                           'description': self.descriptions[(sha, context)]}
                return json.dumps({'statuses': list(latest.values())})
            fields = dict(arg.split('=', 1) for arg in argv if '=' in arg)
            sha = next(arg.rsplit('/', 1)[1] for arg in argv if '/statuses/' in arg)
            if fields['state'] == self.fail_status:
                raise RuntimeError('synthetic publication outage')
            self.statuses.append((sha, fields['context'], fields['state']))
            self.descriptions[(sha, fields['context'])] = fields['description']
            return '{}'
        self.assertTrue(myce)
        self.assertEqual(argv[:3], ['fake-myce', 'run', '--profile'])
        profile = Path(argv[3]).read_text()
        key = argv[-1]
        sha, platform = key.split(':')[1:]
        self.assertIn(f'git: {sha}', profile)
        self.assertNotIn('os: ' + ('linux' if platform == 'darwin' else 'darwin'), profile)
        self.assertIn('command: [bash, scripts/gate-profile.sh]', profile)
        self.assertNotIn('host:', profile)
        self.submissions.append(key)
        self.jobs.setdefault(key, {'status': 'running', 'job_id': str(len(self.jobs))})
        return json.dumps(self.jobs[key])

    def finish(self, sha, linux='succeeded'):
        for platform in ci.PLATFORMS:
            self.jobs[f'main-ci:{sha}:{platform}']['status'] = (
                linux if platform == 'linux' else 'succeeded')

    def test_catchup_both_platforms_failure_and_restart(self):
        self.watcher.tick()
        self.assertEqual(len(self.jobs), 2)
        second = self.commit('second')
        third = self.commit('third')
        # Both new main SHAs discovered, never collapsed by concurrency cancellation.
        self.watcher = self.restart()
        self.watcher.tick()
        self.assertEqual([x['sha'] for x in self.watcher.data['pending']],
                         [self.first, second, third])
        self.assertEqual(len(self.jobs), 2)
        for sha in (second, third):
            self.assertIn((sha, 'mycelium/full-linux', 'pending'), self.statuses)
        self.finish(self.first, linux='failed')
        self.watcher.tick()
        self.assertIn((self.first, 'mycelium/full-linux', 'failure'), self.statuses)
        self.assertIn((self.first, 'mycelium/full-darwin', 'success'), self.statuses)
        self.watcher.tick()
        self.finish(second)
        self.watcher.tick()
        self.watcher.tick()
        self.finish(third)
        self.watcher.tick()
        self.watcher = self.restart()
        self.watcher.tick()
        self.assertEqual(len(self.jobs), 6)
        self.assertEqual(self.watcher.data['pending'], [])
        self.assertEqual(len((self.state / 'results.jsonl').read_text().splitlines()), 3)
        self.assertEqual(list(self.state.glob('*.yaml')), [])

    def test_publication_failure_replays_same_jobs(self):
        self.watcher.tick()
        self.finish(self.first)
        self.fail_status = 'success'
        # Terminal status publication fails; neither job is lost or resubmitted.
        self.watcher.tick()
        self.assertEqual(len(self.watcher.data['pending']), 1)
        self.fail_status = None
        self.watcher = self.restart()
        self.watcher.tick()
        self.assertEqual(len(self.jobs), 2)
        self.assertEqual(self.watcher.data['pending'], [])

    def test_pending_publication_failure_survives_restart(self):
        self.fail_status = 'pending'
        with self.assertRaises(RuntimeError):
            self.watcher.tick()
        self.assertEqual(len(self.jobs), 0)
        self.fail_status = None
        self.watcher = self.restart()
        self.watcher.tick()
        self.assertEqual(len(self.jobs), 2)

    def test_merge_tests_only_first_parent_sha(self):
        self.watcher.tick()
        self.git(self.remote, 'checkout', '-qb', 'feature')
        feature = self.commit('feature')
        self.git(self.remote, 'checkout', '-q', 'main')
        self.git(self.remote, '-c', 'user.name=Fixture', '-c',
                 'user.email=fixture@example.invalid', 'merge', '--no-ff', '-qm', 'merge', 'feature')
        merge = self.git(self.remote, 'rev-parse', 'HEAD')
        self.watcher.tick()
        self.assertEqual([x['sha'] for x in self.watcher.data['pending']], [self.first, merge])
        self.assertFalse(any(x[0] == feature for x in self.statuses))

    def test_one_platform_submission_error_does_not_skip_other(self):
        original = self.command

        def fail_darwin(argv, cwd, *, myce=False):
            if argv[0] == 'fake-myce' and argv[-1].endswith(':darwin'):
                raise RuntimeError('synthetic submit outage')
            return original(argv, cwd, myce=myce)

        with patch.object(ci, 'command', fail_darwin):
            self.watcher.tick()
        self.assertIn((self.first, 'mycelium/full-darwin', 'error'), self.statuses)
        self.assertEqual(list(self.jobs), [f'main-ci:{self.first}:linux'])
        self.watcher = self.restart()
        self.watcher.tick()
        self.assertEqual(len(self.jobs), 2)

    def test_profile_drift_fails_closed(self):
        with self.assertRaises(RuntimeError):
            ci.platform_profile(PROFILE.replace('git: HEAD', 'git: main'), self.first, 'linux')

    def test_origin_cannot_supply_fork_or_pr_code(self):
        # A different local origin contains a commit the canonical main lacks.
        fork = self.root / 'fork'
        self.git(self.root, 'clone', '-q', str(self.remote), str(fork))
        (fork / 'change').write_text('untrusted fork')
        self.git(fork, 'add', '.')
        self.git(fork, '-c', 'user.name=Fixture', '-c',
                 'user.email=fixture@example.invalid', 'commit', '-qm', 'fork')
        fork_sha = self.git(fork, 'rev-parse', 'HEAD')
        self.git(self.repo, 'remote', 'set-url', 'origin', str(fork))
        self.watcher.tick()
        self.assertEqual({key.split(':')[1] for key in self.jobs}, {self.first})
        self.assertFalse(any(s[0] == fork_sha for s in self.statuses))

    def test_downtime_publication_is_bounded_and_cursor_keeps_remaining_merges(self):
        self.watcher.tick()
        commits = [self.commit(f'backlog {n}') for n in range(ci.MAX_PENDING + 2)]
        before = len(self.statuses)
        self.watcher.tick()
        self.assertEqual(len(self.watcher.data['pending']), ci.MAX_PENDING)
        self.assertEqual(len(self.statuses) - before, 2 * (ci.MAX_PENDING - 1))
        self.assertEqual(self.watcher.data['cursor'], commits[ci.MAX_PENDING - 2])
        self.finish(self.first)
        self.watcher.tick()
        self.watcher = self.restart()
        self.watcher.tick()
        self.assertEqual(self.watcher.data['cursor'], commits[ci.MAX_PENDING - 1])
        self.assertEqual(len(self.jobs), 4)

    def test_status_retry_deduplicates_ambiguous_publication(self):
        self.watcher.tick()
        self.finish(self.first)
        original = self.command
        interrupted = False

        def lose_reply(argv, cwd, *, myce=False):
            nonlocal interrupted
            result = original(argv, cwd, myce=myce)
            if argv[0] == 'gh' and 'state=success' in argv and not interrupted:
                interrupted = True
                raise RuntimeError('reply lost after accepted status')
            return result

        # Simulate a crash immediately after posting, before local state is saved.
        with patch.object(ci, 'command', lose_reply):
            with self.assertRaises(RuntimeError):
                self.watcher.status(self.first, 'darwin', 'success', 'accepted')
        self.watcher = self.restart()
        before = len(self.statuses)
        self.watcher.status(self.first, 'darwin', 'success', 'accepted')
        self.assertEqual(len(self.statuses), before)
        for _ in range(3):
            self.watcher.status(self.first, 'linux', 'error', 'retrying')
        self.assertEqual(len(self.statuses), before + 1)

    def test_credentials_do_not_cross_command_boundaries(self):
        patch.stopall()
        with patch.dict(os.environ, {'GH_TOKEN': 'synthetic-github',
                                    'GITHUB_TOKEN': 'synthetic-github',
                                    'GH_ENTERPRISE_TOKEN': 'synthetic-github',
                                    'GITHUB_ENTERPRISE_TOKEN': 'synthetic-github',
                                    'MYCE_TOKEN': 'synthetic-fleet', 'MYCE_NOTIFY': 'unused'}):
            with patch.object(ci.subprocess, 'run') as run:
                run.return_value = subprocess.CompletedProcess([], 0, '{}', '')
                ci.command(['myce', 'run'], self.repo, myce=True)
                env = run.call_args.kwargs['env']
                for name in ('GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN',
                             'GITHUB_ENTERPRISE_TOKEN'):
                    self.assertNotIn(name, env)
                self.assertNotIn('MYCE_NOTIFY', env)
                self.assertEqual(env['MYCE_TOKEN'], 'synthetic-fleet')
                ci.command(['gh', 'api'], self.repo)
                env = run.call_args.kwargs['env']
                self.assertNotIn('MYCE_TOKEN', env)
                self.assertEqual(env['GH_TOKEN'], 'synthetic-github')


if __name__ == '__main__':
    unittest.main()
