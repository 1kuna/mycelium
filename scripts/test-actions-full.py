#!/usr/bin/env python3
"""Offline contract tests: no fleet submissions or full suites."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('actions_full', Path(__file__).with_name('actions-full.py'))
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)
ROOT = Path(__file__).resolve().parents[1]
SHA = 'a' * 40


class ActionsFullTest(unittest.TestCase):
    def exercise(self, linux='succeeded', submission_failure=False, bad_marker=False):
        submitted, waited, results = [], [], []

        def command(*args):
            code, output = 0, ''
            if args[0] == 'run':
                profile = Path(args[2]).read_text()
                platform = args[-1].split(':')[-1]
                submitted.append(platform)
                self.assertIn(f'git: {SHA}', profile)
                self.assertNotIn('os: ' + ('linux' if platform == 'darwin' else 'darwin'), profile)
                self.assertNotIn('host:', profile)
                self.assertIn('check: [grep, -qx,', profile)
                if submission_failure and platform == 'darwin':
                    code, output = 69, 'PRIVATE ERROR'
                else:
                    output = json.dumps({'job_id': platform})
            elif args[0] == 'wait':
                waited.extend(args[1:])
                output = '\n'.join(f'{job} {linux if job == "linux" else "succeeded"} PRIVATE REASON'
                                   for job in args[1:])
                code = int(linux != 'succeeded')
            elif args[0] == 'result':
                results.append(args[1])
                path = Path(args[-1])
                path.mkdir()
                (path / 'gate-result.txt').write_text('wrong\n' if bad_marker else 'gate full: exit=0\n')
            else:
                self.fail(args)
            return subprocess.CompletedProcess(args, code, output, 'PRIVATE ERROR')

        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            with patch.object(ci, 'command', command), patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(scratch / 'summary')}):
                status = ci.run(ROOT, SHA, scratch)
            summary = (scratch / 'summary').read_text()
            self.assertNotIn('PRIVATE', summary)
            self.assertIn('| darwin |', summary)
            self.assertIn('| linux |', summary)
        self.assertEqual(submitted, ['darwin', 'linux'])
        return status, waited, results

    def test_both_platforms_wait_and_verify_accepted_results(self):
        self.assertEqual(self.exercise(), (0, ['darwin', 'linux'], ['darwin', 'linux']))
        self.assertEqual(self.exercise(bad_marker=True)[0], 1)
        with self.assertRaises(ValueError):
            ci.platform_profile('source: {git: main}\n', SHA, 'linux')

    def test_failure_does_not_skip_other_platform(self):
        self.assertEqual(self.exercise(linux='failed'), (1, ['darwin', 'linux'], ['darwin']))
        self.assertEqual(self.exercise(submission_failure=True), (1, ['linux'], ['linux']))

    def test_no_credentials_or_notifier_in_cli_environment(self):
        with patch.dict(os.environ, {'MYCE_TOKEN': 'synthetic-fleet', 'MYCE_NOTIFY': 'unused',
                                    'GITHUB_TOKEN': 'synthetic-github', 'GH_TOKEN': 'synthetic-github'}):
            with patch.object(ci.subprocess, 'run') as run:
                ci.command('wait', 'fixture')
                env = run.call_args.kwargs['env']
                self.assertIn('HOME', env)
                self.assertIn('PATH', env)
                for key in ('MYCE_TOKEN', 'MYCE_NOTIFY', 'GITHUB_TOKEN', 'GH_TOKEN'):
                    self.assertNotIn(key, env)


if __name__ == '__main__':
    unittest.main()
