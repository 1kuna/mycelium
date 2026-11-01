#!/usr/bin/env python3
"""Offline contract tests: no fleet submissions or full suites."""
from contextlib import redirect_stdout
import io
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
    def exercise(self, linux='succeeded', submission_failure=False, bad_marker=False, refusal=None, status_record=None):
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
                self.assertNotIn('allowed_machines:', profile)
                self.assertNotIn('arch: amd64', profile)
                self.assertIn(f'  - {{os: {platform}, arch: arm64, platform: cpu}}', profile)
                self.assertIn(f'  memory_gb: {4 if platform == "darwin" else 7}\n', profile)
                self.assertNotIn('memory: auto', profile)
                self.assertIn('check: [grep, -qx,', profile)
                if submission_failure and platform == 'darwin':
                    code, output = ((78, json.dumps(refusal)) if refusal is not None
                                    else (69, 'PRIVATE ERROR'))
                else:
                    # A busy qualified host returns a queued receipt; wait owns the delay.
                    output = json.dumps({'job_id': platform, 'status': 'queued'})
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
            elif args[0] == 'status':
                output = json.dumps(status_record or {})
            else:
                self.fail(args)
            return subprocess.CompletedProcess(args, code, output, 'PRIVATE ERROR')

        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory)
            with redirect_stdout(io.StringIO()) as public, patch.object(ci, 'command', command), patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': str(scratch / 'summary')}):
                status = ci.run(ROOT, SHA, scratch)
            summary = (scratch / 'summary').read_text()
            self.assertIn('Linux amd64 native full-suite coverage is deferred', public.getvalue())
            self.assertIn('profiles lack a negative host selector', public.getvalue())
            self.assertIn('https://github.com/1kuna/mycelium/issues/527', public.getvalue())
            self.assertNotIn('PRIVATE', summary + public.getvalue())
            if status_record and status_record.get('attempt_history', [{}])[-1].get('out_of_memory'):
                self.assertIn('failed (out of memory)', summary + public.getvalue())
            if refusal is not None:
                self.assertIn('no qualified target', summary)
                self.assertIn('no qualified target', public.getvalue())
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
        self.assertEqual(self.exercise(linux='failed', status_record={
            'state': 'failed', 'attempt': 2, 'attempt_history': [
                {'attempt': 1, 'out_of_memory': False},
                {'attempt': 2, 'out_of_memory': True, 'reason': 'PRIVATE'},
            ]}), (1, ['darwin', 'linux'], ['darwin']))
        self.assertEqual(self.exercise(submission_failure=True), (1, ['linux'], ['linux']))

    def test_oom_requires_current_attempt_and_readable_status(self):
        for output, code in [
            (json.dumps({'state': 'failed', 'attempt': 2, 'attempt_history': [
                {'attempt': 1, 'out_of_memory': True}]}), 0),
            ('PRIVATE malformed', 0),
            (json.dumps({'state': 'failed', 'attempt': 1, 'attempt_history': [
                {'attempt': 1, 'out_of_memory': True}]}), 1),
        ]:
            with patch.object(ci, 'command', return_value=subprocess.CompletedProcess([], code, output)):
                self.assertEqual(ci.failure_state('linux'), 'failed')

    def test_permanent_memory_refusal_is_clear_and_private(self):
        refusal = {
            'refusal_code': 'no_qualified_target',
            'refusal_reason': ('host memory need 9.210817337036133 GiB '
                               '(9.085817337036133 + 0.125 slack) exceeds allowance '
                               '8 GiB on PRIVATE-HOST; intent retained locally'),
        }
        reply = subprocess.CompletedProcess([], 78, json.dumps(refusal), 'PRIVATE ERROR')
        self.assertEqual(ci.submission_failure(reply),
                         'no qualified target; inspect private receipt for eligibility requirements')
        self.assertEqual(self.exercise(submission_failure=True, refusal=refusal),
                         (1, ['linux'], ['linux']))
        refusal['refusal_reason'] = 'PRIVATE unsupported environment or host path'
        self.assertEqual(ci.submission_failure(subprocess.CompletedProcess([], 78, json.dumps(refusal))),
                         'no qualified target; inspect private receipt for eligibility requirements')
        for output in ('PRIVATE malformed', '[]', '{"refusal_reason": "PRIVATE"}'):
            self.assertEqual(ci.submission_failure(subprocess.CompletedProcess([], 78, output)),
                             'submission unavailable (exit 78)')

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
