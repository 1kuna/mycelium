#!/usr/bin/env python3
"""Lightweight Actions submitter; full suites run through Mycelium only."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

PLATFORMS = ('darwin', 'linux')
TERMINAL = {'succeeded', 'failed', 'cancelled'}


def platform_profile(text, sha, platform):
    """Narrow the existing profile without changing its resources or acceptance."""
    text, sources = re.subn(r'(?m)^  git: HEAD$', f'  git: {sha}', text)
    targets = ('  - {os: darwin, arch: arm64, platform: cpu}\n' if platform == 'darwin'
               else '  - {os: linux, arch: amd64, platform: cpu}\n'
                    '  - {os: linux, arch: arm64, platform: cpu}\n')
    text, matrices = re.subn(r'(?m)^targets:\n(?:  - .*\n)+', 'targets:\n' + targets, text)
    if sources != 1 or matrices != 1:
        raise ValueError('gate-full profile format changed')
    return text


def command(*args):
    # Authority is reopened from the host's private installed configuration.
    # No GitHub credentials, MYCE_TOKEN, notifier or other caller settings cross
    # into myce or its observers. Never relay raw receipts/errors to public logs.
    env = {key: value for key, value in os.environ.items()
           if key in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL', 'RUNNER_TRACKING_ID')}
    return subprocess.run(['myce', *args], env=env, capture_output=True, text=True)


def submission_failure(reply):
    """Publish fixed classifications and numeric capacity only, never raw reasons."""
    if reply.returncode == 78:
        try:
            receipt = json.loads(reply.stdout)
            if receipt.get('refusal_code') == 'no_qualified_target':
                reason = receipt.get('refusal_reason', '')
                match = re.fullmatch(
                    r'host memory need ([0-9]+(?:\.[0-9]+)?) GiB '
                    r'\([0-9.]+ \+ [0-9.]+ slack\) exceeds allowance '
                    r'([0-9]+(?:\.[0-9]+)?) GiB on .+', reason)
                if match:
                    need, allowance = map(float, match.groups())
                    return (f'no qualified target: host memory need {need:.3f} GiB '
                            f'exceeds allowance {allowance:.3f} GiB')
                return 'no qualified target; inspect private receipt for eligibility requirements'
        except (ValueError, TypeError, AttributeError):
            pass
    return f'submission unavailable (exit {reply.returncode})'


def run(root, sha, scratch):
    profile = (root / '.mycelium/profiles/gate-full.yaml').read_text()
    jobs, states, refusals = {}, {}, {}
    failed = False
    # Submit both before waiting. An error on one platform must not skip the other.
    for platform in PLATFORMS:
        path = scratch / f'{platform}.yaml'
        path.write_text(platform_profile(profile, sha, platform))
        reply = command('run', '--profile', str(path), '--key', f'main-ci:{sha}:{platform}')
        try:
            receipt = json.loads(reply.stdout)
            job = receipt['job_id']
            if not isinstance(job, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', job):
                raise ValueError('invalid job ID')
            jobs[platform] = job
            print(f'{platform}: submitted job {job}', flush=True)
        except (ValueError, KeyError, TypeError):
            refusals[platform] = submission_failure(reply)
            print(f'{platform}: {refusals[platform]}', flush=True)
            failed = True
        if reply.returncode:
            failed = True

    if jobs:
        # The standard CLI waits for every job, including after one fails, and
        # handles reconnects. No model callback, custom polling or wall deadline.
        reply = command('wait', *jobs.values())
        failed = failed or reply.returncode != 0
        for line in reply.stdout.splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0] in jobs.values() and fields[1] in TERMINAL:
                states[fields[0]] = fields[1]

    rows = []
    for platform in PLATFORMS:
        job = jobs.get(platform)
        state = states.get(job, refusals.get(platform, 'unavailable'))
        if state == 'succeeded':
            destination = scratch / f'{platform}-result'
            reply = command('result', job, '--out', str(destination))
            marker = destination / 'gate-result.txt'
            if reply.returncode or not marker.is_file() or marker.read_text() != 'gate full: exit=0\n':
                state = 'result verification failed'
        failed = failed or state != 'succeeded'
        print(f'{platform}: {state}; job {job or "none"}', flush=True)
        rows.append(f'| {platform} | {state} | {job or "none"} |')
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as stream:
            stream.write('| Platform | Result | Mycelium job |\n| --- | --- | --- |\n')
            stream.write('\n'.join(rows) + '\n')
    return int(failed)


def main():
    sha = os.environ['GITHUB_SHA']
    if not re.fullmatch(r'[0-9a-f]{40}', sha):
        raise ValueError('invalid main SHA')
    root = Path.cwd()
    # Only generated profiles and verified small result markers live here.
    with tempfile.TemporaryDirectory(prefix='mycelium-ci-', dir=os.environ['RUNNER_TEMP']) as scratch:
        return run(root, sha, Path(scratch))


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError):
        print('CI infrastructure unavailable; inspect private runner/queue state', flush=True)
        raise SystemExit(1)
