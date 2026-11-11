#!/usr/bin/env python3
"""Lightweight Actions submitter; full suites run through Mycelium only."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

PLATFORMS = ('darwin', 'linux')
TERMINAL = {'succeeded', 'failed', 'cancelled'}
# GitHub's six-hour job limit is external, not a gate performance deadline.
# Reserve five minutes for cleanup and five for Actions/checkout overhead.
WORK_SECONDS = 350 * 60
CANCEL_SECONDS = 150  # Two keys share the five-minute cleanup reserve.


class SubmitterStopped(Exception):
    pass


def stop_submitter(signum, frame):
    raise SubmitterStopped()


def submission_key(sha, platform, owner):
    return f'main-ci:{sha}:{platform}:{owner}'


def frozen_profile(text, sha):
    """Freeze the selected platform profile, preserving its placement and resources."""
    text, sources = re.subn(r'(?m)^  git: HEAD$', f'  git: {sha}', text)
    if sources != 1:
        raise ValueError('gate-full profile format changed')
    return text


def command(*args, timeout=None, cwd=None):
    # Authority is reopened from the host's private installed configuration.
    # No GitHub credentials, MYCE_TOKEN, notifier or other caller settings cross
    # into myce or its observers. Never relay raw receipts/errors to public logs.
    env = {key: value for key, value in os.environ.items()
           if key in ('HOME', 'PATH', 'TMPDIR', 'LANG', 'LC_ALL', 'RUNNER_TRACKING_ID')}
    return subprocess.run(['myce', *args], env=env, capture_output=True, text=True, timeout=timeout, cwd=cwd)


def cleanup(sha, owner):
    """Fence both intended submissions, even if no receipt reached the runner."""
    confirmed = True
    for platform in PLATFORMS:
        try:
            reply = command('cancel', '--project', 'mycelium', '--key',
                            submission_key(sha, platform, owner), timeout=CANCEL_SECONDS)
            ack = json.loads(reply.stdout)
            # Cleanup needs the fence only; 75 means release is still pending.
            ok = (reply.returncode in (0, 75) and ack.get('will_not_run') is True
                  and ack.get('state') != 'unknown')
        except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired):
            ok = False
        confirmed = confirmed and ok
        print(f'{platform}: cleanup {"confirmed" if ok else "unconfirmed"}', flush=True)
    if not confirmed:
        print('::error title=CI cleanup unconfirmed::Inspect private runner/queue state; '
              'repeat this attempt\'s cleanup before releasing ownership.', flush=True)
    return confirmed


def submission_failure(reply):
    """Publish fixed classifications only, never raw reasons or host capacity."""
    if reply.returncode == 78:
        try:
            receipt = json.loads(reply.stdout)
            if receipt.get('refusal_code') == 'no_qualified_target':
                return 'infrastructure failure: no qualified target; inspect private receipt for eligibility requirements'
        except (ValueError, TypeError, AttributeError):
            pass
    return f'infrastructure failure: submission unavailable (exit {reply.returncode})'


def failure_state(job):
    """Expose only a confirmed OOM on the terminal attempt, never job details."""
    reply = command('status', '--json', job)
    try:
        record = json.loads(reply.stdout)
        if reply.returncode == 0 and record.get('state') == 'failed':
            for attempt in record.get('attempt_history', []):
                if (attempt.get('attempt') == record.get('attempt') and
                        attempt.get('out_of_memory') is True):
                    return 'failed (out of memory)'
    except (ValueError, TypeError, AttributeError):
        pass
    return 'failed'


def run(root, sha, scratch, owner):
    line = f'Main full source: {sha} (github.sha)'
    print(line, flush=True)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
            stream.write(line + '\n\n')
    jobs, states, refusals = {}, {}, {}
    failed, wait_error = False, False
    # Submit both before waiting. An error on one platform must not skip the other.
    for platform in PLATFORMS:
        name = 'gate-full-darwin' if platform == 'darwin' else 'gate-full'
        profile = (root / f'.mycelium/profiles/{name}.yaml').read_text()
        path = scratch / f'{platform}.yaml'
        path.write_text(frozen_profile(profile, sha))
        reply = command('run', '--profile', str(path), '--key', submission_key(sha, platform, owner), cwd=root)
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
        wait_error = reply.returncode != 0
        for line in reply.stdout.splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[0] in jobs.values() and fields[1] in TERMINAL:
                states[fields[0]] = fields[1]

    rows = []
    for platform in PLATFORMS:
        job = jobs.get(platform)
        state = states.get(job, refusals.get(platform, 'unavailable'))
        if state == 'failed':
            state = failure_state(job)
        if state == 'succeeded':
            destination = scratch / f'{platform}-result'
            # myce result owns transient recovery through Client.RetryTransient:
            # shared adaptive pacing, Retry-After and permanent-error handling.
            # Retain one accepted-result/marker check here, not a second backoff.
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
    if refusals:
        print('::error title=CI infrastructure failure::At least one platform submission is unconfirmed; '
              'no suite result was verified for that platform.', flush=True)
    return int(failed or wait_error)


def main(work_seconds=WORK_SECONDS):
    sha = os.environ['GITHUB_SHA']
    if not re.fullmatch(r'[0-9a-f]{40}', sha):
        raise ValueError('invalid main SHA')
    run_id, attempt = os.environ['GITHUB_RUN_ID'], os.environ['GITHUB_RUN_ATTEMPT']
    if not all(re.fullmatch(r'[1-9][0-9]*', value) for value in (run_id, attempt)):
        raise ValueError('invalid workflow attempt')
    owner = f'{run_id}:{attempt}'
    if sys.argv[1:] == ['--cleanup-only']:
        return int(not cleanup(sha, owner))
    if sys.argv[1:]:
        raise ValueError('unexpected arguments')
    root = Path.cwd()
    watched = (signal.SIGINT, signal.SIGTERM, signal.SIGALRM)
    previous = {sig: signal.signal(sig, stop_submitter) for sig in watched}
    status = 1
    try:
        # Actions starts this budget before checkout, so slow checkout cannot
        # consume the cleanup reserve. Never arm zero (which disables the alarm).
        deadline = float(os.environ.get('MYCELIUM_CI_DEADLINE', time.monotonic() + work_seconds))
        remaining = min(work_seconds, deadline - time.monotonic())
        signal.setitimer(signal.ITIMER_REAL, max(0.001, remaining))
        # Generated profiles and accepted markers live here.
        with tempfile.TemporaryDirectory(prefix='mycelium-ci-', dir=os.environ['RUNNER_TEMP']) as scratch:
            status = run(root, sha, Path(scratch), owner)
    except SubmitterStopped:
        print('CI submitter interrupted or timed out; cancelling owned gates', flush=True)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig in watched:
            signal.signal(sig, signal.SIG_IGN)
        try:
            cleaned = cleanup(sha, owner)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    return int(bool(status) or not cleaned)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        print('CI infrastructure unavailable; inspect private runner/queue state', flush=True)
        raise SystemExit(1)
