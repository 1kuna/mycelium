#!/usr/bin/env python3
"""Lightweight Actions submitter; full suites run through Mycelium only."""
import json
import importlib.util
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

_spec = importlib.util.spec_from_file_location('actions_proof', Path(__file__).with_name('actions-proof.py'))
proof = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(proof)

PLATFORMS = ('darwin', 'linux')
TERMINAL = {'succeeded', 'failed', 'cancelled'}
# No job state or wrapper exit is a completed suite verdict.
INCOMPLETE = 2
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
            ok = (reply.returncode == 0 and ack.get('will_not_run') is True
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


def verified_outcome(state, result_code=None, marker=None):
    """Default to incomplete; only an accepted full-gate marker proves success.

    The current gate emits no negative receipt proving source identity, a
    completed suite and cleanup. Even a real test failure therefore remains
    recoverable. Do not infer a suite verdict from terminal job/wrapper states.
    """
    if state == 'succeeded' and result_code == 0 and marker == 'gate full: exit=0\n':
        return 0
    return INCOMPLETE


def run(root, sha, scratch, owner):
    jobs, states, refusals = {}, {}, {}
    incomplete, wait_error = False, False
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
            incomplete = True
        if reply.returncode:
            incomplete = True

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
        outcome = INCOMPLETE
        if state == 'succeeded':
            destination = scratch / f'{platform}-result'
            # myce result owns transient recovery through Client.RetryTransient:
            # shared adaptive pacing, Retry-After and permanent-error handling.
            # Retain one accepted-result/marker check here, not a second backoff.
            reply = command('result', job, '--out', str(destination))
            marker = destination / 'gate-result.txt'
            outcome = verified_outcome(state, reply.returncode,
                                       marker.read_text() if marker.is_file() else None)
            if outcome == INCOMPLETE:
                state = 'result verification failed'
        incomplete = incomplete or outcome == INCOMPLETE
        print(f'{platform}: {state}; job {job or "none"}', flush=True)
        rows.append(f'| {platform} | {state} | {job or "none"} |')
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as stream:
            stream.write('| Platform | Result | Mycelium job |\n| --- | --- | --- |\n')
            stream.write('\n'.join(rows) + '\n')
    if refusals:
        print('::error title=CI infrastructure failure::At least one platform submission is unconfirmed; '
              'no suite result was verified for that platform. Rerun this workflow to recover '
              'the merge with fresh attempt keys.', flush=True)
    # Observation errors cannot establish coverage, even with a partial receipt.
    return INCOMPLETE if incomplete or wait_error else 0


def active_file(owner):
    return Path(os.environ['RUNNER_TEMP']) / ('mycelium-ci-active-' + owner.replace(':', '-') + '.json')


def active_sha(owner, fallback):
    path = active_file(owner)
    if not path.exists():
        return fallback
    sha = json.loads(path.read_text())['sha']
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('invalid active source')
    return sha


def verify_checkout(root, sha, tree):
    """Bind profile, relative inputs and suite helpers to the historical tree."""
    identity = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD', 'HEAD^{tree}'],
                                       text=True).splitlines()
    if identity != [sha, tree]:
        raise ValueError('historical checkout identity changed')
    subprocess.run(['git', '-C', str(root), '-c', 'core.fileMode=true', 'diff', '--exit-code', '--quiet', 'HEAD'], check=True)


def run_pending(root, sha, scratch, owner):
    api = proof.GitHub()
    known, processed, outcomes = {}, set(), {}
    failed = False
    evidence = f'https://github.com/{proof.REPOSITORY}/actions/runs/{owner.split(":")[0]}'
    # GITHUB_SHA can be a delayed B event after C replaced it in the pending
    # slot. Reconcile main itself, never the event's obsolete upper bound.
    tip = proof.fetch_main(api)
    while True:
        for candidate in proof.main_history(tip):
            if candidate in processed:
                continue
            processed.add(candidate)
            tree = proof.git('rev-parse', candidate + '^{tree}')
            # Current-batch outcomes take precedence over durable history. An
            # explicit retry is once per distinct tree, even when it fails.
            if tree in outcomes:
                recorded = outcomes[tree]
                proof.report(candidate, tree, 'REUSE: this batch already attempted tree; outcome ' + recorded['state'],
                             recorded['target_url'])
                api.publish(candidate, tree, recorded['target_url'], recorded['state'])
                failed = failed or recorded['state'] != 'success'
                continue
            recorded = api.status(candidate, tree)
            if recorded is None:
                recorded = known.get(tree)
            if recorded and recorded['state'] == 'success':
                proof.report(candidate, tree, 'SKIP: identical tree already passed Darwin and Linux full',
                             recorded['target_url'])
                # Record identical-tree coverage on each landed main commit.
                if recorded.get('source_sha') != candidate:
                    api.publish(candidate, tree, recorded['target_url'])
                known[tree] = recorded
                continue
            proof.report(candidate, tree, 'RUN: no matching successful full-tree proof')
            # Persist before submission: final Actions cleanup must fence this exact
            # earlier commit too if the workflow was coalesced across several merges.
            path = active_file(owner)
            pending = path.with_suffix('.tmp')
            pending.write_text(json.dumps({'sha': candidate}))
            pending.replace(path)
            destination = scratch / candidate
            destination.mkdir()
            checkout = destination / 'source'
            subprocess.run(['git', 'clone', '--shared', '--no-checkout', '--quiet',
                            str(root), str(checkout)], check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.run(['git', '-C', str(checkout), 'checkout', '--quiet', '--detach', candidate],
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            verify_checkout(checkout, candidate, tree)
            result = run(checkout, candidate, destination, owner)
            verify_checkout(checkout, candidate, tree)
            cleaned = cleanup(candidate, owner)
            if not cleaned:
                return 1
            # No verified negative receipt exists yet: every non-success is
            # recoverable, including terminal command/test failures.
            state = 'success' if result == 0 else 'error'
            api.publish(candidate, tree, evidence, state)
            recorded = {'state': state, 'target_url': evidence, 'source_sha': candidate}
            outcomes[tree] = recorded
            known[tree] = recorded
            failed = failed or bool(result)
            label = 'INCOMPLETE: no two-platform verdict; later runs must recover' if state == 'error' else 'full ' + state
            proof.report(candidate, tree, label, evidence)

        current = proof.fetch_main(api)
        if current == tip:
            return int(failed)
        tip = current


def main(work_seconds=WORK_SECONDS):
    sha = os.environ['GITHUB_SHA']
    if not re.fullmatch(r'[0-9a-f]{40}', sha):
        raise ValueError('invalid main SHA')
    run_id, attempt = os.environ['GITHUB_RUN_ID'], os.environ['GITHUB_RUN_ATTEMPT']
    if not all(re.fullmatch(r'[1-9][0-9]*', value) for value in (run_id, attempt)):
        raise ValueError('invalid workflow attempt')
    owner = f'{run_id}:{attempt}'
    if sys.argv[1:] == ['--cleanup-only']:
        return int(not cleanup(active_sha(owner, sha), owner))
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
        # Private historical checkouts, generated profiles and verified markers live here.
        with tempfile.TemporaryDirectory(prefix='mycelium-ci-', dir=os.environ['RUNNER_TEMP']) as scratch:
            status = run_pending(root, sha, Path(scratch), owner)
    except SubmitterStopped:
        print('CI submitter interrupted or timed out; cancelling owned gates', flush=True)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig in watched:
            signal.signal(sig, signal.SIG_IGN)
        try:
            cleaned = cleanup(active_sha(owner, sha), owner)
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
