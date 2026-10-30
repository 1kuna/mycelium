#!/usr/bin/env python3
"""Fleet-side main observer. Installation and credentials require operator approval."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import time

PLATFORMS = ('darwin', 'linux')
TERMINAL = {'succeeded': 'success', 'failed': 'failure', 'cancelled': 'failure'}
# Bound publication work after downtime; remaining merges stay behind the cursor.
# Costs catch-up latency, but never drops a merge or starts extra concurrent suites.
MAX_PENDING = 16


def command(argv: list[str], cwd: Path, *, myce: bool = False) -> str:
    env = dict(os.environ)
    for name in list(env):
        if (myce and name in ('GH_TOKEN', 'GITHUB_TOKEN', 'GH_ENTERPRISE_TOKEN',
                             'GITHUB_ENTERPRISE_TOKEN', 'MYCE_NOTIFY')) or (
            not myce and name.startswith('MYCE_')
        ):
            env.pop(name)
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True)
    if result.returncode:
        # Never relay command output: it can contain private paths or credentials.
        raise RuntimeError(f'{Path(argv[0]).name} failed (exit {result.returncode})')
    return result.stdout


def platform_profile(text: str, sha: str, platform: str) -> str:
    """Narrow the existing gate profile, retaining tools, resources and acceptance."""
    text, sources = re.subn(r'(?m)^  git: HEAD$', f'  git: {sha}', text)
    targets = ('  - {os: darwin, arch: arm64, platform: cpu}\n' if platform == 'darwin'
               else '  - {os: linux, arch: amd64, platform: cpu}\n'
                    '  - {os: linux, arch: arm64, platform: cpu}\n')
    text, matrices = re.subn(r'(?m)^targets:\n(?:  - .*\n)+', 'targets:\n' + targets, text)
    if sources != 1 or matrices != 1:
        raise RuntimeError('gate-full profile format changed; update main-ci adapter')
    return text


class Watcher:
    def __init__(self, repo: Path, state: Path, github: str, myce: str) -> None:
        self.repo, self.state, self.github, self.myce = repo, state, github, myce
        self.path = state / 'ledger.json'
        self.data = json.loads(self.path.read_text()) if self.path.exists() else None

    def git(self, *args: str) -> str:
        return command(['git', *args], self.repo).strip()

    def save(self) -> None:
        temporary = self.path.with_suffix('.tmp')
        with temporary.open('w') as stream:
            json.dump(self.data, stream)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.path)

    def status(self, sha: str, platform: str, state: str, description: str) -> None:
        # Compare GitHub's latest context, including after an ambiguous POST reply.
        # Reposting an unchanged infrastructure error every minute eventually hits
        # GitHub's per-SHA/context status limit and prevents terminal publication.
        context = f'mycelium/full-{platform}'
        latest = json.loads(command(['gh', 'api',
                                    f'repos/{self.github}/commits/{sha}/status'], self.repo))
        if any(s['context'] == context and s['state'] == state and
               s['description'] == description for s in latest['statuses']):
            return
        command(['gh', 'api', '--method', 'POST',
                 f'repos/{self.github}/statuses/{sha}', '-f', f'state={state}',
                 '-f', f'context=mycelium/full-{platform}', '-f',
                 f'description={description}', '-f',
                 f'target_url=https://github.com/{self.github}/commit/{sha}'], self.repo)

    def discover(self) -> None:
        # Do not trust origin: a fork clone must never supply code for real-repo statuses.
        self.git('fetch', '--quiet', f'https://github.com/{self.github}.git',
                 'refs/heads/main:refs/remotes/origin/main')
        head = self.git('rev-parse', 'refs/remotes/origin/main')
        if self.data is None:
            # Explicit bootstrap: test current main once, not all historical v1 commits.
            commits = [head]
            self.data = {'cursor': head, 'pending': []}
        else:
            cursor = self.data['cursor']
            history = self.git('rev-list', '--first-parent', head).splitlines()
            if cursor not in history:
                raise RuntimeError('main history rewritten; operator must reconcile cursor')
            commits = list(reversed(history[:history.index(cursor)]))[
                :max(0, MAX_PENDING - len(self.data['pending']))]
        for sha in commits:
            self.data['pending'].append({'sha': sha, 'platforms': {}})
        if commits:
            self.data['cursor'] = commits[-1]
        self.save()
        # Publish pending even for backlog; replay if publication was interrupted.
        for item in self.data['pending']:
            for platform in PLATFORMS:
                if platform not in item['platforms']:
                    self.status(item['sha'], platform, 'pending', 'Awaiting Mycelium full gate')
                    item['platforms'][platform] = 'pending'
                    self.save()

    def tick(self) -> None:
        self.discover()
        if not self.data['pending']:
            return
        # Only one commit in flight (two platform jobs). Never cancel older merges.
        item = self.data['pending'][0]
        sha = item['sha']
        for platform in PLATFORMS:
            if item['platforms'].get(platform) in ('success', 'failure'):
                continue
            try:
                profile = self.state / f'{sha}-{platform}.yaml'
                if not profile.exists():
                    text = self.git('show', f'{sha}:.mycelium/profiles/gate-full.yaml') + '\n'
                    temporary = profile.with_suffix('.tmp')
                    temporary.write_text(platform_profile(text, sha, platform))
                    temporary.replace(profile)
                key = f'main-ci:{sha}:{platform}'
                # The existing CLI owns immutable source transport, idempotent submit,
                # detached observation, result verification and observer recovery.
                receipt = json.loads(command([self.myce, 'run', '--profile', str(profile),
                                              '--key', key], self.repo, myce=True))
                state = TERMINAL.get(receipt['status'])
                if state:
                    self.status(sha, platform, state,
                                f"Full gate {receipt['status']}; job {receipt['job_id']}")
                    item['platforms'][platform] = state
                    self.save()
            except (RuntimeError, ValueError, OSError, KeyError):
                try:
                    self.status(sha, platform, 'error', 'CI infrastructure error; watcher will retry')
                except RuntimeError:
                    pass  # Preserve work even when both queue and GitHub are unavailable.
                print(f'{sha} {platform}: infrastructure error; retrying', flush=True)
                # Retain pending locally: retry same submission key, never a new suite.
        if all(item['platforms'].get(p) in ('success', 'failure') for p in PLATFORMS):
            # Keep a small terminal ledger, not source bundles or raw suite logs.
            with (self.state / 'results.jsonl').open('a') as stream:
                stream.write(json.dumps(item) + '\n')
            self.data['pending'].pop(0)
            self.save()
            for platform in PLATFORMS:
                (self.state / f'{sha}-{platform}.yaml').unlink(missing_ok=True)
            # CLI receipts remain tiny recovery records; CLI deletes uploaded bundles.


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True, help='dedicated local repository clone')
    parser.add_argument('--state', type=Path, required=True, help='private persistent state directory')
    parser.add_argument('--github', default='1kuna/mycelium')
    parser.add_argument('--myce', default='myce', help='installed CLI with private queue client config')
    parser.add_argument('--once', action='store_true', help='one discovery/observation pass; no installation')
    args = parser.parse_args()
    os.umask(0o077)
    args.state = args.state.resolve()
    args.state.mkdir(parents=True, exist_ok=True)
    with (args.state / 'watcher.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        watcher = Watcher(args.repo.resolve(), args.state, args.github, args.myce)
        while True:
            try:
                watcher.tick()
            except (RuntimeError, ValueError, OSError):
                print('CI discovery/publication unavailable; retained ledger; retrying', flush=True)
                if args.once:
                    raise SystemExit(1)
            if args.once:
                return
            # Discovery latency <= 60s when healthy; suites run only for new main SHAs.
            time.sleep(60)


if __name__ == '__main__':
    main()
