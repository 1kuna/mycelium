#!/usr/bin/env python3
"""Track completed main CI trees; local receipt reuse is deliberately unavailable."""
from __future__ import annotations

import json
import os
import re
import subprocess
from urllib.request import Request, urlopen

# v1 had unbound provenance; v2/v3 could suppress recovery without a suite receipt.
CONTEXT = 'mycelium/main-full-tree-v4'
ATTEMPT_CONTEXT = 'mycelium/main-full-attempts-v1'
REPOSITORY = '1kuna/mycelium'


def git(*args: str) -> str:
    return subprocess.check_output(['git', *args], text=True).strip()


class GitHub:
    def __init__(self) -> None:
        self.token = os.environ.get('GITHUB_TOKEN') or subprocess.check_output(
            ['gh', 'auth', 'token'], text=True).strip()

    def request(self, path: str, body: dict | None = None) -> object:
        request = Request(f'https://api.github.com/repos/{REPOSITORY}/{path}',
                          data=json.dumps(body).encode() if body is not None else None,
                          headers={'Authorization': f'Bearer {self.token}',
                                   'Accept': 'application/vnd.github+json',
                                   'Content-Type': 'application/json'})
        with urlopen(request) as response:
            return json.load(response)

    def pages(self, path: str) -> list:
        rows = []
        page = 1
        while True:
            batch = self.request(f'{path}?per_page=100&page={page}')
            rows.extend(batch)
            if len(batch) < 100:
                return rows
            page += 1

    def main_tip(self) -> str:
        sha = self.request('git/ref/heads/main')['object']['sha']
        if not re.fullmatch('[0-9a-f]{40}', sha):
            raise ValueError('invalid main tip')
        return sha

    def status(self, sha: str, tree: str) -> dict | None:
        # GitHub returns newest first; a later failure revokes an older success.
        for row in self.pages(f'commits/{sha}/statuses'):
            if row['context'] == CONTEXT:
                return dict(row, source_sha=sha) if row['description'] == f'tree:{tree}' else None
        return None

    def attempts(self, sha: str, tree: str) -> tuple[int, str | None]:
        # The oldest landed commit with this tree is its durable counter anchor.
        # Workflow/ref concurrency serializes reservations across pushes/dispatches.
        for row in self.pages(f'commits/{sha}/statuses'):
            if row['context'] == ATTEMPT_CONTEXT:
                match = re.fullmatch(r'tree:' + re.escape(tree) +
                                     r' attempts:([1-9][0-9]*) owner:([1-9][0-9]*:[1-9][0-9]*)',
                                     row['description'])
                if match is None:
                    raise ValueError('invalid tree attempt counter; inspect trusted statuses')
                return int(match[1]), match[2]
        return 0, None

    def record_attempt(self, sha: str, tree: str, count: int, owner: str, url: str, state: str = 'error') -> None:
        # Charge before submission: a lost reply/interruption cannot refund work.
        # This context is never proof. Clear its error only after verified
        # success, retaining the same count so the combined commit can go green.
        if state not in ('success', 'error'):
            raise ValueError('invalid counter status')
        self.request(f'statuses/{sha}', {'context': ATTEMPT_CONTEXT, 'state': state,
                     'description': f'tree:{tree} attempts:{count} owner:{owner}', 'target_url': url})

    def publish(self, sha: str, tree: str, url: str, state: str = 'success') -> None:
        if state not in ('success', 'error'):
            raise ValueError('no verified negative gate receipt format is supported')
        if not re.fullmatch(r'https://github\.com/1kuna/mycelium/actions/runs/[0-9]+(?:[/?#].*)?', url):
            raise ValueError('coverage needs a public repository run URL')
        self.request(f'statuses/{sha}', {'context': CONTEXT, 'state': state,
                                      'description': f'tree:{tree}', 'target_url': url})


def fetch_main(api: GitHub) -> str:
    """Resolve the authoritative ref, then fetch that immutable object."""
    sha = api.main_tip()
    subprocess.run(['git', 'fetch', '--no-tags', 'origin', sha], check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if git('rev-parse', sha + '^{commit}') != sha:
        raise ValueError('main tip identity mismatch')
    return sha


def main_history(sha: str) -> list[str]:
    # Start with the merge introducing this protocol, not all historical releases.
    start = git('log', '--first-parent', '--diff-filter=A', '--format=%H', sha,
                '--', 'scripts/actions-proof.py').splitlines()
    if len(start) != 1:
        raise ValueError('cannot identify tree-proof activation')
    history = git('rev-list', '--first-parent', '--reverse', f'{start[0]}^..{sha}').splitlines()
    if not history or history[-1] != sha:
        raise ValueError('incomplete main history')
    return history


def report(sha: str, tree: str, result: str, url: str = '') -> None:
    line = f'{sha} tree {tree}: {result}' + (f' ({url})' if url else '')
    print(line, flush=True)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
            stream.write(line + '\n\n')


if __name__ == '__main__':
    raise SystemExit('Local receipt publication is unavailable; helper provenance must be bound first.')
