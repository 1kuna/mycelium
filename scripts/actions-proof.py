#!/usr/bin/env python3
"""Publish verified local full receipts and reuse exact-tree proof on main."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
from urllib.request import Request, urlopen

CONTEXT = 'mycelium/full-tree-v1'
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

    def tree(self, sha: str) -> str:
        if not re.fullmatch('[0-9a-f]{40}', sha):
            raise ValueError('invalid commit identity')
        return self.request(f'git/commits/{sha}')['tree']['sha']

    def status(self, sha: str, tree: str) -> dict | None:
        # GitHub returns newest first; a later failure revokes an older success.
        for row in self.pages(f'commits/{sha}/statuses'):
            if row['context'] == CONTEXT:
                return dict(row, source_sha=sha) if row['description'] == f'tree:{tree}' else None
        return None

    def publish(self, sha: str, tree: str, url: str, state: str = 'success') -> None:
        if not re.fullmatch(r'https://github\.com/1kuna/mycelium/(?:pull|actions/runs)/[0-9]+(?:[/?#].*)?', url):
            raise ValueError('proof needs a public repository PR or run URL')
        self.request(f'statuses/{sha}', {'context': CONTEXT, 'state': state,
                                      'description': f'tree:{tree}', 'target_url': url})


def local_proof(folder: Path) -> tuple[str, str]:
    """Accept only a completed, clean, normal Darwin+Linux local full."""
    manifest = json.loads((folder / 'manifest.json').read_text())
    identity = json.loads((folder / 'source-identity.json').read_text())
    cleanup = json.loads((folder / 'overall-cleanup.json').read_text())
    run_id = manifest['identity']
    if (manifest['mode'] != 'full' or manifest['platform'] != 'both' or manifest['command']
            or cleanup.get('schema') != 1 or cleanup.get('identity') != run_id
            or cleanup.get('stopped') is not True or cleanup.get('status') != 0
            or identity.get('source_dirty') is not False):
        raise ValueError('not a clean completed two-platform full')
    sha = identity['source_sha']
    if not re.fullmatch('[0-9a-f]{40}', sha):
        raise ValueError('invalid receipt source')
    tree = git('rev-parse', sha + '^{tree}')
    # Validate the receipt's content digest against the committed bytes/modes,
    # rather than trusting a SHA label on a dirty or unrelated measurement.
    with tempfile.TemporaryFile() as archive:
        subprocess.run(['git', 'archive', sha], stdout=archive, check=True)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as tar:
            files = {m.name: (0o755 if m.mode & 0o111 else 0o644,
                              hashlib.sha256(tar.extractfile(m).read()).hexdigest())
                     for m in tar if m.isfile()}
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    if identity['source_tree_sha256'] != digest:
        raise ValueError('receipt bytes do not match committed tree')
    rows = [json.loads(line) for line in Path(manifest['report']).read_text().splitlines()]
    rows = [row for row in rows if row.get('run_id') == run_id]
    for phase, platform in [('host', 'darwin/'), ('native', 'darwin/'), ('linux', 'linux/')]:
        matches = [row for row in rows if row.get('phase') == phase]
        if len(matches) != 1:
            raise ValueError('missing or ambiguous platform result')
        row = matches[0]
        if (row.get('schema') != 1 or row.get('mode') != 'full' or row.get('status') != 0 or row.get('refusal')
                or not row.get('platform', '').startswith(platform)
                or any(row.get(key) != identity[key] for key in
                       ('source_sha', 'source_dirty', 'source_tree_sha256', 'instrumentation_sha256'))):
            raise ValueError('failed or mismatched platform result')
    return sha, tree


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


def proof_for_commit(api: GitHub, sha: str, tree: str) -> dict | None:
    own = api.status(sha, tree)
    if own is not None:
        return own
    # Associated PR heads cover ordinary, squash and rebase merges. Compare
    # immutable Git tree IDs, never commit messages or mutable branch names.
    for pr in api.pages(f'commits/{sha}/pulls'):
        if (pr.get('merged_at') and pr['base']['ref'] == 'main'
                and pr['base']['repo']['full_name'] == REPOSITORY):
            head = pr['head']['sha']
            if api.tree(head) == tree:
                proof = api.status(head, tree)
                if proof and proof['state'] == 'success':
                    return proof
    return None


def report(sha: str, tree: str, result: str, url: str = '') -> None:
    line = f'{sha} tree {tree}: {result}' + (f' ({url})' if url else '')
    print(line, flush=True)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as stream:
            stream.write(line + '\n\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('receipts', type=Path)
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    sha, tree = local_proof(args.receipts)
    GitHub().publish(sha, tree, args.evidence)
    print(f'Recorded {CONTEXT}: commit {sha}, tree {tree}')
