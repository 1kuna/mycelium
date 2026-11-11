#!/usr/bin/env python3
"""Flag a branch that restores files to older blobs a newer main commit changed.

usage: scripts/gate-revert-check.py [BASE] [--head REF] [--allow-reverts]

Compares HEAD (or --head REF) with its merge base on BASE (default origin/main). For each file
the branch changes, if the branch's version equals an earlier version of that
file in BASE's history, the branch would undo every later commit to that file.
A file the branch deletes is reported when a commit it undoes added the file.
This catches a stale working tree committed onto a fresh tip (#1224: the holds
commit silently reverted #1356 and #1357). Exit 1 lists the files and commits;
--allow-reverts prints the same report and exits 0 for an intended revert.
"""
import subprocess
import sys


def git(*args):
    return subprocess.check_output(['git', *args], text=True)


def history(base, path):
    """Newest-first (commit, subject, old blob, new blob) for path on base."""
    out = git('log', '--no-renames', '--raw', '--no-abbrev', '--format=commit %H %s',
              base, '--', path)
    records, commit = [], None
    for line in out.splitlines():
        if line.startswith('commit '):
            commit = line[7:].split(' ', 1) + ['']
        elif line.startswith(':') and commit:
            meta, name = line.split('\t', 1)
            if name != path:
                continue
            fields = meta.split()
            records.append((commit[0], commit[1], fields[2], fields[3]))
    return records


def undone(records, blob):
    """Commits after the newest point where the file had this blob, else None."""
    newer = []
    for commit, subject, old, new in records:
        if new == blob:
            return newer
        newer.append((commit, subject, old, new))
        if old == blob:
            return newer
    return None


def check(base, head='HEAD'):
    merge_base = git('merge-base', head, base).strip()
    absent = '0' * 40
    findings = {}
    deleted = []
    for line in git('diff', '--raw', '--no-renames', '--no-abbrev', merge_base, head).splitlines():
        meta, path = line.split('\t', 1)
        fields = meta.split()
        before, after = fields[2], fields[3]
        if after == absent:
            deleted.append(path)
            continue
        commits = undone(history(merge_base, path), after)
        if commits:
            findings[path] = commits
    # A deletion restores the state before the file existed. Report it only
    # when the branch already undoes the commit that added it, so ordinary
    # deletions stay quiet.
    undone_commits = {commit for commits in findings.values() for commit, *_ in commits}
    for path in deleted:
        records = history(merge_base, path)
        added = [record for record in records if record[2] == absent]
        if added and added[0][0] in undone_commits:
            findings[path] = records[:records.index(added[0]) + 1]
    return merge_base, findings


def main(argv):
    allow = '--allow-reverts' in argv
    args = [arg for arg in argv if arg != '--allow-reverts']
    head = 'HEAD'
    if '--head' in args:
        index = args.index('--head')
        head = args[index + 1]
        del args[index:index + 2]
    base = args[0] if args else 'origin/main'
    merge_base, findings = check(base, head)
    if not findings:
        print(f'revert-check: ok (no file restored to an older version since {merge_base[:12]})')
        return 0
    print(f'revert-check: {len(findings)} file(s) restored to an older version; '
          f'merging would undo these commits on {base}:')
    by_commit = {}
    for path, commits in sorted(findings.items()):
        print(f'  {path}')
        for commit, subject, *_ in commits:
            print(f'    undoes {commit[:12]} {subject}')
            by_commit.setdefault((commit, subject), []).append(path)
    print('commits affected:')
    for (commit, subject), paths in sorted(by_commit.items(), key=lambda item: -len(item[1])):
        print(f'  {commit[:12]} {len(paths)} file(s)  {subject}')
    if allow:
        print('revert-check: --allow-reverts given; treating as an intended revert')
        return 0
    print('revert-check: rebuild the branch from current main (stale tree?) or, for an '
          'intended revert, rerun with --allow-reverts and say so in the PR')
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
