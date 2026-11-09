#!/usr/bin/env python3
"""Freeze both platform legs from the same identified source bytes."""
import hashlib
import ast
import io
import json
import os
import stat
from pathlib import Path
import subprocess
import tarfile
import tempfile


def runtime_archive(archive, destination, source, helpers):
    """Overlay only frozen helper bytes; transport never rereads a live checkout."""
    import shutil
    shutil.copyfile(archive, destination)
    with tarfile.open(destination, 'a') as tar:
        for helper in helpers:
            tar.add(source / 'scripts' / helper, arcname='scripts/' + helper)


def worktree_manifest(repo, tar=None):
    names = subprocess.check_output(['git', '-C', str(repo), 'ls-files', '-z',
                                     '--cached', '--others', '--exclude-standard'])
    result = {}
    for name in sorted(set(os.fsdecode(n) for n in names.split(b'\0') if n)):
        path = repo / name
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            continue  # Tracked deletion is represented by absence in the tree.
        with os.fdopen(descriptor, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise RuntimeError('source snapshots require regular files: ' + name)
            data = stream.read()
        # Git identifies executable mode, not the checkout umask or group-write
        # bits that git archive adds. Use the same canonical modes in both legs.
        mode = 0o755 if info.st_mode & 0o111 else 0o644
        result[name] = (mode, hashlib.sha256(data).hexdigest())
        if tar is not None:
            member = tarfile.TarInfo(name)
            member.size, member.mode = len(data), mode
            tar.addfile(member, io.BytesIO(data))
    return result


def archive_manifest(tar):
    return {member.name: (0o755 if member.mode & 0o111 else 0o644,
                         hashlib.sha256(tar.extractfile(member).read()).hexdigest())
            for member in tar if member.isfile()}


def freeze(repo, archive, ref=None):
    repo, archive = Path(repo), Path(archive)
    # Resolve once: a branch moving later cannot change either tested bytes or
    # their recorded commit identity. Archive only this immutable commit ID.
    sha = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--verify',
        '--end-of-options', (ref or 'HEAD') + '^{commit}'], text=True).strip()
    if ref:
        with archive.open('wb') as output:
            subprocess.run(['git', '-C', str(repo), 'archive', sha], stdout=output, check=True)
        with tarfile.open(archive) as tar:
            if tar.pax_headers.get('comment') != sha:
                raise RuntimeError('source archive does not match its pinned commit')
            captured = archive_manifest(tar)
        dirty = False
    else:
        if subprocess.check_output(['git', '-C', str(repo), 'ls-files', '-u', '-z']):
            raise RuntimeError('source has unresolved merge entries; no snapshot created')
        with tarfile.open(archive, 'w') as tar:
            captured = worktree_manifest(repo, tar)
        # Verify bytes, modes, additions/deletions and HEAD, not an earlier
        # cleanliness check that an edit can invalidate before the archive.
        if (worktree_manifest(repo) != captured or subprocess.check_output(
                ['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip() != sha or
                subprocess.check_output(['git', '-C', str(repo), 'ls-files', '-u', '-z'])):
            raise RuntimeError('source changed while capturing the working tree; no test started')
        with tempfile.TemporaryFile() as stream:
            subprocess.run(['git', '-C', str(repo), 'archive', sha], stdout=stream, check=True)
            stream.seek(0)
            with tarfile.open(fileobj=stream) as tar:
                dirty = captured != archive_manifest(tar)
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    tree = hashlib.sha256(json.dumps(captured, sort_keys=True).encode()).hexdigest()
    return dict(source_sha=sha, source_dirty=dirty, source_tree_sha256=tree,
                source_archive_sha256=digest.hexdigest())


def extract(archive, destination):
    with tarfile.open(archive) as tar:
        for member in tar:
            if (Path(member.name).is_absolute() or '..' in Path(member.name).parts
                    or not (member.isfile() or member.isdir())):
                raise RuntimeError('unsafe source member: ' + member.name)
        tar.extractall(destination)


def predecessors(repo, source):
    """Seed exact compatibility sources from the real Git root, never a snapshot cwd."""
    tree = ast.parse((source / 'scripts/upgrade-compat.py').read_text())
    revisions = [node.value.value for node in tree.body
                 if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
                 and any(isinstance(t, ast.Name) and t.id in
                         ('PREVIOUS', 'INVENTORY_BASELINE', 'CADENCE_BASELINE') for t in node.targets)]
    destination = source / '.gate-predecessors'
    destination.mkdir()
    result = []
    for revision in revisions:
        path = destination / ('source-' + revision + '.tar')
        with path.open('wb') as output:
            subprocess.run(['git', '-C', str(repo), 'archive', revision], stdout=output, check=True)
        with tarfile.open(path) as tar:
            names = tar.getnames()
            if (tar.pax_headers.get('comment') != revision or 'go.mod' not in names
                    or not any(n.startswith('internal/myce/') for n in names)):
                raise RuntimeError('predecessor archive identity/content mismatch')
        result.append(path)
    return result
