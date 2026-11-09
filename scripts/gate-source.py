#!/usr/bin/env python3
"""Freeze both platform legs from the same identified source bytes."""
import hashlib
import ast
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile


def runtime_archive(archive, destination, source, helpers):
    """Overlay only frozen helper bytes; transport never rereads a live checkout."""
    import shutil
    shutil.copyfile(archive, destination)
    with tarfile.open(destination, 'a') as tar:
        for helper in helpers:
            tar.add(source / 'scripts' / helper, arcname='scripts/' + helper)


def freeze(repo, archive, ref=None):
    repo, archive = Path(repo), Path(archive)
    if ref:
        with archive.open('wb') as output:
            subprocess.run(['git', '-C', str(repo), 'archive', ref], stdout=output, check=True)
        sha = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', ref], text=True).strip()
        dirty = False
    else:
        if subprocess.check_output(['git', '-C', str(repo), 'ls-files', '-u', '-z']):
            raise RuntimeError('source has unresolved merge entries; no snapshot created')
        sha = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
        dirty = bool(subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain',
                                              '--untracked-files=all']))
        names = subprocess.check_output(['git', '-C', str(repo), 'ls-files', '-z',
                                         '--cached', '--others', '--exclude-standard'])
        with tarfile.open(archive, 'w') as tar:
            for name in sorted(set(os.fsdecode(n) for n in names.split(b'\0') if n)):
                path = repo / name
                if not path.exists():
                    continue
                if path.is_symlink() or not path.is_file():
                    raise RuntimeError('source snapshots require regular files: ' + name)
                data = path.read_bytes()
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), path.stat().st_mode & 0o777
                tar.addfile(info, io.BytesIO(data))
    digest = hashlib.sha256()
    with archive.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return dict(source_sha=sha, source_dirty=dirty, source_archive_sha256=digest.hexdigest())


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
