#!/usr/bin/env python3
"""Read-only evidence for daemon resources outside the gate process UID."""
import json
import os
import re
from pathlib import Path
import shutil
import subprocess


def docker_snapshot(endpoint=None):
    endpoint = endpoint or os.environ.get('DOCKER_HOST') or 'unix:///var/run/docker.sock'
    binary = shutil.which('docker')
    socket_present = endpoint.startswith('unix://') and Path(endpoint[7:]).exists()
    row = dict(schema=1, endpoint=endpoint, configured=bool(binary or socket_present or
               os.environ.get('DOCKER_HOST') or not endpoint.startswith('unix://')), reachable=False)
    if binary:
        try:
            daemon = docker_call(binary, endpoint, ['info', '--format', '{{json .ID}}'])
            identity = json.loads(daemon)
            if not isinstance(identity, str) or not identity:
                raise RuntimeError('Docker daemon identity is unavailable')
            row.update(reachable=True, daemon_identity=identity)
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
            pass  # Entry observation, never affirmative cleanup evidence.
    return row


def docker_call(binary, endpoint, arguments):
    env = {k: v for k, v in os.environ.items() if k != 'DOCKER_CONTEXT' and not k.startswith('MYCE_')}
    result = subprocess.run([binary, '--host', endpoint, *arguments], env=env,
                            capture_output=True, text=True, check=True, timeout=5)
    return result.stdout


def docker_inventory(marker, entry=None, used=()):
    """Never remove resources: an unresolved daemon retains the reservation."""
    if not isinstance(marker, str) or not re.fullmatch('[0-9a-f]{32}', marker):
        raise RuntimeError('Docker gate identity is malformed')
    if entry is not None and (not isinstance(entry, dict) or entry.get('schema') != 1 or
            entry.get('marker') != marker or type(entry.get('labelled_fixtures')) is not bool or
            type(entry.get('configured')) is not bool or type(entry.get('reachable')) is not bool or
            not isinstance(entry.get('endpoint'), str) or not entry['endpoint'] or
            entry['reachable'] and (not entry['configured'] or
                not isinstance(entry.get('daemon_identity'), str) or not entry['daemon_identity'])):
        raise RuntimeError('Docker entry evidence is malformed')
    endpoints = {entry['endpoint']} if entry else {os.environ.get('DOCKER_HOST') or 'unix:///var/run/docker.sock'}
    for row in used:
        if not isinstance(row, dict) or row.get('marker') != marker or not isinstance(row.get('endpoint'), str) or not row['endpoint']:
            raise RuntimeError('Docker fixture journal is malformed')
        endpoints.add(row['endpoint'])
    proof = []
    for endpoint in sorted(endpoints):
        observation = docker_snapshot(endpoint)
        required = bool(used) or bool(entry and (entry['configured'] or entry['reachable']))
        if not observation['reachable']:
            if required or observation['configured']:
                raise RuntimeError('Docker daemon cleanup is unknown or unreachable')
            proof.append(dict(endpoint=endpoint, stopped=True, unavailable=True))
            continue
        if entry and entry['endpoint'] == endpoint and entry['reachable'] and entry.get('daemon_identity') != observation['daemon_identity']:
            raise RuntimeError('Docker daemon identity changed')
        binary = shutil.which('docker')
        found = {}
        for kind, listing in (('container', ['ps', '-a']),
                              ('volume', ['volume', 'ls']), ('network', ['network', 'ls'])):
            # Broad legacy protection also covers pinned pre-label source heads.
            # It can conservatively retain capacity for another Mycelium runtime;
            # it never modifies that runtime or unrelated compose services.
            identifiers = set()
            legacy = not entry or entry.get('labelled_fixtures') is not True
            labels = ['myce.gate=' + marker] + (['myce.attempt', 'myce.worker'] if legacy else [])
            for label in labels:
                identifiers.update(docker_call(binary, endpoint,
                    [*listing, '--quiet', '--filter', 'label=' + label]).split())
            names = docker_call(binary, endpoint, [*listing, '--format', '{{json .Name}}' if kind != 'container' else '{{json .Names}}'])
            for line in names.splitlines():
                name = json.loads(line)
                if not isinstance(name, str):
                    raise RuntimeError('Docker resource inventory is malformed')
                if legacy and name.startswith(('myce-', 'mycelium-')):
                    identifiers.add(name)
            found[kind] = sorted(identifiers)
        if any(found.values()):
            raise RuntimeError('Docker fixture resources remain: ' + json.dumps(found, sort_keys=True))
        proof.append(dict(endpoint=endpoint, daemon_identity=observation['daemon_identity'],
                          stopped=True, resources=found))
    return proof


def runtime_cleanup(directory, marker):
    directory = Path(directory)
    entry_path, used_path = directory / 'docker-entry.json', directory / 'docker-used.jsonl'
    if directory.is_symlink() or entry_path.is_symlink() or used_path.is_symlink():
        raise RuntimeError('linked runtime custody evidence')
    if directory.exists() and (not directory.is_dir() or not entry_path.is_file()):
        raise RuntimeError('runtime custody evidence is incomplete')
    entry = json.loads(entry_path.read_text()) if entry_path.exists() else None
    used = [json.loads(line) for line in used_path.read_text().splitlines()] if used_path.exists() else []
    return docker_inventory(marker, entry, used)


def source_cleanup(source, fallback_marker):
    source = Path(source)
    root = source / '.reports/gate-runtime'
    if root.is_symlink() or root.parent.is_symlink():
        raise RuntimeError('linked runtime custody evidence')
    if root.exists() and not root.is_dir():
        raise RuntimeError('runtime custody evidence is malformed')
    directories = list(root.iterdir()) if root.exists() else []
    proof = []
    for directory in directories or [root / fallback_marker]:
        proof.extend(runtime_cleanup(directory, directory.name))
    return proof
