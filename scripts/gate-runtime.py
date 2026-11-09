#!/usr/bin/env python3
"""Read-only evidence for daemon resources outside the gate process UID."""
import json
import os
import re
from pathlib import Path
import shutil
import stat
import subprocess
import sys


def docker_snapshot(endpoint=None):
    binary = shutil.which('docker')
    context = os.environ.get('DOCKER_CONTEXT')
    endpoint_source = 'explicit'
    if context and not binary:
        raise RuntimeError('effective Docker context endpoint is unknown without its CLI')
    if endpoint is None:
        if context or not os.environ.get('DOCKER_HOST') and binary:
            endpoint_source = 'DOCKER_CONTEXT' if context else 'configured-context'
            arguments = ['context', 'inspect', *([context] if context else []),
                         '--format', '{{json .Endpoints.docker.Host}}']
            endpoint = json.loads(docker_call(binary, None, arguments))
            if not isinstance(endpoint, str) or not endpoint:
                raise RuntimeError('effective Docker context endpoint is unknown')
        else:
            endpoint_source = 'DOCKER_HOST' if os.environ.get('DOCKER_HOST') else 'default-without-cli'
            endpoint = os.environ.get('DOCKER_HOST') or 'unix:///var/run/docker.sock'
    socket_present = endpoint.startswith('unix://') and Path(endpoint[7:]).exists()
    row = dict(schema=1, endpoint=endpoint, context=context, endpoint_source=endpoint_source,
               configured=bool(binary or socket_present or
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
    env = {k: v for k, v in os.environ.items() if k not in ('DOCKER_CONTEXT', 'DOCKER_HOST') and not k.startswith('MYCE_')}
    result = subprocess.run([binary, *(['--host', endpoint] if endpoint else []), *arguments], env=env,
                            capture_output=True, text=True, check=True, timeout=5)
    return result.stdout


def launcher_environment():
    """Inspect readable same-UID launch ancestors, without retaining values."""
    evidence = dict(checked=[], unreadable=[], historical_environment_retained=False)
    evidence['residual_risk'] = ('Historical environment was not retained. A historical non-local or TCP '
        'DOCKER_HOST outside this host may not be discoverable from local contexts and sockets.')
    if os.environ.get('DOCKER_HOST') or os.environ.get('DOCKER_CONTEXT'):
        raise RuntimeError('gate launch environment contains Docker endpoint overrides')
    try:
        selection = '-eo' if sys.platform.startswith('linux') else '-axo'
        output = subprocess.check_output(['ps', 'eww', selection, 'pid=,ppid=,uid=,command='],
                                         text=True, timeout=5)
        commands = subprocess.check_output(['ps', '-ww', selection, 'pid=,ppid=,uid=,command='],
                                           text=True, timeout=5)
        plain = {}
        for line in commands.splitlines():
            fields = line.split(None, 3)
            if len(fields) != 4:
                raise RuntimeError('launcher command inventory is incomplete')
            plain[int(fields[0])] = (int(fields[1]), int(fields[2]), fields[3])
        rows = {}
        for line in output.splitlines():
            fields = line.split(None, 3)
            if len(fields) != 4:
                raise RuntimeError('launcher process inventory is incomplete')
            rows[int(fields[0])] = (int(fields[1]), int(fields[2]), fields[3])
        cursor, visited = os.getpid(), set()
        while cursor:
            if cursor in visited or cursor not in rows:
                raise RuntimeError('historical launcher ancestry is unavailable')
            visited.add(cursor)
            parent, uid, command = rows[cursor]
            if uid == os.getuid():
                baseline = plain.get(cursor)
                if (not baseline or baseline[:2] != (parent, uid) or
                        not command.startswith(baseline[2] + ' ')):
                    evidence['unreadable'].append(cursor)
                    cursor = parent
                    continue
                # Remove argv before examining appended environment fields;
                # a shell -c body can legitimately contain Docker assignments.
                tokens = command[len(baseline[2]):].split()
                names = sorted({t.split('=', 1)[0] for t in tokens
                    if t.startswith(('DOCKER_HOST=', 'DOCKER_CONTEXT=')) and t.split('=', 1)[1]})
                if names:
                    raise RuntimeError('gate launch environment contains Docker endpoint overrides')
                readable = any(t.startswith(('HOME=', 'PATH=')) for t in tokens)
                evidence['checked' if readable else 'unreadable'].append(cursor)
            cursor = parent
    except (OSError, subprocess.SubprocessError, ValueError, RuntimeError) as error:
        if 'contains Docker endpoint overrides' in str(error):
            raise
        evidence['inventory_uncertainty'] = str(error)
    return evidence


def local_socket_paths():
    paths = {Path('/var/run/docker.sock'), Path('/run/docker.sock'),
             Path.home() / '.docker/run/docker.sock', Path.home() / '.colima/default/docker.sock',
             Path.home() / '.rd/docker.sock', Path('/run/user') / str(os.getuid()) / 'docker.sock'}
    if os.environ.get('XDG_RUNTIME_DIR'):
        paths.add(Path(os.environ['XDG_RUNTIME_DIR']) / 'docker.sock')
    return sorted(paths)


def legacy_endpoints():
    """Agent recovery default: inventory every discoverable legacy daemon."""
    environment = launcher_environment()
    binary = shutil.which('docker')
    if not binary:
        raise RuntimeError('legacy Docker context inventory is unavailable')
    contexts = [json.loads(line) for line in docker_call(binary, None,
        ['context', 'ls', '--format', '{{json .Name}}']).splitlines()]
    if not contexts or any(not isinstance(name, str) or not name for name in contexts):
        raise RuntimeError('legacy Docker context inventory is malformed')
    endpoints, context_rows, sockets = set(), [], []
    for name in sorted(set(contexts)):
        endpoint = json.loads(docker_call(binary, None, ['context', 'inspect', name,
            '--format', '{{json .Endpoints.docker.Host}}']))
        if not isinstance(endpoint, str) or not endpoint:
            raise RuntimeError('legacy Docker context endpoint is unknown')
        endpoints.add(endpoint)
        context_rows.append(dict(context=name, endpoint=endpoint))
    for path in local_socket_paths():
        try:
            mode = path.stat().st_mode
        except FileNotFoundError:
            if path.is_symlink():
                raise RuntimeError('legacy Docker socket target is unknown')
            sockets.append(dict(path=str(path), present=False))
            continue
        if not stat.S_ISSOCK(mode):
            raise RuntimeError('legacy Docker socket inventory is not a socket')
        endpoint = 'unix://' + str(path)
        endpoints.add(endpoint)
        sockets.append(dict(path=str(path), present=True))
    return endpoints, dict(contexts=context_rows, sockets=sockets, launch_environment=environment,
                          interpretation='orchestrator agent default, 2026-11-09')


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
    discovery = None
    if entry is None:
        endpoints, discovery = legacy_endpoints()
    else:
        endpoints = {entry['endpoint']}
    for row in used:
        if not isinstance(row, dict) or row.get('marker') != marker or not isinstance(row.get('endpoint'), str) or not row['endpoint']:
            raise RuntimeError('Docker fixture journal is malformed')
        endpoints.add(row['endpoint'])
    proof = []
    daemons = {}
    for endpoint in sorted(endpoints):
        observation = docker_snapshot(endpoint)
        required = entry is None or bool(used) or bool(entry and (entry['configured'] or entry['reachable']))
        if not observation['reachable']:
            if required or observation['configured']:
                raise RuntimeError('Docker daemon cleanup is unknown or unreachable')
            proof.append(dict(endpoint=endpoint, stopped=True, unavailable=True))
            continue
        if entry and entry['endpoint'] == endpoint and entry['reachable'] and entry.get('daemon_identity') != observation['daemon_identity']:
            raise RuntimeError('Docker daemon identity changed')
        if observation['daemon_identity'] in daemons:
            daemons[observation['daemon_identity']]['endpoints'].append(endpoint)
            continue
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
        row = dict(endpoint=endpoint, endpoints=[endpoint], daemon_identity=observation['daemon_identity'],
                   stopped=True, resources=found)
        if discovery is not None:
            row['legacy_discovery'] = discovery
        proof.append(row)
        daemons[observation['daemon_identity']] = row
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
