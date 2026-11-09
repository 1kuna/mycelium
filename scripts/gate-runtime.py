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
import shlex


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
    evidence['residual_risk'] = ('Historical environment was not retained. A historical non-local, TCP or '
        'arbitrary local Unix-socket DOCKER_HOST may not be discoverable from current contexts and sockets. '
        'Legacy recovery additionally bounds this with a positive live Docker-API daemon process inventory; '
        'a historical daemon that has since exited remains outside this host observation.')
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
             Path.home() / '.orbstack/run/docker.sock', Path.home() / '.rd/docker.sock',
             Path.home() / 'Library/Containers/com.docker.docker/Data/docker.raw.sock',
             Path('/run/user') / str(os.getuid()) / 'docker.sock'}
    def children(directory):
        try:
            return sorted(directory.iterdir())
        except FileNotFoundError:
            if directory.is_symlink():
                raise RuntimeError('local Docker runtime directory target is unknown')
            return []
    # Do not use glob: pathlib can suppress directory-inventory errors.
    for profile in children(Path.home() / '.colima'):
        if profile.is_dir():
            paths.add(profile / 'docker.sock')
    for instance in children(Path.home() / '.lima'):
        if instance.is_dir():
            paths.update(p for p in children(instance / 'sock') if p.name.endswith('.sock'))
    if os.environ.get('XDG_RUNTIME_DIR'):
        paths.add(Path(os.environ['XDG_RUNTIME_DIR']) / 'docker.sock')
    return sorted(paths)


def native_socket_paths(pids):
    """Read bound Unix socket addresses without lsof's filesystem walks."""
    import ctypes as c
    lib = c.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    lib.proc_pidinfo.argtypes = [c.c_int, c.c_int, c.c_uint64, c.c_void_p, c.c_int]
    lib.proc_pidfdinfo.argtypes = [c.c_int, c.c_int, c.c_int, c.c_void_p, c.c_int]
    class Descriptor(c.Structure):
        _fields_ = [('number', c.c_int32), ('kind', c.c_uint32)]
    paths = set()
    for pid in pids:
        size = lib.proc_pidinfo(pid, 1, 0, None, 0)
        if size <= 0:
            raise RuntimeError('daemon socket descriptor inventory unavailable')
        entries = (Descriptor * (size // c.sizeof(Descriptor) + 32))()
        count = lib.proc_pidinfo(pid, 1, 0, c.byref(entries), c.sizeof(entries))
        if count <= 0 or count >= c.sizeof(entries) or count % c.sizeof(Descriptor):
            raise RuntimeError('daemon socket descriptor inventory incomplete')
        for fd in entries[:count // c.sizeof(Descriptor)]:
            if fd.kind != 2:  # PROX_FDTYPE_SOCKET
                continue
            # socket_fdinfo ABI in sys/proc_info.h (64-bit Darwin): 792 bytes;
            # soi_kind at 256, un_sockinfo.unsi_addr at 280, sun_path at 282.
            info = c.create_string_buffer(792)
            if lib.proc_pidfdinfo(pid, fd.number, 3, info, 792) != 792:
                raise RuntimeError('daemon socket address inventory incomplete')
            if int.from_bytes(info.raw[256:260], sys.byteorder) == 3:  # SOCKINFO_UN
                path = os.fsdecode(info.raw[282:386].split(b'\0', 1)[0])
                if path.startswith('/'):
                    paths.add(path)
    return paths


def daemon_process_inventory(endpoints):
    """Legacy bound: every live API runtime needs an inventoried endpoint."""
    selection = '-eo' if sys.platform.startswith('linux') else '-axo'
    output = subprocess.check_output(['ps', '-ww', selection, 'pid=,ppid=,command='],
                                     text=True, timeout=5)
    rows = {}
    for line in output.splitlines():
        fields = line.split(None, 2)
        if len(fields) != 3:
            raise RuntimeError('Docker daemon process inventory incomplete')
        argv = shlex.split(fields[2])
        # ps does not quote executable paths with spaces. These application
        # bundles otherwise become an unrecognized truncated argv[0].
        if re.match(r'^/Applications/Rancher Desktop\.app/', fields[2]):
            argv[0] = 'rancher-desktop'
        rows[int(fields[0])] = (int(fields[1]), argv)
    known = {os.path.realpath(e[7:]): e for e in endpoints if e.startswith('unix://')}
    proof, covered, lima = [], set(), None
    def require(pid, kind, candidates, **extra):
        candidates = set(candidates)
        if not candidates or not candidates <= set(endpoints):
            raise RuntimeError('unresolved Docker-API daemon process: ' + str(pid) + ' (' + kind + ')')
        proof.append(dict(pid=pid, kind=kind, endpoints=sorted(candidates), **extra))
    for pid, (parent, argv) in rows.items():
        if not argv or pid in covered:
            continue
        name = Path(argv[0]).name.lower()
        if name == 'dockerd':
            hosts, config = [], Path('/etc/docker/daemon.json')
            for index, arg in enumerate(argv[1:], 1):
                if arg in ('-H', '--host', '--config-file'):
                    if index + 1 >= len(argv):
                        raise RuntimeError('Docker daemon arguments incomplete')
                    if arg == '--config-file':
                        config = Path(argv[index + 1])
                    else:
                        hosts.append(argv[index + 1])
                elif arg.startswith(('--host=', '-H')):
                    hosts.append(arg.split('=', 1)[1] if arg.startswith('--host=') else arg[2:])
                elif arg.startswith('--config-file='):
                    config = Path(arg.split('=', 1)[1])
            if config.exists():
                data = json.loads(config.read_text())
                configured = data.get('hosts', [])
                if not isinstance(configured, list) or any(not isinstance(h, str) for h in configured):
                    raise RuntimeError('Docker daemon host configuration malformed')
                hosts.extend(configured)
            if 'fd://' in hosts:
                service = subprocess.check_output(['systemctl', 'show', 'docker.service', '-p', 'MainPID', '--value'], text=True, timeout=5).strip()
                listening = subprocess.check_output(['systemctl', 'show', 'docker.socket', '-p', 'Listen', '--value'], text=True, timeout=5).strip()
                if service != str(pid) or not re.fullmatch(r'/[^\n]+ \(Stream\)', listening):
                    raise RuntimeError('socket-activated Docker daemon binding unknown')
                hosts = [h for h in hosts if h != 'fd://'] + ['unix://' + listening[:-9]]
            hosts = hosts or ['unix:///var/run/docker.sock']
            hosts = [known.get(os.path.realpath(h[7:]), h) if h.startswith('unix://') else h for h in hosts]
            require(pid, name, hosts)
        elif name == 'limactl' and 'hostagent' in argv:
            if lima is None:
                lima = [json.loads(line) for line in subprocess.check_output(
                    ['limactl', 'list', '--json'], text=True, timeout=5).splitlines()]
            matches = [row for row in lima if row.get('hostAgentPID') == pid]
            if len(matches) != 1 or not isinstance(matches[0].get('config'), dict):
                raise RuntimeError('Lima hostagent forwarding configuration unknown')
            forwards = matches[0]['config'].get('portForwards')
            if forwards not in (None, []):
                # Do not guess whether a forwarding rule exposes a Docker API.
                raise RuntimeError('Lima hostagent forwarding requires endpoint reconciliation')
            proof.append(dict(pid=pid, kind=name, docker_forwarding=False,
                              configuration='positive limactl hostAgentPID/config observation'))
        elif name in ('com.docker.backend', 'vpnkit', 'colima', 'orbstack',
                      'rancher-desktop', 'buildkitd', 'gvproxy', 'docker-proxy') or name.startswith('dockerd-') or (name == 'rootlesskit' and
                      any('dockerd' in a for a in argv[1:])) or (name == 'podman' and
                      ('machine' in argv or 'service' in argv)) or (name == 'nerdctl' and
                      any(a in argv for a in ('daemon', 'serve'))):
            group = {pid}
            while True:
                children = {p for p, row in rows.items() if row[0] in group}
                if children <= group:
                    break
                group.update(children)
            if sys.platform != 'darwin':
                require(pid, name, [])  # Unknown runtime binding stays held.
            if name not in ('com.docker.backend', 'vpnkit'):
                require(pid, name, [])  # Other runtime bindings need positive support.
            # Docker Desktop's backend/service/fork processes expose one VM's
            # engine. Do not inspect unrelated UI processes in that ancestry.
            api_group = {p for p in group if Path(rows[p][1][0]).name in ('com.docker.backend', 'vpnkit')}
            sockets = native_socket_paths(api_group)
            matched = {known[os.path.realpath(p)] for p in sockets if os.path.realpath(p) in known}
            require(pid, name, matched, process_group=sorted(group))
            covered.update(api_group)
        elif name == 'containerd' and parent in rows and Path(rows[parent][1][0]).name == 'dockerd':
            # Its Docker parent is checked independently; containerd alone is
            # not a Docker API endpoint.
            proof.append(dict(pid=pid, kind=name, docker_parent=parent))
    return dict(process_count=len(rows), runtimes=proof,
                historical_launcher_observation='Orchestrator checked no DOCKER_* in launcher environment, 2026-11-09 about 19:00Z; gate entrypoints do not set DOCKER_HOST.',
                residual_risk='Historical remote/TCP or arbitrary local Unix-socket DOCKER_HOST is not reconstructable. A historical daemon that has since exited cannot be inventoried; accepted orchestrator legacy default, 2026-11-09.')


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
    if discovery is not None:
        discovery['daemon_process_inventory'] = daemon_process_inventory(endpoints)
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
