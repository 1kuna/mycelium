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
import importlib.util
import errno


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


def guest_proc_read(pid, command, path, guest, evidence):
    if (not guest or not sys.platform.startswith('linux') or
            command not in ('cat', 'readlink') or
            path != '/proc/' + str(pid) + ('/stat' if command == 'cat' else '/exe')):
        raise RuntimeError('guest executable attestation is unavailable')
    row = dict(pid=pid, operation=command, guest_sudo_noninteractive=True, status='requested')
    evidence.append(row)
    try:
        result = subprocess.check_output(['sudo', '-n', command, path], text=True, timeout=5).strip()
        row['status'] = 'verified'
        return result
    except Exception as error:
        row['status'] = 'failed'
        error.guest_attestation_failed = True
        raise


def process_birth(pid, guest=False, evidence=None):
    evidence = evidence if evidence is not None else []
    if sys.platform.startswith('linux'):
        path = '/proc/' + str(pid) + '/stat'
        try:
            text = Path(path).read_text()
        except PermissionError:
            text = guest_proc_read(pid, 'cat', path, guest, evidence)
        fields = text[text.rfind(')') + 2:].split()
        if not text.startswith(str(pid) + ' (') or len(fields) < 20:
            raise RuntimeError('process birth identity is malformed')
        return dict(parent=int(fields[1]), birth=int(fields[19]))  # field22.
    if sys.platform == 'darwin':
        import ctypes as c
        lib = c.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        lib.proc_pidinfo.argtypes = [c.c_int, c.c_int, c.c_uint64, c.c_void_p, c.c_int]
        # proc_bsdinfo ABI: pbi_pid/ppid at12/16, start sec/usec at120/128.
        info = c.create_string_buffer(136)
        if lib.proc_pidinfo(pid, 3, 0, info, 136) != 136:
            error = c.get_errno()
            if error != errno.EPERM:
                raise OSError(error, 'process birth identity unavailable')
            # KERN_PROC_PID exports the same microsecond birth time to ps
            # without libproc's cross-UID restriction. Public 64-bit
            # kinfo_proc ABI: sizeof=648, timeval=0, pid=40, e_ppid=560.
            if c.sizeof(c.c_void_p) != 8:
                raise RuntimeError('process birth sysctl ABI unsupported')
            system = c.CDLL('/usr/lib/libSystem.B.dylib', use_errno=True)
            system.sysctl.argtypes = [c.POINTER(c.c_int), c.c_uint, c.c_void_p,
                                      c.POINTER(c.c_size_t), c.c_void_p, c.c_size_t]
            mib = (c.c_int * 4)(1, 14, 1, pid)  # CTL_KERN/KERN_PROC/PID.
            record, size = c.create_string_buffer(648), c.c_size_t(648)
            if system.sysctl(mib, 4, record, c.byref(size), None, 0) != 0:
                raise OSError(c.get_errno(), 'process birth sysctl unavailable')
            if size.value == 0:
                raise ProcessLookupError(errno.ESRCH, 'process absent from sysctl')
            if size.value != 648 or int.from_bytes(record.raw[40:44], sys.byteorder) != pid:
                raise RuntimeError('process birth sysctl record malformed')
            evidence.append(dict(pid=pid, birth_source='unprivileged KERN_PROC_PID'))
            return dict(parent=int.from_bytes(record.raw[560:564], sys.byteorder),
                        birth=[int.from_bytes(record.raw[:8], sys.byteorder),
                               int.from_bytes(record.raw[8:12], sys.byteorder)])
        number = lambda offset, size: int.from_bytes(info.raw[offset:offset + size], sys.byteorder)
        if number(12, 4) != pid:
            raise RuntimeError('process birth PID changed')
        return dict(parent=number(16, 4), birth=[number(120, 8), number(128, 8)])
    raise RuntimeError('process birth identity unsupported')


def process_arguments(pid):
    if sys.platform.startswith('linux'):
        try:
            # ps prints a display string, not shell-escaped argv. In
            # particular a Python -c argument can contain unmatched quotes.
            # procfs preserves the actual argument boundaries.
            parent = process_birth(pid)['parent']
            raw = Path('/proc/' + str(pid) + '/cmdline').read_bytes()
            if raw and not raw.endswith(b'\0'):
                raise RuntimeError('process argument inventory malformed')
            return parent, [os.fsdecode(arg) for arg in raw.split(b'\0')[:-1]]
        except FileNotFoundError:
            absence = subprocess.run(['ps', '-p', str(pid), '-o', 'pid='],
                                     capture_output=True, text=True, timeout=5)
            if absence.returncode == 1 and not absence.stdout.strip():
                return None
            raise RuntimeError('live process argument inventory unavailable')
    result = subprocess.run(['ps', '-ww', '-p', str(pid), '-o', 'ppid=,args='],
                            capture_output=True, text=True, timeout=5)
    if result.returncode == 1 and not result.stdout.strip():
        return None
    lines = result.stdout.strip().splitlines()
    if result.returncode or len(lines) != 1:
        raise RuntimeError('fresh process argument inventory incomplete')
    fields = lines[0].split(None, 1)
    if len(fields) != 2:
        raise RuntimeError('fresh process arguments malformed')
    return int(fields[0]), shlex.split(fields[1])


def observe_process(pid, guest, evidence):
    for attempt in range(3):
        try:
            before = process_birth(pid, guest, evidence)
            parent = process_birth(before['parent'], guest, evidence) if before['parent'] else None
            args = process_arguments(pid)
            if args is None:
                evidence.append(dict(pid=pid, excluded='positive PID absence on re-observation'))
                return None
            executable = process_executable(pid, guest, evidence)
            after = process_birth(pid, guest, evidence)
            parent_after = process_birth(before['parent'], guest, evidence) if before['parent'] else None
            if before == after and parent == parent_after and args[0] == before['parent']:
                return (args[0], args[1], executable, before['birth'], parent['birth'] if parent else None)
            evidence.append(dict(pid=pid, reobserve=attempt + 1, reason='process or parent birth changed'))
        except OSError as error:
            if error.errno not in (errno.ESRCH, errno.ENOENT) or getattr(error, 'guest_attestation_failed', False):
                raise
            # A vanished PID gets a fresh observation, never stale argv.
            if process_arguments(pid) is None:
                evidence.append(dict(pid=pid, excluded='positive PID absence on re-observation'))
                return None
            evidence.append(dict(pid=pid, reobserve=attempt + 1, reason='process changed during observation'))
    raise RuntimeError('process birth identity remained unstable after three observations')


def process_executable(pid, guest=False, evidence=None):
    """Kernel executable identity, never argv[0] (which can be an alias)."""
    evidence = evidence if evidence is not None else []
    try:
        if sys.platform.startswith('linux'):
            path = '/proc/' + str(pid)
            try:
                record = Path(path + '/stat').read_text()
            except PermissionError:
                record = guest_proc_read(pid, 'cat', path + '/stat', guest, evidence)
            fields = record[record.rfind(')') + 2:].split()
            if not record.startswith(str(pid) + ' (') or len(fields) < 7:
                raise RuntimeError('process stat identity is malformed')
            flags = int(fields[6])  # /proc/PID/stat field9, after comm/state.
            if flags & 0x00200000:  # PF_KTHREAD; missing exe alone proves nothing.
                evidence.append(dict(pid=pid, excluded='positive PF_KTHREAD', flags=flags))
                return None
            try:
                executable = os.readlink(path + '/exe')
            except PermissionError:
                executable = guest_proc_read(pid, 'readlink', path + '/exe', guest, evidence)
            if executable.endswith(' (deleted)'):
                executable = executable[:-10]
        elif sys.platform == 'darwin':
            import ctypes as c
            lib = c.CDLL('/usr/lib/libproc.dylib', use_errno=True)
            lib.proc_pidpath.argtypes = [c.c_int, c.c_void_p, c.c_uint32]
            buffer = c.create_string_buffer(4096)
            count = lib.proc_pidpath(pid, buffer, len(buffer))
            if count <= 0:
                error = c.get_errno()
                if error != errno.EPERM:
                    raise OSError(error, 'process executable identity unavailable')
                spec = importlib.util.spec_from_file_location('gate_handles', Path(__file__).with_name('gate-handles.py'))
                handles = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(handles)
                exclusion = handles.protected_process(pid, '/.reports/local-gates/', set())
                evidence.append(dict(excluded='reviewed EPERM sealed-system proof', **exclusion))
                executable = exclusion['executable']
            else:
                executable = os.fsdecode(buffer.value)
        else:
            raise RuntimeError('process executable identity is unsupported')
        if not executable.startswith('/') or '\n' in executable:
            raise RuntimeError('process executable identity is malformed')
        return os.path.realpath(executable)
    except Exception as error:
        if (isinstance(error, OSError) and error.errno in (errno.ESRCH, errno.ENOENT)
                and not getattr(error, 'guest_attestation_failed', False)):
            # ps itself and other short-lived processes can exit after the
            # whole-table snapshot. Only positive PID absence can omit them.
            check = subprocess.run(['ps', '-p', str(pid), '-o', 'pid='],
                                   capture_output=True, text=True, timeout=5)
            if check.returncode == 1 and not check.stdout.strip():
                evidence.append(dict(pid=pid, excluded='positive PID absence after snapshot'))
                return None
        evidence.append(dict(pid=pid, status='refused', reason=str(error)))
        error.runtime_evidence = dict(executable_observations=evidence)
        raise


def daemon_process_inventory(endpoints, guest=False):
    evidence = []
    try:
        return _daemon_process_inventory(endpoints, guest, evidence)
    except Exception as error:
        error.runtime_evidence = dict(executable_observations=evidence)
        raise


def _daemon_process_inventory(endpoints, guest, executable_evidence):
    """Legacy bound: every live API runtime needs an inventoried endpoint."""
    selection = '-eo' if sys.platform.startswith('linux') else '-axo'
    output = subprocess.check_output(['ps', '-ww', selection, 'pid=,ppid=,command='],
                                     text=True, timeout=5)
    rows = {}
    for line in output.splitlines():
        fields = line.split(None, 2)
        if len(fields) != 3:
            raise RuntimeError('Docker daemon process inventory incomplete')
        pid = int(fields[0])
        if pid <= 0 or pid in rows:
            raise RuntimeError('Docker process PID inventory malformed')
        observation = observe_process(pid, guest, executable_evidence)
        if observation is not None:
            rows[pid] = observation
    # A parent reused between different PID observations must not connect two
    # unrelated instances into one runtime group. Re-observe both ends, bounded.
    for attempt in range(4):
        mismatches = [pid for pid, row in rows.items() if row[0] and
                      (row[0] not in rows or rows[row[0]][3] != row[4])]
        if not mismatches:
            break
        if attempt == 3:
            raise RuntimeError('Docker parent birth linkage remained unstable')
        for pid in mismatches:
            if pid not in rows:
                continue
            for candidate in (pid, rows[pid][0]):
                observation = observe_process(candidate, guest, executable_evidence)
                if observation is None:
                    rows.pop(candidate, None)
                else:
                    rows[candidate] = observation
    known = {os.path.realpath(e[7:]): e for e in endpoints if e.startswith('unix://')}
    proof, covered, lima = [], set(), None
    def require(pid, kind, candidates, **extra):
        candidates = set(candidates)
        if not candidates or not candidates <= set(endpoints):
            raise RuntimeError('unresolved Docker-API daemon process: ' + str(pid) + ' (' + kind + ')')
        proof.append(dict(pid=pid, kind=kind, endpoints=sorted(candidates),
                          birth=rows[pid][3], parent_birth=rows[pid][4], **extra))
    for pid, (parent, argv, executable, birth, parent_birth) in rows.items():
        if not argv or executable is None or pid in covered:
            continue
        name = Path(executable).name.lower()
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
                      'rancher-desktop', 'rancher desktop', 'buildkitd', 'gvproxy', 'docker-proxy') or name.startswith('dockerd-') or (name == 'rootlesskit' and
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
            api_group = {p for p in group if rows[p][2] and Path(rows[p][2]).name in ('com.docker.backend', 'vpnkit')}
            sockets = native_socket_paths(api_group)
            matched = {known[os.path.realpath(p)] for p in sockets if os.path.realpath(p) in known}
            require(pid, name, matched, process_group=sorted(group))
            covered.update(api_group)
        elif name == 'containerd' and parent in rows and rows[parent][2] and Path(rows[parent][2]).name == 'dockerd':
            # Its Docker parent is checked independently; containerd alone is
            # not a Docker API endpoint.
            proof.append(dict(pid=pid, kind=name, docker_parent=parent))
    return dict(process_count=len(rows), runtimes=proof, executable_observations=executable_evidence,
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


def docker_inventory(marker, entry=None, used=(), guest=False):
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
        discovery['daemon_process_inventory'] = daemon_process_inventory(endpoints, guest)
    return proof


def runtime_cleanup(directory, marker, guest=False):
    directory = Path(directory)
    entry_path, used_path = directory / 'docker-entry.json', directory / 'docker-used.jsonl'
    if directory.is_symlink() or entry_path.is_symlink() or used_path.is_symlink():
        raise RuntimeError('linked runtime custody evidence')
    if directory.exists() and (not directory.is_dir() or not entry_path.is_file()):
        raise RuntimeError('runtime custody evidence is incomplete')
    entry = json.loads(entry_path.read_text()) if entry_path.exists() else None
    used = [json.loads(line) for line in used_path.read_text().splitlines()] if used_path.exists() else []
    return docker_inventory(marker, entry, used, guest)


def source_cleanup(source, fallback_marker, guest=False):
    source = Path(source)
    root = source / '.reports/gate-runtime'
    if root.is_symlink() or root.parent.is_symlink():
        raise RuntimeError('linked runtime custody evidence')
    if root.exists() and not root.is_dir():
        raise RuntimeError('runtime custody evidence is malformed')
    directories = list(root.iterdir()) if root.exists() else []
    proof = []
    for directory in directories or [root / fallback_marker]:
        proof.extend(runtime_cleanup(directory, directory.name, guest))
    return proof
