#!/usr/bin/env python3
"""macOS process-path inventory, isolated by gate-budget's bounded supervisor."""
import ctypes as c
import errno
import json
import os
import subprocess
import sys


# Public ABI from sys/proc_info.h. Paths are kernel snapshots, not filesystem
# lookups; in particular do not resolve/stat them or enumerate mount points.
class VinfoStat(c.Structure):
    _fields_ = [('dev', c.c_uint32), ('mode', c.c_uint16), ('nlink', c.c_uint16),
                ('ino', c.c_uint64), ('uid', c.c_uint32), ('gid', c.c_uint32),
                ('times', c.c_int64 * 8), ('size', c.c_int64),
                ('blocks', c.c_int64), ('blksize', c.c_int32),
                ('flags', c.c_uint32), ('gen', c.c_uint32), ('rdev', c.c_uint32),
                ('spare', c.c_int64 * 2)]


class VnodeInfo(c.Structure):
    _fields_ = [('stat', VinfoStat), ('type', c.c_int), ('pad', c.c_int),
                ('fsid', c.c_int32 * 2)]


class VnodePath(c.Structure):
    _fields_ = [('info', VnodeInfo), ('path', c.c_char * 1024)]


class CwdInfo(c.Structure):
    _fields_ = [('cwd', VnodePath), ('root', VnodePath)]


class FileInfo(c.Structure):
    _fields_ = [('flags', c.c_uint32), ('status', c.c_uint32),
                ('offset', c.c_int64), ('type', c.c_int32), ('guard', c.c_uint32)]


class FdPath(c.Structure):
    _fields_ = [('file', FileInfo), ('vnode', VnodePath)]


class Descriptor(c.Structure):
    _fields_ = [('number', c.c_int32), ('kind', c.c_uint32)]


class RegionInfo(c.Structure):
    _fields_ = [('protection', c.c_uint32 * 4), ('offset', c.c_uint64),
                ('accounting', c.c_uint32 * 14), ('address', c.c_uint64),
                ('size', c.c_uint64)]


class RegionPath(c.Structure):
    _fields_ = [('region', RegionInfo), ('vnode', VnodePath)]


def process_table():
    output = subprocess.check_output(
        ['ps', 'eww', '-axo', 'pid=,ppid=,comm=,args='], text=True, timeout=5)
    rows = {}
    for line in output.splitlines():
        fields = line.split(None, 3)
        if len(fields) != 4:
            raise RuntimeError('protected process ancestry inventory incomplete')
        rows[int(fields[0])] = (int(fields[1]), fields[2], fields[3])
    return rows


def protected_process(pid, scratch, roots, rows=None):
    """An EPERM exclusion needs independent ps evidence, never a guess."""
    if rows is None:
        rows = process_table()
    if pid not in rows:
        raise RuntimeError('protected process identity changed')
    _, _, arguments = rows[pid]
    # comm can contain spaces; obtain this single value independently rather
    # than accepting a whitespace-truncated prefix from the ancestry table.
    executable = subprocess.check_output(['ps', '-p', str(pid), '-o', 'comm='],
                                         text=True, timeout=5).strip()
    if not executable.startswith('/') or '\n' in executable:
        raise RuntimeError('protected executable path is unavailable')
    executable = os.path.realpath(executable)
    prefixes = ('/System/', '/usr/libexec/', '/usr/bin/', '/usr/sbin/', '/sbin/', '/bin/')
    if not executable.startswith(prefixes):
        raise RuntimeError('EPERM process is not a protected system executable')
    # Include every known live gate supervisor as well as persisted identities.
    roots = set(roots)
    for candidate, (_, _, command) in rows.items():
        if any(name in command.split()[:3] or any(os.path.basename(t) == name for t in command.split()[:3])
               for name in ('gate-local.py', 'gate-budget.py', 'gate-measure.py', 'gate-linux.py',
                            'gate-guest.py', 'gate-scheduler.py', 'gate-progress.py', 'gate.sh',
                            'gate-leg.sh', 'gate-command.sh')):
            roots.add(candidate)
    chain, cursor = [], pid
    while cursor:
        if cursor in roots:
            raise RuntimeError('EPERM process descends from a gate supervisor')
        if cursor in chain or cursor not in rows:
            raise RuntimeError('protected process ancestry inventory incomplete')
        chain.append(cursor)
        cursor = rows[cursor][0]
    forbidden = ('GATE_RUN_ID=', 'GATE_CUSTODY_ID=', 'GATE_SCRATCH=', 'GATE_RUNTIME_DIR=',
                 '/mycelium-gates/', '/mycelium-gate-', '/.gate/', '/scripts/gate-',
                 '/scripts/gate.sh', scratch)
    if any(value in arguments for value in forbidden):
        raise RuntimeError('EPERM process has a gate marker or path')
    return dict(pid=pid, executable=executable, ancestry=chain,
                gate_markers_or_paths=False, reason='EPERM protected system process')


def inventory(scratch, pids, all_fds=False, protected_roots=None):
    lib = c.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    lib.proc_pidinfo.argtypes = [c.c_int, c.c_int, c.c_uint64, c.c_void_p, c.c_int]
    lib.proc_pidfdinfo.argtypes = [c.c_int, c.c_int, c.c_int, c.c_void_p, c.c_int]
    lib.proc_pidpath.argtypes = [c.c_int, c.c_void_p, c.c_uint32]

    def checked(result, expected=None, fd=False):
        if result > 0 and (expected is None or result == expected):
            return True
        error = c.get_errno()
        # Processes/descriptors may disappear between ps and this snapshot.
        # Non-vnode descriptors (pipes, sockets) have no file path to inspect.
        gone = {errno.ESRCH, errno.ENOENT}
        if fd:
            gone.add(errno.EBADF)
            if not all_fds:
                gone.update({errno.EINVAL, errno.ENOTSUP})
        if result <= 0 and error in gone:
            return False
        raise OSError(error, 'native process path inventory failed')

    def inside(path):
        name = os.fsdecode(path)
        return name == scratch or name.startswith(scratch + os.sep)

    exclusions_table = None
    for pid in pids:
        try:
            inventory_pid(lib, pid, scratch, all_fds, checked, inside)
        except PermissionError as error:
            if protected_roots is None or error.errno != errno.EPERM:
                raise
            if exclusions_table is None:
                exclusions_table = process_table()
            print('x' + json.dumps(protected_process(pid, scratch, protected_roots, exclusions_table)), flush=True)


def inventory_pid(lib, pid, scratch, all_fds, checked, inside):
    cwd = CwdInfo()
    paths = []
    if checked(lib.proc_pidinfo(pid, 9, 0, c.byref(cwd), c.sizeof(cwd)), c.sizeof(cwd)):
        paths.append(cwd.cwd.path)
    executable = c.create_string_buffer(4096)
    if checked(lib.proc_pidpath(pid, executable, len(executable))):
        paths.append(executable.value)
    descriptors = range(3)
    if all_fds:
        c.set_errno(0)
        size = lib.proc_pidinfo(pid, 1, 0, None, 0)  # PROC_PIDLISTFDS
        if size == 0 and c.get_errno() == 0:
            descriptors = []
        elif not checked(size):
            return
        entries = (Descriptor * (size // c.sizeof(Descriptor) + 1))()
        c.set_errno(0)
        read = lib.proc_pidinfo(pid, 1, 0, c.byref(entries), c.sizeof(entries))
        if read == 0 and c.get_errno() == 0:
            read = 0
        elif not checked(read):
            return
        if read >= c.sizeof(entries) or read % c.sizeof(Descriptor):
            raise RuntimeError('descriptor inventory changed or is incomplete')
        descriptors = [entry.number for entry in entries[:read // c.sizeof(Descriptor)]
                       if entry.kind == 1]  # PROX_FDTYPE_VNODE
    for fd in descriptors:
        info = FdPath()
        if checked(lib.proc_pidfdinfo(pid, fd, 2, c.byref(info), c.sizeof(info)),
                   c.sizeof(info), fd=True):
            paths.append(info.vnode.path)
    if all_fds:
        # PROC_PIDREGIONPATHINFO walks file-backed mappings even after a
        # fixture closes its descriptors and scrubs argv/environment.
        address = 0
        while True:
            region = RegionPath()
            c.set_errno(0)
            count = lib.proc_pidinfo(pid, 8, address, c.byref(region), c.sizeof(region))
            if count == 0 and (c.get_errno() == errno.ESRCH or
                               address > 0 and c.get_errno() == errno.EINVAL):
                break  # End of mapped address space, or a gone PID.
            checked(count, c.sizeof(region))
            paths.append(region.vnode.path)
            following = region.region.address + region.region.size
            if following <= address or following >= 1 << 64:
                raise RuntimeError('mapped-file inventory did not advance')
            address = following
    if any(inside(path) for path in paths):
        print(f'p{pid}\nn{scratch}')

if __name__ == '__main__':
    arguments = sys.argv[1:]
    all_fds = arguments[:1] == ['--all-fds']
    if all_fds:
        arguments = arguments[1:]
    roots = None
    if arguments[:1] == ['--protected-roots']:
        roots = [int(v) for v in arguments[1].split(',') if v]
        arguments = arguments[2:]
    inventory(arguments[0], map(int, arguments[1:]), all_fds, roots)
