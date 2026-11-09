#!/usr/bin/env python3
"""macOS process-path inventory, isolated by gate-budget's bounded supervisor."""
import ctypes as c
import errno
import os
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


def inventory(scratch, pids, all_fds=False):
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

    for pid in pids:
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
                continue
            entries = (Descriptor * (size // c.sizeof(Descriptor) + 1))()
            c.set_errno(0)
            read = lib.proc_pidinfo(pid, 1, 0, c.byref(entries), c.sizeof(entries))
            if read == 0 and c.get_errno() == 0:
                read = 0
            elif not checked(read):
                continue
            if read >= c.sizeof(entries) or read % c.sizeof(Descriptor):
                raise RuntimeError('descriptor inventory changed or is incomplete')
            descriptors = [entry.number for entry in entries[:read // c.sizeof(Descriptor)]
                           if entry.kind == 1]  # PROX_FDTYPE_VNODE
        for fd in descriptors:
            info = FdPath()
            if checked(lib.proc_pidfdinfo(pid, fd, 2, c.byref(info), c.sizeof(info)),
                       c.sizeof(info), fd=True):
                paths.append(info.vnode.path)
        if any(inside(path) for path in paths):
            print(f'p{pid}\nn{scratch}')


if __name__ == '__main__':
    arguments = sys.argv[1:]
    all_fds = arguments[:1] == ['--all-fds']
    if all_fds:
        arguments = arguments[1:]
    inventory(arguments[0], map(int, arguments[1:]), all_fds)
