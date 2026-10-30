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


def inventory(scratch, pids):
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
            gone.update({errno.EBADF, errno.EINVAL, errno.ENOTSUP})
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
        for fd in range(3):
            info = FdPath()
            if checked(lib.proc_pidfdinfo(pid, fd, 2, c.byref(info), c.sizeof(info)),
                       c.sizeof(info), fd=True):
                paths.append(info.vnode.path)
        if any(inside(path) for path in paths):
            print(f'p{pid}\nn{scratch}')


if __name__ == '__main__':
    inventory(sys.argv[1], map(int, sys.argv[2:]))
