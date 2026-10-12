"""One fail-closed reader for transient kernel files and ordinary harness evidence."""
import errno
import hashlib
import json
import os
from pathlib import Path
import sys
import time


class Vanished(FileNotFoundError):
    """The entry disappeared and a fresh parent listing confirmed its absence."""


class UnknownHolder(PermissionError):
    pass


def _read(path, operation):
    if operation=='text':return path.read_text()
    if operation=='bytes':return path.read_bytes()
    if operation=='link':return os.readlink(path)
    if operation in ('list','directories'):
        rows=[]
        with os.scandir(path) as entries:
            for entry in entries:
                if operation=='directories' and not entry.is_dir(follow_symlinks=False):continue
                if path.name in ('fd','fdinfo') and path.parent.name in ('self',str(os.getpid())):
                    # Inspect during iteration: scandir closes its fd on exhaustion.
                    try:target=kernel_read(path.parent/'fd'/entry.name,'link')
                    except Vanished:continue
                    if target in (f'/proc/{os.getpid()}/{path.name}',f'/proc/self/{path.name}'):continue
                rows.append(Path(entry.path))
        return rows
    if operation in ('fdinfo','hash'):
        with path.open('rb') as stream:
            if operation=='fdinfo':return stream.read(65537)
            sha=hashlib.sha256()
            for block in iter(lambda:stream.read(1024*1024),b''):sha.update(block)
            return sha.hexdigest()
    raise ValueError('unsupported read operation')


def _rescan(path):
    try:
        with os.scandir(path.parent) as entries:
            return path.name in {e.name for e in entries}
    except OSError as error:
        if error.errno in (errno.ENOENT,errno.ESRCH):return False
        raise


def kernel_read(path, operation='text'):
    path=Path(path)
    for attempt in range(3):
        try:return _read(path,operation)
        except OSError as error:
            status=('vanished' if error.errno in (errno.ENOENT,errno.ESRCH) else
                    'unknown-holder' if error.errno in (errno.EACCES,errno.EPERM) else 'error')
            # Paths/types only: never kernel contents, arguments, environments or credentials.
            print(json.dumps(dict(kind='kernel-read',status=status,path=str(path),
                                  operation=operation,errno=error.errno,at=time.monotonic())),file=sys.stderr)
            if status=='error':raise
            if status=='vanished':
                _rescan(path)
                raise Vanished(error.errno,'vanished; caller must rescan',str(path)) from error
            try:present=_rescan(path)
            except OSError as scan_error:
                print(json.dumps(dict(kind='kernel-read',status='rescan-error',path=str(path.parent),
                                      errno=scan_error.errno)),file=sys.stderr)
                raise
            if status=='unknown-holder':
                proc=next((Path(*path.parts[:i+1]) for i,v in enumerate(path.parts)
                           if v.isdigit() and 'proc' in path.parts[:i]),None)
                if proc is None or _rescan(proc):raise UnknownHolder(error.errno,'unknown live holder',str(path)) from error
            if not present:raise Vanished(error.errno,'vanished after rescan',str(path)) from error
            if status=='unknown-holder':raise UnknownHolder(error.errno,'unknown live holder',str(path)) from error
            if attempt==2:raise OSError(error.errno,'unresolved after three rescans',str(path)) from error
