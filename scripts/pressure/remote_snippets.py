"""Validate the exact generated source locally before any SSH dispatch."""
from pathlib import Path
import py_compile
import shlex
import subprocess
import tempfile


def checked_python(source):
    with tempfile.TemporaryDirectory(prefix='myce-snippet-') as folder:
        path = Path(folder)/'snippet.py'
        path.write_text(source)
        py_compile.compile(str(path), doraise=True)
    return source


def checked_shell(source):
    result = subprocess.run(['bash', '-n'], input=source, text=True, capture_output=True)
    if result.returncode:
        raise ValueError('generated remote shell syntax invalid')
    return source


def python_command(source, sudo=False):
    return checked_shell(('sudo -n ' if sudo else '')+'python3 -c '+shlex.quote(checked_python(source)))
