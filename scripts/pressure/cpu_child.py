"""Persist a child exit status even when the Go timeout alarm panics."""
import os
from pathlib import Path
import subprocess
import sys
p=Path(sys.argv[1]);result=subprocess.run(sys.argv[2:])
with p.open('x') as stream:
    stream.write(str(result.returncode)+'\n');stream.flush();os.fsync(stream.fileno())
raise SystemExit(result.returncode)
