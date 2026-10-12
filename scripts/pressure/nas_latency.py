"""Eight tiny durable generations, isolated from the local CPU fixture."""
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from kernel_reads import kernel_read
root=Path(sys.argv[1]);root.mkdir();samples=[]
for n in range(8):
    data=os.urandom(128);p=root/str(n);start=time.monotonic()
    with p.with_suffix('.tmp').open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
    p.with_suffix('.tmp').replace(p);fd=os.open(root,os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)
    assert kernel_read(p,'bytes')==data
    samples.append(dict(seconds=time.monotonic()-start,sha256=hashlib.sha256(data).hexdigest()))
Path(sys.argv[2]).write_text(json.dumps(dict(samples=samples,total_seconds=sum(s['seconds'] for s in samples)),indent=2))
