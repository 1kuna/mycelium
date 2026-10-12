"""Small Docker CPU payload with exact placement and atomic retained-unit evidence."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import time
from kernel_reads import kernel_read

root=Path(os.environ['MYCE_CHECKPOINT_DIR']);events=Path('/evidence/events.jsonl')
job=os.environ['MYCE_JOB_ID'];attempt=int(os.environ['MYCE_ATTEMPT']);units=2000 if sys.argv[1]=='ignore' else 200
assert os.environ['MYCE_PLATFORM']=='xpu' and os.environ['MYCE_DEVICES']=='0'
assert os.environ['MYCE_MACHINE']=='b70-pressure-gpu1' and 'MYCE_TOKEN' not in os.environ
identity=os.environ['MYCE_ATTEMPT_ID'];assert os.environ['MYCE_DOCKER_LABEL']=='myce.attempt='+identity
contract=hashlib.sha256(b'docker-cpu-payload-v1').hexdigest()
p=root/'checkpoint.json';saved=json.loads(kernel_read(p,'text')) if p.exists() else None
n=saved['unit'] if saved else 0
if saved:assert saved['job']==job and saved['contract']==contract and saved['hash']==hashlib.sha256(f'{job}:{n}'.encode()).hexdigest()
stop=False

def signal_stop(*_):
    global stop
    if sys.argv[1]!='ignore':stop=True
signal.signal(signal.SIGTERM,signal_stop)

def emit(kind,**fields):
    with events.open('a') as f:
        fcntl.flock(f,fcntl.LOCK_EX)
        f.write(json.dumps(dict(kind=kind,job=job,attempt=attempt,at=time.monotonic(),
            boot=kernel_read(Path('/proc/sys/kernel/random/boot_id'),'text').strip(),**fields))+'\n');f.flush();os.fsync(f.fileno())

emit('start',card='docker-cpu',resumed=n,units=units,resumable=True,contract=contract,
     platform=os.environ['MYCE_PLATFORM'],machine=os.environ['MYCE_MACHINE'],devices=os.environ['MYCE_DEVICES'],identity=identity)
host=bytearray(16*1024**2)
for i in range(0,len(host),4096):host[i]=1
while n<units and not stop:
    time.sleep(.2);n+=1
    with p.with_suffix('.tmp').open('w') as f:
        json.dump(dict(job=job,unit=n,contract=contract,hash=hashlib.sha256(f'{job}:{n}'.encode()).hexdigest()),f);f.flush();os.fsync(f.fileno())
    p.with_suffix('.tmp').replace(p)
    fd=os.open(root,os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)
    emit('commit',unit=n)
emit('finish',unit=n,complete=n==units)
