#!/usr/bin/env python3
"""ExecStopPost: preserve evidence, release only our pause, trash disposable installs."""
import datetime
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.parse import quote

root=Path('/home/kuna/myce-test-stage-ab-r2c-20261011')
sys.path.insert(0,str(root/'source/scripts/pressure'))
from pressure import drm_owners
receipt=dict(when=datetime.datetime.now(datetime.timezone.utc).isoformat(),unit='myce-test-stage-ab-r2c.service')
try:
    baseline=json.loads((root/'display-baseline.json').read_text())
    evidence={}
    owners=drm_owners(['0000:07:00.0'],baseline=baseline,allowed=[],evidence=evidence)
    receipt.update(owners=owners,display=evidence['baseline_clients'],physical='released')
except Exception as e:
    receipt.update(physical='HOLD',error=type(e).__name__)
try:
    stats=json.loads(subprocess.check_output(['xpu-smi','stats','-d','0','--json'],timeout=3))
    assert stats['device_id']==0
    metrics={v['metrics_type']:v.get('value') for v in stats['device_level']}
    receipt['device_memory_mib']=float(metrics['XPUM_STATS_MEMORY_USED'])
    receipt['baseline_memory_mib']=48.140625
    receipt['release_tolerance_mib']=16
    if receipt['device_memory_mib']>48.140625+16:receipt['physical']='HOLD-driver-memory'
except Exception as e:
    receipt.update(physical='HOLD-driver-counter',counter_error=type(e).__name__)
pause=Path('/home/kuna/exp42-queue/PAUSED')
owned=root/'owned-pause'
if owned.exists() and pause.exists() and pause.read_bytes()==owned.read_bytes():
    pause.rename(root/'queue-PAUSED.released')
receipt['queue_pause']='released' if owned.exists() and not pause.exists() else 'not changed'
# At ExecStopPost the kernel has stopped the unit's child processes. Read-only verify it.
remaining=[]
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit() or int(proc.name)==os.getpid():continue
    try:
        if (proc/'cgroup').read_text().strip()=='0::/system.slice/myce-test-stage-ab-r2c.service':remaining.append(int(proc.name))
    except OSError:pass
receipt['remaining_pids']=remaining
ports=json.loads((root/'ports.json').read_text()) if (root/'ports.json').exists() else []
for path in (root/'phases').glob('*/measure-*.pid.json'):
    data=json.loads(path.read_text())
    if 'port' in data:ports.append(data['port'])
occupied=[]
for port in set(ports):
    with socket.socket() as connection:
        connection.settimeout(.2)
        if connection.connect_ex(('127.0.0.1',port))==0:occupied.append(port)
receipt['occupied_test_ports']=occupied
trash=Path('/home/kuna/.local/share/Trash')
for child in ('files','info'):(trash/child).mkdir(parents=True,exist_ok=True)
moved=[]
for name in ('source','myce','cpu-matrix.test','cpu-temp'):
    src=root/name
    if not src.exists():continue
    dest=trash/'files'/f'myce-test-r2-{name}-{time.time_ns()}'
    info=trash/'info'/(dest.name+'.trashinfo')
    info.write_text('[Trash Info]\nPath='+quote(str(src))+'\nDeletionDate='+datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')+'\n')
    os.chown(info,1000,1000)
    src.rename(dest)
    assert dest.exists() and info.exists()
    moved.append(str(dest))
receipt['recoverable_trash']=moved
for child in ('files','info'):
    os.chown(trash/child,1000,1000)
(root/'final-cleanup.json').write_text(json.dumps(receipt,indent=2))
# A hard stop must never be mistaken for a successful matrix.
if not (root/'DONE').exists() and not (root/'FAILED').exists():
    (root/'FAILED').write_text(json.dumps(dict(reason='runner exited without terminal receipt',cleanup=receipt),indent=2))

if receipt['physical']!='released' or remaining or occupied:
    if (root/'DONE').exists():(root/'DONE').rename(root/'DONE.invalid-release')
    (root/'FAILED').write_text(json.dumps(dict(reason='final release not qualified',cleanup=receipt),indent=2))
