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

root=Path('/home/user/synthetic-run')
sys.path.insert(0,str(root/'source/scripts/pressure'))
from kernel_reads import kernel_read, Vanished
from stage_ab import cgroup_pids
from pressure import DiagnosticReaders, drm_owners
receipt=dict(when=datetime.datetime.now(datetime.timezone.utc).isoformat(),unit='myce-test-stage-ab-r9.service')
try:
    from types import SimpleNamespace
    from disable_drill import cleanup as installed_cleanup, UNIT, DROP
    if UNIT.exists() and 'Docker-disable' in kernel_read(UNIT):
        from docker_disable import cleanup as docker_user_cleanup
        run=SimpleNamespace(env=dict(os.environ),worker_cgroup=None,phase=root/'phases/Docker-disable')
        docker_user_cleanup(run)
    if UNIT.exists() or DROP.exists():
        assert DROP.exists() and str(root) in kernel_read(DROP, 'text'), 'test unit ownership changed'
        run=SimpleNamespace(installed=True,env=dict(os.environ),worker_cgroup=None)
        installed_cleanup(run)
    receipt['installed_worker']='flag and unit absent' if not UNIT.exists() and not (Path('/home/user/.local/share/myce')/'DISABLED').exists() else 'HOLD'
except Exception as error:
    receipt['installed_worker']='HOLD:'+type(error).__name__
try:
    cpu_units=json.loads(kernel_read(root/'cpu-units.json')) if (root/'cpu-units.json').exists() else []
    allowed={'myce-test-r9-cpu-fast-matrix.service','myce-test-r9-cpu-large-local-checkpoint.service','myce-test-r9-nas-small-latency-probe.service'}
    for unit in cpu_units:
        assert unit in allowed,'unexpected CPU unit'
        state=subprocess.run(['systemctl','show',unit,'-p','ActiveState','-p','LoadState','-p','ControlGroup'],capture_output=True,text=True)
        d=dict(v.split('=',1) for v in state.stdout.splitlines() if '=' in v)
        if d.get('LoadState')!='not-found':
            subprocess.run(['systemctl','stop',unit],check=True,capture_output=True,timeout=15)
        assert not cgroup_pids('0::/system.slice/'+unit),'CPU test processes remain'
        subprocess.run(['systemctl','reset-failed',unit],capture_output=True)
    receipt['cpu_units_settled']=True
except Exception as e:receipt['cpu_cleanup_error']=type(e).__name__
try:
    baseline=json.loads(kernel_read(root/'display-baseline.json', 'text'))
    evidence={}
    readers=DiagnosticReaders(json.loads(kernel_read(root/'diagnostic-executables.json', 'text')),root/'diagnostics.jsonl')
    end=time.monotonic()+60
    while True:
        owners=drm_owners(['0000:07:00.0'],baseline=baseline,allowed=[],diagnostics=readers,evidence=evidence)
        if not readers.pending:break
        if time.monotonic()>=end:raise RuntimeError('diagnostic exit unconfirmed')
        time.sleep(2)
    receipt.update(owners=owners,display=evidence['baseline_clients'],physical='released')
except Exception as e:
    receipt.update(physical='HOLD',error=type(e).__name__)
try:
    stats=json.loads(subprocess.check_output(['xpu-smi','stats','-d','0','--json'],timeout=3))
    assert stats['device_id']==0
    metrics={v['metrics_type']:v.get('value') for v in stats['device_level']}
    receipt['device_memory_mib']=float(metrics['XPUM_STATS_MEMORY_USED'])
    receipt['baseline_memory_mib']=float(os.environ["MYCE_DIAGNOSTIC_BASELINE_MIB"])
    receipt['release_tolerance_mib']=16
    if receipt['device_memory_mib']>float(os.environ["MYCE_DIAGNOSTIC_BASELINE_MIB"])+16:receipt['physical']='HOLD-driver-memory'
except Exception as e:
    receipt.update(physical='HOLD-driver-counter',counter_error=type(e).__name__)
try:
    for p in (root/'phases').glob('*/late-unit'):
        unit=kernel_read(p).strip();assert unit.startswith('myce-test-r9-late-')
        prefix=['/usr/sbin/runuser','-u','user','--','env','XDG_RUNTIME_DIR=/run/user/1000','systemctl','--user']
        subprocess.run([*prefix,'stop',unit],check=True,capture_output=True)
        state=subprocess.run([*prefix,'show',unit,'-p','ActiveState','-p','ControlGroup'],capture_output=True,text=True)
        d=dict(v.split('=',1) for v in state.stdout.splitlines() if '=' in v)
        assert d.get('ActiveState') in ('inactive','failed') and not d.get('ControlGroup'),'late helper not fenced'
    ids=subprocess.check_output(['/usr/bin/docker','ps','-aq','--filter','label=myce.attempt'],timeout=3).decode().split()
    rows=json.loads(subprocess.check_output(['/usr/bin/docker','inspect',*ids],timeout=3)) if ids else []
    identities={json.loads(kernel_read(p,'text'))['identity'] for p in (root/'phases').glob('*/worker/*.docker.json')}
    assert all(r['Config']['Labels'].get('myce.attempt') in identities for r in rows),'foreign Docker custody'
    for r in rows:
        if r['State']['Running']:subprocess.run(['/usr/bin/docker','kill',r['Id']],check=True,capture_output=True,timeout=3)
    subprocess.run(['systemctl','stop','myce-test-r9-docker.slice'],check=True,capture_output=True)
    rows=json.loads(subprocess.check_output(['/usr/bin/docker','inspect',*ids],timeout=3)) if ids else []
    assert not any(r['State']['Running'] or r['State']['Pid'] for r in rows),'Docker execution not gone'
    receipt['docker_running']=0
except Exception as e:receipt['docker_error']=type(e).__name__
pause=Path('/home/user/exp42-queue/PAUSED')
owned=root/'owned-pause'
headroom=int(next(r.split()[1] for r in kernel_read('/proc/meminfo').splitlines() if r.startswith('MemAvailable:')))/1024**2
receipt['headroom_gib']=headroom
if receipt['physical']=='released' and not receipt.get('cpu_cleanup_error') and not receipt.get('docker_error') and headroom>=18 and owned.exists() and pause.exists() and kernel_read(pause,'bytes')==kernel_read(owned,'bytes'):
    pause.rename(root/'queue-PAUSED.released')
receipt['queue_pause']='released' if owned.exists() and not pause.exists() else 'HOLD; not changed'
# At ExecStopPost the kernel has stopped the unit's child processes. Read-only verify it.
remaining=cgroup_pids('0::/system.slice/myce-test-stage-ab-r9.service')
receipt['remaining_pids']=remaining
ports=json.loads(kernel_read(root/'ports.json', 'text')) if (root/'ports.json').exists() else []
for path in (root/'phases').glob('*/measure-*.pid.json'):
    data=json.loads(kernel_read(path, 'text'))
    if 'port' in data:ports.append(data['port'])
occupied=[]
for port in set(ports):
    with socket.socket() as connection:
        connection.settimeout(.2)
        if connection.connect_ex(('127.0.0.1',port))==0:occupied.append(port)
receipt['occupied_test_ports']=occupied
trash=Path('/home/user/.local/share/Trash')
for child in ('files','info'):(trash/child).mkdir(parents=True,exist_ok=True)
moved=[]
for name in ('source','myce','cpu-matrix.test','cpu-temp','docker-bin'):
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

if receipt['physical']!='released' or remaining or occupied or receipt['installed_worker']!='flag and unit absent' or receipt.get('cpu_cleanup_error') or receipt.get('docker_error') or receipt['queue_pause']!='released' or headroom<18:
    if (root/'DONE').exists():(root/'DONE').rename(root/'DONE.invalid-release')
    (root/'FAILED').write_text(json.dumps(dict(reason='final release not qualified',cleanup=receipt),indent=2))
