#!/usr/bin/env python3
"""Detached Mac transport for the approved Spark worker; no fleet service changes."""
import json
from kernel_reads import kernel_read, Vanished
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import time

ROOT='/home/kuna/myce-test-stage-ab-r8-20261011'
UNIT='myce-test-stage-ab-r8-spark.service'
UUID='GPU-90c3e7f2-22d3-5055-5f80-3ce0250c2830'
procs=[]
ports=[]
state=None

def remote(host,code,data=None):
    if "kernel_read" in code or "Vanished" in code or "disable_drill" in code:code="import sys; sys.path.insert(0,"+repr(ROOT+'/source/scripts/pressure')+"); from kernel_reads import kernel_read, Vanished\n"+code
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',host,
        'python3 -c '+shlex.quote(code)],input=json.dumps(data).encode() if data is not None else b'',
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25)
    if result.returncode:raise RuntimeError('remote '+host+' exit '+str(result.returncode))
    return json.loads(result.stdout) if result.stdout else None

def read_request():
    result=subprocess.run(['ssh','b70','sudo -n python3 -c '+shlex.quote(
        'import sys; sys.path.insert(0,"'+ROOT+'/source/scripts/pressure"); from kernel_reads import kernel_read; from pathlib import Path; import json; p=Path("'+ROOT+'/spark-request.json"); print(kernel_read(p) if p.exists() else "null")')],
        stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=15)
    if result.returncode:raise RuntimeError('B70 request unavailable')
    return json.loads(result.stdout)

def respond(data):
    remote('b70','from pathlib import Path; import sys; p=Path("'+ROOT+'/spark-response.json"); t=p.with_suffix(".tmp"); t.write_bytes(sys.stdin.buffer.read()); t.replace(p)',data)

def stop():
    global state
    if state is not None:
        code="""import subprocess
from disable_drill import empty
def show():
 r=subprocess.run(['systemctl','--user','show','"""+UNIT+"""','-p','ActiveState','-p','ControlGroup','-p','LoadState'],capture_output=True,text=True)
 d=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
 assert r.returncode==0 or d.get('LoadState')=='not-found','unreadable Spark unit'
 return d
group=show()['ControlGroup']
r=subprocess.run(['systemctl','--user','stop','"""+UNIT+"""'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
d=show();assert empty(group,d),'Spark test cgroup unconfirmed'
"""
        remote('spark',code)
    for process in procs:
        if process.poll() is None:process.terminate()
    for process in procs:
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:process.kill();process.wait()
    procs.clear();state=None

def start(request):
    global state
    stop()
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));local=sock.getsockname()[1]
    sparkport=remote('spark','import socket,json; s=socket.socket(); s.bind(("127.0.0.1",0)); print(json.dumps(s.getsockname()[1])); s.close()')
    procs.append(subprocess.Popen(['ssh','-N','-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=5',
        '-L',f'127.0.0.1:{local}:127.0.0.1:{request["port"]}','b70'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    procs.append(subprocess.Popen(['ssh','-N','-o','ExitOnForwardFailure=yes','-o','ServerAliveInterval=5',
        '-R',f'127.0.0.1:{sparkport}:127.0.0.1:{local}','spark'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
    time.sleep(1)
    if any(p.poll() is not None for p in procs):raise RuntimeError('loopback transport failed')
    request={**request,'sparkport':sparkport}
    ports.append(sparkport)
    code='''from pathlib import Path
import json,sys,subprocess,os
r=json.load(sys.stdin);assert Path('/home/kuna/nas-dev').is_mount();root=Path("'''+ROOT+'''")
cg=subprocess.check_output(['systemctl','--user','show','verso-loan-233-next-queue.service','-p','ControlGroup','--value'],text=True).strip()
assert cg and int(kernel_read(Path('/sys/fs/cgroup')/cg.lstrip('/')/'memory.current')) < 14*1024**3
assert int(next(line.split()[1] for line in kernel_read('/proc/meminfo').splitlines() if line.startswith('MemAvailable:'))) >= 40*1024**2
phase=root/'phases'/r['phase'];phase.mkdir(parents=True,exist_ok=True)
env=phase/'worker.env';env.touch(mode=0o600);os.chmod(env,0o600);env.write_text('MYCE_TOKEN='+r['token']+'\\n')
budget='38' if r['mode']=='full' else '40'
args=['systemd-run','--user','--unit='''+UNIT+'''','--collect','--property=MemoryMax=2G','--property=MemorySwapMax=0','--property=CPUQuota=200%','--property=TimeoutStopSec=20s','--property=RuntimeMaxSec=20min','--property=EnvironmentFile='+str(env),str(root/'myce'),'join','--queue','http://127.0.0.1:'+str(r['sparkport']),'--name','spark-pressure','--host-id','spark-pressure-host','--gpu','cuda','--device','''+UUID+'''','--memory-mode','unified','--memory-gb',budget,'--reserve-memory-gb','38','--reserve-host-memory-gb','38','--heartbeat','250ms','--state-dir',str(phase/'worker'),'--checkpoint-root','/home/kuna/nas-dev/Verso/experiments/exp-238-mycelium-stage-ab-r8-20261011/'+r['phase']]
result=subprocess.run(args,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
if result.returncode:raise SystemExit(result.returncode)
print(json.dumps({'started':True,'reservation_gib':38,'atlas_reserve_gib':24,'verso_reserve_gib':14,'budget_gib':int(budget)}))
'''
    state=request
    result=remote('spark',code,request)
    # Map both monotonic domains through the Mac; preserve original clocks and bound RTT.
    offsets={};uncertainty=0
    for host in ('b70','spark'):
        began=time.monotonic();value=remote(host,'import time,json; print(json.dumps(time.monotonic()))');ended=time.monotonic()
        offsets[host]=(began+ended)/2-value;uncertainty+=(ended-began)/2
    state['offset']=offsets['spark']-offsets['b70'];state['uncertainty']=uncertainty
    respond(dict(mode=request['mode'],phase=request['phase'],ok=True,**result))

def collect():
    request=state.copy()
    rows=remote('spark','from pathlib import Path; import json; p=Path("'+ROOT+'/phases/'+request['phase']+'/events.jsonl"); print(json.dumps([json.loads(r) for r in kernel_read(p).splitlines()] if p.exists() else []))')
    stop()
    for row in rows:
        row['original_at']=row['at'];row['original_boot']=row['boot'];row['at']+=request['offset'];row['boot']=request['boot']
        row['clock_uncertainty_seconds']=request['uncertainty']
    remote('b70','from pathlib import Path; import json,sys; rows=json.load(sys.stdin); p=Path("'+ROOT+'/phases/'+request['phase']+'/spark-events.json"); p.write_text(json.dumps(rows))',rows)
    respond(dict(mode='stop',phase=request['phase'],ok=True,events=len(rows),clock_uncertainty_seconds=request['uncertainty']))

def loan():
    return remote('spark','import subprocess,json; d={};\nfor k in ("ActiveState","MainPID","ControlGroup"):\n d[k]=subprocess.check_output(["systemctl","--user","show","verso-loan-233-next-queue.service","-p",k,"--value"],text=True).strip()\nprint(json.dumps(d))')

def cleanup_spark():
    return remote('spark','''from pathlib import Path
import json,sys,subprocess,socket,time,datetime
from urllib.parse import quote
ports=json.load(sys.stdin);occupied=[]
for port in ports:
 with socket.socket() as s:
  s.settimeout(.2)
  if s.connect_ex(('127.0.0.1',port))==0:occupied.append(port)
unit=subprocess.run(['systemctl','--user','is-active','myce-test-stage-ab-r8-spark.service'],capture_output=True,text=True).stdout.strip()
remaining=[]
for p in kernel_read('/proc','list'):
 if not p.name.isdigit():continue
 try:
  if kernel_read(p/'cgroup').strip().endswith('/myce-test-stage-ab-r8-spark.service'):remaining.append(int(p.name))
 except Vanished:pass
assert unit in ('inactive','unknown','failed') and not remaining and not occupied,'Spark cleanup unconfirmed'
root=Path('/home/kuna/myce-test-stage-ab-r8-20261011');trash=Path('/home/kuna/.local/share/Trash')
for sub in ('files','info'):(trash/sub).mkdir(parents=True,exist_ok=True)
dest=trash/'files'/('myce-test-stage-ab-r8-spark-'+str(time.time_ns()))
info=trash/'info'/(dest.name+'.trashinfo')
if root.exists():
 info.write_text('[Trash Info]\\nPath='+quote(str(root))+'\\nDeletionDate='+datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')+'\\n');root.rename(dest)
 assert dest.exists() and info.exists()
print(json.dumps(dict(unit_state=unit,remaining_pids=remaining,occupied_test_ports=occupied,recoverable_trash=str(dest))))
''',ports)

if __name__=='__main__':
    before=loan();assert before['ActiveState']=='active';last=None;error=None
    try:
        until=time.monotonic()+4*3600
        while time.monotonic()<until:
            complete=remote('b70','from pathlib import Path; import json; print(json.dumps(Path("'+ROOT+'/final-cleanup.json").exists()))')
            if complete:break
            source_ready=remote('b70','from pathlib import Path; import json; print(json.dumps(Path("'+ROOT+'/source/scripts/pressure/kernel_reads.py").exists()))')
            if not source_ready:
                time.sleep(1);continue
            request=read_request()
            if request and request['request_id']!=last:
                last=request['request_id']
                try:
                    if request['mode'] in ('active','full'):start(request)
                    elif request['mode']=='stop':collect()
                except Exception as e:
                    respond(dict(mode=request['mode'],phase=request['phase'],ok=False,error=type(e).__name__))
                    stop()
            if state:
                available=remote('spark','from pathlib import Path; import json; print(json.dumps(int(next(r.split()[1] for r in kernel_read("/proc/meminfo").splitlines() if r.startswith("MemAvailable:"))))')
                if available<38*1024**2:raise RuntimeError('Spark reserved headroom breached')
            complete=remote('b70','from pathlib import Path; import json; print(json.dumps(Path("'+ROOT+'/final-cleanup.json").exists()))')
            if complete:break
            time.sleep(1)
        else:raise TimeoutError('follow-on transport deadline')
    except Exception as e:error=type(e).__name__
    finally:
        stop();after=loan();cleanup=cleanup_spark()
        receipt=dict(error=error,loan_identity_unchanged=before==after,loan_before=before,loan_after=after,tunnels_stopped=True,cleanup=cleanup)
        remote('b70','from pathlib import Path; import json,sys; Path("'+ROOT+'/spark-cleanup.json").write_text(json.dumps(json.load(sys.stdin),indent=2))',receipt)
