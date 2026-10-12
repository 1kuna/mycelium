"""Real kernel OOM in an isolated CPU worker; no device access."""
import json
import os
from pathlib import Path
import subprocess
import time
from stage_ab import ROOT, atomic, process, cgroup_pids
from disable_drill import empty
from pressure import api
from kernel_reads import kernel_read, Vanished

UNIT='myce-worker@myce-test-r14-ceiling.service'


def unit_state():
    r=subprocess.run(['systemctl','show',UNIT,'-p','ActiveState','-p','ControlGroup','-p','MainPID','-p','DevicePolicy','-p','LoadState'],
                     capture_output=True,text=True,timeout=15)
    state=dict(v.split('=',1) for v in r.stdout.splitlines() if '=' in v)
    assert r.returncode==0 or state.get('LoadState')=='not-found', 'memory worker state unreadable'
    return state


def cleanup_unit(group):
    state=unit_state()
    if state['LoadState']!='not-found':
        subprocess.run(['systemctl','stop',UNIT],check=True,capture_output=True,timeout=15)
    state=unit_state()
    assert empty(group,state), 'memory worker cleanup unconfirmed'
    return state


def scenario(run):
    run.begin('Memory-ceiling-isolation')
    run.halt('worker');run.release_owned()
    cp=run.phase/'local-checkpoints';cp.mkdir();os.chown(cp,1000,1000)
    run.halt('queue');run.cp=cp;run.start_queue()
    assert subprocess.check_output(['findmnt','-n','-T',str(cp),'-o','FSTYPE'],text=True).strip()=='ext4'
    args=[str(ROOT/'myce'),'join','--queue',run.url,'--name','ceiling-cpu','--host-id','ceiling-cpu-host',
          '--gpu','fake','--memory-gb','4','--host-memory-budget-gb','1','--memory-ceiling-slack-gb','0',
          '--memory-swap-max-bytes','0','--reserve-host-memory-gb','18','--heartbeat','250ms',
          '--state-dir',str(run.phase/'ceiling-worker'),'--checkpoint-root',str(cp),
          '--host-state-dir',str(run.phase/'ceiling-control')]
    env=run.phase/'ceiling.env';env.write_text('MYCE_TOKEN='+run.env['MYCE_TOKEN']+'\n');os.chmod(env,0o600);os.chown(env,1000,1000)
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r14.service','MemoryMax=7680M'],check=True,capture_output=True)
    group=''
    run.active=None
    def control(args):
        return subprocess.run(args,capture_output=True,text=True,timeout=35,check=True)
    assert unit_state()['ActiveState']!='active'
    assert not Path('/etc/systemd/system/'+UNIT).exists()
    atomic(ROOT/'memory-unit.json',dict(unit=UNIT))
    try:
        control(['systemd-run','--unit='+UNIT,'--collect','--property=Delegate=memory',
             '--property=User=1000','--property=Group=1000','--property=DevicePolicy=closed',
             '--property=MemoryMax=512M','--property=MemorySwapMax=0','--property=CPUQuota=100%',
             '--property=KillMode=control-group','--property=RuntimeMaxSec=2min',
             '--property=EnvironmentFile='+str(env),'--property=StandardOutput=append:'+str(run.phase/'worker.log'),*args])
        state=unit_state();group=state['ControlGroup'];assert state['DevicePolicy']=='closed'
        machine=run.wait(lambda:next((m for m in api(run.url)['machines'] if m['id']=='ceiling-cpu'),None),15)
        assert machine['memory_enforcement']=='memory ceilings enforced (cgroup v2)'
        sequential_jobs(run,group,state,machine)
    except Exception as exc:
        import traceback
        (run.phase/'memory-error.log').write_text(traceback.format_exc())
        run.result('Memory-ceiling-isolation','FAIL',error=dict(type=type(exc).__name__,detail=str(exc)))
    finally:
        cleanup_unit(group)
        run.halt('queue');run.scenario_cleanup()
        subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r14.service','MemoryMax=8G'],check=True,capture_output=True)
        atomic(run.phase/'memory-cleanup.json',dict(unit=UNIT,empty=True,cgroup=group))


def memory_snapshot(group):
    root=Path('/sys/fs/cgroup')/group.lstrip('/')
    groups={}
    for path in [root,*kernel_read(root,'directories')]:
        try:
            groups[path.name]={name:kernel_read(path/name).strip() for name in
                ('memory.max','memory.swap.max','memory.events','cgroup.procs','cgroup.events')}
        except Vanished:
            continue
    pids=cgroup_pids('0::'+group)
    return dict(groups=groups,pids=pids,membership=[process(pid) for pid in pids])


def sequential_jobs(run,group,state,machine):
    def submit(name,command,host):
        return run.submit_spec(dict(name=name,project='verso-pressure',priority='experiment',
            run=dict(command=['/usr/bin/python3','-c',command]),
            needs=dict(memory_gb=.01,host_memory_gb=host,platform={'required':['fake']}),
            allowed_machines=['ceiling-cpu'],resumable=False))['id']
    def job(ident):
        return next(j for j in api(run.url)['jobs'] if j['id']==ident)
    release=run.phase/'allocate'
    command=('from pathlib import Path; import time; p=Path('+repr(str(release))+'); '
             'end=time.monotonic()+20\nwhile not p.exists() and time.monotonic()<end: time.sleep(.05)\n'
             'assert p.exists(), "allocation witness timed out"\nx=bytearray(128*1024*1024)')
    evidence=dict(worker_before=state,enforcement=machine['memory_enforcement'],before=memory_snapshot(group))
    atomic(run.phase/'memory-ceiling.json',evidence)
    victim=submit('ceiling-oom',command,.03125)
    evidence['victim']=victim
    def live_attempt():
        snapshot=memory_snapshot(group)
        attempts=[v for k,v in snapshot['groups'].items() if k.startswith('attempt-') and v['cgroup.procs']]
        if not attempts:return None
        assert len(attempts)==1 and attempts[0]['memory.max']==str(32*1024**2)
        assert attempts[0]['memory.swap.max']=='0'
        return snapshot
    evidence['victim_live']=run.wait(live_attempt,15)
    atomic(run.phase/'memory-ceiling.json',evidence)
    release.touch()
    failed=run.wait(lambda:job(victim) if job(victim)['state']=='failed' else None,20)
    evidence.update(victim_result=failed,after_oom=memory_snapshot(group),worker_after_oom=unit_state())
    atomic(run.phase/'memory-ceiling.json',evidence)
    assert 'exceeded its 0.03125 GiB memory ceiling' in failed['reason']
    assert failed['losses']==0 and failed['attempt']==1
    parent=Path(group).name
    def oom_count(snapshot):
        return int(dict(row.split() for row in snapshot['groups'][parent]['memory.events'].splitlines()).get('oom_kill',0))
    assert oom_count(evidence['after_oom'])>oom_count(evidence['before']), 'no kernel OOM event witnessed'
    post=submit('ceiling-post-oom','import time; x=bytearray(1024*1024); time.sleep(2)',.0625)
    evidence['post_oom']=post
    evidence['post_live']=run.wait(live_post_snapshot(group),15)
    atomic(run.phase/'memory-ceiling.json',evidence)
    finished=run.wait(lambda:job(post) if job(post)['state']=='succeeded' else None,20)
    after=unit_state()
    evidence.update(post_result=finished,after=memory_snapshot(group),worker_after=after)
    atomic(run.phase/'memory-ceiling.json',evidence)
    assert finished['attempt']==1 and finished['losses']==0
    assert after['ActiveState']=='active' and after['MainPID']==state['MainPID'] and after['MainPID']!='0', 'OOM restarted or killed the worker'
    assert evidence['worker_after_oom']['MainPID']==state['MainPID']
    run.result('Memory-ceiling-isolation','PASS',victim=victim,post_oom=post,ceiling_bytes=32*1024**2,worker_continuity=True)


def live_post_snapshot(group):
    def read():
        snapshot=memory_snapshot(group)
        attempts=[v for k,v in snapshot['groups'].items() if k.startswith('attempt-') and v['cgroup.procs']]
        if not attempts:return None
        assert len(attempts)==1 and attempts[0]['memory.max']==str(64*1024**2) and attempts[0]['memory.swap.max']=='0'
        return snapshot
    return read
