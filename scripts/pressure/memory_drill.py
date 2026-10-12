"""Real kernel OOM in an isolated CPU worker; no device access."""
import json
import os
from pathlib import Path
import subprocess
import time
from stage_ab import ROOT, atomic
from disable_drill import user, empty
from pressure import api

UNIT='myce-worker@myce-test-r11-ceiling.service'


def scenario(run):
    run.begin('Memory-ceiling-isolation')
    run.halt('worker');run.release_owned()
    cp=run.phase/'local-checkpoints';cp.mkdir();os.chown(cp,1000,1000)
    assert subprocess.check_output(['findmnt','-n','-T',str(cp),'-o','FSTYPE'],text=True).strip()=='ext4'
    args=[str(ROOT/'myce'),'join','--queue',run.url,'--name','ceiling-cpu','--host-id','ceiling-cpu-host',
          '--gpu','fake','--memory-gb','4','--host-memory-budget-gb','1','--memory-ceiling-slack-gb','0',
          '--memory-swap-max-bytes','0','--reserve-host-memory-gb','18','--heartbeat','250ms',
          '--state-dir',str(run.phase/'ceiling-worker'),'--checkpoint-root',str(cp),
          '--host-state-dir',str(run.phase/'ceiling-control')]
    env=run.phase/'ceiling.env';env.write_text('MYCE_TOKEN='+run.env['MYCE_TOKEN']+'\n');os.chmod(env,0o600);os.chown(env,1000,1000)
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r11.service','MemoryMax=7680M'],check=True,capture_output=True)
    group=''
    run.active=None
    def show():
        r=user(run,['systemctl','--user','show',UNIT,'-p','ActiveState','-p','ControlGroup','-p','MainPID'])
        return dict(v.split('=',1) for v in r.stdout.splitlines() if '=' in v)
    assert not user(run,['systemctl','--user','is-active',UNIT],check=False).stdout.strip()=='active'
    assert not Path('/home/kuna/.config/systemd/user/'+UNIT).exists()
    atomic(ROOT/'memory-unit.json',dict(unit=UNIT))
    try:
        user(run,['systemd-run','--user','--unit='+UNIT,'--collect','--property=Delegate=memory',
             '--property=MemoryMax=512M','--property=MemorySwapMax=0','--property=CPUQuota=100%',
             '--property=KillMode=control-group','--property=RuntimeMaxSec=2min',
             '--property=EnvironmentFile='+str(env),'--property=StandardOutput=append:'+str(run.phase/'worker.log'),*args])
        group=show()['ControlGroup']
        machine=run.wait(lambda:next((m for m in api(run.url)['machines'] if m['id']=='ceiling-cpu'),None),15)
        assert machine['memory_enforcement']=='memory ceilings enforced (cgroup v2)'
        def submit(name,command,host):
            return run.submit_spec(dict(name=name,project='verso-pressure',priority='experiment',
                run=dict(command=['/usr/bin/python3','-c',command]),needs=dict(memory_gb=.01,host_memory_gb=host,platform={'required':['fake']}),
                allowed_machines=['ceiling-cpu'],resumable=False))['id']
        survivor=submit('ceiling-survivor','import time; time.sleep(10)',.0625)
        run.wait(lambda:any(j['id']==survivor and j['state']=='running' for j in api(run.url)['jobs']),15)
        victim=submit('ceiling-oom','x=bytearray(128*1024*1024)',.03125)
        def completed():
            states={j['id']:j for j in api(run.url)['jobs'] if j['id'] in (victim,survivor)}
            return states if len(states)==2 and all(j['state'] in ('failed','succeeded') for j in states.values()) else None
        witness=run.wait(lambda:next((j for j in api(run.url)['jobs'] if j['id']==victim and j['state']=='failed'),None),15)
        other=next(j for j in api(run.url)['jobs'] if j['id']==survivor)
        assert other['state']=='running', 'neighbor did not survive OOM concurrently'
        states=run.wait(completed,45)
        assert states[victim]['state']=='failed' and 'exceeded its 0.03125 GiB memory ceiling' in states[victim]['reason']
        assert states[victim]['losses']==0 and states[victim]['attempt']==1
        assert states[survivor]['state']=='succeeded' and states[survivor]['attempt']==1
        atomic(run.phase/'memory-ceiling.json',dict(victim=victim,survivor=survivor,states=states,enforcement=machine['memory_enforcement']))
        run.result('Memory-ceiling-isolation','PASS',victim=victim,survivor=survivor,ceiling_bytes=32*1024**2,other_attempt_unaffected=True)
    except Exception as exc:
        run.result('Memory-ceiling-isolation','FAIL',error=type(exc).__name__)
    finally:
        user(run,['systemctl','--user','stop',UNIT]);state=show()
        assert empty(group,state),'memory worker cleanup unconfirmed'
        run.halt('queue');run.scenario_cleanup()
        subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r11.service','MemoryMax=8G'],check=True,capture_output=True)
        atomic(run.phase/'memory-cleanup.json',dict(unit=UNIT,empty=True,cgroup=group))
