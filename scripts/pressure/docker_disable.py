"""Empty-user-cgroup plus labeled-Docker disable, without loading the XPU runtime."""
import json
import os
from pathlib import Path
import subprocess
import time
from stage_ab import ROOT, atomic
from disable_drill import user, show, empty, UNIT
from pressure import api
from kernel_reads import kernel_read


def install_worker(run):
    assert not UNIT.exists() and not UNIT.with_name(UNIT.name+'.d').exists()
    run.halt('worker')
    env=run.phase/'user-worker.env'
    with env.open('x') as f:
        os.chmod(env,0o600)
        for k in ('MYCE_TOKEN','PATH','MYCE_TEST_DOCKER_PHASE','DOCKER_HOST'):
            value=run.env[k];assert '\n' not in value
            f.write(k+'='+value+'\n')
    os.chown(env,1000,1000)
    import shlex
    args=run.wargs
    UNIT.write_text('[Unit]\nConditionPathExists=!'+str(run.phase/'host-state/DISABLED')+'\n'
        '[Service]\nType=simple\nKillMode=control-group\nSendSIGKILL=yes\nTimeoutStopSec=10s\n'
        'MemoryMax=512M\nMemorySwapMax=0\nCPUQuota=100%\nRuntimeMaxSec=10min\n'
        'EnvironmentFile='+str(env)+'\nExecStart='+shlex.join(args)+'\n')
    os.chown(UNIT,1000,1000)
    user(run,['systemctl','--user','daemon-reload'])
    user(run,['systemctl','--user','start','myce-worker.service'])
    run.wait(lambda:show(run)['MainPID']!='0',10)
    group=show(run)['ControlGroup'];assert group.endswith('/myce-worker.service')
    run.worker_cgroup='0::'+group
    run.wait(lambda:next((m for m in api(run.url)['machines'] if m['id']=='b70-pressure-gpu1' and not m['gone']),None),15)


def drill(run,job):
    from docker_drill import current
    group=show(run)['ControlGroup'];start=time.monotonic()
    result=user(run,[str(ROOT/'myce'),'disable','--host-state-dir',str(run.phase/'host-state'),
                     '--now','--reason','r9 synthetic Docker cleanup','--timeout','25s'],check=False)
    state=show(run)
    flag=json.loads(kernel_read(run.phase / 'host-state/DISABLED','text'))
    receipt=json.loads(kernel_read(run.phase / 'host-state/disable-receipt.json','text'))
    evidence=dict(disable_exit=result.returncode,seconds=time.monotonic()-start,
        empty_cgroup=empty(group,state),generation=flag['generation'],receipt_generation=receipt['generation'],
        no_labeled_execution=not any(r['State']['Running'] for r in current(run)),
        cli_confirmation='Host cleanup confirmed: empty systemd worker cgroup and Mycelium Docker execution gone' in result.stdout)
    atomic(run.phase/'disable.json',evidence)
    assert result.returncode==0 and evidence['seconds']<=25
    assert evidence['empty_cgroup'] and evidence['no_labeled_execution'] and evidence['cli_confirmation']
    assert receipt['generation']==flag['generation']
    user(run,['systemctl','--user','start','myce-worker.service'],check=False)
    assert show(run)['MainPID']=='0' and show(run)['ConditionResult']=='no'
    return evidence


def cleanup(run):
    if not UNIT.exists():return
    assert str(ROOT/'myce') in kernel_read(UNIT,'text') and 'Docker-disable' in kernel_read(UNIT,'text'),'user unit ownership changed'
    state=show(run);group=state['ControlGroup'] or (run.worker_cgroup or '0::').split('::',1)[-1]
    user(run,['systemctl','--user','stop','myce-worker.service'])
    assert empty(group,show(run)),'user worker cgroup unconfirmed'
    # Remove the test unit first, so enable cannot relaunch it.
    dest=ROOT/'docker-worker-unit.retired';assert not dest.exists();UNIT.rename(dest)
    user(run,['systemctl','--user','daemon-reload'])
    if (run.phase/'host-state/DISABLED').exists():
        flag=json.loads(kernel_read(run.phase/'host-state/DISABLED'))
        receipt=run.phase/'host-state/disable-receipt.json'
        assert receipt.exists() and json.loads(kernel_read(receipt))['generation']==flag['generation'],'disable custody lacks confirmation; keep flag'
        result=user(run,[str(ROOT/'myce'),'enable','--host-state-dir',str(run.phase/'host-state')],check=False)
        assert result.returncode==0 or not (run.phase/'host-state/DISABLED').exists(),'enable did not clear acknowledged flag'
    assert not (run.phase/'host-state/DISABLED').exists() and not UNIT.exists()
    run.worker_cgroup=None
