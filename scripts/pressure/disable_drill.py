"""Installed-user-service host switch drill, confined to the disposable GPU1 worker."""
import json
from kernel_reads import kernel_read, Vanished
import os
from pathlib import Path
import subprocess
import time
from pressure import api

USER_HOME=Path('/home/user')
UNIT=USER_HOME/'.config/systemd/user/myce-worker.service'
GPU_UNIT=UNIT.with_name('myce-worker@gpu1.service')
DROP=GPU_UNIT.with_name(GPU_UNIT.name+'.d')/'myce-test-r11.conf'
from stage_ab import ROOT
CONTROL=ROOT/'installed-host-state'
SCOPED=CONTROL/'units/myce-worker@gpu1.service'


def user(run,args,check=True):
    result=subprocess.run(['/usr/sbin/runuser','-u','user','--',*args],
        env={**run.env,'XDG_RUNTIME_DIR':'/run/user/1000','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1000/bus'},
        capture_output=True,text=True,timeout=35)
    if check and result.returncode:raise RuntimeError('test user command failed: '+args[0]+', exit '+str(result.returncode))
    return result


def show(run):
    r=user(run,['systemctl','--user','show','myce-worker@gpu1.service' if getattr(run,'installed',False) else 'myce-worker.service','-p','ActiveState','-p','ControlGroup',
                '-p','MainPID','-p','ConditionResult','-p','KillMode','-p','SendSIGKILL'])
    return dict(v.split('=',1) for v in r.stdout.splitlines() if '=' in v)


def empty(group,state):
    if not group:return state['ActiveState'] in ('inactive','failed') and not state['ControlGroup']
    path=Path('/sys/fs/cgroup')/group.lstrip('/')
    try:
        return not kernel_read(path/'cgroup.procs').strip() and 'populated 0' in kernel_read(path/'cgroup.events')
    except Vanished:
        return state['ActiveState'] in ('inactive','failed') and not state['ControlGroup']


def drill(run):
    from stage_ab import ROOT
    evidence={}
    def record(step, **values):
        evidence.update(step=step,**values)
        (run.phase/'disable-drill.json').write_text(json.dumps(evidence,indent=2))
    record('preflight')
    assert not GPU_UNIT.exists() and not DROP.parent.exists() and not (SCOPED/'DISABLED').exists()
    # The installer starts its unit; a missing private readiness file gates that initial start.
    DROP.parent.mkdir(mode=0o700);os.chown(DROP.parent,1000,1000)
    DROP.write_text('[Unit]\nConditionPathExists='+str(ROOT/'installed-ready')+'\n[Service]\n'
        'ExecStart=\nExecStart=/usr/bin/sudo -n /usr/bin/python3 '+str(ROOT/'source/scripts/pressure/worker_fence.py')+' '+str(ROOT/'installation/worker-gpu1/config.json')+'\n'
        'MemoryMax=6G\nMemorySwapMax=0\nCPUQuota=200%\nRuntimeMaxSec=10min\n'
        'StandardOutput=append:'+str(ROOT/'logs/installed-worker.log')+'\nStandardError=append:'+str(ROOT/'logs/installed-worker.log')+'\n')
    os.chown(DROP,1000,1000)
    run.halt('worker');run.release_owned()
    # Combined enforced memory allowance stays 8GiB while the worker is in a separate user cgroup.
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r11b.service','MemoryMax=2G'],check=True)
    run.installed=True
    from installed_validation import install
    result=install(run,[str(ROOT/'myce'),'install','worker','--instance','gpu1','--root',str(ROOT/'installation'),
        '--nas',str(run.cp),'--queue',run.url,'--name','b70-pressure-gpu1','--gpu','xpu',
        '--memory-mode','fixed','--memory-gb','4','--device','0','--host-memory-budget-gb','6','--memory-ceiling-slack-gb','0','--memory-swap-max-bytes','0','--host-state-dir',str(CONTROL)])
    record('installed',install_exit=result.returncode,installed_condition=show(run)['ConditionResult'])
    assert result.returncode==0 and show(run)['ConditionResult']=='no'
    run.installed_config=ROOT/'installation/worker-gpu1/config.json'
    c=json.loads(kernel_read(run.installed_config, 'text'));c['checkpoint_root']=str(run.cp)
    run.installed_config.write_text(json.dumps(c));os.chown(run.installed_config,1000,1000)
    (ROOT/'installed-ready').touch()
    user(run,['systemctl','--user','start','myce-worker@gpu1.service'])
    run.wait(lambda:show(run)['MainPID']!='0',10)
    state=show(run);group=state['ControlGroup'];run.worker_cgroup='0::'+group
    assert group.endswith('/myce-worker@gpu1.service') and state['KillMode']=='control-group' and state['SendSIGKILL']=='yes'
    run.wait(lambda:any(m['id']=='b70-pressure-gpu1' and m.get('host_id')!='b70-pressure-host' for m in api(run.url)['machines']),15)
    run.a.units=200;experiment=run.submit('experiment',200)
    run.wait_event(experiment,'commit')
    before=next(j for j in api(run.url)['jobs'] if j['id']==experiment)
    record('experiment-running',job=experiment,initial_attempt=before['attempt'],losses_before=before['losses'])
    began=time.monotonic()
    disabled=user(run,[str(ROOT/'myce'),'disable','--config',str(run.installed_config),'--unit','myce-worker@gpu1.service',
        '--now','--reason','drill','--timeout','20s'],check=False)
    elapsed=time.monotonic()-began
    state=show(run)
    flag=json.loads(kernel_read(SCOPED/'DISABLED', 'text'))
    receipt=json.loads(kernel_read(SCOPED/'disable-receipt.json', 'text'))
    record('disabled',disable_exit=disabled.returncode,disable_seconds=elapsed,empty_cgroup=empty(group,state),
           flag_observed=True,generation=flag['generation'],receipt_generation=receipt['generation'],
           cli_cgroup_confirmation='Host cleanup confirmed: empty systemd worker cgroup and Mycelium Docker execution gone' in disabled.stdout)
    assert disabled.returncode==0
    assert elapsed<=20 and empty(group,state)
    assert 'Host cleanup confirmed: empty systemd worker cgroup and Mycelium Docker execution gone' in disabled.stdout
    assert receipt['generation']==flag['generation'], 'no matching cgroup cleanup receipt'
    refused=user(run,['systemctl','--user','start','myce-worker@gpu1.service'],check=False)
    state=show(run)
    record('start-refused',refused_start_exit=refused.returncode,refused_start_state=state)
    assert state['MainPID']=='0' and state['ActiveState'] in ('inactive','failed') and state['ConditionResult']=='no'
    assert not (CONTROL/'DISABLED').exists(), 'targeted disable set the host flag'
    def queued():
        value=next(j for j in api(run.url)['jobs'] if j['id']==experiment)
        return value if value['state']=='queued' else None
    waiting=run.wait(queued,75)
    record('requeued',waiting_state=waiting['state'],losses_after_disable=waiting['losses'])
    generation=flag['generation']
    user(run,[str(ROOT/'myce'),'enable','--config',str(run.installed_config),'--unit','myce-worker@gpu1.service'])
    record('enabled',flag_absent=not (SCOPED/'DISABLED').exists())
    assert not (SCOPED/'DISABLED').exists()
    run.finished([experiment],90)
    resumed=[e for e in run.rows() if e['kind']=='start' and e.get('job')==experiment and e['attempt']>1]
    assert len(resumed)==1 and resumed[0]['resumed']>0
    current=next(j for j in api(run.url)['jobs'] if j['id']==experiment)
    assert current['state']=='succeeded'
    record('resumed-complete',final_state=current['state'],final_attempt=current['attempt'],losses_after=current['losses'])
    return dict(disable_seconds=elapsed,confirmed_empty=True,cgroup=group,flag_observed=True,
        refused_start_exit=refused.returncode,refused_start_condition=state['ConditionResult'],
        waiting_state=waiting['state'],losses_before=before['losses'],losses_after=current['losses'],
        generation=generation,targeted_unit='myce-worker@gpu1.service',shared_host_flag_absent=True,resumed=resumed,
        enabled_service_back=True,docker='covered by independently verified managed-Docker phases; empty installed GPU drill inventory')


def cleanup(run):
    from stage_ab import ROOT
    if not getattr(run,'installed',False):return
    assert DROP.exists() and str(ROOT) in kernel_read(DROP, 'text'), 'test drop-in ownership changed'
    assert GPU_UNIT.exists() and str(ROOT/'installation') in kernel_read(GPU_UNIT, 'text'), 'test unit ownership changed'
    state=show(run);group=state['ControlGroup'] or (run.worker_cgroup or '0::').split('::',1)[1]
    user(run,['systemctl','--user','stop','myce-worker@gpu1.service'])
    state=show(run);assert empty(group,state),'installed test worker did not settle'
    config=ROOT/'installation/worker-gpu1/config.json'
    if (SCOPED/'DISABLED').exists():
        ready=ROOT/'installed-ready'
        if ready.exists():ready.rename(ROOT/'installed-ready.retired')
        flag=json.loads(kernel_read(SCOPED/'DISABLED', 'text'))
        receipt=SCOPED/'disable-receipt.json'
        if not receipt.exists() or json.loads(kernel_read(receipt, 'text'))['generation']!=flag['generation']:
            user(run,[str(ROOT/'myce'),'disable','--now','--reason','test cleanup','--config',str(config),'--unit','myce-worker@gpu1.service','--timeout','20s'])
        user(run,[str(ROOT/'myce'),'enable','--config',str(config),'--unit','myce-worker@gpu1.service'])
    assert not (SCOPED/'DISABLED').exists()
    if config.exists():user(run,[str(ROOT/'myce'),'uninstall','--config',str(config)])
    if DROP.parent.exists():DROP.parent.rename(ROOT/'installed-dropin.retired')
    user(run,['systemctl','--user','daemon-reload'])
    user(run,['systemctl','--user','reset-failed','myce-worker@gpu1.service'],check=False)
    state=user(run,['systemctl','--user','show','myce-worker@gpu1.service','-p','MainPID'],check=False)
    assert not GPU_UNIT.exists() and not DROP.parent.exists() and 'MainPID=0' in state.stdout
    run.worker_cgroup=None;run.installed=False
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r11b.service','MemoryMax=8G'],check=True)
    (ROOT/'installed-cleanup.json').write_text(json.dumps(dict(flag_absent=True,unit_absent=True,
        confirmed_empty=True,installation_recoverable_trash=str(ROOT/'installation/.Trash'))))
