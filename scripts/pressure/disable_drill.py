"""Installed-user-service host switch drill, confined to the disposable GPU1 worker."""
import json
from kernel_reads import kernel_read, Vanished
import os
from pathlib import Path
import subprocess
import time
import urllib.error
from pressure import api

USER_HOME=Path('/home/user')
UNIT=USER_HOME/'.config/systemd/user/myce-worker.service'
DROP=UNIT.with_name(UNIT.name+'.d')/'myce-test-r8.conf'
CONTROL=USER_HOME/'.local/share/myce'


def user(run,args,check=True):
    result=subprocess.run(['/usr/sbin/runuser','-u','user','--',*args],
        env={**run.env,'XDG_RUNTIME_DIR':'/run/user/1000','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1000/bus'},
        capture_output=True,text=True,timeout=35)
    if check and result.returncode:raise RuntimeError('test user command failed: '+args[0]+', exit '+str(result.returncode))
    return result


def show(run):
    r=user(run,['systemctl','--user','show','myce-worker.service','-p','ActiveState','-p','ControlGroup',
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
    assert not UNIT.exists() and not DROP.parent.exists() and not (CONTROL/'DISABLED').exists()
    # The installer starts its unit; a missing private readiness file gates that initial start.
    DROP.parent.mkdir(mode=0o700);os.chown(DROP.parent,1000,1000)
    DROP.write_text('[Unit]\nConditionPathExists='+str(ROOT/'installed-ready')+'\n[Service]\n'
        'ExecStart=\nExecStart=/usr/bin/sudo -n /usr/bin/python3 '+str(ROOT/'source/scripts/pressure/worker_fence.py')+' '+str(ROOT/'installation/worker/config.json')+'\n'
        'MemoryMax=6G\nMemorySwapMax=0\nCPUQuota=200%\nRuntimeMaxSec=10min\n'
        'StandardOutput=append:'+str(ROOT/'logs/installed-worker.log')+'\nStandardError=append:'+str(ROOT/'logs/installed-worker.log')+'\n')
    os.chown(DROP,1000,1000)
    run.halt('worker');run.release_owned()
    # Combined enforced memory allowance stays 8GiB while the worker is in a separate user cgroup.
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r8.service','MemoryMax=2G'],check=True)
    run.installed=True
    result=user(run,[str(ROOT/'myce'),'install','worker','--root',str(ROOT/'installation'),
        '--nas',str(run.cp),'--queue',run.url,'--name','b70-pressure-gpu1','--gpu','xpu',
        '--memory-mode','fixed','--memory-gb','4'])
    record('installed',install_exit=result.returncode,installed_condition=show(run)['ConditionResult'])
    assert result.returncode==0 and show(run)['ConditionResult']=='no'
    run.installed_config=ROOT/'installation/worker/config.json'
    c=json.loads(kernel_read(run.installed_config, 'text'));c['checkpoint_root']=str(run.cp)
    run.installed_config.write_text(json.dumps(c));os.chown(run.installed_config,1000,1000)
    (ROOT/'installed-ready').touch()
    user(run,['systemctl','--user','start','myce-worker.service'])
    run.wait(lambda:show(run)['MainPID']!='0',10)
    state=show(run);group=state['ControlGroup'];run.worker_cgroup='0::'+group
    assert group.endswith('/myce-worker.service') and state['KillMode']=='control-group' and state['SendSIGKILL']=='yes'
    run.wait(lambda:any(m['id']=='b70-pressure-gpu1' and m.get('host_id')!='b70-pressure-host' for m in api(run.url)['machines']),15)
    run.a.units=200;experiment=run.submit('experiment',200)
    run.wait(lambda:run.event(experiment,'commit'))
    before=next(j for j in api(run.url)['jobs'] if j['id']==experiment)
    record('experiment-running',job=experiment,initial_attempt=before['attempt'],losses_before=before['losses'])
    began=time.monotonic()
    disabled=user(run,[str(ROOT/'myce'),'disable','--config',str(run.installed_config),
        '--now','--reason','drill','--timeout','20s'],check=False)
    elapsed=time.monotonic()-began
    state=show(run)
    flag=json.loads(kernel_read(CONTROL/'DISABLED', 'text'))
    receipt=json.loads(kernel_read(CONTROL/'disable-receipt.json', 'text'))
    record('disabled',disable_exit=disabled.returncode,disable_seconds=elapsed,empty_cgroup=empty(group,state),
           flag_observed=True,generation=flag['generation'],receipt_generation=receipt['generation'],
           cli_cgroup_confirmation='confirmed by empty systemd worker cgroup' in disabled.stdout)
    assert disabled.returncode==0
    assert elapsed<=20 and empty(group,state)
    assert 'confirmed by empty systemd worker cgroup' in disabled.stdout
    assert receipt['generation']==flag['generation'], 'no matching cgroup cleanup receipt'
    refused=user(run,['systemctl','--user','start','myce-worker.service'],check=False)
    state=show(run)
    record('start-refused',refused_start_exit=refused.returncode,refused_start_state=state)
    assert state['MainPID']=='0' and state['ActiveState'] in ('inactive','failed') and state['ConditionResult']=='no'
    host=next(m['host_id'] for m in api(run.url)['machines'] if m['id']=='b70-pressure-gpu1')
    def queued():
        value=next(j for j in api(run.url)['jobs'] if j['id']==experiment)
        return value if value['state']=='queued' and value.get('requeued_by_operator') else None
    waiting=run.wait(queued,15)
    record('requeued',waiting_state=waiting['state'],losses_after_disable=waiting['losses'])
    assert waiting['losses']==before['losses']
    generation=flag['generation'];stale_status=None
    try:api(run.url,'POST','/api/hosts/'+host+'/enable',dict(generation=generation-1))
    except urllib.error.HTTPError as error:stale_status=error.code
    assert stale_status==400, 'stale enable unexpectedly accepted'
    record('stale-enable-rejected',stale_enable_http=stale_status)
    assert next(m for m in api(run.url)['machines'] if m['id']=='b70-pressure-gpu1').get('disabled')
    user(run,[str(ROOT/'myce'),'enable','--config',str(run.installed_config)])
    record('enabled',flag_absent=not (CONTROL/'DISABLED').exists())
    assert not (CONTROL/'DISABLED').exists()
    run.finished([experiment],90)
    resumed=[e for e in run.rows() if e['kind']=='start' and e.get('job')==experiment and e['attempt']>1]
    assert len(resumed)==1 and resumed[0]['resumed']>0
    current=next(j for j in api(run.url)['jobs'] if j['id']==experiment)
    assert current['state']=='succeeded' and current['losses']==before['losses']
    record('resumed-complete',final_state=current['state'],final_attempt=current['attempt'],losses_after=current['losses'])
    return dict(disable_seconds=elapsed,confirmed_empty=True,cgroup=group,flag_observed=True,
        refused_start_exit=refused.returncode,refused_start_condition=state['ConditionResult'],
        waiting_state=waiting['state'],losses_before=before['losses'],losses_after=current['losses'],
        generation=generation,stale_enable_http=stale_status,resumed=resumed,
        enabled_service_back=True,docker='EXPECTED FAIL: not covered by disable')


def cleanup(run):
    from stage_ab import ROOT
    if not getattr(run,'installed',False):return
    assert DROP.exists() and str(ROOT) in kernel_read(DROP, 'text'), 'test drop-in ownership changed'
    assert UNIT.exists() and str(ROOT/'installation') in kernel_read(UNIT, 'text'), 'test unit ownership changed'
    state=show(run);group=state['ControlGroup'] or (run.worker_cgroup or '0::').split('::',1)[1]
    user(run,['systemctl','--user','stop','myce-worker.service'])
    state=show(run);assert empty(group,state),'installed test worker did not settle'
    config=ROOT/'installation/worker/config.json'
    if (CONTROL/'DISABLED').exists():
        ready=ROOT/'installed-ready'
        if ready.exists():ready.rename(ROOT/'installed-ready.retired')
        flag=json.loads(kernel_read(CONTROL/'DISABLED', 'text'))
        receipt=CONTROL/'disable-receipt.json'
        if not receipt.exists() or json.loads(kernel_read(receipt, 'text'))['generation']!=flag['generation']:
            user(run,[str(ROOT/'myce'),'disable','--now','--reason','test cleanup','--config',str(config),'--timeout','20s'])
        user(run,[str(ROOT/'myce'),'enable','--config',str(config)])
    assert not (CONTROL/'DISABLED').exists()
    if config.exists():user(run,[str(ROOT/'myce'),'uninstall','--config',str(config)])
    if DROP.parent.exists():DROP.parent.rename(ROOT/'installed-dropin.retired')
    user(run,['systemctl','--user','daemon-reload'])
    assert not UNIT.exists() and not DROP.parent.exists() and show(run)['MainPID']=='0'
    run.worker_cgroup=None;run.installed=False
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r8.service','MemoryMax=8G'],check=True)
    (ROOT/'installed-cleanup.json').write_text(json.dumps(dict(flag_absent=True,unit_absent=True,
        confirmed_empty=True,installation_recoverable_trash=str(ROOT/'installation/.Trash'))))
