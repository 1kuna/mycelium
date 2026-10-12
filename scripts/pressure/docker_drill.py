"""Native managed Docker checks on exclusively owned labels, with no GPU mapping."""
import json
import os
from pathlib import Path
import subprocess
import time
from stage_ab import ROOT, BASE, atomic
from pressure import api, emit
from kernel_reads import kernel_read

IMAGE='verso-render@sha256:226e1e5d062ddbf4feda0fbfc4d299e2039f773b19f5ac8c2f93934201910a23'
REAL='/usr/bin/docker'
SLICE='myce-test-r9-docker.slice'


def containers(run):
    identities={json.loads(kernel_read(p,'text'))['identity'] for p in (ROOT/'phases').glob('*/worker/*.docker.json')}
    ids=subprocess.check_output([REAL,'ps','-aq','--filter','label=myce.attempt'],timeout=3).decode().split()
    rows=json.loads(subprocess.check_output([REAL,'inspect',*ids],timeout=3)) if ids else []
    assert all(row['Config']['Labels'].get('myce.attempt') in identities for row in rows), 'foreign Docker label ownership'
    return rows


def current(run):
    identities={json.loads(kernel_read(p,'text'))['identity'] for p in (run.phase/'worker').glob('*.docker.json')}
    return [row for row in containers(run) if row['Config']['Labels']['myce.attempt'] in identities]


def checkpoint_done(run,job):
    return run.wait_event(job,'commit',30)


def submit(run,ignore=False):
    request=dict(name='docker-synthetic',project='verso-pressure',priority='experiment',resumable=True,
        run=dict(docker=True,command=['docker','run','--entrypoint','/usr/local/bin/python',
            '-v',str(BASE/'docker_job.py')+':/payload.py:ro',
            '-v',str(BASE/'kernel_reads.py')+':/kernel_reads.py:ro','-v',str(run.phase)+':/evidence',IMAGE,'/payload.py','ignore' if ignore else 'save']),
        needs=dict(memory_gb=4,host_memory_gb=.25,platform={'required':['xpu']}),allowed_machines=['b70-pressure-gpu1'])
    at=time.monotonic();job=run.submit_spec(request)['id']
    emit(run.events,'submit',at=at,job=job,priority='experiment',units=2000 if ignore else 200)
    return job


def cleanup(run):
    # Fence the only delayed helper before claiming no future starts; no daemon restart.
    path=run.phase/'late-unit'
    if path.exists():
        unit=kernel_read(path,'text');assert unit.startswith('myce-test-r9-late-')
        subprocess.run(['/usr/sbin/runuser','-u','user','--','env','XDG_RUNTIME_DIR=/run/user/1000',
                        'systemctl','--user','stop',unit],check=True,capture_output=True)
        state=subprocess.run(['/usr/sbin/runuser','-u','user','--','env','XDG_RUNTIME_DIR=/run/user/1000',
                              'systemctl','--user','is-active',unit],capture_output=True,text=True).stdout.strip()
        assert state in ('inactive','failed','unknown'),'delayed helper remains'
    atomic(run.phase/'docker-config.json',dict(mode='normal'));os.chown(run.phase/'docker-config.json',1000,1000)
    for row in current(run):
        if row['State']['Running']:subprocess.run([REAL,'kill',row['Id']],check=True,capture_output=True,timeout=3)
    assert not any(row['State']['Running'] for row in current(run)), 'Docker execution remains'
    # Once helper dispatch is fenced, prove native execution/RAM gone before resolving an unobserved start.
    run.halt('worker')
    for p in (run.phase/'worker').glob('*.docker.json'):
        record=json.loads(kernel_read(p,'text'))
        if not record.get('released'):
            r=subprocess.run([str(ROOT/'myce'),'resolve-docker','--state-dir',str(run.phase/'worker'),
                '--attempt-id',record['identity'],'--confirm-gone'],capture_output=True,text=True,env=run.env,timeout=10)
            assert r.returncode==0,'offline exact custody resolution failed'
    atomic(run.phase/'docker-cleanup.json',dict(running=0,delayed_dispatch_fenced=True,
        stopped_evidence=[dict(id=r['Id'],identity=r['Config']['Labels']['myce.attempt']) for r in current(run)]))


def scenarios(run):
    assert not subprocess.check_output([REAL,'ps','-aq','--filter','label=myce.attempt']).strip(),'existing Docker attempt: isolation unavailable'
    subprocess.run([REAL,'image','inspect',IMAGE],check=True,stdout=subprocess.DEVNULL)
    # The adapter owns lifecycle; this test-only wrapper owns additional OS containment.
    subprocess.run(['systemctl','set-property','--runtime',SLICE,'MemoryMax=512M','MemorySwapMax=0',
                    'CPUQuota=100%','DevicePolicy=closed'],check=True,capture_output=True)
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r9.service','MemoryMax=7G'],check=True,capture_output=True)
    bindir=ROOT/'docker-bin';bindir.mkdir();(bindir/'docker').symlink_to(BASE/'docker_wrapper.py')
    old_env=run.env.copy()
    try:
        for mode in ('grace','force','restart','loss','late','slow','disable'):
            name='Docker-'+mode
            try:
                run.begin(name)
                atomic(run.phase/'docker-config.json',dict(mode='normal'));os.chown(run.phase/'docker-config.json',1000,1000)
                run.halt('worker')
                run.env={**old_env,'PATH':str(bindir)+':'+old_env.get('PATH','/usr/bin:/bin'),
                         'MYCE_TEST_DOCKER_PHASE':str(run.phase),'DOCKER_HOST':'unix:///var/run/docker.sock',
                         'XDG_RUNTIME_DIR':'/run/user/1000','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1000/bus'}
                run.start_worker()
                if mode=='late':atomic(run.phase/'docker-config.json',dict(mode='late'));os.chown(run.phase/'docker-config.json',1000,1000)
                if mode=='disable':
                    from docker_disable import install_worker
                    install_worker(run)
                job=submit(run,ignore=mode in ('force','slow','loss'))
                if mode=='late':
                    run.wait(lambda:(run.phase/'delayed-accepted').exists(),15)
                    assert not any(r['State']['Running'] for r in current(run))
                    api(run.url,'POST','/api/jobs/'+job+'/cancel')
                    pending=api(run.url);assert any('Docker cleanup unconfirmed' in h for h in pending.get('host_holds') or [])
                    run.wait(lambda:(run.phase/'delayed-started').exists(),20)
                else:
                    checkpoint_done(run,job)
                    rows=current(run);assert rows and any(r['State']['Running'] for r in rows)
                    for row in rows:
                        assert row['HostConfig']['Memory']==128*1024**2
                        assert row['HostConfig']['CgroupParent']==SLICE
                        assert not row['HostConfig']['Devices'] and not row['HostConfig'].get('DeviceRequests')
                    if mode=='restart':
                        run.halt('worker',True);run.start_worker()
                    elif mode=='loss':
                        sentinel=[str(ROOT/'myce'),'join','--queue',run.url,'--name','cpu-loss-sentinel',
                            '--host-id','test-cpu-sentinel','--gpu','fake','--memory-gb','.001',
                            '--memory-mode','fixed','--heartbeat','250ms','--reserve-host-memory-gb','18',
                            '--state-dir',str(run.phase/'sentinel'),'--host-state-dir',str(run.phase/'sentinel-control'),
                            '--checkpoint-root',str(run.cp)]
                        run.spawn('loss-sentinel',sentinel)
                        run.wait(lambda:len(api(run.url)['machines'])==2,10)
                        run.halt('worker',True)
                        run.wait(lambda:any(m['id']=='b70-pressure-gpu1' and m.get('gone') for m in api(run.url)['machines']),75)
                        assert any('Docker cleanup unconfirmed' in h for h in api(run.url).get('host_holds') or [])
                        run.start_worker()
                    elif mode=='disable':
                        # Register this exclusive test worker as the disposable systemd user cgroup, CPU-only here.
                        from docker_disable import drill
                        evidence=drill(run,job)
                        run.result(name,'PASS',**evidence);continue
                    if mode=='slow':atomic(run.phase/'docker-config.json',dict(mode='slow'));os.chown(run.phase/'docker-config.json',1000,1000)
                    api(run.url,'POST','/api/jobs/'+job+'/cancel')
                    if mode=='slow':
                        held=run.wait(lambda:next((h for h in api(run.url).get('host_holds') or [] if 'operator action required' in h),None),160)
                        state=api(run.url);machine=next(m for m in state['machines'] if m['id']=='b70-pressure-gpu1')
                        assert machine['free_memory_gb']==0 and machine['host_available_memory_gb']-machine['free_host_memory_gb']>=.25, 'reservation silently freed'
                        assert any(r['State']['Running'] for r in current(run)), 'slow test execution vanished before witness'
                        assert 'event=docker_cleanup_escalated' in kernel_read(ROOT / 'logs/queue.log','text')
                        atomic(run.phase/'escalation.json',dict(host_hold=held,machine=machine))
                        atomic(run.phase/'docker-config.json',dict(mode='normal'));os.chown(run.phase/'docker-config.json',1000,1000)
                state=run.finished([job],160)[job]
                assert state['state'] in ('cancelled','failed')
                assert not any(r['State']['Running'] for r in current(run))
                custody=[json.loads(kernel_read(p)) for p in (run.phase/'worker').glob('*.docker.json')]
                if mode=='force':assert any(r.get('forced') for r in custody),'grace never forced the TERM-ignoring container'
                if mode=='grace':assert all(not r.get('forced') for r in custody),'cooperative stop unexpectedly forced'
                run.result(name,'PASS',job=job,terminal=state['state'],labels=[r['Config']['Labels']['myce.attempt'] for r in current(run)],gpu_mapping=False)
            except Exception as exc:
                run.result(name,'HOLD' if run.safety else 'FAIL',error=type(exc).__name__,detail=str(exc))
            finally:
                if hasattr(run,'phase') and run.phase.name==name and (run.phase/'docker-config.json').exists():cleanup(run)
                if mode=='disable':
                    from docker_disable import cleanup as disable_cleanup
                    disable_cleanup(run)
                run.scenario_cleanup();run.env=old_env.copy()
    finally:
        assert not any(r['State']['Running'] for r in containers(run))
        subprocess.run(['systemctl','stop',SLICE],check=True,capture_output=True)
        subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r9.service','MemoryMax=8G'],check=True,capture_output=True)
        run.env=old_env
