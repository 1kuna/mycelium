"""Separate short controls, local storage stress and a small NAS latency receipt."""
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from stage_ab import ROOT, NAS, atomic, cgroup_pids
from kernel_reads import kernel_read, Vanished

FAST=('TestPressureDockerProxyMatrix|TestPressureForeignVersoStateMustReject|'
      'TestWorkerStateLockAndCorruption|TestAPIRejectsMalformedRequests|'
      'TestWorkerAttemptCheckpointIsolation|TestWorkerOrphanCleanupVerifiesIdentity|'
      'TestWorkerLegacyRootCheckpointAndSymlink|TestWorkerStopEventPreservesQueueDeadline|'
      'TestWorkerStopAndGraceDuringBlockedUpload|TestWorkerStopAndGraceDuringBlockedCheckin|'
      'TestProductionPreemptsDuringNASLoss|TestWorkerProductionWithoutNAS|TestBlockedNASDoesNotHoldQueue|'
      'TestBlockedCheckpointPreparationIsBounded|TestWorkerForceStopsBlockedSaveAtGrace|'
      'TestDocker|TestDisableDocker|TestVersoPressure')


def show(unit):
    r=subprocess.run(['systemctl','show',unit,'-p','ActiveState','-p','SubState','-p','ControlGroup','-p','ExecMainStatus','-p','ExecMainCode','-p','Result'],capture_output=True,text=True,check=True)
    return dict(row.split('=',1) for row in r.stdout.splitlines() if '=' in row)


def isolated(run,name,args,seconds):
    run.gate()
    run.phase=ROOT/'phases'/name;run.phase.mkdir();os.chown(run.phase,1000,1000)
    run.idle_charged=int(kernel_read(Path('/sys/fs/cgroup')/run.cgroup.split('::',1)[1].lstrip('/')/'memory.current'))
    temp=run.phase/'local-temp';temp.mkdir();os.chown(temp,1000,1000)
    # Test-only root must be local ext4; NAS is measured separately, never the CPU TMPDIR.
    fs=subprocess.check_output(['findmnt','-n','-T',str(temp),'-o','FSTYPE'],text=True).strip()
    assert fs=='ext4', 'CPU checkpoint root must be local ext4'
    unit='myce-test-r14-'+name.lower()+'.service'
    registry=ROOT/'cpu-units.json'
    units=json.loads(kernel_read(registry)) if registry.exists() else []
    atomic(registry,units+[unit])
    log=run.phase/'cpu.log';marker=run.phase/'child.exit'
    launch=['systemd-run','--unit='+unit,'--property=User=1000','--property=Group=1000',
        '--property=MemoryMax=4G','--property=MemorySwapMax=0','--property=CPUQuota=200%',
        '--property=DevicePolicy=closed','--property=KillMode=control-group',
        '--property=TimeoutStopSec=10s','--property=RuntimeMaxSec='+str(seconds+15),
        '--property=StandardOutput=append:'+str(log),'--property=StandardError=append:'+str(log),
        '--setenv=TMPDIR='+str(temp),'/usr/bin/python3',str(ROOT/'source/scripts/pressure/cpu_child.py'),str(marker),*args]
    error=None;exitcode=None;child_state={};accounting=[];group='/system.slice/'+unit;start=time.monotonic()
    try:
        subprocess.run(launch,check=True,capture_output=True)
        assert show(unit)['ControlGroup']==group
        end=time.monotonic()+seconds+10
        while not marker.exists():
            run.gate()
            state=show(unit)
            cg=Path('/sys/fs/cgroup')/group.lstrip('/')
            try:
                accounting.append(dict(at=time.monotonic(),current=int(kernel_read(cg/'memory.current')),
                    stat=kernel_read(cg/'memory.stat'),events=kernel_read(cg/'memory.events')))
            except Vanished:pass
            if state['ActiveState'] in ('inactive','failed') and not marker.exists():raise RuntimeError('CPU child exited without receipt')
            if time.monotonic()>=end:raise TimeoutError('isolated CPU duration exhausted')
            time.sleep(2)
        exitcode=int(kernel_read(marker,'text'))
        child_state=show(unit)
        outcome=classify(exitcode,child_state,accounting,kernel_read(log))
        if outcome!='passed':error=dict(type=outcome,detail='inspect retained cpu.log')
    except Exception as exc:error=dict(type=type(exc).__name__,detail=str(exc))
    finally:
        child_state=child_state or show(unit)
        if group:
            cg=Path('/sys/fs/cgroup')/group.lstrip('/')
            try:accounting.append(dict(at=time.monotonic(),events=kernel_read(cg/'memory.events')))
            except Vanished:pass
        subprocess.run(['systemctl','stop',unit],check=True,capture_output=True)
        state=show(unit)
        child_state=child_state or state
        if marker.exists():exitcode=int(kernel_read(marker))
        outcome=classify(exitcode,child_state,accounting,kernel_read(log) if log.exists() else '',error)
        if outcome!='passed':error=dict(type=outcome,cause=error)
        assert state['ActiveState'] in ('inactive','failed') and not cgroup_pids('0::'+group),'isolated CPU cgroup unconfirmed'
        subprocess.run(['systemctl','reset-failed',unit],capture_output=True)
        try:
            kernel_read(Path('/sys/fs/cgroup')/group.lstrip('/'),'list')
        except Vanished:pass
        else:raise RuntimeError('CPU charge group still exists')
        atomic(run.phase/'cgroup-accounting.json',dict(samples=accounting,stopped=state,cgroup_removed=True))
        trash=Path('/home/user/.local/share/Trash');(trash/'files').mkdir(parents=True,exist_ok=True);(trash/'info').mkdir(parents=True,exist_ok=True)
        dest=trash/'files'/(name+'-'+str(time.time_ns()))
        from urllib.parse import quote
        info=trash/'info'/(dest.name+'.trashinfo')
        info.write_text('[Trash Info]\nPath='+quote(str(temp))+'\nDeletionDate='+time.strftime('%Y-%m-%dT%H:%M:%S')+'\n')
        temp.rename(dest);assert dest.exists() and info.exists()
        os.chown(info,1000,1000)
        run.result(name,'FAIL' if error else 'PASS',error=error,seconds=time.monotonic()-start,
                   fs=fs,budget_seconds=seconds,log=str(log),child_exit=exitcode,child_state=child_state,outcome=outcome,checkpoint_recoverable_trash=str(dest),cgroup_removed=True)
        run.scenario_cleanup()


def scenarios(run):
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r14.service','MemoryMax=4G'],check=True,capture_output=True)
    isolated(run,'CPU-fast-matrix',[str(ROOT/'cpu-matrix.test'),'-test.run','^('+FAST+')','-test.v','-test.timeout','5m'],300)
    subprocess.run(['systemctl','set-property','--runtime','myce-test-stage-ab-r14.service','MemoryMax=8G'],check=True,capture_output=True)


def classify(exitcode,state,samples,log,error=None):
    events=[dict(row.split() for row in sample['events'].splitlines()) for sample in samples]
    if state.get('Result')=='oom-kill' or any(int(e.get('oom_kill',0))>0 for e in events):return 'oom'
    if state.get('Result')=='timeout' or 'panic: test timed out after' in log or (error or {}).get('type')=='TimeoutError':return 'timeout'
    if exitcode==0 and not error:return 'passed'
    if exitcode is not None and ('--- FAIL:' in log or exitcode==1):return 'assertion_failure'
    return 'execution_failure'
