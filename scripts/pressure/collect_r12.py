"""Detached terminal receipts and independent cleanup inventory; never prints tokens."""
from pathlib import Path
import datetime
import json
import subprocess
import time
from kernel_reads import kernel_read
from remote_snippets import python_command
HERE=Path('/Users/zach/Documents/Git/Verso/.private/missions/mycelium-stage-ab-20261011')
ROOT='/home/kuna/myce-test-stage-ab-r12-20261012'
NAMES=['launch-manifest.json','launch-receipt.json','results.json','final-cleanup.json','DONE','FAILED']
SCENARIOS=[*['Docker-'+m for m in ('grace','force','restart','loss','late','slow','disable')],
           'Memory-ceiling-isolation','CPU-fast-matrix','B-installed-disable-enable']


def receipt_source():
    return kernel_read(Path(__file__).with_name('kernel_reads.py'))+'\nfrom pathlib import Path; import json; root=Path('+repr(ROOT)+'); names='+repr(NAMES)+'; print(json.dumps({n:json.loads(kernel_read(root/n)) for n in names if (root/n).exists()}))'


def collect():
    p=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','b70',python_command(receipt_source())],capture_output=True,text=True,timeout=20)
    assert p.returncode==0,'receipt transport failed'
    values=json.loads(p.stdout);folder=HERE/'native-r12-receipts';folder.mkdir(exist_ok=True)
    for name,value in values.items():(folder/name).write_text(json.dumps(value,indent=2))
    return values


def final_inventory():
    # The runner's runtime override is retired only after its finalizer has exited.
    retire='''from pathlib import Path
import json,subprocess,shutil
root=Path("'''+ROOT+'''")
r=subprocess.run(['systemctl','show','myce-test-stage-ab-r12.service','-p','ActiveState','-p','MainPID','-p','ControlGroup'],capture_output=True,text=True)
d=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
assert d['ActiveState'] in ('inactive','failed') and d['MainPID']=='0' and not d['ControlGroup']
p=Path('/run/systemd/system.control/myce-test-stage-ab-r12.service.d')
if p.exists():
 dest=root/'runtime-units.retired/runner-override';assert not dest.exists();shutil.move(str(p),str(dest));assert dest.exists() and not p.exists()
subprocess.run(['systemctl','daemon-reload'],check=True,capture_output=True)
'''
    code=kernel_read(Path(__file__).with_name('kernel_reads.py'))+'\n'+retire
    p=subprocess.run(['ssh','b70',python_command(code,sudo=True)],capture_output=True,text=True,timeout=25)
    assert p.returncode==0,'runner runtime cleanup not yet confirmed'
    summary={}
    source=kernel_read(Path(__file__).with_name('kernel_reads.py'))+'\n'+kernel_read(Path(__file__).with_name('inventory_r11.py')).replace('from kernel_reads import kernel_read\n','')
    for host in ('b70','spark'):
        prefix='import sys;sys.argv=["inventory",'+repr(host)+']\n'
        if host=='b70':prefix+='from pathlib import Path;import json;sys.argv.append(",".join(map(str,json.loads(Path('+repr(ROOT+'/ports.json')+').read_text()))))\n'
        p=subprocess.run(['ssh',host,python_command(prefix+source,sudo=host=='b70')],capture_output=True,text=True,timeout=45)
        assert p.returncode==0,'independent inventory unavailable'
        data=json.loads(p.stdout);(HERE/('r12-final-'+host+'-inventory.json')).write_text(json.dumps(data,indent=2))
        assert not any(data[k] for k in ('test_pids','scan_errors','test_cgroups','disabled_flags','test_slices','occupied_test_ports'))
        assert not any(c['running'] for c in data['test_containers'])
        summary[host]=dict(live_test_pids=0,live_test_cgroups=0,running_test_containers=0,disabled_flags=0,occupied_test_ports=0,retained_stopped_containers=len(data['test_containers']))
        if host=='b70':
            assert json.loads(data['health']['stdout'])==dict(status='ok',worker_connected=True)
            assert data['pause_marker_exists']
            assert len(data['protected_containers'])==4 and all(c['running'] and c['health'] in (None,'healthy') for c in data['protected_containers'])
    (HERE/'r12-final-inventory-summary.json').write_text(json.dumps(summary,indent=2));return summary


def render(values,error=None):
    status='FAILED' if 'FAILED' in values else 'DONE' if 'DONE' in values else 'RUNNING; native outcomes pending'
    manifest=values.get('launch-manifest.json',{});rows=values.get('results.json',{}).get('results',[])
    lines=['# Mycelium Stage A/B round 12 — 2026-10-12','',status+'. Final acceptance requires terminal and independent cleanup receipts.','',
        'Issue #238. The original Mycelium checkout is read-only; no core source changes. Queue and workers share origin/main '+manifest.get('main','968c15d5d3e39060e997b1b3ba7f6450db881269')+'. Harness '+manifest.get('harness','pending')+' on feat/verso-pressure-harness-r12 is pushed and verified by git ls-remote. Source and executable hashes are in launch-manifest.json.','',
        '## Safety and repair','',
        'Fresh B70 and Spark inventories found no live test processes, running test containers, flags or ports. The round-11 empty Docker slice configuration was recovered across filesystems. One prior stopped labelled test container remains as recoverable evidence. Spark is clean and has no round-12 workload or new units. Production health was ok with worker_connected=true.','',
        'The failing-first KeyError regression is in r12-failing-first.log. Native ownership ignores Docker payload receipts; container holders require exact container ID, attempt label and native cgroup, without treating a PID as custody. Foreign running labels and wrong cgroups fail closed. Prior inert objects are preserved. The finalizer uses a recoverable cross-filesystem move for runtime configuration.','',
        'Mac local verification: 84 tests passed, two Linux-only skips. New end-to-end fake runs execute seven Docker scenario bodies, wrapper create/start/inspect/ps/logs/TERM/KILL/slow/late code, OOM isolation, isolated CPU matrix and installed-worker disable/enable. Real B70 labelled-test-only inspect and ps JSON shapes are fixtures. Fake proc and queue responses exercise missing-PID ownership and native container cgroup checks. B70 harness tests and GPU qualification have separate logs.','',
        '## Run contract','',
        'Fresh databases, worker spools and host controls. Order: Docker grace/force/restart/loss/late/slow/disable; kernel OOM isolation; isolated local-ext4 CPU matrix; installed GPU1 disable/enable. Round-11 migration PASS results are retained and not rerun. No new collector soak or Spark workload.','',
        'Docker uses the already-local digest-pinned CPU image, exact attempt labels, MYCE_MEMORY_LIMIT_BYTES for memory and no-swap limits, no GPU mapping and no network. Slowness and delayed starts exist only in the owned wrapper. Unknown/unlabelled containers are never signalled. Stopped objects remain evidence.','',
        'The isolated 32 MiB OOM victim must fail with the ceiling reason, one attempt and no infrastructure losses. Its concurrent neighbor must survive and finish; the worker PID must remain unchanged. GPU1 only is visible to B70 GPU workers through the qualified private device fence; GPU0 is never exposed. The real installer uses instance gpu1 and logical device 0 inside that GPU1-only shell. Targeted myce disable/enable uses myce-worker@gpu1.service.','',
        '564 stays disabled. The legacy queue stays held by the persistent verso-legacy-gpu1-disabled guard, including after test cleanup, as explicitly requested. Production containers, GPU controller, display service and atlas units are protected. No reboot, models, or Mac office application. Aggregate test memory 8 GiB, no swap, host headroom at least 18 GiB and device envelope 4 GiB.','',
        '## Detached paths','',ROOT+'/DONE or '+ROOT+'/FAILED.',ROOT+'/results.json; final-cleanup.json; phases/; launch-manifest.json.',
        'NAS: Verso/experiments/exp-238-mycelium-stage-ab-r12-20261012/.',
        'Mac: report-sync-r12.log/.pid, report-sync-r12-DONE or report-sync-r12-FAILED; native-r12-receipts/.','',
        '## Native outcomes','']
    for name in SCENARIOS:
        matches=[r for r in rows if r['scenario']==name];row=matches[-1] if matches else None
        lines.append(name+': '+(row['result'] if row else 'NOT RUN' if 'FAILED' in values else 'pending')+'.')
    if values.get('final-cleanup.json'):
        final=values['final-cleanup.json'];lines+=['','B70 final: '+json.dumps({k:final.get(k) for k in ('physical','queue_pause','remaining_pids','occupied_test_ports','docker_error','cpu_cleanup_error','runtime_unit_error','installed_worker','production_health')})]
    summary=HERE/'r12-final-inventory-summary.json'
    if summary.exists():lines+=['','Independent final inventory: '+kernel_read(summary)]
    if error:lines+=['','Receipt collection error: '+error+'. Completion is unconfirmed.']
    lines+=['','## DIGEST',
        'Round 11 stopped because our harness treated a Docker start as a PID record.',
        'The failing-first regression passes; Docker custody uses ID, label and cgroup.',
        'All remaining scenario bodies and Docker wrapper paths pass on Mac fakes.',
        'Real labelled-test Docker inspect and ps shapes are captured as fixtures.',
        'Queue and workers share current main 968c15d; the harness SHA is pushed and verified.',
        'Docker, OOM isolation, local CPU and installed GPU1 drills run in that order.',
        'GPU0, production, controller, display and Atlas remain protected; 564 stays disabled.',
        'The legacy GPU1 queue guard intentionally remains after cleanup.',
        'Native state: '+status+'; independent cleanup '+('recorded' if summary.exists() else 'pending')+'.',
        'No reboots or models; local fake passes and detached launch are not native acceptance.']
    assert len(lines[lines.index('## DIGEST')+1:])<=10
    (HERE/'report-r12.md').write_text('\n'.join(lines)+'\n')

if __name__=='__main__':
    deadline=time.monotonic()+3*3600;last={};errors=0
    while time.monotonic()<deadline:
        try:
            last=collect()
            if 'final-cleanup.json' in last and ('DONE' in last or 'FAILED' in last):
                final_inventory();render(last)
                marker='report-sync-r12-FAILED' if 'FAILED' in last else 'report-sync-r12-DONE'
                (HERE/marker).write_text(json.dumps(dict(native_terminal='FAILED' if 'FAILED' in last else 'DONE',when=datetime.datetime.now(datetime.timezone.utc).isoformat())))
                break
            render(last);errors=0
        except Exception as exc:
            errors+=1;render(last,type(exc).__name__)
            if errors>=6:
                (HERE/'report-sync-r12-FAILED').write_text(json.dumps(dict(reason='receipt or independent inventory failure',native_terminal='unconfirmed')));break
        time.sleep(10)
    else:
        render(last,'receipt deadline');(HERE/'report-sync-r12-FAILED').write_text('{"reason":"receipt deadline","native_terminal":"unconfirmed"}')
