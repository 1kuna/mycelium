"""Launch one fresh remaining-scenario runner; no Spark workers or tunnels."""
from pathlib import Path
import datetime
import hashlib
import json
import shlex
import subprocess
from kernel_reads import kernel_read
from remote_snippets import python_command, checked_shell
import launch_r11 as gate
import collect_r12 as collector

if __name__=='__main__':
    gate.SHA=subprocess.check_output(['git','-C',str(gate.WORK),'rev-parse','HEAD'],text=True).strip()
    assert subprocess.check_output(['git','-C',str(gate.WORK),'ls-remote','origin','refs/heads/feat/verso-pressure-harness-r12'],text=True).split()[0]==gate.SHA
    assert subprocess.check_output(['git','-C',str(gate.WORK),'ls-remote','origin','refs/heads/main'],text=True).split()[0]==gate.MAIN
    r=subprocess.run(['ssh','b70',python_command(gate.generated_0(),sudo=True)],capture_output=True,text=True,timeout=30)
    (gate.HERE/'r12-final-launch-gate.log').write_text(r.stdout+r.stderr);assert r.returncode==0
    args=gate.launch_args();args[-1]=gate.ROOT+'/source/scripts/pressure/stage_ab.py'
    r=subprocess.run(['ssh','b70',checked_shell(shlex.join(args))],capture_output=True,text=True,timeout=20)
    (gate.HERE/'r12-launch-command.log').write_text(r.stdout+r.stderr);assert r.returncode==0,'inspect unit before retrying launch'
    fields=['ActiveState','SubState','MainPID','ControlGroup','MemoryMax','MemorySwapMax','DevicePolicy','DeviceAllow','CPUQuotaPerSecUSec']
    r=subprocess.run(['ssh','b70',checked_shell(shlex.join(['systemctl','show',gate.UNIT,*[v for f in fields for v in ('-p',f)]]))],capture_output=True,text=True,timeout=15)
    assert r.returncode==0
    receipt=dict(when=datetime.datetime.now(datetime.timezone.utc).isoformat(),main=gate.MAIN,harness=gate.SHA,systemd=r.stdout,unit=gate.UNIT)
    (gate.HERE/'r12-launch-receipt.json').write_text(json.dumps(receipt,indent=2))
    r=subprocess.run(['ssh','b70',python_command(gate.receipt_writer_source())],input=json.dumps(receipt).encode(),capture_output=True,timeout=15);assert r.returncode==0
    collector.render(collector.collect())
    log=(gate.HERE/'report-sync-r12.log').open('ab')
    p=subprocess.Popen(['python3',str(Path(__file__).with_name('collect_r12.py'))],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    (gate.HERE/'report-sync-r12.pid').write_text(str(p.pid)+'\n');log.close()
    print(json.dumps(dict(launched=True,root=gate.ROOT,unit=gate.UNIT,receipt_collector_pid=p.pid)))
