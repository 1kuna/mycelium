from kernel_reads import kernel_read
from pathlib import Path
import subprocess, json, datetime, shlex, importlib.util, hashlib
from remote_snippets import checked_python, checked_shell, python_command
HERE = Path('/Users/zach/Documents/Git/Verso/.private/missions/mycelium-stage-ab-20261011')
ROOT = '/home/kuna/myce-test-stage-ab-r13-20261012'
UNIT = 'myce-test-stage-ab-r13.service'
WORK = Path('/Users/zach/Developer/worktrees/mycelium-verso-pressure-r13')
MAIN = '968c15d5d3e39060e997b1b3ba7f6450db881269'
SHA = 'unprepared'

def generated_0():
    return 'from pathlib import Path\nimport json,sys,hashlib,subprocess\nroot=Path("' + ROOT + '");sys.path.insert(0,str(root/\'source/scripts/pressure\'))\nfrom kernel_reads import kernel_read\nfrom pressure import DiagnosticReaders,drm_owners\nmanifest=json.loads(kernel_read(root/\'launch-manifest.json\'))\nassert manifest[\'main\']=="' + MAIN + '" and manifest[\'harness\']=="' + SHA + '"\nfor n,sha in manifest[\'files\'].items():assert hashlib.sha256(kernel_read(root/n,\'bytes\')).hexdigest()==sha\nassert not (root/\'DONE\').exists() and not (root/\'FAILED\').exists()\nassert json.loads(kernel_read(\'/home/kuna/exp42-queue/PAUSED\'))[\'owner\']==\'verso-legacy-gpu1-disabled\'\nassert Path(\'/mnt/verso-nas\').is_mount()\nreaders=DiagnosticReaders(json.loads(kernel_read(root/\'diagnostic-executables.json\')),root/\'diagnostics.jsonl\')\nassert drm_owners([\'0000:07:00.0\'],baseline=json.loads(kernel_read(root/\'display-baseline.json\')),allowed=[],diagnostics=readers)=={\'0000:07:00.0\':[]}\nassert not subprocess.check_output([\'/usr/bin/docker\',\'ps\',\'-q\',\'--filter\',\'label=myce.attempt\']).strip()\nprint(json.dumps({\'launch_gate\':True}))\n'
def launch_args():
    return ['sudo', '-n', 'systemd-run', '--unit=' + UNIT, '--collect', '--property=DevicePolicy=closed', '--property=DeviceAllow=/dev/dri/card1 rw', '--property=DeviceAllow=/dev/dri/renderD129 rw', '--property=MemoryMax=8G', '--property=MemorySwapMax=0', '--property=CPUQuota=200%', '--property=TasksMax=512', '--property=RuntimeMaxSec=3h', '--property=TimeoutStopSec=90s', '--property=KillMode=control-group', '--property=StandardOutput=append:' + ROOT + '/runner.log', '--property=StandardError=append:' + ROOT + '/runner.log', '--property=ExecStopPost=/usr/bin/python3 ' + ROOT + '/finalize.py', '/usr/bin/python3', ROOT + '/source/scripts/pressure/collector_soak.py']

def receipt_writer_source():
    return 'from pathlib import Path; import sys; Path('+repr(ROOT+'/launch-receipt.json')+').write_bytes(sys.stdin.buffer.read())'

if __name__ == '__main__':
    SHA = subprocess.check_output(['git', '-C', str(WORK), 'rev-parse', 'HEAD'], text=True).strip()
    assert subprocess.check_output(['git', '-C', str(WORK), 'merge-base', '--is-ancestor', MAIN, 'HEAD']) == b''
    assert subprocess.check_output(['git', '-C', str(WORK), 'ls-remote', 'origin', 'refs/heads/feat/verso-pressure-harness-r13'], text=True).split()[0] == SHA
    assert json.loads(kernel_read(HERE / 'r13-cross-mount-receipt.json', 'text'))['cross_mount_verified']
    code = generated_0()
    r = subprocess.run(['ssh', 'b70', checked_shell('sudo -n python3 -')], input=checked_python(code), capture_output=True, text=True, timeout=25)
    (HERE / 'r13-final-launch-gate.log').write_text(r.stdout + r.stderr)
    assert r.returncode == 0
    manager = HERE / 'manage_followons_r11.py'
    manager.write_bytes(kernel_read(WORK / 'scripts/pressure/manage_followons.py', 'bytes'))
    (HERE / 'kernel_reads.py').write_bytes(kernel_read(WORK / 'scripts/pressure/kernel_reads.py', 'bytes'))
    (HERE / 'remote_snippets.py').write_bytes(kernel_read(WORK / 'scripts/pressure/remote_snippets.py', 'bytes'))
    (HERE / 'sync_r11_report.py').write_bytes(kernel_read(WORK / 'scripts/pressure/collect_r11.py', 'bytes'))
    (HERE / 'inventory_r11.py').write_bytes(kernel_read(WORK / 'scripts/pressure/inventory_r11.py', 'bytes'))
    log = (HERE / 'spark-controller-r11.log').open('ab')
    p = subprocess.Popen(['/usr/bin/env', 'python3', str(manager)], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (HERE / 'spark-controller-r11.pid').write_text(str(p.pid) + '\n')
    log.close()
    args = launch_args()
    r = subprocess.run(['ssh', 'b70', checked_shell(shlex.join(args))], capture_output=True, text=True, timeout=20)
    (HERE / 'r13-launch-command.log').write_text(r.stdout + r.stderr)
    assert r.returncode == 0, 'launch failed; inspect exact unit, do not duplicate'
    fields = ['ActiveState', 'SubState', 'MainPID', 'ControlGroup', 'MemoryMax', 'MemorySwapMax', 'DevicePolicy', 'DeviceAllow', 'CPUQuotaPerSecUSec']
    args = ['systemctl', 'show', UNIT] + [v for f in fields for v in ('-p', f)]
    r = subprocess.run(['ssh', 'b70', checked_shell(shlex.join(args))], capture_output=True, text=True, timeout=15)
    assert r.returncode == 0
    receipt = dict(when=datetime.datetime.now(datetime.timezone.utc).isoformat(), main=MAIN, harness=SHA, systemd=r.stdout, manager_pid=p.pid, unit=UNIT)
    (HERE / 'r13-launch-receipt.json').write_text(json.dumps(receipt, indent=2))
    remote = receipt_writer_source()
    r2 = subprocess.run(['ssh', 'b70', python_command(remote)], input=json.dumps(receipt).encode(), capture_output=True, timeout=15)
    assert r2.returncode == 0
    spec = importlib.util.spec_from_file_location('sync_r11', HERE / 'sync_r11_report.py')
    sync = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync)
    sync.render(sync.collect())
    log = (HERE / 'report-sync-r11.log').open('ab')
    p2 = subprocess.Popen(['/usr/bin/env', 'python3', str(HERE / 'sync_r11_report.py')], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (HERE / 'report-sync-r11.pid').write_text(str(p2.pid) + '\n')
    log.close()
    print(json.dumps(dict(launched=True, unit=UNIT, root=ROOT, systemd=r.stdout, manager_pid=p.pid, receipt_collector_pid=p2.pid, report=str(HERE / 'report-r11.md'))))
