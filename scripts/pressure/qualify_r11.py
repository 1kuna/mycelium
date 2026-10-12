from kernel_reads import kernel_read
"""Fresh fenced runtime and cross-mount durability receipts before launch."""
from pathlib import Path
import hashlib
import json
import shlex
import subprocess
from remote_snippets import checked_python, checked_shell, python_command
HERE = Path('/Users/zach/Documents/Git/Verso/.private/missions/mycelium-stage-ab-20261011')
ROOT = '/home/kuna/myce-test-stage-ab-r14-20261012'
PROOF = 'Verso/experiments/exp-238-mycelium-stage-ab-r14-proof-20261011'

def runtime_source():
    source = kernel_read(Path(__file__).with_name('worker_fence.py'), 'text')
    source = source[:source.index('c=json.loads(kernel_read(config,')]
    source += "os.environ['ONEAPI_DEVICE_SELECTOR']='level_zero:0'\nos.environ['ZE_AFFINITY_MASK']='0'\nos.environ.pop('MYCE_TOKEN',None)\ncode='import torch,json; n=torch.xpu.device_count(); a=torch.xpu.is_available(); print(json.dumps(dict(available=a,visible_devices=n))); assert a and n==1'\nos.execv('/usr/bin/setpriv',['setpriv','--reuid=1000','--regid=1000','--init-groups','--no-new-privs','/home/kuna/verso-labs/runtimes/experiment-xpu-venv/bin/python','-c',code])\n"
    return 'import sys;sys.path.insert(0,' + repr(ROOT + '/source/scripts/pressure') + ');\n' + source

def proof_source(mode):
    base = '/mnt/verso-nas/' if mode in ('write', 'reread') else '/home/kuna/nas-dev/'
    return "from pathlib import Path\nimport os,json,hashlib\nimport sys;sys.path.insert(0,'/home/kuna/myce-test-stage-ab-r14-20261012/source/scripts/pressure');from kernel_reads import kernel_read\np=Path(" + repr(base + PROOF) + ')\nmode=' + repr(mode) + "\nif mode=='write':p.mkdir(mode=0o700)\npayload=b'myce-r11-synthetic-proof'*128\nif mode in ('write','copy'):\n name='b70' if mode=='write' else 'spark'\n if mode=='copy':assert kernel_read(p/'b70','bytes')==payload\n tmp=p/(name+'.tmp')\n with tmp.open('xb') as f:f.write(payload);f.flush();os.fsync(f.fileno())\n tmp.rename(p/name)\n fd=os.open(p,os.O_DIRECTORY);os.fsync(fd);os.close(fd)\nelse:assert kernel_read(p/'spark','bytes')==kernel_read(p/'b70','bytes')==payload\nprint(json.dumps(dict(mode=mode,bytes=len(payload),sha256=hashlib.sha256(payload).hexdigest())))\n"

def writer_source():
    return 'from pathlib import Path; import sys; Path('+repr(ROOT+'/runtime-qualification.py')+').write_bytes(sys.stdin.buffer.read())'

def remote(host, source):
    p = subprocess.run(['ssh', host, python_command(source)], capture_output=True, text=True, timeout=45)
    assert p.returncode == 0, 'qualification failed; no native run launched'
    return json.loads(p.stdout)
if __name__ == '__main__':
    source = checked_python(runtime_source())
    path = ROOT + '/runtime-qualification.py'
    writer = writer_source()
    p = subprocess.run(['ssh', 'b70', python_command(writer)], input=source.encode(), capture_output=True, timeout=15)
    assert p.returncode == 0
    args = ['sudo', '-n', 'systemd-run', '--unit=myce-test-r14-installed-validation.service', '--wait', '--pipe', '--collect', '--property=MemoryMax=1G', '--property=MemorySwapMax=0', '--property=CPUQuota=100%', '--property=RuntimeMaxSec=45s', '--property=DevicePolicy=closed', '--property=DeviceAllow=/dev/dri/card1 rw', '--property=DeviceAllow=/dev/dri/renderD129 rw', '/usr/bin/python3', path, ROOT + '/installation/worker-versotest-r14-gpu1/config.json', 'detect']
    p = subprocess.run(['ssh', 'b70', checked_shell(shlex.join(args))], capture_output=True, text=True, timeout=55)
    (HERE / 'r14-runtime-qualification.log').write_text(p.stdout + p.stderr)
    assert p.returncode == 0
    result = json.loads(p.stdout)
    assert result == {'available': True, 'visible_devices': 1}
    (HERE / 'r14-runtime-qualification.json').write_text(json.dumps(result, indent=2))
    rows = [remote('b70', proof_source('write')), remote('spark', proof_source('copy')), remote('b70', proof_source('reread'))]
    assert len({r['sha256'] for r in rows}) == 1
    (HERE / 'r14-cross-mount-receipt.json').write_text(json.dumps(dict(cross_mount_verified=True, rows=rows), indent=2))
    print(json.dumps(dict(runtime_qualified=True, cross_mount_verified=True)))
