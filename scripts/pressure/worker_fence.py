#!/usr/bin/env python3
"""Give the disposable installed user worker a private GPU1-only /dev/dri."""
import ctypes
import json
from kernel_reads import kernel_read, Vanished
import os
from pathlib import Path
import stat
import subprocess
import sys

root=Path('/home/kuna/myce-test-stage-ab-r8-20261011')
config=root/'installation/worker/config.json'
assert os.getuid()==0 and sys.argv[1:]==[str(config)]
assert kernel_read(Path('/proc/self/cgroup'), 'text').strip().endswith('/myce-worker.service')
assert {p.name for p in kernel_read(Path('/sys/bus/pci/devices/0000:07:00.0/drm'), 'list')} >= {'card1','renderD129'}
nodes={name:os.stat('/dev/dri/'+name) for name in ('card1','renderD129')}
assert all(stat.S_ISCHR(v.st_mode) for v in nodes.values())
libc=ctypes.CDLL(None,use_errno=True)
if libc.unshare(0x00020000):raise OSError(ctypes.get_errno(),'private mount namespace')
subprocess.run(['mount','--make-rprivate','/'],check=True)
subprocess.run(['mount','-t','tmpfs','-o','size=64k,mode=755','myce-test-gpu1','/dev/dri'],check=True)
for name,st in nodes.items():
    os.mknod('/dev/dri/'+name,stat.S_IFCHR|0o666,st.st_rdev)
assert sorted(p.name for p in kernel_read(Path('/dev/dri'), 'list'))==['card1','renderD129']
(root/'installed-fence.json').write_text(json.dumps(dict(pci='0000:07:00.0',nodes=list(nodes),
    cgroup=kernel_read(Path('/proc/self/cgroup'), 'text').strip(),mount_namespace=kernel_read('/proc/self/ns/mnt', 'link'))))
c=json.loads(kernel_read(config, 'text'))
os.environ['MYCE_HOST_STATE_DIR']='/home/kuna/.local/share/myce'
os.environ['MYCE_TOKEN']=json.loads(kernel_read(Path(c['credentials']), 'text'))['token']
# The installer has no device option; bind its installed binary explicitly inside this private view.
args=[c['binary'],'join','--name',c['name'],'--queue',c['queue'],'--gpu','xpu','--device','0',
      '--memory-mode','fixed','--memory-gb','4','--reserve-host-memory-gb','18','--heartbeat','250ms',
      '--state-dir',c['state_dir'],'--checkpoint-root',c['checkpoint_root']]
os.execv('/usr/bin/setpriv',['setpriv','--reuid=1000','--regid=1000','--init-groups','--no-new-privs',
                           *args])
