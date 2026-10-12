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

root=Path('/home/user/synthetic-run')
config=root/'installation/worker-gpu1/config.json'
assert os.getuid()==0 and sys.argv[1:] in ([str(config)],[str(config),'detect'])
validation=sys.argv[-1]=='detect'
group=kernel_read(Path('/proc/self/cgroup')).strip()
assert group.endswith('/myce-test-r11-installed-validation.service' if validation else '/myce-worker@gpu1.service')
assert {p.name for p in kernel_read(Path('/sys/bus/pci/devices/0000:07:00.0/drm'), 'list')} >= {'card1','renderD129'}
nodes={name:os.stat('/dev/dri/'+name) for name in ('card1','renderD129')}
assert all(stat.S_ISCHR(v.st_mode) for v in nodes.values())
libc=ctypes.CDLL(None,use_errno=True)
if libc.unshare(0x00020000):raise OSError(ctypes.get_errno(),'private mount namespace')
subprocess.run(['mount','--make-rprivate','/'],check=True)
# Retain the real DRM directory, symlinks and permissions; mask every other card.
for node in kernel_read('/dev/dri','list'):
    if node.name not in nodes and stat.S_ISCHR(os.stat(node).st_mode):
        subprocess.run(['mount','--bind','/dev/null',str(node)],check=True)
for name,st in nodes.items():assert os.stat('/dev/dri/'+name).st_rdev==st.st_rdev
assert all(os.stat(p).st_rdev==os.stat('/dev/null').st_rdev for p in kernel_read('/dev/dri','list')
           if p.name not in nodes and stat.S_ISCHR(os.stat(p).st_mode))
(root/('installed-validation-fence.json' if validation else 'installed-fence.json')).write_text(json.dumps(dict(pci='0000:07:00.0',nodes=list(nodes),
    cgroup=kernel_read(Path('/proc/self/cgroup'), 'text').strip(),mount_namespace=kernel_read('/proc/self/ns/mnt', 'link'))))
c=json.loads(kernel_read(config, 'text'))
os.environ['MYCE_HOST_STATE_DIR']=str(root/'installed-host-state')
if not validation:os.environ['MYCE_TOKEN']=json.loads(kernel_read(Path(c['credentials']), 'text'))['token']
# Keep the installed selector and paths inside this GPU1-only view.
args=[c['binary'],'join','--name',c['name'],'--queue',c['queue'],'--gpu','xpu','--device','0',
      '--memory-mode','fixed','--memory-gb','4','--reserve-host-memory-gb','18','--host-memory-budget-gb','6','--memory-ceiling-slack-gb','0','--memory-swap-max-bytes','0','--heartbeat','250ms',
      '--state-dir',c['state_dir'],'--checkpoint-root',c['checkpoint_root'],'--host-state-dir',str(root/'installed-host-state')]
if validation:args=[c['binary'],'detect','--config',str(config)]
os.execv('/usr/bin/setpriv',['setpriv','--reuid=1000','--regid=1000','--init-groups','--no-new-privs',
                           *args])
