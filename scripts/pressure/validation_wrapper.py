#!/usr/bin/python3
"""Only the exact installed detect call is elevated, device/memory fenced."""
import os
from pathlib import Path
import subprocess
import sys
root=Path('/home/user/synthetic-run')
args=sys.argv[1:];config=root/'installation/worker-gpu1/config.json'
expected=[str(root/'installation/worker-gpu1/myce'),'detect','--config',str(config)]
# Installer binary name is read from its generated, private config; never from argv.
sys.path.insert(0,str(root/'source/scripts/pressure'))
from kernel_reads import kernel_read
import json
c=json.loads(kernel_read(config));expected[0]=c['binary']
assert args[-4:]==expected and '--wait' in args and '--pipe' in args
assert '--property=EnvironmentFile='+str(root/'installation/worker-gpu1/gpu.env') in args
command=['sudo','-n','/usr/bin/systemd-run','--unit=myce-test-r12-installed-validation.service',
    '--wait','--pipe','--collect','--property=MemoryMax=1G','--property=MemorySwapMax=0',
    '--property=CPUQuota=100%','--property=RuntimeMaxSec=45s','--property=DevicePolicy=closed',
    '--property=DeviceAllow=/dev/dri/card1 rw','--property=DeviceAllow=/dev/dri/renderD129 rw',
    '--property=EnvironmentFile='+str(root/'installation/worker-gpu1/gpu.env'),
    '/usr/bin/python3',str(root/'source/scripts/pressure/worker_fence.py'),str(config),'detect']
raise SystemExit(subprocess.run(command).returncode)
