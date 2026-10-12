"""Route installer GPU validation into the same hard GPU1 fence as its worker."""
import os
import json
import subprocess
from kernel_reads import kernel_read
from remote_snippets import checked_python
from pathlib import Path
from stage_ab import ROOT, BASE
from disable_drill import user


def install(run,args):
    bindir=ROOT/'validation-bin';bindir.mkdir(exist_ok=True)
    target=bindir/'systemd-run'
    if not target.exists():target.symlink_to(BASE/'validation_wrapper.py')
    previous=run.env['PATH'];run.env['PATH']=str(bindir)+':'+previous
    try:
        # Foreground installer detection gets the same physical GPU1-only view as join.
        source='import sys;sys.path.insert(0,'+repr(str(BASE))+');\n'+kernel_read(BASE/'worker_fence.py').split('c=json.loads(kernel_read(config,')[0]
        source=source.replace('myce-test-r13-installed-validation.service','myce-test-r13-installed-invocation.service')
        # The installer's exact privileged service-validation helper still needs sudo.
        command=['/usr/bin/setpriv','--reuid=1000','--regid=1000','--init-groups','/bin/bash','-c',
            'source /opt/intel/oneapi/setvars.sh >/dev/null 2>&1 && exec "$@"','fenced-install',
            '/usr/bin/python3',str(BASE/'installed_shell.py'),*args]
        source+='os.execv('+repr(command[0])+','+repr(command)+')\n'
        script=ROOT/'installed-invocation.py';script.write_text(checked_python(source))
        env=ROOT/'installed-invocation.env'
        with env.open('x') as stream:
            os.chmod(env,0o600)
            for key in ('MYCE_TOKEN','PATH','XDG_RUNTIME_DIR','DBUS_SESSION_BUS_ADDRESS'):
                value=run.env.get(key,{'XDG_RUNTIME_DIR':'/run/user/1000','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1000/bus'}.get(key,''))
                assert '\n' not in value;stream.write(key+'='+value+'\n')
        return subprocess.run(['systemd-run','--unit=myce-test-r13-installed-invocation.service','--wait','--pipe','--collect',
            '--property=MemoryMax=1G','--property=MemorySwapMax=0','--property=CPUQuota=100%',
            '--property=RuntimeMaxSec=60s','--property=DevicePolicy=closed',
            '--property=DeviceAllow=/dev/dri/card1 rw','--property=DeviceAllow=/dev/dri/renderD129 rw',
            '--property=EnvironmentFile='+str(env),'/usr/bin/python3',str(script),
            str(ROOT/'installation/worker-versotest-r13-gpu1/config.json'),'detect'],capture_output=True,text=True,timeout=65)
    finally:run.env['PATH']=previous
