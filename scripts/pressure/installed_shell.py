"""Prove foreground join in the actual installer shell and GPU1 namespace."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from pressure import api
from stage_ab import ROOT


def probe(args):
    queue=args[args.index('--queue')+1];cp=args[args.index('--nas')+1]
    command=[args[0],'join','--queue',queue,'--name','installed-shell-probe','--host-id','installed-shell-probe-host',
        '--device','0','--memory-mode','fixed','--memory-gb','4','--reserve-host-memory-gb','18',
        '--heartbeat','250ms','--checkpoint-root',cp,'--state-dir',str(ROOT/'installed-shell-spool'),
        '--host-state-dir',str(ROOT/'installed-shell-control')]
    with (ROOT/'installed-shell-join.log').open('x') as log:
        child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
        try:
            end=time.monotonic()+15
            while time.monotonic()<end:
                assert child.poll() is None,'installer-shell join failed'
                machine=next((m for m in api(queue)['machines'] if m['id']=='installed-shell-probe'),None)
                if machine:break
                time.sleep(.1)
            else:raise TimeoutError('installer-shell join registration')
            assert machine['gpu']=='xpu' and machine['device']=='0'
            (ROOT/'installed-shell-join.json').write_text(json.dumps(dict(authenticated=True,pci='0000:07:00.0',gpu=machine['gpu'],device=machine['device'])))
        finally:
            if child.poll() is None:child.terminate()
            child.wait(timeout=15)
    os.execv(args[0],args)


if __name__=='__main__':probe(sys.argv[1:])
