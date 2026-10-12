#!/usr/bin/python3
"""Delay only disposable labeled attempts; never delay or signal production Docker."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parent))
from kernel_reads import kernel_read

ROOT=Path('/home/kuna/myce-test-stage-ab-r13-20261012')
REAL='/usr/bin/docker'
PHASE=Path(os.environ['MYCE_TEST_DOCKER_PHASE'])
assert PHASE.parent==ROOT/'phases'


def known():
    return {json.loads(kernel_read(p,'text'))['identity'] for p in (ROOT/'phases').glob('*/worker/*.docker.json')}


def inspect(ids, inventory=False):
    if not ids:return []
    rows=json.loads(subprocess.check_output([REAL,'inspect',*ids],timeout=2))
    assert len(rows)==len(ids)
    for row in rows:
        owned=row['Config']['Labels'].get('myce.attempt') in known()
        assert owned or (inventory and not row['State']['Running'] and row['State']['Pid']==0),'foreign label: no operation allowed'
    return [row for row in rows if row['Config']['Labels'].get('myce.attempt') in known()]


def main(argv):
    delayed=argv[0]=='--delayed'
    if delayed:
        ident=argv[1];inspect([ident]);time.sleep(6)
        inspect([ident]);subprocess.run([REAL,'start',ident],check=True,stdout=subprocess.DEVNULL)
        (PHASE/'delayed-started').write_text(str(time.monotonic()))
        return
    assert argv[:2]==['--host','unix:///var/run/docker.sock'],'test endpoint mismatch'
    args=argv[2:];verb=args[0]
    config=json.loads(kernel_read(PHASE / 'docker-config.json','text'))
    if verb=='create':
        labels=[args[i+1] for i,a in enumerate(args[:-1]) if a=='--label']
        assert any(label.startswith('myce.attempt=') and label.split('=',1)[1] in known() for label in labels)
        assert not any(a in args for a in ('--device','--gpus','--privileged','--network'))
        limits=[args[i+1] for i,a in enumerate(args[:-1]) if a=='--env' and args[i+1].startswith('MYCE_MEMORY_LIMIT_BYTES=')]
        assert len(limits)==1
        limit=int(limits[0].split('=',1)[1]);assert limit==128*1024**2
        assert args[args.index('--memory')+1]==str(limit) and args[args.index('--memory-swap')+1]==str(limit)
        args=[verb,'--cpus=.5','--network=none',
              '--cgroup-parent=myce-test-r13-docker.slice',*args[1:]]
    elif verb in ('kill','start','logs'):
        inspect([args[-1]])
    elif verb=='inspect':inspect(args[1:])
    elif verb=='ps':
        # Retained stopped objects are evidence, never current-attempt custody.
        ids=subprocess.check_output([REAL,*args],timeout=2).decode().split()
        rows=inspect(ids,inventory=True)
    else:raise ValueError('unqualified Docker verb')
    with (PHASE/'docker-calls.jsonl').open('a') as stream:
        fcntl.flock(stream,fcntl.LOCK_EX)
        stream.write(json.dumps(dict(at=time.monotonic(),verb=verb,signal=args[1] if verb=='kill' else None))+'\n');stream.flush();os.fsync(stream.fileno())
    if config['mode']=='late' and verb=='start':
        unit='myce-test-r13-late-'+args[-1][:12]+'.service'
        (PHASE/'late-unit').write_text(unit)
        subprocess.run(['systemd-run','--user','--unit='+unit,'--collect','--property=MemoryMax=128M',
            '--property=RuntimeMaxSec=20s','--setenv=MYCE_TEST_DOCKER_PHASE='+str(PHASE),
            '/usr/bin/python3',str(Path(__file__).resolve()),'--delayed',args[-1]],check=True,capture_output=True)
        (PHASE/'delayed-accepted').touch()
        time.sleep(20)
    if config['mode']=='slow' and verb in ('ps','inspect','kill'):time.sleep(10)
    if verb=='ps':
        if rows:print('\n'.join(row['Id'] for row in rows))
        return
    os.execv(REAL,[REAL,*argv[:2],*args])


if __name__=='__main__':
    try:main(sys.argv[1:])
    except Exception as exc:
        print('test Docker wrapper refused: '+type(exc).__name__,file=sys.stderr);raise SystemExit(2)
