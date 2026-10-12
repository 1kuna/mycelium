"""Local immutable core build plus pressure-test overlays, in a fresh retry root."""
from kernel_reads import kernel_read
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile


def sha(path):
    return hashlib.sha256(kernel_read(path,'bytes')).hexdigest()


def build(repo,commit,root,goos,goarch):
    repo=Path(repo).resolve();root=Path(root).resolve()
    core=subprocess.check_output(['git','-C',str(repo),'rev-parse',commit+'^{commit}'],text=True).strip()
    harness=Path(__file__).resolve().parents[2]
    harness_sha=subprocess.check_output(['git','-C',str(harness),'rev-parse','HEAD'],text=True).strip()
    root.mkdir()  # Refuse reuse, including failed preparations.
    for name in ('source','queue','worker-spool','host-control','checkpoints','artifacts'):
        (root/name).mkdir()
    archive=subprocess.check_output(['git','-C',str(repo),'archive',core])
    with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
        stream.extractall(root/'source',filter='data')
    overlays={}
    for name in ('pressure_matrix_test.go','pressure_fixed_test.go'):
        path=harness/'scripts/pressure'/name
        shutil.copyfile(path,root/'source/internal/myce'/name)
        overlays[name]=sha(path)
    env={**os.environ,'GOOS':goos,'GOARCH':goarch,'GOMAXPROCS':'2','GOFLAGS':'-p=1'}
    artifacts=root/'artifacts'
    commands=[['go','build','-o',str(artifacts/'myce'),'./cmd/myce'],
              ['go','test','-c','-o',str(artifacts/'cpu-matrix.test'),'./internal/myce']]
    for index,command in enumerate(commands):
        with (artifacts/f'build-{index}.log').open('x') as log:
            subprocess.run(command,cwd=root/'source',env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    manifest=dict(core_commit=core,harness_commit=harness_sha,goos=goos,goarch=goarch,
                  commands=commands,overlays=overlays,
                  files={name:sha(artifacts/name) for name in ('myce','cpu-matrix.test')},
                  queue_binary='myce',worker_binary='myce',
                  state={name:str(root/name) for name in ('queue','worker-spool','host-control','checkpoints')})
    (artifacts/'build-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--commit',required=True)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--goos',default=subprocess.check_output(['go','env','GOOS'],text=True).strip())
    parser.add_argument('--goarch',default=subprocess.check_output(['go','env','GOARCH'],text=True).strip())
    args=parser.parse_args()
    print(json.dumps(build(args.repo,args.commit,args.root,args.goos,args.goarch)))
