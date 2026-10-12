"""Route installer GPU validation into the same hard GPU1 fence as its worker."""
import os
from pathlib import Path
from stage_ab import ROOT, BASE
from disable_drill import user


def install(run,args):
    bindir=ROOT/'validation-bin';bindir.mkdir(exist_ok=True)
    target=bindir/'systemd-run'
    if not target.exists():target.symlink_to(BASE/'validation_wrapper.py')
    previous=run.env['PATH'];run.env['PATH']=str(bindir)+':'+previous
    try:return user(run,args)
    finally:run.env['PATH']=previous
