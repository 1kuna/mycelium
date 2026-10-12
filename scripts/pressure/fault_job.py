#!/usr/bin/env python3
"""Pause one synthetic command at startup or checkpoint-file fsync, never a service."""
import os
from kernel_reads import kernel_read, Vanished
from pathlib import Path
import sys
import time
import pressure

boundary, events = sys.argv[1], Path(sys.argv[2])
sys.argv = [sys.argv[0]] + sys.argv[3:]
assert boundary in ('startup','save')
original_emit, original_fsync = pressure.emit, os.fsync
paused = False

def pause():
    global paused
    if paused:return
    paused = True
    original_emit(events,'fault',boundary=boundary,job=os.environ['MYCE_JOB_ID'])
    time.sleep(5)

def emit(path,kind,**fields):
    original_emit(path,kind,**fields)
    if boundary=='startup' and kind=='start':pause()

def fsync(fd):
    if boundary=='save':
        target=Path(kernel_read(f'/proc/self/fd/{fd}', 'link'))
        if target.parent==Path(os.environ['MYCE_CHECKPOINT_DIR']) and target.suffix=='.tmp':pause()
    return original_fsync(fd)

pressure.emit=emit
os.fsync=fsync
raise SystemExit(pressure.main())
