#!/usr/bin/env python3
"""Small cross-platform allocation with the same durable progress contract."""
import ctypes
import json
from kernel_reads import kernel_read, Vanished
import os
from pathlib import Path
import sys
import time
import pressure

platform=os.environ['MYCE_PLATFORM']
if platform=='xpu' and sys.prefix!='/home/user/verso-labs/runtimes/experiment-xpu-venv':
    python='/home/user/verso-labs/runtimes/experiment-xpu-venv/bin/python'
    os.execv(python,[python,*sys.argv])
events=Path(sys.argv[1]);units=int(sys.argv[2])
assert platform in ('cuda','xpu') and 'MYCE_TOKEN' not in os.environ
assert os.environ['MYCE_MACHINE']==('b70-pressure-gpu1' if platform=='xpu' else 'spark-pressure')
assert os.environ['MYCE_DEVICES']==('0' if platform=='xpu' else 'GPU-00000000-0000-0000-0000-000000000000')
checkpoint=pressure.Checkpoint();saved=checkpoint.load()
job=os.environ['MYCE_JOB_ID'];attempt=int(os.environ['MYCE_ATTEMPT'])
contract=pressure.digest(['portable-allocation-v1',units,16])
n=pressure.validate_save(saved,job,contract,units) if saved else 0
pressure.emit(events,'start',job=job,attempt=attempt,card=platform,resumed=n,units=units,
    resumable=True,contract=contract,pid=os.getpid(),start_ticks=int(kernel_read(Path('/proc/self/stat'), 'bytes').rsplit(b')',1)[1].split()[19]),
    platform=platform,devices=os.environ['MYCE_DEVICES'],machine=os.environ['MYCE_MACHINE'])
host=bytearray(16*1024**2)
for i in range(0,len(host),4096):host[i]=1
if platform=='xpu':
    assert os.environ['ONEAPI_DEVICE_SELECTOR']=='level_zero:0' and os.environ['ZE_AFFINITY_MASK']=='0'
    import torch
    assert torch.xpu.is_available() and torch.xpu.device_count()==1
    payload=torch.ones(16*1024**2,dtype=torch.uint8,device='xpu:0');torch.xpu.synchronize()
else:
    assert os.environ['CUDA_VISIBLE_DEVICES']==os.environ['MYCE_DEVICES']
    cuda=ctypes.CDLL('/usr/local/cuda/lib64/libcudart.so')
    cuda.cudaMalloc.argtypes=[ctypes.POINTER(ctypes.c_void_p),ctypes.c_size_t]
    cuda.cudaMemset.argtypes=[ctypes.c_void_p,ctypes.c_int,ctypes.c_size_t]
    cuda.cudaFree.argtypes=[ctypes.c_void_p]
    count=ctypes.c_int();assert cuda.cudaGetDeviceCount(ctypes.byref(count))==0 and count.value==1
    payload=ctypes.c_void_p();assert cuda.cudaMalloc(ctypes.byref(payload),16*1024**2)==0
    assert cuda.cudaMemset(payload,1,16*1024**2)==0 and cuda.cudaDeviceSynchronize()==0
pressure.emit(events,'allocated',job=job,attempt=attempt,host_mib=16,gpu_mib=16,backend=platform)
with pressure.StopSignal() as stop:
    while n<units and not stop.requested:
        time.sleep(.2)
        checkpoint.save(dict(job=job,contract=contract,unit=n+1,digest=pressure.digest([job,contract,n+1])))
        n+=1;pressure.emit(events,'commit',job=job,attempt=attempt,unit=n)
if platform=='cuda':assert cuda.cudaFree(payload)==0
else:
    del payload;torch.xpu.empty_cache();torch.xpu.synchronize()
pressure.emit(events,'finish',job=job,attempt=attempt,unit=n,complete=n==units)
