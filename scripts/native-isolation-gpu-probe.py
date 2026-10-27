#!/usr/bin/env python3
"""Opt-in #177 fixture: standard-library driver APIs; no packages or downloads.

The parent reserves one GPU through Mycelium, uploads this script into a trusted
Worker.EnvironmentRoot, and runs it only through TestNativeIsolationHardware.
Uses the scheduler's existing CUDA/Level Zero visibility mask without rewriting
it. A success receipt means 4096 bytes were allocated, filled, read, and freed,
and the command context was destroyed. It is not an OOM or driver-memory-leak
benchmark. Level Zero declarations follow include/ze_api.h in oneapi-src/level-zero.
"""

import ctypes as c
import os

SIZE = 4096
PATTERN = 0xA5


def api(library, name, result, *arguments):
    fn = getattr(library, name)
    fn.restype = result
    fn.argtypes = arguments

    def checked(*values):
        code = fn(*values)
        if code:
            raise RuntimeError(f"{name}: 0x{code & 0xffffffff:x}")

    return checked


def cuda():
    z = c.CDLL("libcuda.so.1")
    init = api(z, "cuInit", c.c_int, c.c_uint)
    count = api(z, "cuDeviceGetCount", c.c_int, c.POINTER(c.c_int))
    device = api(z, "cuDeviceGet", c.c_int, c.POINTER(c.c_int), c.c_int)
    create = api(z, "cuCtxCreate_v2", c.c_int, c.POINTER(c.c_void_p), c.c_uint, c.c_int)
    destroy = api(z, "cuCtxDestroy_v2", c.c_int, c.c_void_p)
    alloc = api(z, "cuMemAlloc_v2", c.c_int, c.POINTER(c.c_uint64), c.c_size_t)
    free = api(z, "cuMemFree_v2", c.c_int, c.c_uint64)
    fill = api(z, "cuMemsetD8_v2", c.c_int, c.c_uint64, c.c_ubyte, c.c_size_t)
    read = api(z, "cuMemcpyDtoH_v2", c.c_int, c.c_void_p, c.c_uint64, c.c_size_t)
    init(0)
    n = c.c_int()
    count(c.byref(n))
    if n.value != 1:
        raise RuntimeError(f"CUDA mask exposes {n.value} devices, expected exactly one")
    dev = c.c_int()
    device(c.byref(dev), 0)
    context = c.c_void_p()
    create(c.byref(context), 0, dev)
    try:
        pointer = c.c_uint64()
        alloc(c.byref(pointer), SIZE)
        try:
            fill(pointer, PATTERN, SIZE)
            data = (c.c_ubyte * SIZE)()
            read(data, pointer, SIZE)
            if bytes(data) != bytes([PATTERN]) * SIZE:
                raise RuntimeError("CUDA fill/read mismatch")
        finally:
            free(pointer)
    finally:
        destroy(context)


class ContextDesc(c.Structure):
    _fields_ = [("stype", c.c_uint32), ("pNext", c.c_void_p), ("flags", c.c_uint32)]


class DeviceAllocDesc(c.Structure):
    _fields_ = ContextDesc._fields_ + [("ordinal", c.c_uint32)]


class QueueDesc(c.Structure):
    _fields_ = [
        ("stype", c.c_uint32), ("pNext", c.c_void_p), ("ordinal", c.c_uint32),
        ("index", c.c_uint32), ("flags", c.c_uint32), ("mode", c.c_uint32),
        ("priority", c.c_uint32),
    ]


class QueueGroup(c.Structure):
    _fields_ = [
        ("stype", c.c_uint32), ("pNext", c.c_void_p), ("flags", c.c_uint32),
        ("maxMemoryFillPatternSize", c.c_size_t), ("numQueues", c.c_uint32),
    ]


def xpu():
    z = c.CDLL("libze_loader.so.1")
    handle, u32 = c.c_void_p, c.c_uint32
    ph, pu = c.POINTER(handle), c.POINTER(u32)
    init = api(z, "zeInit", u32, u32)
    drivers = api(z, "zeDriverGet", u32, pu, ph)
    devices = api(z, "zeDeviceGet", u32, handle, pu, ph)
    create = api(z, "zeContextCreate", u32, handle, c.POINTER(ContextDesc), ph)
    destroy = api(z, "zeContextDestroy", u32, handle)
    alloc = api(z, "zeMemAllocDevice", u32, handle, c.POINTER(DeviceAllocDesc), c.c_size_t, c.c_size_t, handle, ph)
    host_alloc = api(z, "zeMemAllocHost", u32, handle, c.POINTER(ContextDesc), c.c_size_t, c.c_size_t, ph)
    free = api(z, "zeMemFree", u32, handle, handle)
    groups = api(z, "zeDeviceGetCommandQueueGroupProperties", u32, handle, pu, c.POINTER(QueueGroup))
    immediate = api(z, "zeCommandListCreateImmediate", u32, handle, handle, c.POINTER(QueueDesc), ph)
    list_destroy = api(z, "zeCommandListDestroy", u32, handle)
    fill = api(z, "zeCommandListAppendMemoryFill", u32, handle, handle, handle, c.c_size_t, c.c_size_t, handle, u32, ph)
    read = api(z, "zeCommandListAppendMemoryCopy", u32, handle, handle, handle, c.c_size_t, handle, u32, ph)
    init(1)  # ZE_INIT_FLAG_GPU_ONLY; preserves ZE_AFFINITY_MASK.
    n = u32()
    drivers(c.byref(n), None)
    driver_handles = (handle * n.value)()
    drivers(c.byref(n), driver_handles)
    available = []
    for driver in driver_handles:
        n = u32()
        devices(driver, c.byref(n), None)
        device_handles = (handle * n.value)()
        devices(driver, c.byref(n), device_handles)
        available.extend((driver, dev) for dev in device_handles)
    if len(available) != 1:
        raise RuntimeError(f"Level Zero mask exposes {len(available)} devices, expected exactly one")
    driver, device = available[0]
    n = u32()
    groups(device, c.byref(n), None)
    properties = (QueueGroup * n.value)()
    for prop in properties:
        prop.stype = 6
    groups(device, c.byref(n), properties)
    ordinal = next((i for i, p in enumerate(properties) if p.numQueues and p.flags & 3 and p.maxMemoryFillPatternSize >= 1), None)
    if ordinal is None:
        raise RuntimeError("Level Zero has no memory fill/copy command queue")
    context = handle()
    create(driver, c.byref(ContextDesc(0xD, None, 0)), c.byref(context))
    try:
        pointer = handle()
        alloc(context, c.byref(DeviceAllocDesc(0x15, None, 0, 0)), SIZE, 1, device, c.byref(pointer))
        try:
            host = handle()
            host_alloc(context, c.byref(ContextDesc(0x16, None, 0)), SIZE, 1, c.byref(host))
            try:
                commands = handle()
                # ZE_COMMAND_QUEUE_MODE_SYNCHRONOUS: each append waits for
                # completion, including the device-to-host read, before free.
                immediate(context, device, c.byref(QueueDesc(0xE, None, ordinal, 0, 0, 1, 0)), c.byref(commands))
                try:
                    pattern = c.c_ubyte(PATTERN)
                    fill(commands, pointer, c.byref(pattern), 1, SIZE, None, 0, None)
                    read(commands, host, pointer, SIZE, None, 0, None)
                    if c.string_at(host, SIZE) != bytes([PATTERN]) * SIZE:
                        raise RuntimeError("Level Zero fill/read mismatch")
                finally:
                    list_destroy(commands)
            finally:
                free(context, host)
        finally:
            free(context, pointer)
    finally:
        destroy(context)


if __name__ == "__main__":
    platform = os.environ.get("MYCE_PLATFORM")
    if platform == "cuda":
        cuda()
    elif platform == "xpu":
        xpu()
    else:
        raise RuntimeError("requires scheduler MYCE_PLATFORM=cuda|xpu")
    print(f"native-isolation-gpu-ok:{platform}", flush=True)
