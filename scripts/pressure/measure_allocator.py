#!/usr/bin/env python3
"""Synthetic XPU allocator/HTTP warm-up target for myce measure; no model inference."""
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import time

p = argparse.ArgumentParser()
p.add_argument('--port', type=int, required=True)
p.add_argument('--receipt', type=Path, required=True)
p.add_argument('--host-mib', type=int, default=512)
p.add_argument('--gpu-mib', type=int, default=1024)
a = p.parse_args()
assert Path('/proc/self/cgroup').read_text().strip() == '0::/system.slice/myce-test-stage-ab-r2d.service'
assert 'MYCE_TOKEN' not in os.environ
assert os.environ.get('ONEAPI_DEVICE_SELECTOR') == 'level_zero:0'
assert os.environ.get('ZE_AFFINITY_MASK') == '0'
assert 0 < a.host_mib <= 4096 and 0 < a.gpu_mib <= 1024
start = int(Path('/proc/self/stat').read_bytes().rsplit(b')', 1)[1].split()[19])
a.receipt.write_text(json.dumps(dict(pid=os.getpid(), start_ticks=start,port=a.port)))
import torch
assert torch.xpu.is_available() and torch.xpu.device_count() == 1
host = bytearray(a.host_mib * 1024**2)
for n in range(0, len(host), 4096):
    host[n] = 1
payload = torch.ones(a.gpu_mib * 1024**2, dtype=torch.uint8, device='xpu:0')
left = torch.ones((1024, 1024), device='xpu:0')
right, result = torch.ones_like(left), torch.empty_like(left)
torch.xpu.synchronize()
a.receipt.with_suffix('.runtime.json').write_text(json.dumps(dict(torch=torch.__version__,
    gpu_count=torch.xpu.device_count(), device=str(torch.xpu.get_device_properties(0)),
    allocated=torch.xpu.memory_allocated(), reserved=torch.xpu.memory_reserved(),
    host_payload_mib=a.host_mib, gpu_payload_mib=a.gpu_mib)))

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b'{}')

    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length', '0')))
        until, kernels = time.monotonic() + 10, 0
        while time.monotonic() < until:
            torch.mm(left, right, out=result); torch.xpu.synchronize(); kernels += 1
        self.send_response(200); self.end_headers()
        self.wfile.write(json.dumps(dict(kernels=kernels)).encode())

HTTPServer(('127.0.0.1', a.port), Handler).serve_forever()
