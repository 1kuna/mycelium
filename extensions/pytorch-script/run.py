"""Run the program on the assigned resource and report observed memory."""
import os
from pathlib import Path
import sys
import json
import runpy
import resource
import threading

# This file is entered with cached Python via worker-owned PATH. Checking the
# actual backend must precede running arbitrary programs; CPU is never selected.
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
import torch

device = os.environ["MYCE_DEVICE"]
available = {"mps": torch.backends.mps.is_available,
             "cuda:0": torch.cuda.is_available,
             "xpu:0": torch.xpu.is_available}
if device not in available or not available[device]():
    raise SystemExit("assigned PyTorch resource unavailable; no CPU fallback")
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"

# CUDA/XPU expose allocator high-water marks; MPS currently exposes only current
# allocation. Its bounded sampler reports an observed peak, not an exact peak.
backend = torch.mps if device == "mps" else (torch.cuda if device == "cuda:0" else torch.xpu)
peak = 0
stopped = threading.Event()
oom = False

def observe():
    global peak
    try:
        if device == "mps":
            peak = max(peak, backend.current_allocated_memory())
        else:
            peak = max(peak, backend.max_memory_reserved())
    except (RuntimeError, AttributeError):
        pass  # Unknown remains unknown, never an invented budget.

def sample():
    while not stopped.wait(0.05):
        observe()

observer = threading.Thread(target=sample, daemon=True)
observer.start()
sys.argv = sys.argv[1:]
sys.path.insert(0, str(Path(sys.argv[0]).resolve().parent))
try:
    runpy.run_path(sys.argv[0], run_name="__main__")
except RuntimeError as error:
    oom = isinstance(error, torch.OutOfMemoryError) or str(error).startswith("MPS backend out of memory")
    raise
finally:
    stopped.set()
    observer.join()
    observe()
    # The worker owns this attempt-specific path; it is not a published output.
    path = os.environ.get("MYCE_USAGE_FILE")
    if path:
        with open(path, "x", encoding="utf-8") as report:
            ram = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform != "darwin":
                ram *= 1024
            json.dump({"memory_gb": peak / (1 << 30),
                       "host_memory_gb": ram / (1 << 30),
                       "out_of_memory": oom}, report)
