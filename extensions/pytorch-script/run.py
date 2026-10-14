"""Validate the assigned resource, then exec the program unchanged."""
import os
from pathlib import Path
import sys

python = Path(os.environ["MYCE_EXTENSION_CACHE"]) / "venv/bin/python"
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
os.execv(str(python), [str(python), *sys.argv[1:]])
