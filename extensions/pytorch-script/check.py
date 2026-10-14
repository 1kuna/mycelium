"""Replay bundled inputs on the assigned accelerator, never known-answer grading."""
import json
import os
from pathlib import Path
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
import torch

resource = os.environ["MYCE_DEVICE"]
available = {"mps": torch.backends.mps.is_available,
             "cuda:0": torch.cuda.is_available,
             "xpu:0": torch.xpu.is_available}
if resource not in available or not available[resource]():
    raise SystemExit("assigned PyTorch resource unavailable; no CPU fallback")
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
values = json.loads(Path(os.environ["MYCE_CHECK_INPUT"]).read_text())
x = torch.tensor(values, dtype=torch.float32, device=resource)
result = ((x * x) + (x * 2)).cpu().tolist()
Path(os.environ["MYCE_CHECK_OUTPUT"]).write_text(json.dumps(result) + "\n")
