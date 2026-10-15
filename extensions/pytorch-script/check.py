"""Replay bundled inputs on the assigned accelerator, never known-answer grading."""
import json
import os
from pathlib import Path
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
import torch

from backend import assigned_device
resource = assigned_device()
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
values = json.loads(Path(os.environ["MYCE_CHECK_INPUT"]).read_text())
x = torch.tensor(values, dtype=torch.float32, device=resource)
result = ((x * x) + (x * 2)).cpu().tolist()
Path(os.environ["MYCE_CHECK_OUTPUT"]).write_text(json.dumps(result) + "\n")
