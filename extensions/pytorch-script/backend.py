"""Fail closed on the assigned backend, including a real kernel launch."""
import os
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"
import torch


def assigned_device():
    device = os.environ["MYCE_DEVICE"]
    available = {"mps": torch.backends.mps.is_available,
                 "cuda:0": torch.cuda.is_available,
                 "xpu:0": torch.xpu.is_available}
    if device not in available or not available[device]():
        raise SystemExit("assigned PyTorch resource unavailable; no CPU fallback")
    if device == "cuda:0" and not torch.version.cuda:
        raise SystemExit("installed PyTorch has no CUDA build; setup refused")
    return device


if __name__ == "__main__":
    device = assigned_device()
    # Availability alone does not prove that this wheel can execute kernels on
    # the assigned GPU architecture (for example GB10/sm_121).
    x = torch.tensor([1.0, 2.0], device=device)
    if (x * x).cpu().tolist() != [1.0, 4.0]:
        raise SystemExit("assigned PyTorch backend kernel check failed")
    print("PyTorch", torch.__version__, "backend", device, "kernel check passed")
