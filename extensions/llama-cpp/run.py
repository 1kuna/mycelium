"""Force accelerator use; backend availability is established by qualification."""
import os
from pathlib import Path
import sys

args = sys.argv[1:]
if any(a.split("=")[0] in {"-ngl", "--gpu-layers", "--n-gpu-layers", "--device", "-dev", "--split-mode", "-sm", "--tensor-split", "-ts", "--main-gpu", "-mg"} for a in args):
    raise SystemExit("resource/offload options are owned by the extension")
cache = Path(os.environ["MYCE_EXTENSION_CACHE"])
binary = cache / "build/bin/llama-cli"
backend = (cache / "backend").read_text().strip()
# llama.cpp names are framework-local after the worker's visibility mask.
resource = {"metal": "Metal", "cuda": "CUDA0", "sycl": "SYCL0", "vulkan": "Vulkan0"}[backend]
os.execv(str(binary), [str(binary), "--device", resource, "-ngl", "999", *args])
