"""Small explicit setup; downloads and the environment stay in Mycelium's cache."""
import os
from pathlib import Path
import subprocess
import sys

cache = Path(os.environ["MYCE_EXTENSION_CACHE"])
venv = cache / "venv"
if not (venv / "bin/python").exists():
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
command = [str(venv / "bin/python"), "-m", "pip", "install", "--cache-dir",
           str(cache / "downloads"), "torch==2.8.0"]
if os.environ["MYCE_PLATFORM"] == "cuda":
    if sys.platform != "linux":
        raise SystemExit("CUDA setup requires Linux")
    # Official CUDA 13 wheels include Linux aarch64 and x86_64, including GB10.
    command[-1] = "torch==2.9.1+cu130"
    command += ["--index-url", "https://download.pytorch.org/whl/cu130"]
elif os.environ["MYCE_PLATFORM"] == "xpu":
    command += ["--index-url", "https://download.pytorch.org/whl/xpu"]
subprocess.run(command, check=True)

# Setup success requires the assigned backend, not merely a successful pip exit.
subprocess.run([str(venv / "bin/python"),
                str(Path(os.environ["MYCE_EXTENSION_DIR"]) / "backend.py")], check=True)
