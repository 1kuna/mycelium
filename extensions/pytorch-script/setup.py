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
if os.environ["MYCE_PLATFORM"] == "xpu":
    command += ["--index-url", "https://download.pytorch.org/whl/xpu"]
subprocess.run(command, check=True)
