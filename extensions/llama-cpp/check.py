"""Replay the bundled native prompt with the frozen reference model and settings."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

reference = json.loads(Path(os.environ["MYCE_CHECK_INPUT"]).read_text())
cache = Path(os.environ["MYCE_EXTENSION_CACHE"])
model = cache / reference["model"]
if hashlib.sha256(model.read_bytes()).hexdigest() != reference["sha256"]:
    raise SystemExit("native-reference model hash mismatch")
command = [sys.executable, str(Path(os.environ["MYCE_EXTENSION_DIR"]) / "run.py"),
           "-m", str(model), "-p", reference["prompt"], "-n", str(reference["tokens"]),
           "--seed", str(reference["seed"]), "--temp", str(reference["temperature"]),
           "--no-display-prompt", "--simple-io"]
with Path(os.environ["MYCE_CHECK_OUTPUT"]).open("wb") as output:
    subprocess.run(command, stdout=output, check=True)
