"""Replay the bundled native prompt with the frozen reference model and settings."""
import hashlib
import json
import os
import re
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
    if os.environ.get("MYCE_BENCHMARK_OUTPUT"):
        run = subprocess.run(command, stdout=output, stderr=subprocess.PIPE, check=True)
        # Use llama.cpp's completed token evaluation time, excluding model load
        # and prompt evaluation. No invented prompt/model or CPU fallback.
        timing = re.search(rb"(?m)^.*:\s+eval time\s*=\s*([0-9.]+) ms\s*/\s*(\d+) runs", run.stderr)
        if not timing:
            raise SystemExit("llama.cpp token evaluation timing unavailable")
        tokens = int(timing[2])
        Path(os.environ["MYCE_BENCHMARK_OUTPUT"]).write_text(json.dumps({
            "units": tokens, "seconds": float(timing[1]) / 1000,
            "unit": "tokens/s", "reference_units": tokens
        }))
    else:
        subprocess.run(command, stdout=output, check=True)
