"""Replay the real app; never copy the expected output into the actual result."""
import os
from pathlib import Path
import subprocess
import sys

assert "MYCE_TOKEN" not in os.environ, "fleet token inherited"

subprocess.run([sys.executable, str(Path(__file__).with_name("app.py")),
                os.environ["MYCE_CHECK_INPUT"], os.environ["MYCE_CHECK_OUTPUT"]], check=True)
