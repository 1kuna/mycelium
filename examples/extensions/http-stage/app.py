"""An existing CPU app: keep its CLI/result contract and swap SERVICE_URL only."""
import json
import os
from pathlib import Path
import sys
from urllib.request import urlopen

with urlopen(os.environ["SERVICE_URL"] + "/value", timeout=5) as response:
    increment = json.load(response)["increment"]
value = json.loads(Path(sys.argv[1]).read_text())["value"]
Path(sys.argv[2]).write_text(json.dumps({"value": value + increment}) + "\n")
