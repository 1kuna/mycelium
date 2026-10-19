#!/usr/bin/env python3
"""Reproduce release fixtures with the original writers; never reads host state."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument("--scratch", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
args.scratch.mkdir(parents=True, exist_ok=False)
args.output.mkdir(parents=True, exist_ok=False)
repo = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
archive = subprocess.check_output(["git", "archive", "7285a88", "go.mod", "go.sum", "internal"])
with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
    bundle.extractall(args.scratch, filter="data")
writers = {}
for name in ("queue.go", "types.go", "worker_recovery.go", "models.go", "docker.go", "host_disable.go"):
    data = subprocess.check_output(["git", "show", "7285a88:internal/myce/" + name])
    assert (args.scratch / "internal/myce" / name).read_bytes() == data
    writers[name] = hashlib.sha256(data).hexdigest()
(args.scratch / "internal/myce/wp8_fixture_test.go").write_bytes((Path(__file__).parent / "release-writer_test.go.txt").read_bytes())
for kind, dev in (("release-7285a88", ""), ("release-7285a88-dev", "1")):
    env = dict(os.environ, WP8_FIXTURE_OUTPUT=str((args.output / kind).resolve()), WP8_FIXTURE_DEV=dev)
    subprocess.run(["go", "test", "-C", str(args.scratch), "./internal/myce", "-run", "^TestWriteWP8ReleaseFixture$", "-count=1"], env=env, check=True)
(args.output / "writers.json").write_text(json.dumps({"commit": subprocess.check_output(["git", "rev-parse", "7285a88"], text=True).strip(), "sha256": writers}, indent=2) + "\n")
