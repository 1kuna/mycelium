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
parser.add_argument("--install-home", type=Path)
args = parser.parse_args()
args.scratch.mkdir(parents=True, exist_ok=False)
args.output.mkdir(parents=True, exist_ok=False)
repo = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
archive = subprocess.check_output(["git", "-C", str(repo), "archive", "7285a88", "go.mod", "go.sum", "internal"])
with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
    bundle.extractall(args.scratch, filter="data")
writers = {}
for name in ("queue.go", "types.go", "worker_recovery.go", "models.go", "docker.go", "host_disable.go"):
    data = subprocess.check_output(["git", "-C", str(repo), "show", "7285a88:internal/myce/" + name])
    assert (args.scratch / "internal/myce" / name).read_bytes() == data
    writers[name] = hashlib.sha256(data).hexdigest()
(args.scratch / "internal/myce/wp8_fixture_test.go").write_bytes((Path(__file__).parent / "release-writer_test.go.txt").read_bytes())
for kind, dev, native, idle in (("release-7285a88", "", "", ""), ("release-7285a88-dev", "1", "", ""), ("release-7285a88-native", "", "1", ""), ("release-7285a88-idle", "", "", "1")):
    env = dict(os.environ, WP8_FIXTURE_OUTPUT=str((args.output / kind).resolve()), WP8_FIXTURE_DEV=dev, WP8_FIXTURE_NATIVE=native, WP8_FIXTURE_IDLE=idle)
    subprocess.run(["go", "test", "-C", str(args.scratch), "./internal/myce", "-run", "^TestWriteWP8ReleaseFixture$", "-count=1"], env=env, check=True)
(args.output / "writers.json").write_text(json.dumps({"commit": subprocess.check_output(["git", "-C", str(repo), "rev-parse", "7285a88"], text=True).strip(), "sha256": writers}, indent=2) + "\n")

if args.install_home:
    args.install_home.mkdir(parents=True, exist_ok=False)
    binary = args.output / "synthetic-executable"
    binary.write_text("#!/bin/sh\nexit 99\n")
    binary.chmod(0o700)
    (args.scratch / "internal/myce/wp8_install_fixture_test.go").write_bytes((Path(__file__).parent / "release-install-writer_test.go.txt").read_bytes())
    env = dict(os.environ, MYCE_HOST_STATE_DIR=str(args.install_home / ".local/share/myce"), WP8_REVIEW_RC_HOME=str(args.install_home), WP8_REVIEW_RC_FIXTURE=str(args.output / "release-7285a88-idle"), WP8_REVIEW_RC_BINARY=str(binary))
    subprocess.run(["go", "test", "-C", str(args.scratch), "./internal/myce", "-run", "^TestWriteReviewRCInstallation$", "-count=1"], env=env, check=True)
