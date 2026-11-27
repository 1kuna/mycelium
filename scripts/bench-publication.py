#!/usr/bin/env python3
"""Measure file publication against the temporary queue started by myce dev."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
from statistics import median
import tempfile
import time
from urllib.parse import urlsplit

from mycelium.client import Client


def report(**facts):
    print(json.dumps(facts), flush=True)


def measure_upload(client, path, delay):
    # Hold a fixed service cost per PATCH at the client boundary. The real
    # queue still accepts, hashes, syncs and publishes every byte.
    file_request = client._file_request_once
    offsets = []

    def timed_request(method, request_path, *args, **kwargs):
        if method == "PATCH":
            offsets.append(int(args[1]["Upload-Offset"]))
            time.sleep(delay)
        return file_request(method, request_path, *args, **kwargs)

    client._file_request_once = timed_request
    try:
        started = time.monotonic()
        ref = client.upload_file(path, project="publication-bench")
        elapsed = time.monotonic() - started
    finally:
        client._file_request_once = file_request
    assert ref["size_bytes"] == path.stat().st_size
    report(test="input", seconds=round(elapsed, 3), patch_requests=len(offsets),
           patch_delay_seconds=delay, mb_per_second=round(ref["size_bytes"] / elapsed / 1e6, 2))


def measure_outputs(client, seed, waiting, samples):
    for i in range(waiting):
        job = client.submit({
            "name": f"backlog-{i}", "project": "publication-backlog", "class": "experiment",
            "needs": {"platform": "cuda", "memory_gb": 1024},
            "run": {"command": ["/usr/bin/false"]},
        })
        assert job["state"] == "queued", job["state"]
    results = []
    for i in range(samples):
        job = client.submit({
            "name": f"output-{i}", "project": "publication-bench", "class": "experiment",
            "needs": {"cpu_cores": 1}, "files": {"seed": seed},
            "outputs": {"result.bin": "results"},
            "run": {"command": ["/bin/sh", "-ec",
                                f"printf '%03d' {i} > result.bin; head -c 999997 /dev/zero >> result.bin"]},
        })
        job = client.wait(job["id"], timeout=60)
        assert job["state"] == "succeeded", job.get("reason")
        job = client.get(job["id"])
        attempt = next(copy for copy in job["copies"] if copy["attempt"] == job["attempt"])
        assert job["output_refs"]["result.bin"]["size_bytes"] == 1_000_000
        released, finished = (datetime.fromisoformat(value.replace("Z", "+00:00"))
                              for value in (attempt["released_at"], job["finished_at"]))
        results.append((finished - released).total_seconds())
        report(test="output", seconds=results[-1], waiting=waiting,
               publication_timings=job.get("publication_timings"))
    report(test="output-p50", seconds=median(results), waiting=waiting, samples=samples)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=128)
    parser.add_argument("--patch-delay", type=float, default=0.17)
    parser.add_argument("--waiting", type=int, default=55)
    parser.add_argument("--samples", type=int, default=7)
    args = parser.parse_args()
    endpoint = os.environ.get("MYCE_QUEUE", "")
    dev_root = os.environ.get("MYCE_DEV_SESSION_ROOT")
    if not dev_root or urlsplit(endpoint).hostname not in {"localhost", "127.0.0.1", "::1"}:
        parser.error("run this benchmark inside myce dev -- COMMAND")
    client = Client(endpoint, os.environ["MYCE_TOKEN"])
    with tempfile.TemporaryDirectory(dir=dev_root) as scratch:
        path = Path(scratch) / "input"
        for label, delay in [("native", 0), ("paced", args.patch_delay)]:
            with path.open("wb") as stream:
                stream.write(label.encode())  # Distinct content prevents dedup.
                stream.truncate(args.size_mib << 20)
            measure_upload(client, path, delay)
        path.write_text("seed")
        seed = client.upload_file(path, project="publication-bench")
        measure_outputs(client, seed, args.waiting, args.samples)


if __name__ == "__main__":
    main()
