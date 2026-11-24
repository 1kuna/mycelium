#!/usr/bin/env python3
"""Stream Go output and retain compact, named failures for fleet results."""
import json
import os
import sys

failures = {}
for line in sys.stdin:
    try:
        event = json.loads(line)
    except ValueError:
        print(line, end="", flush=True)
        continue
    if output := event.get("Output"):
        print(output, end="", flush=True)
    if event.get("Action") in ("fail", "build-fail"):
        package = event.get("Package") or event["ImportPath"]
        tests = failures.setdefault(package, set())
        if test := event.get("Test"):
            tests.add(test)

if report := os.environ.get("GATE_FAILURE_REPORT"):
    with open(report, "a") as receipt:
        for package, tests in sorted(failures.items()):
            names = f" ({', '.join(sorted(tests))})" if tests else ""
            receipt.write(f"{package}: failed{names}\n")
