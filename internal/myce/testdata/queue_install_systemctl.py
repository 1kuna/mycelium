#!/usr/bin/env python3
"""Opt-in native queue test only: exact unit-name and runtime-definition adapter.

No fake service lifecycle. All permitted operations reach the real user manager.
Persistent enablement is replaced by runtime enablement for cleanup/isolation.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

args = sys.argv[1:]
unit = os.environ["MYCE_TEST_QUEUE_UNIT"]
if not unit.startswith("myce-cutover-test-") or not unit.endswith(".service") or "/" in unit:
    sys.exit("invalid isolated unit")
if len(args) < 2 or args[0] != "--user":
    sys.exit("only the user manager is permitted")
action = args[1]
if action == "daemon-reload":
    if len(args) != 2:
        sys.exit("unexpected reload arguments")
    source = Path(os.environ["MYCE_TEST_QUEUE_DEFINITION"])
    target = Path(os.environ["XDG_RUNTIME_DIR"]) / "systemd/user" / unit
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.exists():
        target.write_bytes(source.read_bytes())
        target.chmod(0o600)
else:
    if action not in {"show", "is-active", "enable", "disable", "start", "stop", "restart"}:
        sys.exit("unexpected manager operation")
    if args.count("myce-queue.service") != 1:
        sys.exit("only myce-queue.service may be mapped")
    flags = {
        "show": {"--property=UnitFileState", "--property=DropInPaths",
                 "--property=TimeoutStopUSec", "--property=TimeoutStartUSec", "--value"},
        "is-active": {"--quiet"}, "enable": {"--now", "--runtime"},
        "disable": {"--now"}, "start": set(), "stop": set(), "restart": set(),
    }
    if any(arg != "myce-queue.service" and arg not in flags[action] for arg in args[2:]):
        sys.exit("unexpected unit or flag")
    args = [unit if arg == "myce-queue.service" else arg for arg in args]
    if action == "enable" and "--runtime" not in args:
        args.insert(2, "--runtime")

def event(phase, code=None):
    row = {"event": "manager_" + phase, "action": action, "at_ns": time.time_ns()}
    if code is not None:
        row["exit"] = code
    with open(os.environ["MYCE_TEST_QUEUE_EVENTS"], "a") as output:
        output.write(json.dumps(row) + "\n")

event("start")
result = subprocess.run([os.environ["MYCE_TEST_SYSTEMCTL"], *args])
event("end", result.returncode)
sys.exit(result.returncode)
