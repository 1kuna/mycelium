#!/usr/bin/env python3
"""Synthetic commands and a fail-closed evidence checker; never controls fleet services."""
from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
from typing import Any
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "clients/python/src"))
from mycelium import Checkpoint, StopSignal

FINAL = {"succeeded", "failed", "cancelled"}


def clock_domain() -> str:
    path = Path("/proc/sys/kernel/random/boot_id")
    return path.read_text().strip() if path.exists() else "local-" + socket.gethostname()


def emit(path: Path, kind: str, *, at: float | None = None, **fields: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps(dict(kind=kind, at=time.monotonic() if at is None else at,
                                    boot=clock_domain(), **fields), allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def validate_save(saved: dict[str, Any], job: str, contract: str, units: int) -> int:
    n = saved.get("unit")
    if (saved.get("job") != job or saved.get("contract") != contract
            or type(n) is not int or not 0 <= n <= units
            or saved.get("digest") != digest([job, contract, n])):
        raise ValueError("checkpoint identity/integrity mismatch")
    return n


def workload(a: argparse.Namespace) -> int:
    jid, attempt = os.environ["MYCE_JOB_ID"], int(os.environ["MYCE_ATTEMPT"])
    if not re.fullmatch(r"[A-Za-z0-9_-]+", a.card):
        raise ValueError("card must be a plain stable identifier")
    if "MYCE_TOKEN" in os.environ:
        emit(a.events, "violation", code="token_present", job=jid)
        return 90
    checkpoint = Checkpoint()
    contract = digest([a.units, a.unit_seconds, a.host_mib, a.gpu_mib, a.backend, a.resumable])
    saved = checkpoint.load() if a.resumable else None
    n = validate_save(saved, jid, contract, a.units) if saved is not None else 0
    lock_path = a.events.parent / ("card-" + a.card + ".lock")
    with lock_path.open("a+") as lock, StopSignal() as stop:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            emit(a.events, "violation", code="test_card_overlap", job=jid, attempt=attempt, card=a.card)
            return 91
        # This lock detects participating test commands, not arbitrary GPU holders.
        emit(a.events, "start", job=jid, attempt=attempt, card=a.card,
             resumed=n, units=a.units, contract=contract, resumable=a.resumable, pid=os.getpid(), start_ticks=(
                 int(Path("/proc/self/stat").read_bytes().rsplit(b")", 1)[1].split()[19])
                 if sys.platform == "linux" else None))
        host = bytearray(a.host_mib * 1024 * 1024)
        for offset in range(0, len(host), 4096):
            host[offset] = 1
        torch = None
        selector = a.selector if a.selector is not None else a.card
        if a.backend != "cpu":
            if a.backend == "xpu" and (os.environ.get("ONEAPI_DEVICE_SELECTOR") != "level_zero:" + selector
                                       or os.environ.get("ZE_AFFINITY_MASK") != selector):
                raise RuntimeError("XPU worker selector does not match approved physical card")
            import torch
            backend = getattr(torch, a.backend)
            if not backend.is_available() or backend.device_count() != 1:
                raise RuntimeError("expected exactly one visible real GPU; no fallback")
            device = a.backend + ":0"
            # Explicit payload plus three matrices; reserve runtime/driver overhead separately.
            payload = torch.ones(a.gpu_mib * 1024 * 1024, dtype=torch.uint8, device=device)
            left = torch.ones((1024, 1024), device=device)
            right, result = torch.ones_like(left), torch.empty_like(left)
            backend.synchronize()
        kernels, began = 0, time.monotonic()
        emit(a.events, "allocated", job=jid, attempt=attempt, card=a.card,
             host_mib=a.host_mib, gpu_mib=a.gpu_mib, backend=a.backend)
        while n < a.units and (a.refuse_stop or not stop.requested):
            until = time.monotonic() + a.unit_seconds
            while time.monotonic() < until:
                if torch is None:
                    time.sleep(min(0.01, max(0, until - time.monotonic())))
                else:
                    torch.mm(left, right, out=result)
                    backend.synchronize()
                    kernels += 1
            if a.resumable:
                while True:
                    try:
                        checkpoint.save(dict(job=jid, contract=contract, unit=n + 1,
                                             digest=digest([jid, contract, n + 1])))
                        break
                    except OSError:
                        emit(a.events, "save_retry", job=jid, attempt=attempt)
                        time.sleep(0.2)
            n += 1
            emit(a.events, "commit", job=jid, attempt=attempt, unit=n)
        if torch is not None:
            del payload, left, right, result
            backend.empty_cache()
            backend.synchronize()
        emit(a.events, "finish", job=jid, attempt=attempt, unit=n, complete=n == a.units,
             stopped=stop.requested, card=a.card, kernels=kernels, compute_seconds=time.monotonic() - began)
    return 0


class Checker:
    """Monotonic observer deadlines remain in force during faults and outages."""

    def __init__(self, bound: float, require_physical: bool = False, cards: tuple[str, ...] = ()) -> None:
        self.bound = bound
        self.require_physical = require_physical
        self.cards = cards
        self.jobs: dict[str, dict[str, Any]] = {}
        self.attempts: dict[tuple[str, int], dict[str, Any]] = {}
        self.saved: dict[str, int] = {}
        self.commits: set[tuple[str, int]] = set()
        self.terminal: dict[str, tuple[str, int]] = {}
        self.errors: set[str] = set()
        self.last_physical: tuple[str, float] | None = None
        self.domain: str | None = None

    def feed(self, e: dict[str, Any]) -> None:
        at, boot, kind = e["at"], e["boot"], e["kind"]
        if type(at) not in (int, float) or not math.isfinite(at) or at < 0 or not isinstance(boot, str) or not boot:
            raise ValueError("invalid monotonic timestamp")
        if self.domain is not None and self.domain != boot:
            self.errors.add("clock_domain_changed: reconcile outstanding deadlines before new boot")
        self.domain = boot
        if kind == "submit":
            if e["job"] in self.jobs:
                self.errors.add("duplicate_submission")
            self.jobs[e["job"]] = dict(at=at, boot=boot, priority=e["priority"], units=e["units"],
                                       started=any(j == e["job"] for j, _ in self.attempts))
            if e["priority"] == "production" and any(
                    j == e["job"] and start["at"] - at > self.bound
                    for (j, _), start in self.attempts.items()):
                self.errors.add("production_start_bound_exceeded")
        elif kind == "start":
            key = (e["job"], e["attempt"])
            if key in self.attempts:
                self.errors.add("duplicate_attempt_start")
            self.attempts[key] = e
            if e["job"] in self.jobs:
                j = self.jobs[e["job"]]
                if j["priority"] == "production" and at - j["at"] > self.bound:
                    self.errors.add("production_start_bound_exceeded")
                j["started"] = True
            if e["resumable"] and e["resumed"] < self.saved.get(e["job"], 0):
                self.errors.add("saved_units_lost")
            self.saved[e["job"]] = max(self.saved.get(e["job"], 0), e["resumed"] if e["resumable"] else 0)
        elif kind == "commit":
            key = (e["job"], e["attempt"])
            start = self.attempts[key]
            unit = e["unit"]
            if type(unit) is not int or not start["resumed"] < unit <= start["units"]:
                self.errors.add("invalid_unit")
            if (e["job"], unit) in self.commits:
                self.errors.add("completed_unit_duplicated")
            self.commits.add((e["job"], unit))
            if start["resumable"]:
                self.saved[e["job"]] = max(self.saved.get(e["job"], 0), unit)
        elif kind == "finish":
            self.attempts[(e["job"], e["attempt"])]["finish"] = e
        elif kind == "status":
            occupied: set[str] = set()
            if not self.jobs.keys() <= {j["id"] for j in e["jobs"]}:
                self.errors.add("submitted_job_missing")
            for j in e["jobs"]:
                jid = j["id"]
                if jid not in self.jobs:
                    continue
                if j["state"] in {"running", "stopping"}:
                    if j["machine"] in occupied:
                        self.errors.add("queue_double_owner")
                    occupied.add(j["machine"])
                signature = (j["state"], j["attempt"])
                if jid in self.terminal and signature != self.terminal[jid]:
                    self.errors.add("terminal_regressed")
                if j["state"] in FINAL:
                    self.terminal[jid] = signature
                    if j["state"] == "succeeded":
                        record = self.attempts.get((jid, j["attempt"]), {})
                        if not record.get("finish", {}).get("complete"):
                            self.errors.add("success_without_complete_output")
                    elif j["state"] == "failed":
                        self.errors.add("job_failed")
        elif kind == "physical":
            if (type(e["valid"]) is not bool or not isinstance(e["owners"], dict)
                    or not e["owners"] and e["valid"]
                    or any(not isinstance(v, list) or any(not isinstance(x, str) or not x for x in v)
                           for v in e["owners"].values())):
                raise ValueError("invalid physical ownership evidence")
            if not set(self.cards) <= e["owners"].keys():
                self.errors.add("physical_scope_missing")
            if not e["valid"]:
                self.errors.add("physical_ownership_unknown")
            else:
                self.last_physical = (boot, at)
                if any(len(owners) > 1 for owners in e["owners"].values()):
                    self.errors.add("physical_double_owner")
        elif kind == "memory":
            if not e["valid"]:
                self.errors.add("host_memory_unknown")
            elif e["available_gib"] < e["minimum_available_gib"]:
                self.errors.add("host_memory_headroom_exceeded")
        elif kind == "violation":
            self.errors.add(e["code"])
        elif kind not in {"allocated", "save_retry", "outage", "tick", "fault"}:
            raise ValueError("unknown evidence event")
        for j in self.jobs.values():
            if j["priority"] == "production" and j["boot"] == boot:
                if not j["started"] and at - j["at"] > self.bound:
                    self.errors.add("production_start_bound_exceeded")
        if self.require_physical and (self.last_physical is None or self.last_physical[0] != boot
                                     or at - self.last_physical[1] > 3):
            self.errors.add("physical_evidence_missing_or_stale")

    def finish(self) -> dict[str, Any]:
        if not self.jobs:
            self.errors.add("no_submitted_jobs")
        for jid in self.jobs:
            if jid not in self.terminal:
                self.errors.add("job_not_terminal")
            state = self.terminal.get(jid, (None, 0))
            if state[0] == "succeeded" and self.jobs[jid]["units"] != self.attempts.get((jid, state[1]), {}).get("finish", {}).get("unit"):
                self.errors.add("incomplete_units")
        return dict(jobs=len(self.jobs), attempts=len(self.attempts), violations=sorted(self.errors))


def replay(path: Path, checker: Checker, offset: int = 0) -> int:
    with path.open() as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        stream.seek(offset)
        while line := stream.readline(1024 * 1024 + 1):
            if len(line) > 1024 * 1024 or not line.endswith("\n"):
                raise ValueError("oversized evidence record")
            checker.feed(json.loads(line))
        return stream.tell()


def api(url: str, method: str = "GET", path: str = "/api/status", data: Any = None, timeout: float = 2) -> Any:
    req = urllib.request.Request(url + path, method=method,
        data=None if data is None else json.dumps(data).encode(),
        headers={"Authorization": "Bearer " + os.environ["MYCE_TOKEN"], "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def observe(url: str, events: Path, timeout: float = 2) -> None:
    status = api(url, timeout=timeout)
    # Never copy command arguments, input paths, logs, names, or secrets into evidence.
    emit(events, "status", jobs=[{k: j[k] for k in ("id", "state", "attempt", "machine") if k in j}
                                 for j in status.get("jobs") or []])


def drm_inventory(pcis: list[str], proc: Path = Path("/proc")) -> list[dict[str, Any]]:
    """One receipt per process, with unique device/client pairs and measured memory."""
    if not pcis or any(not re.fullmatch(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]", p) for p in pcis):
        raise ValueError("explicit lowercase PCI identities required")
    nodes = None
    if proc == Path("/proc") and sys.platform == "linux":
        nodes = set()
        for pci in pcis:
            directory = Path("/sys/bus/pci/devices") / pci / "drm"
            if not directory.is_dir():
                raise ValueError("PCI DRM device mapping unavailable")
            nodes.update("/dev/dri/" + p.name for p in directory.iterdir()
                         if re.fullmatch(r"(?:card|renderD)[0-9]+", p.name))
        if not nodes:
            raise ValueError("PCI DRM device nodes unavailable")
    holders = []
    for pid in proc.iterdir():
        if not pid.name.isdigit():
            continue
        try:
            before = (pid / "stat").read_bytes()
            fields = before[before.rfind(b")") + 2:].split()
            group, start = int(fields[2]), int(fields[19])
            clients = {}
            # Keep the collector's own directory descriptor alive during inspection.
            with os.scandir(pid / "fdinfo") as descriptors:
                for info in descriptors:
                    descriptor = pid / "fd" / info.name
                    raw = None
                    for retry in range(3):
                        try:
                            target = os.readlink(descriptor)
                            if not target.startswith("/dev/dri/") or nodes is not None and target not in nodes:
                                break
                            with open(info.path, "rb") as stream:
                                raw = stream.read(65537)
                            if os.readlink(descriptor) != target:
                                raw = None
                                if retry == 2:raise ValueError("DRM descriptor repeatedly reused")
                                continue
                            break
                        except OSError as error:
                            if error.errno not in (errno.ENOENT, errno.ESRCH):
                                raise
                            raw = None
                            # fdinfo can linger after closure; confirm against a fresh fd listing.
                            with os.scandir(pid / "fd") as current:
                                if info.name not in {entry.name for entry in current}:
                                    break
                            if retry == 2:raise
                    if raw is None:
                        continue
                    if len(raw) > 65536:
                        raise ValueError("oversized fdinfo")
                    metrics = {line.split(b":", 1)[0].decode("ascii"):
                               line.split(b":", 1)[1].strip().decode("ascii")
                               for line in raw.splitlines() if line.startswith(b"drm-")}
                    pci = metrics.get("drm-pdev")
                    if pci is None:
                        raise ValueError("DRM descriptor lacks PCI identity")
                    if pci not in pcis:
                        continue
                    client = metrics.get("drm-client-id", "")
                    if not client.isdigit():
                        raise ValueError("DRM descriptor lacks numeric client identity")
                    resident = {}
                    for key, value in metrics.items():
                        if key.startswith("drm-resident-"):
                            parts = value.split()
                            if len(parts) == 1 and parts[0] == "0":
                                resident[key] = 0
                            elif len(parts) == 2 and parts[0].isdigit() and parts[1] in {"B", "bytes", "KiB", "MiB", "GiB"}:
                                resident[key] = int(parts[0]) * {"B":1,"bytes":1,"KiB":1024,"MiB":1024**2,"GiB":1024**3}[parts[1]]
                            else:
                                raise ValueError("unsupported DRM memory units: " + key + "=" + repr(value))
                    key = (pci, target, int(client))
                    old = clients.get(key, {}).get("resident_bytes", {})
                    clients[key] = dict(pci=pci, device=target, client=int(client),
                                        resident_bytes={k: max(v, old.get(k, 0)) for k, v in resident.items()})
            if not clients:
                continue
            after = (pid / "stat").read_bytes()
            after_fields = after[after.rfind(b")") + 2:].split()
            if (int(after_fields[2]), int(after_fields[19])) != (group, start):
                raise ValueError("process identity changed during DRM sample")
            executable=os.readlink(pid/'exe')
            stats_readonly=(os.path.basename(executable)=='xpu-smi' and
                (pid/'cmdline').read_bytes().split(b'\0')[1:5]==[b'stats',b'-d',b'0',b'--json'])
            holders.append(dict(boot=clock_domain(), pid=int(pid.name), start=start,
                                ppid=int(after_fields[1]),stats_readonly=stats_readonly,
                                exe=executable, cgroup=(pid / "cgroup").read_text().strip(),
                                clients=sorted(clients.values(), key=lambda c: (c["pci"], c["device"], c["client"]))))
        except OSError as error:
            if error.errno in (errno.ENOENT, errno.ESRCH):
                if pid.name not in {entry.name for entry in proc.iterdir()}:
                    continue
            raise
    return sorted(holders, key=lambda h: h["pid"])


def drm_identity(holder: dict[str, Any]) -> dict[str, Any]:
    return {**{k: holder[k] for k in ("boot", "pid", "start", "exe", "cgroup")},
            "clients": [{k: c[k] for k in ("pci", "device", "client")} for c in holder["clients"]]}


def drm_owners(pcis: list[str], proc: Path = Path("/proc"), *,
               baseline: list[dict[str, Any]] | None = None,
               allowed: list[dict[str, Any]] | None = None,
               observers: list[dict[str, Any]] = (),
               evidence: dict[str, Any] | None = None) -> dict[str, list[str]]:
    """Pinned display baseline is separate from allocation owners; unknowns fail closed."""
    holders = drm_inventory(pcis, proc)
    allowed = allowed() if callable(allowed) else allowed
    observers = observers(holders) if callable(observers) else observers
    if evidence is not None:evidence["holders"] = holders
    owners: dict[str, set[str]] = {p: set() for p in pcis}
    seen_baseline = []
    shared = {}
    for holder in holders:
        identity = drm_identity(holder)
        if baseline is not None and identity in baseline:
            seen_baseline.append(identity)
            for c in holder["clients"]:
                key = (c["pci"], c["client"])
                old = shared.get(key, {})
                shared[key] = {k: max(v, old.get(k, 0)) for k, v in c["resident_bytes"].items()}
            continue
        process = {k: holder[k] for k in ("boot", "pid", "start", "exe", "cgroup")}
        if process in observers:
            continue  # Exact PID/start identity of an operator-launched passive counter reader.
        if allowed is not None and process not in allowed:
            raise ValueError("UNKNOWN GPU user: " + digest(identity)[:24])
        token = digest([holder["boot"], holder["pid"], holder["start"]])[:24]
        for c in holder["clients"]:
            owners[c["pci"]].add(token)
    if baseline is not None and len(seen_baseline) != len(baseline):
        raise ValueError("display baseline identity changed or disappeared")
    if evidence is not None:
        evidence.update(holders=holders, baseline_clients=[dict(pci=p, client=c, resident_bytes=m)
                        for (p, c), m in sorted(shared.items())])
    return {pci: sorted(tokens) for pci, tokens in owners.items()}


def submit(a: argparse.Namespace, url: str, priority: str, units: int) -> str:
    started = time.monotonic()
    j = api(url, "POST", "/api/jobs", spec(a, priority, units))
    emit(a.events, "submit", at=started, job=j["id"], priority=priority, units=units)
    return j["id"]


def spec(a: argparse.Namespace, priority: str, units: int) -> dict[str, Any]:
    command = [a.python, str(Path(__file__).resolve()), "job", "--events", str(a.events.resolve()),
               "--card", a.card, "--backend", a.backend, "--units", str(units),
               "--unit-seconds", str(a.unit_seconds), "--host-mib", str(a.host_mib), "--gpu-mib", str(a.gpu_mib)]
    if getattr(a,"selector",None) is not None:
        command.extend(["--selector",a.selector])
    if priority == "experiment":
        command.append("--resumable")
    if a.refuse_stop:
        command.append("--refuse-stop")
    return dict(name="pressure-" + priority, project="verso-synthetic-pressure", priority=priority,
                run=dict(command=command, env={"PYTHONUNBUFFERED": "1"}),
                needs=dict(memory_gb=a.memory_gib, host_memory_gb=a.host_reservation_gib,
                           platform=dict(required=["fake" if a.backend == "cpu" else a.backend])),
                allowed_machines=[a.machine], resumable=priority == "experiment")


def smoke(a: argparse.Namespace) -> int:
    """One real local queue, fake inventory; experiment -> production -> resume."""
    root = a.events.parent.resolve()
    root.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_")}
    env["MYCE_TOKEN"] = "local-synthetic-only"
    os.environ["MYCE_TOKEN"] = env["MYCE_TOKEN"]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    url = "http://127.0.0.1:" + str(port)
    log = (root / "dev.log").open("w")
    process = subprocess.Popen([str(a.binary.resolve()), "dev", "--listen", "127.0.0.1:" + str(port),
        "--workers", "1", "--memory-gb", "8", "--db", str(root / "queue.db"),
        "--checkpoints", str(root / "checkpoints")], env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 60
        while True:
            if process.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("local dev startup failed")
            try:
                api(url)
                break
            except OSError:
                time.sleep(0.05)
        submit(a, url, "experiment", a.units)
        while not list((root / "checkpoints").glob("*/attempt-*/checkpoint.json")):
            observe(url, a.events)
            if time.monotonic() > deadline:
                raise RuntimeError("experiment checkpoint timeout")
            time.sleep(0.05)
        submit(a, url, "production", 1)
        checker, offset = Checker(a.bound), 0
        while True:
            observe(url, a.events)
            offset = replay(a.events, checker, offset)
            if checker.errors:
                raise RuntimeError("local invariant violation: " + ",".join(sorted(checker.errors)))
            if len(checker.terminal) == 2:
                result = checker.finish()
                if not any(e["attempt"] > 1 and e["resumed"] > 0 for e in checker.attempts.values()):
                    result["violations"].append("no_checkpoint_resume_observed")
                Checkpoint(root).save(result, "result.json")
                print(json.dumps(result))
                return bool(result["violations"])
            if time.monotonic() > deadline:
                raise RuntimeError("local smoke completion timeout")
            time.sleep(0.1)
    finally:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        log.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=["job", "spec", "submit", "check", "watch", "physical", "smoke"])
    p.add_argument("--events", type=Path, required=True)
    p.add_argument("--backend", choices=["cpu", "xpu", "cuda"], default="cpu")
    p.add_argument("--card", default="fake-0")
    p.add_argument("--selector", help="explicit runtime index from qualified PCI/device-fence mapping")
    p.add_argument("--machine", default="fake-1")
    p.add_argument("--python", default=sys.executable)
    p.add_argument("--units", type=int, default=30)
    p.add_argument("--unit-seconds", type=float, default=0.2)
    p.add_argument("--host-mib", type=int, default=8)
    p.add_argument("--gpu-mib", type=int, default=256)
    p.add_argument("--memory-gib", type=float, default=8)
    p.add_argument("--host-reservation-gib", type=float, default=0.25)
    p.add_argument("--priority", choices=["production", "experiment"], default="experiment")
    p.add_argument("--resumable", action="store_true")
    p.add_argument("--refuse-stop", action="store_true")
    p.add_argument("--bound", type=float, default=30)
    p.add_argument("--require-physical", action="store_true")
    p.add_argument("--minimum-available-gib", type=float, default=4)
    p.add_argument("--pci", action="append", default=[])
    p.add_argument("--queue", default=os.environ.get("MYCE_QUEUE"))
    p.add_argument("--binary", type=Path)
    a = p.parse_args()
    if (a.units <= 0 or a.host_mib < 0 or a.gpu_mib < 0 or not math.isfinite(a.unit_seconds)
            or a.unit_seconds <= 0 or not math.isfinite(a.bound) or a.bound <= 0
            or not math.isfinite(a.memory_gib) or a.memory_gib <= 0
            or not math.isfinite(a.host_reservation_gib) or a.host_reservation_gib < a.host_mib / 1024
            or not math.isfinite(a.minimum_available_gib) or a.minimum_available_gib < 0):
        p.error("invalid sizes, duration, or host reservation below allocation")
    if a.backend != "cpu" and a.memory_gib < a.gpu_mib / 1024 + 12 / 1024:
        p.error("GPU reservation below payload plus matrices; add runtime overhead too")
    if a.mode == "job":
        return workload(a)
    if a.mode == "spec":
        print(json.dumps(spec(a, a.priority, a.units), indent=2))
        return 0
    if a.mode == "physical":
        if sys.platform != "linux" or not a.pci or any(
                not re.fullmatch(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]", pci)
                or not (Path("/sys/bus/pci/devices") / pci / "drm").is_dir() for pci in a.pci):
            p.error("physical needs Linux and approved --pci identities")
        while True:
            try:
                emit(a.events, "physical", valid=True, owners=drm_owners(a.pci))
            except (OSError, ValueError, IndexError, UnicodeError):
                emit(a.events, "physical", valid=False, owners={})
                return 1
            time.sleep(0.5)
    if a.mode == "submit":
        if not a.queue:
            p.error("submit needs an explicit --queue or MYCE_QUEUE")
        print(submit(a, a.queue, a.priority, a.units))
        return 0
    if a.mode == "smoke":
        if not a.binary or a.backend != "cpu" or a.refuse_stop:
            p.error("smoke needs --binary, cpu backend, cooperative stop")
        return smoke(a)
    if a.mode == "check":
        if a.require_physical and not a.pci:
            p.error("physical acceptance needs the complete approved --pci scope")
        checker = Checker(a.bound, a.require_physical, tuple(a.pci))
        replay(a.events, checker)
        result = checker.finish()
        print(json.dumps(result))
        return bool(result["violations"])
    if not a.queue:
        p.error("watch needs an explicit --queue or MYCE_QUEUE")
    if a.require_physical and not a.pci:
        p.error("physical acceptance needs the complete approved --pci scope")
    # Watch is a read-only observer. Submissions/faults need their own evidence events.
    checker, offset = Checker(a.bound, a.require_physical, tuple(a.pci)), 0
    while True:
        try:
            observe(a.queue, a.events)
        except OSError:
            emit(a.events, "outage")
        memory = Path("/proc/meminfo")
        if memory.exists():
            try:
                fields = {row.split()[0]: int(row.split()[1]) for row in memory.read_text().splitlines()
                          if row.startswith(("MemTotal:", "MemAvailable:"))}
                emit(a.events, "memory", valid=True, total_gib=fields["MemTotal:"] / 1024**2,
                     available_gib=fields["MemAvailable:"] / 1024**2,
                     minimum_available_gib=a.minimum_available_gib)
            except (OSError, ValueError, KeyError):
                emit(a.events, "memory", valid=False)
        emit(a.events, "tick")
        offset = replay(a.events, checker, offset)
        if checker.errors:
            print(json.dumps(dict(violations=sorted(checker.errors))))
            return 1
        time.sleep(0.5)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, ValueError, OSError, RuntimeError) as error:
        # Error type only: upstream responses/paths may contain private information.
        print(json.dumps(dict(error=type(error).__name__)), file=sys.stderr)
        raise SystemExit(2)
