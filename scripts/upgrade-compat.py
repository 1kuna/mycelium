#!/usr/bin/env python3
"""Bounded release interoperability using real production code on loopback only."""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

REPO = Path(__file__).resolve().parent.parent
# No v3 release tag was published. This is the verified deployed s071 source.
PREVIOUS = "100840b4094d77bbc69a80d807dba58722db877c"
OLD_VERSION = "v3.1.0-rc.20261104.s071.100840b"
VERSION = "v3.1.0-rc.20261104.s072.abce497.1"
FIXTURE = REPO / "scripts/testdata/upgrade_compat_test.go.txt"


def command(args, *, cwd=REPO, timeout=180, env=None):
    child = subprocess.Popen(args, cwd=cwd, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, start_new_session=True)
    try:
        output, _ = child.communicate(timeout=timeout)
    except BaseException:
        # Build drivers can have compiler children. Signal only this command's
        # freshly created process group, then join before scratch is removed.
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
        raise
    if child.returncode:
        raise RuntimeError(f"{args[0]} failed ({child.returncode}):\n{output[-6000:]}")
    return output.strip()


def cache_root():
    path = Path(command(["go", "env", "GOCACHE"])) / "myce-upgrade-compat"
    path.mkdir(parents=True, exist_ok=True)
    return path


def archive(revision, offline=False):
    cached = cache_root() / f"source-{revision}.tar"
    if cached.exists():
        return cached
    # The normal checkout contains the predecessor. Fleet source archives don't;
    # profile preparation fetches this exact public commit BEFORE the offline gate.
    with tempfile.TemporaryDirectory(prefix="upgrade-fetch-", dir=cache_root()) as tmp:
        repo = REPO
        found = subprocess.run(["git", "cat-file", "-e", revision + "^{commit}"],
                               cwd=repo, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
        if not found:
            if offline:
                raise RuntimeError("previous source unavailable offline; run scripts/upgrade-compat.py prepare first")
            repo = Path(tmp)
            command(["git", "init", "--bare"], cwd=repo)
            command(["git", "fetch", "--depth=1", "https://github.com/1kuna/mycelium.git", revision], cwd=repo)
        if command(["git", "rev-parse", revision + "^{commit}"], cwd=repo) != revision:
            raise RuntimeError("previous release source identity mismatch")
        destination = Path(tmp) / "source.tar"
        command(["git", "archive", "--format=tar", "-o", str(destination), revision], cwd=repo)
        os.replace(destination, cached)
    return cached


def extract(source, destination):
    with tarfile.open(source) as tar:
        # All archives come from git archive of the verified commit, not a URL tar.
        destination = Path(destination)
        for member in tar:
            parts = Path(member.name).parts
            if not parts or parts[0] not in ("go.mod", "go.sum", "internal", "cmd"):
                continue
            if member.name.startswith("/") or ".." in parts or not (member.isfile() or member.isdir()):
                raise RuntimeError("unsafe source archive member")
            tar.extract(member, destination)


def prepare():
    source = archive(PREVIOUS)
    with tempfile.TemporaryDirectory(prefix="upgrade-modules-", dir=cache_root()) as tmp:
        extract(source, tmp)
        command(["go", "mod", "download"], cwd=tmp)
    print(f"upgrade-compat: previous source {PREVIOUS[:12]} prepared", flush=True)


def binaries(source, identity, version, scratch):
    toolchain = command(["go", "version"])
    digest = hashlib.sha256((identity + version + toolchain).encode() + FIXTURE.read_bytes()).hexdigest()
    cached = cache_root() / digest
    helper, cli = cached / "fixture.test", cached / "myce"
    if helper.exists() and cli.exists():
        return helper, cli
    # Build in scratch and publish only completed executables. Go's own cache
    # shares compilation across gate runs; a failed build never poisons this key.
    tree = scratch / ("build-" + digest[:12])
    if isinstance(source, Path) and source.is_file():
        extract(source, tree)
    else:
        tree.mkdir()
        for name in ("go.mod", "go.sum", "internal", "cmd"):
            src = Path(source) / name
            if src.is_dir():
                shutil.copytree(src, tree / name)
            else:
                shutil.copy2(src, tree / name)
    for path in (tree / "internal/myce").glob("*_test.go"):
        path.unlink()
    shutil.copy2(FIXTURE, tree / "internal/myce/upgrade_compat_test.go")
    flags = f"-X mycelium/internal/myce.BuildVersion={version} -X mycelium/internal/myce.BuildCommit={identity}"
    command(["go", "test", "-c", "-race", "-vet=off", "-ldflags", flags,
             "-o", str(tree / "fixture.test"), "./internal/myce"], cwd=tree)
    command(["go", "build", "-ldflags", flags, "-o", str(tree / "myce"), "./cmd/myce"], cwd=tree)
    cached.mkdir(exist_ok=True)
    for name in ("fixture.test", "myce"):
        os.replace(tree / name, cached / name)
    shutil.rmtree(tree)
    return helper, cli


def candidate_identity(source):
    # Include actual bytes (including dirty edits), not just HEAD. Source archives
    # have no Git metadata. Build constraints are handled by Go, never rewritten.
    digest = hashlib.sha256()
    files = [source / "go.mod", source / "go.sum"]
    for name in ("internal", "cmd"):
        files.extend(sorted((source / name).rglob("*")))
    for path in files:
        if path.is_file() and not path.name.endswith("_test.go"):
            digest.update(str(path.relative_to(source)).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


@contextlib.contextmanager
def process(binary, mode, root, *, queue="", config="", tools=None):
    root.mkdir(parents=True, exist_ok=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_") and not k.startswith("UPGRADE_")}
    home = root / "home"
    home.mkdir(exist_ok=True)
    env.update(HOME=str(home), USERPROFILE=str(home), XDG_CONFIG_HOME=str(home / ".config"),
               XDG_CACHE_HOME=str(home / ".cache"), XDG_DATA_HOME=str(home / ".local/share"),
               UPGRADE_MODE=mode, UPGRADE_ROOT=str(root), UPGRADE_QUEUE=queue,
               UPGRADE_CONFIG=str(config or ""), GORACE="atexit_sleep_ms=0")
    if tools:
        env["PATH"] = str(tools)  # Worker discovers exactly the authored tools.
    with (root / "process.log").open("w+") as log:
        child = subprocess.Popen([str(binary), "-test.run=^TestUpgradeCompatProcess$", "-test.v", "-test.timeout=45s"],
                                 env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            yield child
        finally:
            active_error = sys.exc_info()[1]
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
                try:
                    child.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                    raise RuntimeError(f"{mode} fixture required kill")
            if child.returncode and (active_error is None or isinstance(active_error, Exception)):
                log.seek(0)
                detail = f"{active_error}; " if active_error else ""
                raise RuntimeError(f"{detail}{mode} fixture exited {child.returncode}:\n{log.read()[-6000:]}")


def await_file(path, child, predicate=lambda value: True, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            value = path.read_text()
            if predicate(value):
                return value
        if child.poll() is not None:
            raise RuntimeError("fixture exited before readiness")
        time.sleep(.05)
    raise RuntimeError(f"fixture did not reach {path.name} within {timeout}s")


def pairing(name, queue_binary, worker_binary, root, tools, *, large=False, refusal=False, config=None):
    with process(queue_binary, "queue", root / "queue") as queue:
        url = await_file(root / "queue/url", queue)
        with process(worker_binary, "worker", root / "worker", queue=url, tools=tools, config=config) as worker:
            def ready(value):
                rows = json.loads(value)
                if refusal:
                    return any(row["status"] == 400 for row in rows)
                if any(row["status"] != 200 for row in rows):
                    raise RuntimeError(f"check-in refused: {rows}")
                return len(rows) >= 2
            rows = json.loads(await_file(root / "queue/receipts.json", worker, ready))
            if refusal:
                # s071 predates tool_inventory and has strict JSON decoding. Its
                # reverse pairing is explicitly unsupported, never silently skipped.
                return
            request = urllib.request.Request(url + "/api/status?detail=full", headers={"Authorization": "Bearer upgrade-fixture"})
            with urllib.request.urlopen(request, timeout=10) as response:
                status = json.load(response)
            assert len(status["jobs"]) == 6286, "retained history missing"
            assert len(status["work_curves"]) == 1655, "curves missing"
            machines = status["machines"]
            assert len(machines) == 1 and machines[0]["session"], "worker did not join"
            assert machines[0]["release"]["version"] in (VERSION, OLD_VERSION), "wrong compiled version"
            if large:
                assert max(row["bytes"] for row in rows) >= 1_871_955, "incident-size check-in no longer exercised"
                assert max(row["inventory_keys"] for row in rows) >= 41, "prepared inventory missing"
            print(f"upgrade-compat: PASS {name} (max check-in {max(row['bytes'] for row in rows)} bytes)", flush=True)


def run(args):
    scratch_parent = REPO / ".gate"
    scratch_parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="upgrade-compat-", dir=scratch_parent) as tmp:
        scratch = Path(tmp)
        previous = binaries(archive(PREVIOUS, offline=True), PREVIOUS, OLD_VERSION, scratch)
        source = REPO
        if args.candidate:
            source = scratch / "candidate"
            extract(archive(command(["git", "rev-parse", args.candidate]), offline=True), source)
        candidate = binaries(source, candidate_identity(source), VERSION, scratch)
        tools = scratch / "tools"
        tools.mkdir()
        # 2,400 executable names repeated through 40 prepared environments, like
        # the incident. Empty executable fixtures are only statted, never run.
        for i in range(2400):
            (tools / f"native-tool-{i:06d}").write_bytes(b"")
            (tools / f"native-tool-{i:06d}").chmod(0o700)
        failures = []
        cases = [
            ("candidate queue + candidate worker", lambda: pairing("same-source suffix", candidate[0], candidate[0], scratch / "same", tools, large=True)),
            ("candidate queue + previous worker", lambda: pairing("previous worker", candidate[0], previous[0], scratch / "old-worker", tools)),
            ("previous queue + candidate worker", lambda: reverse(previous, candidate, scratch, tools)),
            ("saved config downgrade", lambda: downgrade(previous, candidate, scratch, tools)),
        ]
        for name, case in cases:
            try:
                case()
            except Exception as error:
                failures.append(name)
                print(f"upgrade-compat: FAIL {name}: {error}", flush=True)
        if failures:
            raise RuntimeError("failed cases: " + ", ".join(failures))


def reverse(previous, candidate, scratch, tools):
    # Candidate's tool inventory didn't exist in s071; its strict decoder rejects
    # it. Check the refusal through the real worker and retain it as an explicit
    # support boundary until a negotiated protocol makes this direction work.
    try:
        pairing("previous queue", previous[0], candidate[0], scratch / "old-queue", tools, refusal=True)
    except RuntimeError as error:
        if "worker_protocol_invalid" not in str(error):
            raise
    print("upgrade-compat: PASS s071 reverse pairing explicitly refuses unsupported inventory", flush=True)


def downgrade(previous, candidate, scratch, tools):
    root = scratch / "config"
    root.mkdir()
    with process(candidate[0], "queue", root / "queue") as queue:
        url = await_file(root / "queue/url", queue)
        install_root = root / "install"
        with process(candidate[0], "install", install_root, queue=url) as install:
            install.wait(timeout=30)
        config = (install_root / "config-path").read_text()
        # The previous, unmodified CLI exercises its full strict decoder and
        # installer validation, without running a system service manager.
        env = {k:v for k,v in os.environ.items() if not k.startswith("MYCE_")}
        env["HOME"] = str(install_root)
        command([str(previous[1]), "install", "worker", "--config", config, "--dry-run"], env=env, timeout=30)
        with process(previous[0], "worker", root / "worker", queue=url, config=config, tools=tools):
            await_file(root / "queue/receipts.json", queue, lambda value: len(json.loads(value)) >= 2)
            rows = json.loads((root / "queue/receipts.json").read_text())
            assert all(row["status"] == 200 for row in rows), rows
        print("upgrade-compat: PASS candidate install -> s071 installer -> s071 worker", flush=True)


class SuiteDeadline(BaseException):
    pass


def suite_deadline(_signal, _frame):
    raise SuiteDeadline("ten-minute compatibility-suite budget exhausted")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument("--candidate", help="Git revision for incident reproduction; default actual checkout bytes")
    args = parser.parse_args()
    signal.signal(signal.SIGALRM, suite_deadline)
    signal.alarm(600)
    try:
        prepare() if args.action == "prepare" else run(args)
    except (Exception, KeyboardInterrupt, SuiteDeadline) as error:
        print(f"upgrade-compat: {error}", file=sys.stderr)
        sys.exit(1)
