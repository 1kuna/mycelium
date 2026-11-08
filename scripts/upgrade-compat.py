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
import urllib.parse

REPO = Path(__file__).resolve().parent.parent
# No v3 release tag was published. This is the verified deployed s071 source.
PREVIOUS = "100840b4094d77bbc69a80d807dba58722db877c"
INVENTORY_BASELINE = "8b2585e1d7edaff376e96430bc614daa9f788342"
CADENCE_BASELINE = "9b1180db3fe2c6e580e1565352c263aceb020932"
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
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
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
    for revision in (PREVIOUS, INVENTORY_BASELINE, CADENCE_BASELINE):
        source = archive(revision)
        with tempfile.TemporaryDirectory(prefix="upgrade-modules-", dir=cache_root()) as tmp:
            extract(source, tmp)
            command(["go", "mod", "download"], cwd=tmp)
        print(f"upgrade-compat: baseline source {revision[:12]} prepared", flush=True)


def binaries(source, identity, version, scratch):
    toolchain = command(["go", "version"])
    digest = hashlib.sha256((identity + version + toolchain).encode() + FIXTURE.read_bytes()).hexdigest()
    cached = cache_root() / digest
    helper, cli = cached / "fixture.test", cached / "myce"
    if helper.exists() and cli.exists():
        return helper, cli
    print(f"upgrade-compat: building source {identity[:12]}", flush=True)
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
def process(binary, mode, root, *, queue="", config="", tools=None, expected_refusal=False, listen=""):
    root.mkdir(parents=True, exist_ok=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_") and not k.startswith("UPGRADE_")}
    home = root / "home"
    home.mkdir(exist_ok=True)
    env.update(HOME=str(home), USERPROFILE=str(home), XDG_CONFIG_HOME=str(home / ".config"),
               XDG_CACHE_HOME=str(home / ".cache"), XDG_DATA_HOME=str(home / ".local/share"),
               UPGRADE_MODE=mode, UPGRADE_ROOT=str(root), UPGRADE_QUEUE=queue,
               UPGRADE_CONFIG=str(config or ""), UPGRADE_LISTEN=listen, GORACE="atexit_sleep_ms=0")
    if mode == "queue":
        (root / "url").unlink(missing_ok=True)
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
            if expected_refusal and active_error is None:
                log.seek(0)
                output = log.read()
                if (child.returncode != 1 or "HTTP 426: worker_protocol_invalid" not in output
                        or "DATA RACE" in output):
                    raise RuntimeError(f"worker did not exit with the expected protocol refusal:\n{output[-6000:]}")
            elif child.returncode and (active_error is None or isinstance(active_error, Exception)):
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


def pairing(name, queue_binary, worker_binary, root, tools, *, large=False, refusal=False, config=None, full_only=False):
    with process(queue_binary, "queue", root / "queue") as queue:
        url = await_file(root / "queue/url", queue)
        with process(worker_binary, "worker", root / "worker", queue=url, tools=tools,
                     config=config, expected_refusal=refusal) as worker:
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
                assert all(row["status"] == 400 for row in rows), rows
                worker.wait(timeout=5)
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
            if full_only:
                assert all(row["inventory_keys"] == 41 and not row["hash"] and not row["version"] for row in rows), rows
                assert max(row["bytes"] for row in rows) >= 1_871_955, "full legacy inventory missing"
            assert all(row["attempt_protocol"] == "keyed-attempt-v1" for row in rows), "keyed attempts downgraded"
            print(f"upgrade-compat: PASS {name} (max check-in {max(row['bytes'] for row in rows)} bytes)", flush=True)


def run(args):
    scratch_parent = REPO / ".gate"
    scratch_parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="upgrade-compat-", dir=scratch_parent) as tmp:
        scratch = Path(tmp)
        previous = binaries(archive(PREVIOUS, offline=True), PREVIOUS, OLD_VERSION, scratch)
        baseline = binaries(archive(INVENTORY_BASELINE, offline=True), INVENTORY_BASELINE, VERSION, scratch)
        legacy = binaries(archive(CADENCE_BASELINE, offline=True), CADENCE_BASELINE, VERSION, scratch)
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
            ("legacy worker first work", lambda: legacy_first_work(candidate, legacy, scratch, tools)),
            ("previous queue + candidate worker", lambda: reverse(previous, candidate, scratch, tools)),
            ("saved config downgrade", lambda: downgrade(previous, candidate, scratch, tools)),
            ("full-inventory worker + candidate queue", lambda: pairing("#790 full worker", candidate[0], baseline[0], scratch / "full-worker", tools, full_only=True)),
            ("candidate worker + strict inventory queue", lambda: pairing("#790 strict queue", baseline[0], candidate[0], scratch / "strict-queue", tools, full_only=True)),
            ("negotiated worker + queue restart and downgrade", lambda: inventory_restarts(candidate, baseline, scratch, tools)),
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
    pairing("previous queue", previous[0], candidate[0], scratch / "old-queue", tools, refusal=True)
    print("upgrade-compat: PASS s071 reverse pairing explicitly refuses unsupported inventory", flush=True)


def inventory_restarts(candidate, baseline, scratch, tools):
    root = scratch / "inventory-restarts"
    queue_root = root / "queue"

    def accepted_hash_only(value):
        rows = json.loads(value)
        if any(row["status"] != 200 for row in rows):
            raise RuntimeError(f"inventory check-in refused: {rows}")
        return len(rows) >= 3 and rows[-1]["hash"] and rows[-1]["inventory_keys"] == 0

    with contextlib.ExitStack() as queues:
        queue = queues.enter_context(process(candidate[0], "queue", queue_root))
        url = await_file(queue_root / "url", queue)
        address = urllib.parse.urlparse(url).netloc
        with process(candidate[0], "worker", root / "worker", queue=url, tools=tools) as worker:
            rows = json.loads(await_file(queue_root / "receipts.json", worker, accepted_hash_only))
            assert rows[0]["inventory_keys"] == 41 and not rows[0]["hash"], "initial request was not legacy full"
            assert rows[0]["protocol"] == "tool-inventory-v1", "queue did not advertise support"
            session, inventory_hash = rows[-1]["session"], rows[-1]["hash"]

            # Keep the worker and its negotiated state alive. Reopen the same
            # disposable queue through production Open, retaining durable fences.
            queues.close()
            queue = queues.enter_context(process(candidate[0], "queue", queue_root, listen=address))
            assert await_file(queue_root / "url", queue) == url

            def refreshed(value):
                rows = json.loads(value)
                if not accepted_hash_only(value):
                    return False
                return (any(row["required"] is True for row in rows)
                        and any(row["inventory_keys"] == 41 and row["hash"] for row in rows))

            rows = json.loads(await_file(queue_root / "receipts.json", worker, refreshed))
            assert rows[0]["hash"] == inventory_hash and rows[0]["inventory_keys"] == 0, "restart did not begin hash-only"
            assert rows[0]["required"] is True, "lost inventory did not request full refresh"
            assert all(row["session"] == session for row in rows), "worker restarted instead of recovering"
            full = next(row for row in rows[1:] if row["inventory_keys"] == 41)
            assert full["status"] == 200 and full["ack_hash"] == inventory_hash and not full["required"], "full refresh not acknowledged"
            assert rows[-1]["inventory_keys"] == 0, "unchanged heartbeats still send full inventory"

            # An older queue uses a fresh disposable DB on the same endpoint:
            # this tests worker protocol fallback, not queue-data downgrades.
            queues.close()
            strict_root = root / "strict-queue"
            queue = queues.enter_context(process(baseline[0], "queue", strict_root, listen=address))
            assert await_file(strict_root / "url", queue) == url
            rows = json.loads(await_file(strict_root / "receipts.json", worker,
                                         lambda value: len(json.loads(value)) >= 3))
            assert rows[0]["status"] == 400 and rows[0]["hash"] and rows[0]["inventory_keys"] == 0, "negotiated request did not hit strict decoder"
            assert all(row["status"] == 200 and row["inventory_keys"] == 41
                       and not row["hash"] and not row["version"] for row in rows[1:]), "legacy full fallback did not rejoin"
            assert all(row["session"] == session and row["attempt_protocol"] == "keyed-attempt-v1" for row in rows), "fallback changed session or attempt protocol"
    print("upgrade-compat: PASS hash-only -> queue restart -> explicit full refresh -> #790 strict fallback", flush=True)


def legacy_first_work(candidate, previous, scratch, tools):
    root = scratch / "legacy-first-work"
    with process(candidate[0], "queue-work", root / "queue") as queue:
        url = await_file(root / "queue/url", queue)
        with process(previous[0], "worker", root / "worker", queue=url, tools=tools) as worker:
            def offered(value):
                rows = json.loads(value)
                assert all(row["status"] == 200 and not row["period"] for row in rows), rows
                return any(row["offered"] for row in rows)
            rows = json.loads(await_file(root / "queue/receipts.json", worker, offered))
            # Startup recovery can qualify cadence before ordinary admission.
            # Otherwise at most the first ordinary idle report needs a sample.
            eligible = [row for row in rows if not row["recovering"]]
            offer = next(i for i, row in enumerate(eligible) if row["offered"])
            assert offer <= 1, "legacy admission waited beyond one observed send interval"
            request = urllib.request.Request(url + "/api/jobs/cadence-work", headers={"Authorization": "Bearer upgrade-fixture"})
            deadline = time.monotonic() + 30
            while True:
                with urllib.request.urlopen(request, timeout=10) as response:
                    job = json.load(response)
                if job["state"] in ("succeeded", "failed", "cancelled"):
                    assert job["state"] == "succeeded" and job.get("losses", 0) == 0, job["state"]
                    break
                assert time.monotonic() < deadline, "legacy command did not finish"
                time.sleep(.2)
            print("upgrade-compat: PASS legacy worker empty idle reply -> measured cadence -> completed first work", flush=True)


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
        install_command = [str(previous[1]), "install", "worker", "--config", config, "--dry-run"]
        config_path = Path(config)
        saved_bytes = config_path.read_bytes()
        saved = json.loads(saved_bytes)
        if saved["file_retention"] == 0 or saved["log_retention"] == 0:
            # s071 cannot represent retain-until-release. Refusing is required:
            # defaulting an omitted zero would silently restore age-based loss.
            try:
                command(install_command, env=env, timeout=30)
            except RuntimeError as error:
                field = "file-retention" if saved["file_retention"] == 0 else "log-retention"
                assert f"{field}: duration must be positive" in str(error), error
            else:
                raise AssertionError("s071 accepted unsupported retain-until-release policy")
            assert config_path.read_bytes() == saved_bytes, "refused downgrade changed saved policy"
            print("upgrade-compat: PASS s071 refuses retain-until-release without changing config", flush=True)
            # Separately exercise the existing supported downgrade contract with
            # explicit operator policies; these are fixture intent, not defaults.
            saved["file_retention"] = 11 * 60 * 60 * 1_000_000_000
            saved["log_retention"] = 13 * 60 * 60 * 1_000_000_000
            config_path.write_text(json.dumps(saved))
        command(install_command, env=env, timeout=30)
        restored = json.loads(config_path.read_bytes())
        assert all(restored[field] == saved[field] for field in ("file_retention", "log_retention")), "downgrade changed explicit retention"
        with process(previous[0], "worker", root / "worker", queue=url, config=config, tools=tools):
            await_file(root / "queue/receipts.json", queue, lambda value: len(json.loads(value)) >= 2)
            rows = json.loads((root / "queue/receipts.json").read_text())
            assert all(row["status"] == 200 for row in rows), rows
        print("upgrade-compat: PASS candidate install -> s071 installer -> s071 worker", flush=True)


class SuiteDeadline(BaseException):
    pass


class SuiteTerminated(BaseException):
    pass


def suite_deadline(_signal, _frame):
    raise SuiteDeadline("ten-minute compatibility-suite budget exhausted")


def suite_terminated(signum, _frame):
    # BaseException bypasses per-case failure collection and unwinds every owned
    # process context before TemporaryDirectory removes its state.
    raise SuiteTerminated(f"compatibility suite terminated by signal {signum}")


def install_signal_handlers():
    signal.signal(signal.SIGALRM, suite_deadline)
    signal.signal(signal.SIGTERM, suite_terminated)
    signal.signal(signal.SIGHUP, suite_terminated)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument("--candidate", help="Git revision for incident reproduction; default actual checkout bytes")
    args = parser.parse_args()
    install_signal_handlers()
    signal.alarm(600)
    try:
        prepare() if args.action == "prepare" else run(args)
    except (Exception, KeyboardInterrupt, SuiteDeadline, SuiteTerminated) as error:
        print(f"upgrade-compat: {error}", file=sys.stderr)
        sys.exit(1)
