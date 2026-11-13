import http.client
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from mycelium import (APIError, Checkpoint, Client, EntryFailedError, EntryInterrupted, EntryPreparationError,
                      IncompleteReplyError, StopSignal)

ROOT = Path(__file__).resolve().parents[3]

# #1557: a default `myce run` wakes the agent whose harness variables it sees
# (cmd/myce/harness_wake.go). Every myce these tests start inherits os.environ,
# so drop those variables or a finished test job wakes the agent running them.
HARNESS_WAKE_ENV = sorted({name for block in re.findall(r"env:\s*\[\]string\{([^}]*)\}",
                                                         (ROOT / "cmd/myce/harness_wake.go").read_text())
                           for name in re.findall(r'"([A-Z0-9_]+)"', block)})
assert HARNESS_WAKE_ENV, "harness wake variables moved; update this parser"
for _name in HARNESS_WAKE_ENV:
    os.environ.pop(_name, None)


class Reply(io.BytesIO):
    def __init__(self, body=b"", length=None):
        super().__init__(body)
        self.headers = {} if length is None else {"Content-Length": str(length)}


# #218: a nominal 0.4 s sleep measured 0.50–0.53 s in Darwin workers.
# Separate the phases: old API timeout (0.2 s) < header delay < idle budget
# < genuine stall. Three header periods leave scheduling room; the true stall
# stays beyond both the idle deadline and the proportional completion bound.
SLOW_HEADER_DELAY = 0.4
TRANSFER_IDLE_BUDGET = 3 * SLOW_HEADER_DELAY
GENUINE_STALL_DELAY = 4 * TRANSFER_IDLE_BUDGET
STALL_COMPLETION_BOUND = 3 * TRANSFER_IDLE_BUDGET


class FileTransferTimeoutTests(unittest.TestCase):
    """#218: streamed results have progress budgets, not a whole-request timeout."""

    def serve(self, body, *, stall):
        import hashlib
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        ref = {"sha256": hashlib.sha256(body).hexdigest(), "size_bytes": len(body)}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                if self.path == "/api/jobs/job":
                    reply = json.dumps({"id": "job", "state": "succeeded", "output_refs": {"result": ref}}).encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(reply)))
                    self.end_headers()
                    self.wfile.write(reply)
                    return
                time.sleep(SLOW_HEADER_DELAY)  # The queue rehashes before replying.
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                chunk = len(body) // 10
                for i in range(10):
                    if stall and i == 5:
                        time.sleep(GENUINE_STALL_DELAY)
                        return
                    self.wfile.write(body[i * chunk:(i + 1) * chunk])
                    self.wfile.flush()
                    time.sleep(0.06)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        # The old fixed client timeout is shorter than the header wait and stream.
        client = Client(f"http://127.0.0.1:{server.server_address[1]}", "fixture", timeout=0.2)
        client._transfer_idle = TRANSFER_IDLE_BUDGET
        return client

    def test_slow_stream_succeeds_and_stall_fails_typed(self):
        from mycelium import TransferStalledError
        body = b"frame" * 2000
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "result"
            self.serve(body, stall=False).download_result("job", "result", destination)
            self.assertEqual(destination.read_bytes(), body)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "result"
            start = time.monotonic()
            with self.assertRaises(TransferStalledError) as caught:
                self.serve(body, stall=True).download_result("job", "result", destination)
            self.assertLess(time.monotonic() - start, STALL_COMPLETION_BOUND)
            self.assertEqual(caught.exception.code, "result_download_failed")
            self.assertIn("result_download_failed", str(caught.exception))
            self.assertEqual(os.listdir(directory), [])

    def test_progressing_chunked_upload_succeeds(self):
        import hashlib
        import socket
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        body = b"data" * (1024 * 1024)  # The reported 4 MiB sendall failure.
        received = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_PATCH(self):
                remaining = int(self.headers["Content-Length"])
                digest = hashlib.sha256()
                while remaining:
                    chunk = self.rfile.read(min(8192, remaining))
                    if not chunk:
                        return
                    digest.update(chunk)
                    remaining -= len(chunk)
                    time.sleep(0.005)
                received.append(digest.hexdigest())
                self.send_response(204)
                self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        client = Client(f"http://127.0.0.1:{server.server_address[1]}", "fixture", timeout=0.2)
        client._transfer_idle = TRANSFER_IDLE_BUDGET
        connect = http.client.HTTPConnection.connect

        def small_send_buffer(connection):
            connect(connection)
            connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)

        start = time.monotonic()
        with patch.object(http.client.HTTPConnection, "connect", small_send_buffer):
            with client._file_request("PATCH", "/upload", body, size=len(body)) as response:
                self.assertEqual(response.status, 204)
        self.assertGreater(time.monotonic() - start, client._transfer_idle)
        self.assertEqual(received, [hashlib.sha256(body).hexdigest()])


class ClientTests(unittest.TestCase):
    def test_batch_wait_keeps_terminal_payload(self):
        client = Client(token="fixture")
        terminal = {"id": "job/one", "state": "succeeded", "output_refs": {"result": {"size_bytes": 10}}}
        replies = [[{"id": "job/one", "state": "running", "revision": 1, "spec": {}}],
                   [{"id": "job/one", "state": "succeeded", "revision": 2, "spec": {}}]]
        with patch.object(client, "_wait_batch", side_effect=replies), patch.object(client, "_call", return_value=json.dumps(terminal).encode()) as call, patch("mycelium.client.time.sleep") as pause:
            self.assertEqual(client.wait("job/one"), terminal)
            self.assertEqual([item.args for item in call.call_args_list], [("GET", "/api/jobs/job%2Fone?view=wait")])
            self.assertEqual(pause.call_count, 1)
            self.assertGreater(pause.call_args.args[0], 0)

    def test_job_reply_invalid(self):
        client = Client(token="test-only")
        for body in (b'{"jobs":[{"id":"fixture","state":"succeeded"}]}',
                     b'{"id":"","state":"succeeded"}',
                     b'{"id":"fixture","state":""}',
                     b"<html>wrong endpoint</html>",
                     b'{"id":"fixture","state":tru',
                     b"\xff",
                     b""):
            for read in (client.get, client.wait):
                with self.subTest(body=body, read=read.__name__):
                    with patch.object(client, "_urlopen", side_effect=lambda *_, **__: Reply(body, len(body))), patch(
                            "mycelium.client.time.sleep", side_effect=AssertionError("invalid reply was polled")):
                        with self.assertRaises(APIError) as caught:
                            read("fixture")
                        self.assertEqual(caught.exception.code, "job_reply_invalid")

    def test_job_reply_truncated(self):
        class Dropped(Reply):
            def read(self, *_):
                raise http.client.IncompleteRead(b'{"id":"fixture"')
        short = b'{"id":"fixture","state":tru'
        bodies = [Dropped(), Reply(short, len(short) + 2), Reply(b'{"jobs":[{"id":"fixture","state":"succeeded","revision":1}]}'), Reply(b'{"id":"fixture","state":"succeeded"}')]
        client = Client(token="test-only")
        with patch.object(client, "_urlopen", side_effect=lambda *_, **__: bodies.pop(0)), patch("mycelium.client.time.sleep"):
            self.assertEqual(client.wait("fixture")["state"], "succeeded")
        with patch.object(client, "_urlopen", side_effect=lambda *_, **__: Reply(short, len(short) + 2)):
            with self.assertRaises(IncompleteReplyError):
                client.get("fixture")

    def test_job_reply_unauthorized_truncated_body_not_retried(self):
        class Dropped(io.BytesIO):
            def read(self, *_):
                raise http.client.IncompleteRead(b'{"code":"unauth')
        client = Client(token="test-only")
        response = HTTPError("http://queue/api/jobs/job", 401, "Unauthorized", {}, Dropped())
        with patch.object(client, "_urlopen", side_effect=response), patch(
                "mycelium.client.time.sleep", side_effect=AssertionError("authentication failure was polled")):
            with self.assertRaises(http.client.IncompleteRead):
                client.wait("job")

    def test_installed_libressl_requires_openssl(self):
        with patch("ssl.OPENSSL_VERSION", "LibreSSL 2.8.3"):
            with patch("mycelium.installed.installed_authority") as authority:
                authority.return_value = ("https://queue.invalid", "fixture", object())
                with self.assertRaisesRegex(RuntimeError, r"requires OpenSSL \(3 recommended\); macOS system LibreSSL is unsupported") as raised:
                    Client.from_installed()
                from mycelium import UnsupportedTLSRuntimeError
                self.assertIsInstance(raised.exception, UnsupportedTLSRuntimeError)
                self.assertIn("LibreSSL", str(raised.exception))
                authority.assert_not_called()
        with patch("ssl.OPENSSL_VERSION", "OpenSSL 3.5.0"):
            with patch("mycelium.installed.installed_authority", return_value=(
                    "https://queue.invalid", "fixture", object())):
                self.assertEqual(Client.from_installed().url, "https://queue.invalid")

    def test_installed_defers_to_dev_session(self):
        session = {"MYCE_DEV_SESSION_ROOT": "/dev-session", "MYCE_QUEUE": "http://127.0.0.1:9/",
                   "MYCE_TOKEN": "dev-session-token"}
        with patch.dict(os.environ, session), patch("ssl.OPENSSL_VERSION", "LibreSSL 2.8.3"), patch(
                "mycelium.installed.installed_authority", side_effect=AssertionError("installed authority read")):
            client = Client.from_installed()
        self.assertEqual((client.url, client.token, client._authority), ("http://127.0.0.1:9", "dev-session-token", "environment"))

    def test_job_reply_old_queue(self):
        client = Client(token="fixture")
        for body in (b"404 page not found", b'{"code":"job_unknown","error":"job is unknown"}'):
            for read in (client.get, client.wait):
                with self.subTest(body=body, read=read.__name__):
                    def missing(*args, **kwargs):
                        raise HTTPError("http://queue/api/jobs/job", 404, "Not Found", {}, io.BytesIO(body))
                    with patch.object(client, "_urlopen", side_effect=missing):
                        with self.assertRaises(APIError) as caught:
                            read("job")
                    self.assertEqual(caught.exception.status, 404)
                    if b"job_unknown" in body:
                        self.assertEqual(caught.exception.code, "job_unknown")
                    else:
                        self.assertEqual(caught.exception.code, "queue_upgrade_required")
                        self.assertIn("queue too old for this client; upgrade the queue first", str(caught.exception))

    def test_job_spec_invalid_message(self):
        reason = "needs.host_memory_gb must be finite and nonnegative"
        error = APIError(400, json.dumps({"code": "job_spec_invalid", "error": reason}))
        self.assertEqual(error.status, 400)
        self.assertEqual(error.code, "job_spec_invalid")
        self.assertEqual(str(error), "job_spec_invalid: " + reason)

    def test_ledger_snapshot_pagination_and_get_summary(self):
        client = Client(token="test-only")
        pages = [
            {"job_id": "job/one", "complete": True, "high_water": 2,
             "next": 1, "has_more": True, "entries": [{"sequence": 1}]},
            {"job_id": "job/one", "complete": True, "high_water": 2,
             "next": 2, "has_more": False, "entries": [{"sequence": 2}]},
        ]
        with patch.object(client, "_call", side_effect=pages) as call:
            first = client.ledger("job/one", limit=1)
            second = client.ledger("job/one", after=first["next"],
                                   high_water=first["high_water"], limit=1)
            self.assertEqual([p["sequence"] for page in (first, second)
                              for p in page["entries"]], [1, 2])
            self.assertEqual(call.call_args_list[0].args,
                             ("GET", "/api/jobs/job%2Fone/ledger?after=0&limit=1"))
            self.assertEqual(call.call_args_list[1].args,
                             ("GET", "/api/jobs/job%2Fone/ledger?after=1&limit=1&high_water=2"))
        job = {"id": "job", "state": "queued", "ledger": {"complete": True, "high_water": 0}}
        with patch.object(client, "_read_job", return_value=job):
            self.assertEqual(client.get("job")["ledger"], job["ledger"])
        with patch.object(client, "_call", side_effect=APIError(410, '{"code":"job_ledger_unavailable"}')):
            with self.assertRaises(APIError) as caught:
                client.ledger("expired")
            self.assertEqual(caught.exception.code, "job_ledger_unavailable")

    def test_cancellation_acknowledgements(self):
        client = Client(token="test-only")
        ack = {"job_id": None, "project": "test", "submission_key": "key",
               "state": "absent", "will_not_run": True,
               "release": "not_applicable", "acknowledged_at": "2026-10-17T12:00:00Z"}
        with patch.object(client, "_call", return_value=ack) as call:
            self.assertEqual(client.cancel_submission("test", "key"), ack)
            self.assertEqual(call.call_args.args, ("POST", "/api/jobs/cancel",
                             {"project": "test", "submission_key": "key"}))
            ack = {**ack, "job_id": "job/one", "state": "stopping", "release": "pending"}
            call.return_value = ack
            self.assertEqual(client.cancel("job/one"), ack)
            self.assertEqual(call.call_args.args, ("POST", "/api/jobs/job%2Fone/cancel", {}))
        self.assertEqual(APIError(404, '{"code":"job_unknown","will_not_run":null}').code, "job_unknown")

    def test_production_lease_requests(self):
        client = Client(token="test-only")
        stage = {"needs": {"memory_gb": 8, "platform": {"required": ["cuda"]}}}
        with patch.object(client, "_call", return_value={"leases": [{"key": "run/1"}]}) as call:
            client.open_production_lease("render", "run-1", "2h", [stage])
            self.assertEqual(call.call_args.args, ("POST", "/api/production-leases",
                             {"project": "render", "key": "run-1", "ttl": "2h", "stages": [stage]}))
            client.close_production_lease("render", "run 1")
            self.assertEqual(call.call_args.args, ("DELETE", "/api/production-leases/render/run%201"))
            self.assertEqual(client.production_leases(), [{"key": "run/1"}])



    def test_log_pages_and_terminal_drain(self):
        client = Client(token="test-only")
        page = [{"seq": n, "attempt": 1, "text": str(n)} for n in range(1, 257)]
        responses = [page, [], [{"seq": 257, "attempt": 2, "text": "final"}]]
        with patch.object(client, "_call", side_effect=responses) as call:
            with patch.object(client, "_read_job", return_value={"state": "succeeded"}):
                chunks = list(client.logs("test", follow=True))
        self.assertEqual(len(chunks), 257)
        self.assertEqual(chunks[-1]["text"], "final")
        self.assertEqual(call.call_args_list[-1].args[1], "/api/jobs/test/logs?after=256")

    def test_optional_imports_are_lazy(self):
        client = Client(token="test-only")
        with patch.dict(sys.modules, {"yaml": None, "openai": None}):
            with self.assertRaisesRegex(ImportError, "yaml"):
                client.submit_yaml("name: test")
            with self.assertRaisesRegex(ImportError, "openai"):
                client.openai(project="test")


class CheckpointTests(unittest.TestCase):
    def test_newest_atomic_and_temp_ignored(self):
        with tempfile.TemporaryDirectory() as folder:
            checkpoint = Checkpoint(folder)
            old = checkpoint.save({"step": 1}, "one.json")
            os.utime(old, ns=(1, 1))
            checkpoint.save({"step": 2}, "two.json")
            Path(folder, "unfinished.tmp").write_text("garbage")
            self.assertEqual(checkpoint.load(), {"step": 2})
            with self.assertRaises(ValueError):
                checkpoint.save({}, "../outside.json")
            with patch("mycelium.checkpoint.os.fsync", side_effect=OSError("NAS down")):
                with self.assertRaises(OSError):
                    checkpoint.save({"step": 3}, "two.json")
            self.assertEqual(checkpoint.load(), {"step": 2})
            Path(folder, "two.json").write_text("corrupt")
            with self.assertRaises(json.JSONDecodeError):
                checkpoint.load()

    def test_cifs_save_keeps_file_sync_and_atomic_generations(self):
        with tempfile.TemporaryDirectory() as folder:
            checkpoint = Checkpoint(folder)
            sync = os.fsync
            calls = []
            def file_sync_only(fd):
                import stat
                self.assertFalse(stat.S_ISDIR(os.fstat(fd).st_mode), "CIFS directory fsync must be skipped")
                calls.append(fd)
                sync(fd)
            with patch("mycelium.checkpoint._is_linux_cifs", return_value=True, create=True):
                with patch("mycelium.checkpoint.os.fsync", side_effect=file_sync_only):
                    checkpoint.save({"step": 1})
                    checkpoint.save({"step": 2})
                self.assertEqual(checkpoint.load(), {"step": 2})
                self.assertEqual(len(calls), 2)
                with patch("mycelium.checkpoint.os.fsync", side_effect=OSError("file flush failed")):
                    with self.assertRaises(OSError):
                        checkpoint.save({"step": 3})
                self.assertEqual(checkpoint.load(), {"step": 2})
                self.assertEqual(json.loads(Path(folder, "checkpoint.json").read_text())["myce_checkpoint"]["generation"], 2)

    def test_cifs_mount_selection_and_unknown_mounts_keep_sync(self):
        from mycelium.checkpoint import _is_linux_cifs
        mounts = "1 0 8:1 / / rw - ext4 /dev/root rw\n2 1 0:1 / /mnt/verso\\040nas rw - cifs //nas/share rw\n3 2 8:2 / /mnt/verso\\040nas/local rw - ext4 /dev/local rw\n"
        with patch("mycelium.checkpoint.sys.platform", "linux"):
            with patch("mycelium.checkpoint.Path.read_text", return_value=mounts):
                self.assertTrue(_is_linux_cifs(Path("/mnt/verso nas/checkpoints")))
                self.assertFalse(_is_linux_cifs(Path("/mnt/verso nas/local/checkpoints")))
                self.assertFalse(_is_linux_cifs(Path("/mnt/verso nas-other/checkpoints")))
            ambiguous = "1 0 0:1 / /mnt rw - cifs //nas/share rw\n2 1 8:1 / /mnt rw - ext4 /dev/root rw\n"
            with patch("mycelium.checkpoint.Path.read_text", return_value=ambiguous):
                self.assertFalse(_is_linux_cifs(Path("/mnt/checkpoints")))
            with patch("mycelium.checkpoint.Path.read_text", side_effect=OSError("unavailable")):
                self.assertFalse(_is_linux_cifs(Path("/mnt/verso nas/checkpoints")))
        with tempfile.TemporaryDirectory() as folder:
            checkpoint = Checkpoint(folder)
            sync = os.fsync
            calls = []
            with patch("mycelium.checkpoint._is_linux_cifs", return_value=False):
                with patch("mycelium.checkpoint.os.fsync", side_effect=lambda fd: (calls.append(fd), sync(fd))):
                    checkpoint.save({"step": 1})
            self.assertEqual(len(calls), 2)

    def test_generation_survives_skew_and_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            checkpoint = Checkpoint(folder)
            old = checkpoint.save({"step": 2}, "z-old.json")
            new = Checkpoint(folder).save({"step": 9}, "a-new.json")
            os.utime(old, ns=(999999999999999999, 999999999999999999))
            os.utime(new, ns=(1, 1))
            self.assertEqual(Checkpoint(folder).load(), {"step": 9})
            self.assertEqual(json.loads(new.read_text())["myce_checkpoint"]["generation"], 2)

    def test_legacy_tie_and_invalid_generation(self):
        with tempfile.TemporaryDirectory() as folder:
            Path(folder, "a.json").write_text('{"step": 9}')
            Path(folder, "z.json").write_text('{"step": 2}')
            os.utime(Path(folder, "a.json"), ns=(999999999999999999, 999999999999999999))
            self.assertEqual(Checkpoint(folder).load(), {"step": 2})
            checkpoint = Checkpoint(folder)
            path = checkpoint.save({"step": 10}, "a.json")
            self.assertEqual(json.loads(path.read_text())["myce_checkpoint"]["generation"], 1)
            self.assertEqual(checkpoint.load(), {"step": 10})
            path.write_text('{"myce_checkpoint":{"generation":-1},"value":0}')
            with self.assertRaises(ValueError):
                checkpoint.load()

    def test_legacy_metadata_name_without_sequence(self):
        with tempfile.TemporaryDirectory() as folder:
            value = {"myce_checkpoint": {"description": "legacy"}, "step": 9}
            Path(folder, "checkpoint.json").write_text(json.dumps(value))
            self.assertEqual(Checkpoint(folder).load(), value)

    def test_environment_and_signals(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {"MYCE_CHECKPOINT_DIR": folder}):
                self.assertEqual(Checkpoint().load(0), 0)
        previous = signal.getsignal(signal.SIGTERM)
        with StopSignal() as stop:
            os.kill(os.getpid(), signal.SIGTERM)
            self.assertTrue(stop.requested)
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)


def process_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:  # A zombie has exited; Linux reports it in /proc until it is reaped.
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except (OSError, IndexError):
        return True


class DevTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="myce-python-")
        cls.root = Path(cls.directory.name)
        binary = cls.root / "myce"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/myce"], cwd=ROOT, check=True, timeout=120)
        cls.output = (cls.root / "dev.log").open("w+")
        cls.process = subprocess.Popen([str(binary), "dev", "--environment-cache-bytes", "1073741824", "--listen", "127.0.0.1:0", "--workers", "1", "--db", str(cls.root / "state" / "queue.db"), "--checkpoints", str(cls.root / "checkpoints")], stdout=cls.output, stderr=cls.output)
        try:
            end = time.monotonic() + 20
            while time.monotonic() < end:
                cls.output.seek(0)
                text = cls.output.read()
                match = re.search(r"Mycelium dev: (http://\S+)", text)
                if match:
                    cls.client = Client(match[1], "dev-token", poll_interval=0.05)
                    cls.client.status()
                    return
                if cls.process.poll() is not None:
                    raise RuntimeError(text)
                time.sleep(0.05)
            raise TimeoutError("myce dev did not start")
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.process.send_signal(signal.SIGINT)
        cls.process.wait(timeout=30)
        cls.output.close()
        cls.directory.cleanup()

    def wait_observers(self, home):
        # myce run arms a detached receipt observer under HOME. Once every job is
        # terminal it exits on its own, but only while the queue is reachable, and
        # it writes under HOME until then, so wait before HOME is removed.
        observers = home / ("Library/Caches" if sys.platform == "darwin" else ".cache") / "myce/observers"
        end = time.monotonic() + 30
        while list(observers.glob("*.json")) and time.monotonic() < end:
            time.sleep(0.1)
        self.assertEqual(list(observers.glob("*.json")), [], "receipt observers did not finish")
        # The registry empties before the processes exit: the child observer then
        # records its final status through a temporary file and the supervisor
        # its supervision state, which raced HOME's removal ("Directory not
        # empty: 'observers'", #1401). Wait for every recorded process to exit.
        seen, live = set(), []
        while time.monotonic() < end:
            for name, keys in (("observer.status", ("pid",)), ("observer.supervision", ("pid", "child_pid"))):
                try:
                    state = json.loads((observers / name).read_text())
                except (OSError, ValueError):
                    continue
                seen.update(state[key] for key in keys if state.get(key))
            live = [pid for pid in seen if process_alive(pid)]
            if not live:
                break
            time.sleep(0.05)
        self.assertEqual(live, [], "receipt observer processes did not exit")

    def spec(self, command, **extra):
        return {"name": "python-test", "project": "test", "priority": "experiment", "run": {"command": command}, "needs": {"memory_gb": 1, "platform": ["fake"]}, "resumable": False, **extra}

    def test_consumer_skill_examples(self):
        # The guide's ladder, executed as written in one Git project against the
        # real dev API: one command, files back, myce init, then the entry's whole
        # trip. The YAML block is the entry after adding outputs; the Python
        # example then reruns that entry and adopts the same job.
        guide = (ROOT / "skills/mycelium/SKILL.md").read_text()
        blocks = re.findall(r"^```sh\n(.*?)^```$", guide, re.M | re.S)
        self.assertEqual(len(blocks), 4, "consumer guide CLI ladder changed shape")
        entry = re.findall(r"^```yaml\n(project: .*?)^```$", guide, re.M | re.S)
        self.assertEqual(len(entry), 1, "consumer guide must show the edited entry once")
        examples = re.findall(r"^```python\n(.*?)^```$", guide, re.M | re.S)
        self.assertEqual(len(examples), 1, "consumer guide must keep one runnable Python example")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / "project"
            project.mkdir()
            git = {"GIT_AUTHOR_NAME": "guide", "GIT_AUTHOR_EMAIL": "guide@example.invalid",
                   "GIT_COMMITTER_NAME": "guide", "GIT_COMMITTER_EMAIL": "guide@example.invalid"}
            environment = {**os.environ, **git, "HOME": str(root), "XDG_CACHE_HOME": str(root / ".cache"),
                           "MYCE_QUEUE": self.client.url, "MYCE_TOKEN": "dev-token",
                           "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}"}
            # The guide assumes an existing project repository.
            subprocess.run(["sh", "-ec", "git init -q && git commit -q --allow-empty -m start"],
                           cwd=project, env=environment, check=True)
            for index, block in enumerate(blocks):
                result = subprocess.run(["sh", "-ec", block], cwd=project, env=environment,
                                        capture_output=True, text=True, timeout=90)
                self.assertEqual(result.returncode, 0, block + result.stdout + result.stderr)
                if index == 1:
                    self.assertEqual(result.stdout.splitlines()[-1], "mycelium-ease-audit-ok")
                if index == 2:
                    (project / ".mycelium/adapter.yaml").write_text(entry[0])
            self.assertEqual((project / "accepted/result.txt").read_bytes(),
                             b"mycelium-ease-audit-ok\n")
            (project / "accepted/result.txt").unlink()
            # run_entry drives the real CLI, which can't read this Python-only
            # installed-config fixture, so the example uses environment authority
            # here; installed authority has its own tests above.
            self.assertIn("Client.from_installed()", examples[0])
            example = examples[0].replace("Client.from_installed()", "Client(os.environ['MYCE_QUEUE'])")
            result = subprocess.run([sys.executable, "-c", "import os\n" + example], cwd=project,
                                    env={**environment, "PYTHONPATH": str(ROOT / "clients/python/src")},
                                    capture_output=True, text=True, timeout=90)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.wait_observers(root)

    def test_consumer_skill_relative_pixi_lock(self):
        # The submit reference's relative Pixi lock example: the CLI uploads the
        # project's lock and manifest, so the queue never reads env/pixi.lock.
        guide = ROOT / "skills/mycelium/references/submit.md"
        blocks = [b for b in re.findall(r"^```sh\n(.*?)^```$", guide.read_text(), re.M | re.S)
                  if "lock_file: env/pixi.lock" in b]
        self.assertEqual(len(blocks), 1, "submit reference must keep its relative Pixi lock example")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, text in {"source/README.txt": "fixture\n",
                               "env/pixi.lock": "version: 6\nenvironments: {}\n",
                               "env/pixi.toml": "[workspace]\nname = 'example'\n"}.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            (root / ".mycelium").mkdir()
            environment = {**os.environ, "HOME": str(root), "XDG_CACHE_HOME": str(root / ".cache"),
                           "MYCE_QUEUE": self.client.url,
                           "MYCE_TOKEN": "dev-token",
                           "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}"}
            result = subprocess.run(["sh", "-ec", blocks[0]], cwd=root, env=environment,
                                    capture_output=True, text=True, timeout=90)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            job = re.search(r'"job_id":"([^"]+)"', result.stdout)[1]
            self.assertRegex(result.stdout, r'"files":\{"pixi\.lock":"[^"]+","pixi\.toml":"[^"]+"\}')
            lock = self.client.get(job)["environment_lock"]
            self.assertEqual(lock["backend"], "pixi")
            self.assertEqual(lock["sha256"]["pixi.lock"],
                             hashlib.sha256((root / "env/pixi.lock").read_bytes()).hexdigest())
            self.client.cancel(job)
            self.client.wait(job, timeout=30)
            self.wait_observers(root)

    def test_harness_wake_never_reaches_runner(self):
        # #1557: a myce run started by these tests, under a parent whose harness
        # variables name a reachable agent, must deliver no wake to that agent.
        home = self.root / "wake-home"
        project = home / "project"
        # Only a profile or adapter submit arms the default wake (cmd/myce/run.go).
        for name in (".mycelium/profiles/quickstart.yaml", "examples/quickstart-input/hello.txt"):
            (project / name).parent.mkdir(parents=True, exist_ok=True)
            (project / name).write_bytes((ROOT / name).read_bytes())
        woken = home / "woken"
        cli = home / "fake-harness-cli"
        cli.write_text(f"#!/bin/sh\necho \"$@\" >> '{woken}'\ncat >> '{woken}'\n")
        cli.chmod(0o755)
        parent = {**os.environ, "HOME": str(home), "XDG_CACHE_HOME": str(home / ".cache"),
                  "MYCE_TOKEN": "dev-token", "PYTHONPATH": str(ROOT / "clients/python/src")}
        parent.update({"T3CODE_THREAD_ID": "fixture-thread", "T3CODE_CLI_PATH": str(cli)})
        def submit(strip, key):
            child = ("import subprocess, sys\n"
                     f"sys.path.insert(0, {str(Path(__file__).parent)!r})\n"
                     + ("import test_client\n" if strip else "") +
                     f"r = subprocess.run([{str(self.root / 'myce')!r}, 'run', '--queue', {self.client.url!r}, "
                     "'--profile', '.mycelium/profiles/quickstart.yaml', '--key', " + repr(key) + "], capture_output=True, text=True, timeout=60)\n"
                     "sys.stdout.write(r.stdout + r.stderr)\nsys.exit(r.returncode)\n")
            result = subprocess.run([sys.executable, "-c", child], cwd=project, env=parent,
                                    capture_output=True, text=True, timeout=90)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            job = re.search(r'"job_id":"([^"]+)"', result.stdout)[1]
            self.assertEqual(self.client.wait(job, timeout=30)["state"], "succeeded")
            self.wait_observers(home)

        submit(strip=True, key="wake-stripped")
        self.assertFalse(woken.exists(), woken.read_text() if woken.exists() else "")
        # Control: without this module's cleanup the same run does wake the
        # fake harness, so the assertion above can fail.
        submit(strip=False, key="wake-control")
        self.assertIn("thread send fixture-thread -", woken.read_text())

    def test_run_entry_matches_cli_trip(self):
        # #1341: run_entry submits through myce run --entry (same source, key and
        # receipt), then waits, downloads, verifies and cancels on Ctrl-C itself.
        project = self.root / "entry-project"
        (project / "source").mkdir(parents=True)
        (project / ".mycelium").mkdir()
        (project / "source/README.txt").write_text("fixture\n")
        targets = "[{os: darwin, arch: arm64, platform: cpu}, {os: darwin, arch: amd64, platform: cpu}, " \
                  "{os: linux, arch: amd64, platform: cpu}, {os: linux, arch: arm64, platform: cpu}]"
        recipe = f"source: {{directory: source}}\n    needs: {{platform: cpu, cpu_cores: 1, memory_gb: 0.125}}\n    targets: {targets}\n"
        (project / ".mycelium/adapter.yaml").write_text(f"""schema: 1
project: python-entry
default: write
entries:
  write:
    {recipe}    run: {{command: [sh, -c, 'mkdir -p out && printf "$0\\n" > out/result.txt']}}
    outputs: {{out/result.txt: results}}
  fail:
    {recipe}    run: {{command: [sh, -c, 'exit 7']}}
  sleep:
    {recipe}    run: {{command: [sh, -c, 'sleep 60']}}
""")
        home = self.root / "entry-home"
        home.mkdir(exist_ok=True)
        # No installed authority in HOME: the CLI uses the environment token only.
        with patch.dict(os.environ, {"MYCE_TOKEN": "dev-token", "HOME": str(home),
                                     "XDG_CACHE_HOME": str(home / ".cache")}):
            client = Client(self.client.url, poll_interval=0.05)
            myce = self.root / "myce"
            outcome = client.run_entry(out=project / "accepted", root=project, args=["hello"],
                                       key="entry-ok", timeout=60, myce=myce)
            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.files["out/result.txt"].read_bytes(), b"hello\n")
            self.assertEqual(outcome.job["spec"]["authoring_origin"], "adapter")
            # The same key is the same job, whether through Python or the CLI.
            again = client.run_entry(out=project / "again", root=project, args=["hello"],
                                     key="entry-ok", wait=False, myce=myce)
            self.assertEqual(again["job_id"], outcome.job["id"])
            cli = subprocess.run([str(myce), "run", "--queue", client.url, "--entry", "write", "--key", "entry-ok", "--", "hello"],
                                 cwd=project, capture_output=True, text=True, timeout=60)
            self.assertEqual(cli.returncode, 0, cli.stdout + cli.stderr)
            self.assertEqual(json.loads(cli.stdout.splitlines()[-1])["job_id"], outcome.job["id"])
            self.assertEqual(client.download_results(outcome.job["id"], project / "fetched"),
                             {"out/result.txt": (project / "fetched/out/result.txt").absolute()})

            with self.assertRaises(EntryFailedError) as failed:
                client.run_entry("fail", out=project / "failed", root=project, key="entry-fail", timeout=60, myce=myce)
            self.assertEqual((failed.exception.exit_code, failed.exception.outcome.job["state"]), (7, "failed"))
            unchecked = client.run_entry("fail", out=project / "failed", root=project, key="entry-fail",
                                         check=False, myce=myce)
            self.assertEqual((unchecked.exit_code, unchecked.files), (7, {}))

            with patch.object(client, "wait", side_effect=KeyboardInterrupt):
                with self.assertRaises(EntryInterrupted) as interrupted:
                    client.run_entry("sleep", out=project / "slept", root=project, key="entry-sleep", myce=myce)
            self.assertTrue(interrupted.exception.cancelled)
            self.assertEqual(client.wait(interrupted.exception.job_id, timeout=30)["state"], "cancelled")

            with self.assertRaises(EntryPreparationError) as refused:
                client.run_entry("absent", out=project / "x", root=project, myce=myce)
            self.assertEqual(refused.exception.exit_code, 64)
        self.wait_observers(home)
        with self.assertRaisesRegex(ValueError, "installed authority"):
            self.client.run_entry(out=project / "x", root=project, myce=myce)

    def test_cancel_running_by_key(self):
        spec = self.spec([sys.executable, "-u", "-c",
                          "import time; print('cancel-ready', flush=True); time.sleep(60)"],
                         submission_key="python-running-cancel")
        job = self.client.submit(spec)
        end = time.monotonic() + 15
        while "cancel-ready" not in "".join(chunk["text"] for chunk in self.client.logs(job["id"])):
            if time.monotonic() >= end:
                self.fail("running job did not report ready")
            time.sleep(0.05)
        ack = self.client.cancel_submission("test", spec["submission_key"])
        self.assertEqual(ack["job_id"], job["id"])
        self.assertTrue(ack["will_not_run"])
        self.assertEqual(ack["release"], "pending")
        self.assertEqual(self.client.wait(job["id"], timeout=15)["state"], "cancelled")
        ack = self.client.cancel_submission("test", spec["submission_key"])
        self.assertEqual(ack["release"], "confirmed")
        self.assertEqual(self.client.submit(spec)["id"], job["id"])
        self.assertEqual(self.client.get(job["id"])["attempt"], 1)

    def test_keyed_submission_repeat_and_conflict(self):
        spec = self.spec([sys.executable, "-c", "print('keyed')"], submission_key="Exact.Key_182:Python-01")
        first = self.client.submit(spec)
        final = self.client.wait(first["id"], timeout=10)
        self.assertEqual(final["state"], "succeeded")
        replay = self.client.submit(spec)
        for job in (first, self.client.get(first["id"]), final, replay):
            self.assertEqual(job["spec"]["project"], spec["project"])
            self.assertEqual(job["spec"].get("submission_key"), spec["submission_key"])
        self.assertEqual(replay["id"], first["id"])
        spec["name"] = "changed"
        with self.assertRaises(APIError) as caught:
            self.client.submit(spec)
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(caught.exception.code, "submission_key_conflict")

    def test_production_start_deadline(self):
        with self.assertRaises(APIError) as caught:
            self.client.submit(self.spec(
                [sys.executable, "-c", "raise SystemExit('must not run')"],
                priority="production", start_by="2000-01-01T00:00:00Z",
                needs={"memory_gb": 1, "platform": ["xpu"]}))
        self.assertEqual(caught.exception.status, 400)
        self.assertEqual(caught.exception.code, "job_deadline_invalid")
        # A new submission must arrive before its deadline. Ineligible hardware
        # keeps it queued until that accepted deadline expires.
        start_by = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat(
            timespec="seconds").replace("+00:00", "Z")
        job = self.client.submit(self.spec(
            [sys.executable, "-c", "raise SystemExit('must not run')"],
            priority="production", start_by=start_by,
            needs={"memory_gb": 1, "platform": ["xpu"]}))
        failed = self.client.wait(job["id"], timeout=10)
        self.assertEqual(failed["state"], "failed")
        # Public status strips internal placement blockers from this diagnostic.
        self.assertEqual(failed["reason"], "production start deadline exceeded")
        self.assertEqual(failed["spec"]["start_by"], start_by)
        self.assertEqual(failed["attempt"], 0)
        self.assertIsNone(failed.get("started_at"))
        self.assertIsNotNone(failed.get("finished_at"))

    def test_submit_wait_logs_and_failure(self):
        original = self.spec([sys.executable, "-c", "print('first', flush=True); print('last', flush=True)"])
        job = self.client.submit(original)
        chunks = list(self.client.logs(job["id"], follow=True))
        self.assertIn("first", "".join(c["text"] for c in chunks))
        self.assertIn("last", "".join(c["text"] for c in chunks))
        self.assertEqual(self.client.wait(job["id"], timeout=10)["state"], "succeeded")
        self.assertEqual(self.client.get(job["id"])["spec"]["authoring_origin"], "python")
        self.assertEqual(list(self.client.logs(job["id"], after=chunks[-1]["seq"])), [])
        self.assertEqual(original["needs"]["platform"], ["fake"])
        failed = self.client.submit(self.spec([sys.executable, "-c", "raise SystemExit(7)"]))
        self.assertEqual(self.client.wait(failed["id"], timeout=10)["state"], "failed")
        with self.assertRaises(APIError) as caught:
            self.client.get("missing")
        self.assertEqual((caught.exception.status, caught.exception.code), (404, "job_unknown"))

    def test_status_cancel_timeout_auth(self):
        job = self.client.submit(self.spec([sys.executable, "-c", "print('never')"], allowed_machines=["absent"]))
        self.assertIn(job["id"], [j["id"] for j in self.client.list()])
        self.assertTrue(self.client.status()["hyphas"])
        self.assertTrue(self.client.status(snapshot=True)["machines"])
        with self.assertRaises(TimeoutError):
            self.client.wait(job["id"], timeout=0.1)
        self.client.cancel(job["id"])
        self.assertEqual(self.client.wait(job["id"], timeout=2)["state"], "cancelled")
        with self.assertRaises(APIError) as caught:
            Client(self.client.url, "wrong").status()
        self.assertEqual(caught.exception.status, 401)

    def test_yaml(self):
        try:
            import yaml
        except ImportError:
            self.skipTest("install yaml extra")
        text = yaml.safe_dump(self.spec([sys.executable, "-c", "print('yaml')"]))
        path = self.root / "job.yaml"
        path.write_text(text)
        for source in (text, path):
            job = self.client.submit(source)
            self.assertEqual(self.client.wait(job["id"], timeout=10)["state"], "succeeded")
        with self.assertRaises(ValueError):
            self.client.submit_yaml("- not-a-job")

    def test_openai_sdk(self):
        try:
            import openai
        except ImportError:
            self.skipTest("install openai extra")
        with self.client.openai(project="python-test", priority="production") as sdk:
            embedding = sdk.embeddings.create(model="dev-model", input="hello")
            self.assertTrue(embedding.data[0].embedding)
            chat = sdk.chat.completions.create(model="dev-model", messages=[{"role": "user", "content": "hello"}])
            self.assertTrue(chat.choices[0].message.content)
            chunks = list(sdk.chat.completions.create(model="dev-model", messages=[{"role": "user", "content": "hello"}], stream=True))
            self.assertTrue(chunks)
        with self.assertRaises(ValueError):
            self.client.openai(project="test", priority="deadline")

    def test_00_resumable_command_preempted_and_reloaded(self):
        # Before SDK tests load a resident model: this job occupies all fake memory.
        # Real runner, cooperative SIGTERM, attempt-specific checkpoint copy.
        script = self.root / "resumable.py"
        script.write_text('''import time, os
from mycelium import Checkpoint, StopSignal
checkpoint = Checkpoint()
n = checkpoint.load(0)
print(f"loaded {n}; attempt {os.environ['MYCE_ATTEMPT']}", flush=True)
with StopSignal() as stop:
    while n < 40 and not stop.requested:
        time.sleep(.1)
        n += 1
        checkpoint.save(n)
    checkpoint.save(n)
print(f"saved {n}", flush=True)
''')
        spec = self.spec([sys.executable, str(script)], resumable=True)
        spec["needs"]["memory_gb"] = 64
        spec["run"]["env"] = {"PYTHONPATH": str(ROOT / "clients/python/src")}
        job = self.client.submit(spec)
        end = time.monotonic() + 10
        while not list(self.client.logs(job["id"])):
            if time.monotonic() > end:
                self.fail("resumable job did not start")
            time.sleep(.05)
        urgent = self.spec([sys.executable, "-c", "print('production')"], priority="production")
        urgent["needs"]["memory_gb"] = 64
        production = self.client.submit(urgent)
        self.assertEqual(self.client.wait(production["id"], timeout=20)["state"], "succeeded")
        result = self.client.wait(job["id"], timeout=20)
        self.assertEqual(result["state"], "succeeded")
        self.assertGreaterEqual(result["attempt"], 2)
        logs = "".join(c["text"] for c in self.client.logs(job["id"]))
        self.assertRegex(logs, r"loaded [1-9][0-9]*; attempt 2")




class StatusPagingTests(unittest.TestCase):
    def test_status_default_full_and_paged_list(self):
        client = Client("http://queue.example", "fixture")
        with patch.object(client, "_call", return_value={}) as call:
            client.status()
            call.assert_called_with("GET", "/api/status")
            client.status(full=True)
            call.assert_called_with("GET", "/api/status?detail=full")
            client.status(snapshot=True)
            call.assert_called_with("GET", "/api/status?view=snapshot")
        with patch.object(client, "_call", return_value={}) as call:
            client.status(sections="work,storage")
            call.assert_called_with("GET", "/api/status?sections=work%2Cstorage")
        with patch.object(client, "_call", side_effect=[
                {"jobs": [{"id": "new"}], "jobs_page": {"next_before": "new"}},
                {"jobs": [{"id": "old"}], "jobs_page": {}},
        ]) as call:
            self.assertEqual([j["id"] for j in client.list(project="example")], ["new", "old"])
            self.assertEqual([c.args[1] for c in call.call_args_list], [
                "/api/status?jobs=all&project=example",
                "/api/status?jobs=all&project=example&before=new",
            ])
        # Older queues ignore the filter but return their complete legacy list.
        with patch.object(client, "_call", return_value={"jobs": [{"id": "old-queue"}]}):
            self.assertEqual(client.list(), [{"id": "old-queue"}])


if __name__ == "__main__":
    unittest.main()


class ClientIdentityTests(unittest.TestCase):
    def test_job_reads_send_package_identity(self):
        from mycelium.client import USER_AGENT
        from importlib.metadata import version, PackageNotFoundError
        try:
            expected = version("mycelium-client")
        except PackageNotFoundError:
            expected = "development"
        self.assertEqual(USER_AGENT, "mycelium-python/" + expected)
        client = Client("http://queue.invalid", "fixture")
        requests = []
        def respond(request, **kwargs):
            requests.append(request)
            if request.selector == "/api/wait":
                return Reply(b'{"jobs":[{"id":"job","state":"succeeded","revision":1}]}')
            return Reply(b'{"id":"job","state":"succeeded"}')
        client._urlopen = respond
        client.get("job")
        client.wait("job")
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[1].selector, "/api/wait")
        self.assertEqual(requests[2].selector, "/api/jobs/job?view=wait")
        for request in requests:
            self.assertEqual(request.get_header("User-agent"), USER_AGENT)


class LogFollowWaitViewTests(unittest.TestCase):
    def test_pending_wait_reads_and_final_log_drain(self):
        client = Client("http://queue.invalid", "fixture", poll_interval=0.001)
        requests = []
        replies = [b'[{"seq":1,"text":"first"}]',
                   b'{"id":"job","state":"running"}', b'[]',
                   b'{"id":"job","state":"succeeded"}',
                   b'[{"seq":2,"text":"final"}]']
        def respond(request, **kwargs):
            requests.append(request.selector)
            return Reply(replies.pop(0))
        client._urlopen = respond
        chunks = list(client.logs("job", follow=True))
        self.assertEqual([chunk["text"] for chunk in chunks], ["first", "final"])
        self.assertEqual(requests, ["/api/jobs/job/logs?after=0",
                                   "/api/jobs/job?view=wait", "/api/jobs/job/logs?after=1",
                                   "/api/jobs/job?view=wait", "/api/jobs/job/logs?after=1"])

class WaitFloodBackoffTests(unittest.TestCase):
    def test_rate_limit_is_retried_with_backoff(self):
        client = Client(token="fixture")
        error = APIError(429, '{"code":"client_rate_limited"}')
        terminal = {"id": "job", "state": "succeeded"}
        # Retry the batch transport twice, retaining the existing backoff contract.
        with patch.object(client, "_read_job", return_value=terminal), patch.object(
                client, "_wait_batch", side_effect=[error, error, [dict(terminal, revision=1)]]), patch("mycelium.client.time.sleep") as pause:
            self.assertEqual(client.wait("job"), terminal)
        self.assertEqual(pause.call_count, 2)
        self.assertGreater(pause.call_args_list[1].args[0], 0)
