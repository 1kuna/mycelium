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

from mycelium import APIError, Checkpoint, Client, StopSignal

ROOT = Path(__file__).resolve().parents[3]


class ClientTests(unittest.TestCase):
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

    def test_submission_key_and_error_code(self):
        client = Client(token="test-only")
        spec = {"project": "test", "submission_key": "stable-key"}
        with patch.object(client, "_call", return_value={"id": "original"}) as call:
            self.assertEqual(client.submit(spec)["id"], "original")
        self.assertEqual(call.call_args.args[2]["submission_key"], "stable-key")
        self.assertEqual(spec, {"project": "test", "submission_key": "stable-key"})
        error = APIError(409, '{"code":"submission_key_conflict","error":"different spec"}')
        self.assertEqual(error.status, 409)
        self.assertEqual(error.code, "submission_key_conflict")
        self.assertIsNone(APIError(400, "invalid request").code)

    def test_wait_reports_production_start_deadline(self):
        client = Client(token="test-only")
        failed = {"id": "prod", "state": "failed", "reason": "production start deadline exceeded: host b70 disabled by operator"}
        with patch.object(client, "_call", return_value={"jobs": [failed]}):
            self.assertEqual(client.wait("prod"), failed)


    def test_log_pages_and_terminal_drain(self):
        client = Client(token="test-only")
        page = [{"seq": n, "attempt": 1, "text": str(n)} for n in range(1, 257)]
        responses = [page, [], [{"seq": 257, "attempt": 2, "text": "final"}]]
        with patch.object(client, "_call", side_effect=responses) as call:
            with patch.object(client, "get", return_value={"state": "succeeded"}):
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


class DevTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="myce-python-")
        cls.root = Path(cls.directory.name)
        binary = cls.root / "myce"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/myce"], cwd=ROOT, check=True, timeout=120)
        cls.output = (cls.root / "dev.log").open("w+")
        cls.process = subprocess.Popen([str(binary), "dev", "--listen", "127.0.0.1:0", "--workers", "1", "--db", str(cls.root / "queue.db"), "--checkpoints", str(cls.root / "checkpoints")], stdout=cls.output, stderr=cls.output)
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

    def spec(self, command, **extra):
        return {"name": "python-test", "project": "test", "priority": "experiment", "run": {"command": command}, "needs": {"memory_gb": 1, "platform": ["fake"]}, "resumable": False, **extra}

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
        spec = self.spec([sys.executable, "-c", "print('keyed')"], submission_key="python-key")
        first = self.client.submit(spec)
        self.assertEqual(self.client.wait(first["id"], timeout=10)["state"], "succeeded")
        self.assertEqual(self.client.submit(spec)["id"], first["id"])
        spec["name"] = "changed"
        with self.assertRaises(APIError) as caught:
            self.client.submit(spec)
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(caught.exception.code, "submission_key_conflict")

    def test_production_start_deadline(self):
        job = self.client.submit(self.spec(
            [sys.executable, "-c", "raise SystemExit('must not run')"],
            priority="production", start_by="2000-01-01T00:00:00Z",
            needs={"memory_gb": 1, "platform": ["xpu"]}))
        failed = self.client.wait(job["id"], timeout=2)
        self.assertEqual(failed["state"], "failed")
        self.assertIn("production start deadline exceeded", failed["reason"])
        self.assertIn("needs xpu", failed["reason"])
        self.assertEqual(failed["attempt"], 0)

    def test_submit_wait_logs_and_failure(self):
        original = self.spec([sys.executable, "-c", "print('first', flush=True); print('last', flush=True)"])
        job = self.client.submit(original)
        chunks = list(self.client.logs(job["id"], follow=True))
        self.assertIn("first", "".join(c["text"] for c in chunks))
        self.assertIn("last", "".join(c["text"] for c in chunks))
        self.assertEqual(self.client.wait(job["id"], timeout=10)["state"], "succeeded")
        self.assertEqual(list(self.client.logs(job["id"], after=chunks[-1]["seq"])), [])
        self.assertEqual(original["needs"]["platform"], ["fake"])
        failed = self.client.submit(self.spec([sys.executable, "-c", "raise SystemExit(7)"]))
        self.assertEqual(self.client.wait(failed["id"], timeout=10)["state"], "failed")
        with self.assertRaises(KeyError):
            self.client.get("missing")

    def test_status_cancel_timeout_auth(self):
        job = self.client.submit(self.spec([sys.executable, "-c", "print('never')"], allowed_machines=["absent"]))
        self.assertIn(job["id"], [j["id"] for j in self.client.list()])
        self.assertTrue(self.client.status()["machines"])
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


if __name__ == "__main__":
    unittest.main()
