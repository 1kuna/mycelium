import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError
from mycelium import APIError

from mycelium import Client


class ExtensionClientTests(unittest.TestCase):
    def test_keyed_submit_wait_and_terminal_failure(self):
        client = Client(token="fixture")
        with self.assertRaisesRegex(ValueError, "submission_key"):
            client.submit_and_wait({"project": "demo"})
        spec = {"project": "demo", "submission_key": "stable"}
        with patch.object(client, "submit", return_value={"id": "original"}) as submit:
            with patch.object(client, "wait", return_value={"id": "original", "state": "failed"}) as wait:
                self.assertEqual(client.submit_and_wait(spec, timeout=5)["state"], "failed")
                submit.assert_called_once_with(spec)
                wait.assert_called_once_with("original", timeout=5)

    def test_lost_response_retries_same_key_but_conflict_does_not(self):
        client = Client(token="fixture")
        spec = {"project": "demo", "submission_key": "stable"}
        with patch.object(client, "submit", side_effect=[URLError("lost response"), {"id": "original"}]) as submit:
            with patch.object(client, "wait", return_value={"id": "original", "state": "succeeded"}):
                client.submit_and_wait(spec)
                self.assertEqual(submit.call_count, 2)
                self.assertEqual(submit.call_args_list[0], submit.call_args_list[1])
        with patch.object(client, "submit", side_effect=APIError(409, "conflict")) as submit:
            with self.assertRaises(APIError):
                client.submit_and_wait(spec)
            self.assertEqual(submit.call_count, 1)

    def test_artifact_verification_and_explicit_destination(self):
        client = Client(token="fixture")
        body = b'actual accepted bytes'
        ref = {"sha256": hashlib.sha256(body).hexdigest(), "size_bytes": len(body)}
        job = {"state": "succeeded", "output_refs": {"../../worker-path": ref}}
        with tempfile.TemporaryDirectory() as folder:
            dest = Path(folder, "chosen-result")
            dest.write_bytes(b"prior app result")
            with patch.object(client, "get", return_value=job):
                with patch.object(client, "_file_request", return_value=io.BytesIO(body)) as request:
                    self.assertEqual(client.download_result("job", "../../worker-path", dest), dest)
                    self.assertEqual(dest.read_bytes(), body)
                    self.assertEqual(request.call_args.args, ("GET", "/api/files/blobs/" + ref["sha256"]))
                for corrupt in (b'corrupt', body + b'extra'):
                    with patch.object(client, "_file_request", return_value=io.BytesIO(corrupt)):
                        with self.assertRaisesRegex(ValueError, "artifact"):
                            client.download_result("job", "../../worker-path", dest)
                    self.assertEqual(dest.read_bytes(), body)
                self.assertEqual(list(Path(folder).iterdir()), [dest])
            for state in ("queued", "running", "failed", "cancelled"):
                with patch.object(client, "get", return_value={**job, "state": state}):
                    with self.assertRaises(APIError) as caught:
                        client.download_result("job", "../../worker-path", dest)
                    self.assertEqual((caught.exception.status, caught.exception.code), (409, "request_stale"))

    def test_missing_result_errors_are_typed(self):
        client = Client(token="fixture")
        with patch.object(client, "list", return_value=[]):
            with self.assertRaises(APIError) as caught:
                client.download_result("missing", "result", "unused")
            self.assertEqual((caught.exception.status, caught.exception.code), (404, "job_unknown"))
        with patch.object(client, "get", return_value={"state": "succeeded", "output_refs": {}}):
            with self.assertRaises(APIError) as caught:
                client.download_result("job", "missing", "unused")
            self.assertEqual((caught.exception.status, caught.exception.code), (404, "artifact_unknown"))

    def test_rejects_invalid_artifact_identity(self):
        client = Client(token="fixture")
        with patch.object(client, "get", return_value={"state": "succeeded", "output_refs": {"result": {"sha256": "../escape", "size_bytes": 3}}}):
            with self.assertRaisesRegex(ValueError, "identity"):
                client.download_result("job", "result", "unused")

    def test_run_uploads_submits_and_downloads_in_one_call(self):
        client = Client(token="fixture")
        ref = {"sha256": "a" * 64, "size_bytes": 1}
        job = {"id": "12345678abcdef", "state": "succeeded", "output_refs": {"result.json": ref}}
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(client, "upload_file", return_value=ref) as upload, \
                 patch.object(client, "submit_and_wait", return_value=job) as submit, \
                 patch.object(client, "download_result") as download:
                self.assertEqual(client.run("example@1", inputs={"input.json": "local.json"},
                    outputs=directory, project="demo", class_="batch", key="stable", timeout=5), job)
                upload.assert_called_once_with("local.json")
                submit.assert_called_once_with({"name": "example@1", "project": "demo", "class": "batch",
                    "extension": "example@1", "submission_key": "stable", "files": {"input.json": ref}}, timeout=5)
                download.assert_called_once_with(job["id"], "result.json", Path(directory) / "result.json")

    def test_run_failed_job_points_to_logs_and_never_downloads(self):
        client = Client(token="fixture")
        with patch.object(client, "submit_and_wait", return_value={"id": "12345678abcdef", "state": "failed", "reason": "execution failed"}), \
             patch.object(client, "download_result") as download:
            with self.assertRaises(APIError) as caught:
                client.run("example@1", inputs={}, outputs="results", project="demo", key="stable")
            self.assertEqual(caught.exception.code, "job_failed")
            self.assertEqual(caught.exception.job_id, "12345678abcdef")
            self.assertNotIn("HTTP", str(caught.exception))
            self.assertIn("myce logs 12345678", str(caught.exception))
            download.assert_not_called()

    def test_run_rejects_unsafe_paths(self):
        client = Client(token="fixture")
        with patch.object(client, "upload_file") as upload:
            for name in ("../escape", "/absolute", "a/../b", "a\\b", "a:b"):
                with self.assertRaisesRegex(ValueError, "relative"):
                    client.run("example@1", inputs={name: "local"}, outputs="results", project="demo", key="stable")
            upload.assert_not_called()
        with tempfile.TemporaryDirectory() as directory:
            for name in ("../escape", "/absolute", "a/../b", "a\\b", "a:b"):
                with patch.object(client, "submit_and_wait", return_value={"id": "job", "state": "succeeded", "output_refs": {name: {}}}), \
                     patch.object(client, "download_result") as download:
                    with self.assertRaisesRegex(ValueError, "relative"):
                        client.run("example@1", inputs={}, outputs=directory, project="demo", key="stable")
                    download.assert_not_called()
            Path(directory, "nested").symlink_to(Path(directory).parent, target_is_directory=True)
            with patch.object(client, "submit_and_wait", return_value={"id": "job", "state": "succeeded", "output_refs": {"nested/escape": {}}}):
                with self.assertRaisesRegex(ValueError, "symlink"):
                    client.run("example@1", inputs={}, outputs=directory, project="demo", key="stable")

    def test_short_job_lookup_and_ambiguous_prefix(self):
        job = {"id": "12345678abcdef"}
        self.assertEqual(Client._find([job], "12345678"), job)
        with self.assertRaises(APIError) as caught:
            Client._find([job, {"id": "12345678other"}], "12345678")
        self.assertEqual(caught.exception.code, "job_id_ambiguous")

    def test_wait_and_run_timeout_report_actual_reason_without_cancelling(self):
        client = Client(token="fixture")
        job = {"id": "12345678abcdef", "state": "queued",
               "reason": "no hypha has a qualified extension for this kind of work"}
        for run in (False, True):
            with patch.object(client, "_call", return_value={"jobs": [job]}), \
                 patch.object(client, "submit", return_value=job), \
                 patch.object(client, "cancel_submission") as cancel, \
                 patch("mycelium.client.time.monotonic", side_effect=[0, 0, .06, .06]), \
                 patch("mycelium.client.time.sleep"):
                with self.assertRaises(TimeoutError) as caught:
                    if run:
                        client.run("sample@1", inputs={}, outputs="unused", project="demo", key="key", timeout=.05)
                    else:
                        client.wait(job["id"], timeout=.05)
                for expected in (job["id"], "still queued", job["reason"]):
                    self.assertIn(expected, str(caught.exception))
                cancel.assert_not_called()
                self.assertEqual(job["state"], "queued")

    def test_wait_timeout_for_running_job_does_not_claim_waiting_for_hypha(self):
        client = Client(token="fixture")
        job = {"id": "12345678abcdef", "state": "running"}
        with patch.object(client, "_call", return_value={"jobs": [job]}), \
             patch("mycelium.client.time.monotonic", side_effect=[0, 0, .06, .06]), \
             patch("mycelium.client.time.sleep"):
            with self.assertRaises(TimeoutError) as caught:
                client.wait(job["id"], timeout=.05)
        message = str(caught.exception)
        self.assertIn("still running", message)
        self.assertNotIn("waiting for a hypha", message)
