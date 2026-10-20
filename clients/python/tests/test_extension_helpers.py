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
            self.assertEqual((caught.exception.status, caught.exception.code), (404, "request_failed"))

    def test_rejects_invalid_artifact_identity(self):
        client = Client(token="fixture")
        with patch.object(client, "get", return_value={"state": "succeeded", "output_refs": {"result": {"sha256": "../escape", "size_bytes": 3}}}):
            with self.assertRaisesRegex(ValueError, "identity"):
                client.download_result("job", "result", "unused")
