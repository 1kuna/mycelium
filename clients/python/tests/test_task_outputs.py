import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mycelium import Client


class TaskOutputTests(unittest.TestCase):
    def test_verified_synced_delivery_replay_and_partial_failure(self):
        payloads = {"out.txt": b"unique result", "nested/second.txt": b"second result"}
        files = {name: {"sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}
                 for name, data in payloads.items()}
        receipt = {"protocol": "worker-custody/1", "execution": {"id": "job", "attempt": 3},
                   "machine": "producer", "session": "original", "allocation": "allocation",
                   "files": files, "destination_id": "submitter", "manifest_sha256": "a" * 64,
                   "verified": True}
        task = {"protocol": "task-output/1", "kind": "submitter", "receipt": receipt,
                "directory_receipt": {"destination_id": "submitter", "id": "job", "attempt": 3,
                                      "machine": "producer", "session": "original",
                                      "policy": "task-output/1", "manifest_sha256": "a" * 64,
                                      "files": files}}
        client = Client(token="fixture")
        acknowledgements, requests = [], []
        with tempfile.TemporaryDirectory() as root:
            target = Path(root).resolve() / "job" / "3"

            def call(method, path, body=None):
                if method == "GET":
                    return copy.deepcopy(task)
                self.assertEqual(path, "/api/task-outputs/verified")
                self.assertEqual(body, receipt)
                for name, data in payloads.items():
                    self.assertEqual((target / name).read_bytes(), data)
                self.assertEqual(json.loads((target / ".myce-delivery.json").read_text()),
                                 task["directory_receipt"])
                acknowledgements.append(body)
                return {}

            def transfer(method, path, **kwargs):
                if path.endswith("/preservation"):
                    self.assertEqual(method, "PUT")
                    self.assertEqual(kwargs["headers"]["X-Myce-Delivery"], receipt["manifest_sha256"])
                    chunks = []
                    while chunk := kwargs["data"].read(4):
                        self.assertLessEqual(len(chunk), 4)
                        chunks.append(chunk)
                    self.assertEqual(b"".join(chunks), b"".join(payloads[name] for name in sorted(files)))
                    self.assertEqual(int(kwargs["headers"]["Content-Length"]), sum(map(len, payloads.values())))
                    response = io.BytesIO(b"{}")
                    response.headers = {}
                    return response
                requests.append(path)
                self.assertTrue(path.startswith("/api/task-outputs/job/files/"))
                self.assertIn("?manifest=" + "a" * 64, path)
                name = "nested/second.txt" if "nested" in path else "out.txt"
                return io.BytesIO(payloads[name])

            with patch.object(client, "_call", side_effect=call), \
                    patch.object(client, "get", return_value={"state": "succeeded", "local_outputs": {"files": files}}), \
                    patch.object(client, "_file_request", side_effect=transfer):
                self.assertEqual(client.deliver_outputs("job", root), target)
                self.assertEqual(len(requests), 2)
                self.assertEqual(client.deliver_outputs("job", root), target)
                self.assertEqual(len(requests), 2)  # Lost ACK replay verifies the local tree.
                self.assertEqual(len(acknowledgements), 2)
                task["collection_pending"] = True
                self.assertEqual(client.deliver_outputs("job", root), target)
                self.assertEqual(len(requests), 2)
                self.assertEqual(len(acknowledgements), 3)
                (target / "out.txt").write_bytes(b"corrupt")
                with self.assertRaisesRegex(ValueError, "content mismatch"):
                    client.deliver_outputs("job", root)
                self.assertEqual(len(acknowledgements), 3)

        # A partial multi-file pull removes only its own temporary stage and
        # leaves no whole-task acknowledgement or falsely exposed directory.
        with tempfile.TemporaryDirectory() as root, \
                patch.object(client, "_call", return_value=copy.deepcopy(task)) as calls, \
                patch.object(client, "get", return_value={"state": "succeeded", "local_outputs": {"files": files}}), \
                patch.object(client, "_file_request", side_effect=[io.BytesIO(payloads["out.txt"]), io.BytesIO(b"partial")]):
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                client.deliver_outputs("job", root)
            self.assertFalse((Path(root) / "job" / "3").exists())
            self.assertEqual(list((Path(root) / "job").iterdir()), [])
            self.assertTrue(all(call.args[0] == "GET" for call in calls.call_args_list))

    def test_delivery_refuses_unsafe_or_wrong_version_metadata(self):
        client = Client(token="fixture")
        with tempfile.TemporaryDirectory() as root, \
                patch.object(client, "_call", return_value={"protocol": "old", "kind": "submitter"}), \
                patch.object(client, "_file_request", side_effect=AssertionError("must not stream")):
            with self.assertRaisesRegex(ValueError, "task-output/1"):
                client.deliver_outputs("job", root)
