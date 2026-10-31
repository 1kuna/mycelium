import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mycelium import Client


class RecoveredResultTests(unittest.TestCase):
    def test_recovery_captures_execution_before_upload(self):
        client = Client(token="fixture")
        ref = {"sha256": "a" * 64, "size_bytes": 12}
        plan = {"id": "job", "attempt": 3, "machine": "worker", "session": "original",
                "outputs": ["out.txt"]}
        order = []

        def call(*args):
            order.append("plan")
            return plan

        def upload(path):
            order.append("upload")
            return ref

        def attach(method, path, data, headers, size):
            order.append("attach")
            request = json.loads(data)
            self.assertEqual(request["session"], "original")
            self.assertEqual(request["attempt"], 3)
            self.assertEqual(request["files"], {"out.txt": ref})
            self.assertEqual(size, 12)
            return io.BytesIO(b'{"kind":"recovered","result_id":"record"}')

        with tempfile.NamedTemporaryFile() as source, \
                patch.object(client, "_call", side_effect=call), \
                patch.object(client, "upload_file", side_effect=upload), \
                patch.object(client, "_file_request", side_effect=attach):
            result = client.recover_result("job", files={"out.txt": source.name},
                                           key="recovery", requested_by="owner", reason="retained bytes",
                                           source={"kind": "nas", "location": "results/attempt"})
        self.assertEqual(order, ["plan", "upload", "attach"])
        self.assertEqual(result["kind"], "recovered")

    def test_recovery_rejects_special_sources_before_upload(self):
        client = Client(token="fixture")
        with tempfile.TemporaryDirectory() as root:
            fifo = Path(root) / "fifo"
            os.mkfifo(fifo)
            link = Path(root) / "link"
            link.symlink_to(fifo)
            for path in (fifo, link):
                with patch.object(client, "_call", return_value={"outputs": ["out.txt"]}), \
                        patch.object(client, "upload_file", side_effect=AssertionError("special source opened")):
                    with self.assertRaisesRegex(ValueError, "regular files"):
                        client.recover_result("job", files={"out.txt": path}, key="recovery",
                                              requested_by="owner", reason="retained bytes",
                                              source={"kind": "upload", "location": "retained"})

    def test_explicit_recovered_download_verifies_bytes(self):
        client = Client(token="fixture")
        payload = b"retained output"
        ref = {"sha256": hashlib.sha256(payload).hexdigest(), "size_bytes": len(payload)}
        record = {"kind": "recovered", "files": {"out.txt": ref}}
        with tempfile.TemporaryDirectory() as root:
            dest = Path(root) / "out.txt"
            for content in (payload, b"x" * len(payload)):
                with patch.object(client, "_call", return_value=record) as lookup, \
                        patch.object(client, "get", side_effect=AssertionError("ordinary success path")), \
                        patch.object(client, "_file_request", return_value=io.BytesIO(content)):
                    if content == payload:
                        client.download_result("failed-job", "out.txt", dest, recovered_result_id="record")
                    else:
                        with self.assertRaisesRegex(ValueError, "identity mismatch"):
                            client.download_result("failed-job", "out.txt", dest, recovered_result_id="record")
                    self.assertEqual(dest.read_bytes(), payload)
                    self.assertEqual(lookup.call_args.args[1],
                                     "/api/jobs/failed-job/recovered-results/record?require_files=true")
