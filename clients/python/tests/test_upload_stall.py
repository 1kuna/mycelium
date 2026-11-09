import hashlib
import http.client
import socket
import threading
import time
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from mycelium import Client, TransferStalledError
from mycelium.transfer import UploadTiming, _ProgressSocket


class UploadStallTests(unittest.TestCase):
    def test_paused_patch_tolerates_or_resumes_accepted_prefix(self):
        # One pause exceeds the old 523ms detector but fits the protocol floor;
        # the other forces a real stall and accepted-offset reconciliation.
        for pause, lost_ack, delayed_publication in ((.7, False, False), (1.5, False, False), (0, True, False), (0, True, True)):
            with self.subTest(pause=pause, lost_ack=lost_ack, delayed_publication=delayed_publication):
                body = b"source" * (2 << 20)
                digest = hashlib.sha256(body).hexdigest()
                accepted = bytearray()
                offsets = []
                published = [not delayed_publication]
                lock = threading.Lock()
                class Handler(BaseHTTPRequestHandler):
                    def log_message(self, *_):
                        pass
                    def do_HEAD(self):
                        with lock:
                            if "/blobs/" in self.path:
                                self.send_response(200 if len(accepted) == len(body) and published[0] else 404)
                                self.send_header("Content-Length", str(len(body)))
                                self.send_header("ETag", '"' + digest + '"')
                            else:
                                if delayed_publication and len(accepted) == len(body):
                                    published[0] = True
                                    self.send_response(404)
                                    self.end_headers()
                                    return
                                self.send_response(200)
                                self.send_header("Upload-Offset", str(len(accepted)))
                                self.send_header("Upload-Length", str(len(body)))
                        self.end_headers()
                    def do_POST(self):
                        self.send_response(201)
                        self.end_headers()
                    def do_PATCH(self):
                        length = int(self.headers["Content-Length"])
                        offset = int(self.headers["Upload-Offset"])
                        with lock:
                            if offset != len(accepted):
                                self.send_response(409)
                                self.end_headers()
                                return
                            offsets.append(offset)
                            first = len(offsets) == 1
                        if first:
                            prefix = self.rfile.read(64 << 10)
                            with lock:
                                accepted.extend(prefix)
                            length -= len(prefix)
                            time.sleep(pause)
                            if pause > 1:
                                self.close_connection = True
                                return
                        rest = self.rfile.read(length)
                        with lock:
                            accepted.extend(rest)
                            next_offset = len(accepted)
                        if lost_ack and next_offset == len(body):
                            self.close_connection = True
                            return
                        self.send_response(204)
                        self.send_header("Upload-Offset", str(next_offset))
                        self.end_headers()
                server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                threading.Thread(target=server.serve_forever, daemon=True).start()
                client = Client(f"http://127.0.0.1:{server.server_address[1]}", "fixture", transfer_timeout=30)
                client._jitter = lambda: 0
                connect = http.client.HTTPConnection.connect
                def small_buffer(connection):
                    connect(connection)
                    connection.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                try:
                    with tempfile.TemporaryDirectory() as directory:
                        source = Path(directory, "source")
                        source.write_bytes(body)
                        with patch.object(http.client.HTTPConnection, "connect", small_buffer):
                            self.assertEqual(client.upload_file(source), {"sha256": digest, "size_bytes": len(body)})
                    self.assertEqual(accepted, body)
                    self.assertEqual(len(offsets), 12 if pause < 1 else 13)
                    if pause > 1:
                        self.assertEqual(offsets[1], 64 << 10)
                finally:
                    server.shutdown()
                    server.server_close()

    def test_protocol_floor_pressure_and_service(self):
        timing = UploadTiming()
        self.assertEqual(timing.window(), 1)
        timing.observe(.131)
        self.assertEqual(timing.window(), 1)
        client = Client("http://fixture.invalid", "fixture")
        import io
        response = io.BytesIO()
        response.headers = {"Retry-After": "13"}
        with patch.object(client, "_urlopen", return_value=response):
            with client._file_request("HEAD", "/upload", timing=timing):
                pass
        self.assertEqual(timing.window(), 13)
        timing.observe(4)
        self.assertEqual(timing.window(), 16)

    def test_dead_connection_detected(self):
        receiver, sender = socket.socketpair()
        try:
            transfer = _ProgressSocket(receiver, adaptive=True, timing=UploadTiming())
            started = time.monotonic()
            with self.assertRaises(socket.timeout):
                transfer.recv(256)
            self.assertGreaterEqual(time.monotonic() - started, 1)
        finally:
            receiver.close()
            sender.close()
