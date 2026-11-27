import hashlib
import http.client
import io
import socket
import threading
import time
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from mycelium import Client
from mycelium.client import APIError, TransferStalledError
from mycelium.transfer import UploadTiming, _ProgressSocket


class UploadStallTests(unittest.TestCase):
    def test_paused_patch_tolerates_or_resumes_accepted_prefix(self):
        # One pause exceeds the old 523ms detector but fits the protocol floor;
        # the other forces a real stall and accepted-offset reconciliation.
        cases = ((.7, False, False, False), (1.5, False, False, False),
                 (0, True, False, False), (0, True, True, False), (0, False, True, True))
        for pause, lost_ack, delayed_publication, slow_finalize in cases:
            with self.subTest(pause=pause, lost_ack=lost_ack, delayed_publication=delayed_publication,
                              slow_finalize=slow_finalize):
                body = b"source" * (2 << 20)
                digest = hashlib.sha256(body).hexdigest()
                accepted = bytearray()
                offsets = []
                lost = [False]
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
                        if lost_ack and next_offset == len(body) and not lost[0]:
                            lost[0] = True
                            self.close_connection = True
                            return
                        if slow_finalize:
                            if self.headers.get("X-Myce-Upload-Finalizing") != "1":
                                self.send_error(400)
                                return
                            for _ in range(7):
                                time.sleep(.25)
                                self.send_response_only(102)
                                self.send_header("X-Myce-Upload-Finalizing", "1")
                                self.end_headers()
                        if next_offset == len(body):
                            published[0] = True
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
                    expected = [0]
                    if pause > 1:
                        expected.append(64 << 10)
                    if delayed_publication and lost_ack:
                        expected.append(len(body))
                    self.assertEqual(offsets, expected)
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


class _Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
    def monotonic(self):
        return self.now
    def time(self):
        return 1793880000 + self.now
    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class _Reply(io.BytesIO):
    def __init__(self, headers):
        super().__init__(b"")
        self.headers = headers


class UploadTimingRegressionTests(unittest.TestCase):
    def test_abandoned_requests_do_not_compound_the_window(self):
        # #2165: a silent server is given up within a bounded window per request.
        clock = _Clock()
        client = Client("http://fixture.invalid", "fixture")
        client._clock = clock
        timing = UploadTiming()
        timing.observe(2)
        windows = []
        def silent(*args, **kwargs):
            windows.append(timing.window())
            clock.now += windows[-1]
            raise TransferStalledError("file transfer made no network progress")
        with patch.object(client, "_file_request_once", side_effect=silent):
            for _ in range(6):
                with self.assertRaises(TransferStalledError):
                    client._file_request("PATCH", "/upload", b"x", timing=timing, retry=False)
        self.assertEqual(windows[0], 8)
        self.assertLessEqual(max(windows), 60, windows)
        self.assertEqual(windows[-1], 60, windows)

    def test_late_abandoned_chunk_retries_within_seconds(self):
        # #2164: one chunk abandoned late in a long upload is retried after one
        # recovery period, not after a pause as long as the upload so far.
        clock = _Clock()
        client = Client("http://fixture.invalid", "fixture")
        client._clock = clock
        client._jitter = lambda: 0
        body = bytes(6 << 20)
        digest = hashlib.sha256(body).hexdigest()
        state = {"accepted": 0, "stalled": False}
        def server(method, path, data=None, headers=None, size=0, owner_timeout=None, timing=None):
            if method == "HEAD" and "/blobs/" in path:
                if state["accepted"] < len(body):
                    raise APIError(404, '{"code":"file_not_found","error":"missing"}')
                return _Reply({"Content-Length": str(len(body)), "ETag": '"' + digest + '"'})
            if method == "POST":
                clock.now += 60
                return _Reply({})
            if method == "HEAD":
                return _Reply({"Upload-Offset": str(state["accepted"]), "Upload-Length": str(len(body))})
            while chunk := data.read(1 << 20):
                state["accepted"] += len(chunk)
                clock.now += 60
            if not state["stalled"]:
                state["stalled"] = True
                clock.now += timing.window()
                raise TransferStalledError("file transfer made no network progress")
            return _Reply({"Upload-Offset": str(state["accepted"])})
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory, "source")
            source.write_bytes(body)
            with patch.object(client, "_file_request_once", side_effect=server):
                self.assertEqual(client.upload_file(source), {"sha256": digest, "size_bytes": len(body)})
        self.assertTrue(state["stalled"])
        self.assertGreater(clock.now, 5 * 60)
        self.assertEqual(clock.sleeps, [1.0])
