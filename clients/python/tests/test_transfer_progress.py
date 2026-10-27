import hashlib
import http.client
import io
import json
from pathlib import Path
import socket
import select
import ssl
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import URLError

from mycelium import APIError, Client
from mycelium.transfer import _ProgressSocket, _ProgressTLS


class PartialUploadTests(unittest.TestCase):
    def test_tls_encrypted_progress_and_true_stall(self):
        """TLS writes must observe encrypted partial sends within a record."""
        fixtures = Path(__file__).with_name("fixtures")
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(fixtures / "transfer-cert.pem", fixtures / "transfer-key.pem")
        client_context = ssl.create_default_context(cafile=fixtures / "transfer-cert.pem")
        for stall in (False, True):
            with self.subTest(stall=stall):
                sender, relay_in = socket.socketpair()
                relay_out, receiver = socket.socketpair()
                sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
                idle = 1.2  # Six measured 0.2 s worker scheduling periods.
                sender.settimeout(idle)
                body = b"TLS progress" * 8192
                received = bytearray()
                stop, throttle, ready = threading.Event(), threading.Event(), threading.Event()

                def relay():
                    try:
                        while not stop.is_set():
                            for source in select.select([relay_in, relay_out], [], [], 0.05)[0]:
                                if source is relay_in and throttle.is_set() and stall:
                                    stop.wait()
                                    return
                                data = source.recv(1024)
                                if not data:
                                    return
                                (relay_out if source is relay_in else relay_in).sendall(data)
                                if source is relay_in and throttle.is_set():
                                    stop.wait(0.05)
                    except (OSError, ValueError):
                        pass

                def consume():
                    try:
                        with server_context.wrap_socket(receiver, server_side=True) as connection:
                            ready.set()
                            while len(received) < len(body):
                                chunk = connection.recv(8192)
                                if not chunk:
                                    return
                                received.extend(chunk)
                            connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                    except OSError:
                        pass

                threads = [threading.Thread(target=relay, daemon=True), threading.Thread(target=consume, daemon=True)]
                for thread in threads:
                    thread.start()
                try:
                    connection = _ProgressTLS(sender, client_context, "localhost")
                    self.assertTrue(ready.wait(idle))
                    throttle.set()
                    start = time.monotonic()
                    if stall:
                        with self.assertRaises(TimeoutError):
                            connection.sendall(body)
                    else:
                        connection.sendall(body)
                        self.assertGreater(time.monotonic() - start, idle)
                        response = http.client.HTTPResponse(connection)
                        response.begin()
                        connection.close()  # HTTPResponse's file must keep TLS alive.
                        with response:
                            self.assertEqual(response.read(), b"ok")
                        self.assertEqual(hashlib.sha256(received).digest(), hashlib.sha256(body).digest())
                finally:
                    stop.set()
                    for connection in (sender, receiver, relay_in, relay_out):
                        connection.close()
                    for thread in threads:
                        thread.join()

    def test_installed_tls_pin_preserves_authority(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        fixtures = Path(__file__).with_name("fixtures")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(fixtures / "transfer-cert.pem", fixtures / "transfer-key.pem")
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_PATCH(self):
                requests.append((self.headers.get("Authorization"), self.rfile.read(int(self.headers["Content-Length"]))))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory() as directory:
            authority = Path(directory) / "joining.json"
            config = Path(directory) / "config.json"
            config.write_text(json.dumps({"joining_file": str(authority)}))
            cert = ssl.PEM_cert_to_DER_cert((fixtures / "transfer-cert.pem").read_text())
            for pin in (hashlib.sha256(cert).hexdigest(), "0" * 64):
                authority.write_text(json.dumps({"version": 1, "address": f"https://127.0.0.1:{server.server_port}", "token": "fixture", "fingerprint": pin}))
                authority.chmod(0o600)
                client = Client.from_installed(config)
                if pin == "0" * 64:
                    with self.assertRaises(URLError):
                        client._file_request("PATCH", "/upload", b"must remain private")
                else:
                    with client._file_request("PATCH", "/upload", b"pin retained") as response:
                        self.assertEqual(response.read(), b"ok")
            self.assertEqual(requests, [("Bearer fixture", b"pin retained")])

    def test_installed_credentials_preserve_partial_sends_and_redirect_policy(self):
        """Installed credentials use progress sends before disclosing authority."""
        for scheme, credential_key in (("http", "environment_file"), ("https", "credentials")):
            with self.subTest(scheme=scheme), tempfile.TemporaryDirectory() as directory:
                sent = bytearray()

                class Socket:
                    def send(self, data, flags=0):
                        count = min(7, len(data))
                        sent.extend(data[:count])
                        return count

                    def sendall(self, data):
                        raise AssertionError("installed file upload used cumulative sendall")

                    def makefile(self, *args):
                        return io.BytesIO(b"HTTP/1.1 302 Found\r\nLocation: https://elsewhere.invalid/\r\nContent-Length: 0\r\n\r\n")

                    def close(self):
                        pass

                credentials = Path(directory) / "authority"
                credentials.write_text(f"MYCE_QUEUE={scheme}://queue.invalid\nMYCE_TOKEN=fixture\n")
                credentials.chmod(0o600)
                config = Path(directory) / "config.json"
                config.write_text(json.dumps({"queue": f"{scheme}://queue.invalid", credential_key: str(credentials)}))
                client = Client.from_installed(config)
                connection = http.client.HTTPConnection if scheme == "http" else http.client.HTTPSConnection
                with patch.object(connection, "connect", lambda connection: setattr(connection, "sock", Socket())):
                    with self.assertRaises(APIError) as caught:
                        client._file_request("PATCH", "/upload", b"complete payload")
                self.assertEqual(caught.exception.status, 302)
                self.assertIn(b"Authorization: Bearer fixture\r\n", sent)
                self.assertTrue(sent.endswith(b"complete payload"))
                self.assertEqual(sent.count(b"PATCH /upload"), 1)

    def test_partial_socket_sends_renew_budget(self):
        """Real socket sends progress beyond one cumulative sendall timeout."""
        sender, receiver = socket.socketpair()
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        sender.settimeout(0.25)
        body = b"upload" * (32 * 1024)
        received = bytearray()
        stop = threading.Event()

        def consume():
            try:
                while not stop.is_set():
                    chunk = receiver.recv(1024)
                    if not chunk:
                        break
                    received.extend(chunk)
                    stop.wait(0.01)
            finally:
                receiver.close()

        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
        start = time.monotonic()
        try:
            _ProgressSocket(sender).sendall(body)
            sender.shutdown(socket.SHUT_WR)
            thread.join()
            self.assertGreater(time.monotonic() - start, sender.gettimeout())
            self.assertEqual(hashlib.sha256(received).digest(), hashlib.sha256(body).digest())
        finally:
            stop.set()
            sender.close()
            thread.join()
