"""The OpenAI SDK must retain installed authority for image chat and streams."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import ssl
import tempfile
import threading
import unittest

from mycelium import Client


class InstalledOpenAITests(unittest.TestCase):
    def test_image_chat_retains_tls_pin_and_streaming(self):
        try:
            import openai
        except ImportError:
            self.skipTest("install mycelium-client[openai]")
        fixtures = Path(__file__).with_name("fixtures")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(fixtures / "transfer-cert.pem", fixtures / "transfer-key.pem")
        requests, redirected = [], []
        finish_stream = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                redirected.append(self.path)
                self.send_error(500)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append((self.path, self.headers, body))
                if body["model"] == "redirect":
                    self.send_response(302)
                    self.send_header("Location", "/must-not-follow")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(200)
                if body.get("stream"):
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    chunk = {"id": "fixture", "object": "chat.completion.chunk", "created": 0,
                             "model": "vision", "choices": [{"index": 0, "delta": {"content": "image"}}]}
                    self.wfile.write(b"data: " + json.dumps(chunk).encode() + b"\n\n")
                    self.wfile.flush()
                    finish_stream.wait(10)
                    self.wfile.write(b"data: [DONE]\n\n")
                else:
                    reply = json.dumps({"id": "fixture", "object": "chat.completion", "created": 0,
                                        "model": "vision", "choices": [{"index": 0, "finish_reason": "stop",
                                        "message": {"role": "assistant", "content": "two images"}}]}).encode()
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(reply)))
                    self.end_headers()
                    self.wfile.write(reply)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                authority, config = Path(directory) / "joining.json", Path(directory) / "config.json"
                config.write_text(json.dumps({"joining_file": str(authority)}))
                cert = ssl.PEM_cert_to_DER_cert((fixtures / "transfer-cert.pem").read_text())
                invitation = {"version": 1, "address": f"https://127.0.0.1:{server.server_port}",
                              "token": "fixture-token", "fingerprint": hashlib.sha256(cert).hexdigest()}
                authority.write_text(json.dumps(invitation))
                authority.chmod(0o600)
                messages = [{"role": "user", "content": [
                    {"type": "text", "text": "Compare the images"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,first"}},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,second"}},
                ]}]
                with Client.from_installed(config).openai(project="test", timeout=5) as sdk:
                    reply = sdk.chat.completions.create(model="vision", messages=messages)
                    self.assertEqual(reply.choices[0].message.content, "two images")
                    self.assertEqual(requests[0][0], "/v1/chat/completions")
                    self.assertEqual(requests[0][1]["Authorization"], "Bearer fixture-token")
                    self.assertEqual(requests[0][1]["X-Myce-Project"], "test")
                    self.assertEqual(requests[0][2]["messages"], messages)
                    with sdk.chat.completions.create(model="vision", messages=messages, stream=True) as stream:
                        # The first event must arrive while the response remains open.
                        self.assertEqual(next(stream).choices[0].delta.content, "image")
                        finish_stream.set()
                        self.assertEqual(list(stream), [])
                    with self.assertRaises(openai.APIStatusError):
                        sdk.chat.completions.create(model="redirect", messages=messages)
                    self.assertEqual(redirected, [])
                invitation["fingerprint"] = "0" * 64
                authority.write_text(json.dumps(invitation))
                with Client.from_installed(config).openai(project="test", timeout=5) as sdk:
                    with self.assertRaises(openai.APIConnectionError):
                        sdk.chat.completions.create(model="vision", messages=messages)
                self.assertEqual(len(requests), 3)  # Wrong pin sent neither token nor payload.
        finally:
            finish_stream.set()
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
