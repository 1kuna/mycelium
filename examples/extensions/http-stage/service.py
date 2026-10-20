"""The app's existing HTTP dependency, launched as an attempt-owned sidecar."""
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"increment": 1}\n')

HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
