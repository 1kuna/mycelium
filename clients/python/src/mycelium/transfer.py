"""urllib connections that renew the upload budget after each partial send."""
import http.client
import socket
import ssl
from urllib.request import HTTPHandler, HTTPSHandler, HTTPRedirectHandler, build_opener


class _ProgressSocket:
    def __init__(self, sock):
        self._socket = sock

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def sendall(self, data, flags=0):
        # socket.sendall's timeout is cumulative, even while bytes progress.
        # send returns partial progress and starts a fresh socket-operation
        # timeout on the next call. TLS uses MemoryBIO below, so encrypted
        # output reaches this loop without a cumulative SSLSocket.write.
        remaining = memoryview(data)
        while remaining:
            sent = self._socket.send(remaining, flags)
            if sent == 0:
                raise ConnectionError("file transfer connection closed during upload")
            remaining = remaining[sent:]


class _ProgressTLS:
    """Standard TLS engine with separately observable encrypted socket I/O."""

    def __init__(self, sock, context, server_hostname):
        self._raw = sock
        self._sender = _ProgressSocket(sock)
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl = context.wrap_bio(self._incoming, self._outgoing, server_hostname=server_hostname)
        self._io_refs = 0
        self._closed = False
        self._call(self._ssl.do_handshake)

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def _flush(self):
        while self._outgoing.pending:
            self._sender.sendall(self._outgoing.read())

    def _call(self, operation, *args):
        while True:
            try:
                result = operation(*args)
                self._flush()
                return result
            except ssl.SSLWantWriteError:
                self._flush()
            except ssl.SSLWantReadError:
                self._flush()
                data = self._raw.recv(64 * 1024)
                if data:
                    self._incoming.write(data)
                else:
                    self._incoming.write_eof()

    def send(self, data, flags=0):
        if flags:
            raise ValueError("nonzero TLS send flags are unsupported")
        # One TLS record bounds buffered ciphertext independently of body size.
        return self._call(self._ssl.write, memoryview(data)[:16384])

    def sendall(self, data, flags=0):
        remaining = memoryview(data)
        while remaining:
            remaining = remaining[self.send(remaining, flags):]

    def recv_into(self, buffer, nbytes=0, flags=0):
        if flags:
            raise ValueError("nonzero TLS receive flags are unsupported")
        try:
            return self._call(self._ssl.read, nbytes or len(buffer), buffer)
        except (ssl.SSLZeroReturnError, ssl.SSLEOFError):
            return 0  # Match SSLSocket's default suppress_ragged_eofs behavior.

    # urllib closes the connection after headers, while HTTPResponse keeps its
    # socket file alive. Reuse standard SocketIO and its reference-count policy.
    makefile = socket.socket.makefile

    def _decref_socketios(self):
        self._io_refs -= 1
        if self._closed:
            self.close()

    def close(self):
        self._closed = True
        if not self._io_refs:
            self._raw.close()

    def getpeercert(self, binary_form=False):
        return self._ssl.getpeercert(binary_form)


class _TLSContext:
    def __init__(self, context):
        self._context = context

    def wrap_socket(self, sock, *, server_hostname):
        return _ProgressTLS(sock, self._context, server_hostname)


class ProgressConnection:
    def _transfer_context(self, context):
        return context if isinstance(context, _TLSContext) else _TLSContext(context)

    def connect(self):
        super().connect()
        self.sock = _ProgressSocket(self.sock)


class _HTTPConnection(ProgressConnection, http.client.HTTPConnection):
    pass


class _HTTPSConnection(ProgressConnection, http.client.HTTPSConnection):
    def connect(self):
        self._context = self._transfer_context(self._context)
        super().connect()


class _HTTPHandler(HTTPHandler):
    def http_open(self, req):
        connection = _HTTPConnection if getattr(req, "_mycelium_file_transfer", False) else http.client.HTTPConnection
        return self.do_open(connection, req)


class _HTTPSHandler(HTTPSHandler):
    def https_open(self, req):
        options = {"context": self._context}
        if hasattr(self, "_check_hostname"):  # Python 3.10/3.11
            options["check_hostname"] = self._check_hostname
        connection = _HTTPSConnection if getattr(req, "_mycelium_file_transfer", False) else http.client.HTTPSConnection
        return self.do_open(connection, req, **options)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def file_handlers():
    return _HTTPHandler(), _HTTPSHandler()


def file_opener():
    return build_opener(*file_handlers(), _NoRedirect()).open
