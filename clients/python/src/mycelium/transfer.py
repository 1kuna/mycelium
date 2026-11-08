"""urllib connections that renew the upload budget after each partial send."""
import http.client
import functools
import socket
import select
import ssl
import time
from .adaptive import Progress
from urllib.request import HTTPHandler, HTTPSHandler, HTTPRedirectHandler, build_opener


class UploadTiming:
    """Evidence shared by one tus upload, never unrelated requests.

    RFC 6298 2.1/2.4 gives TCP a one-second initial/minimum retransmission
    timer. Faster buffered copies cannot prove a dead peer before retransmit.
    Four measured service periods retain Progress's jitter allowance; a server
    Retry-After may raise the window. An owner's deadline still wins.
    """
    def __init__(self):
        self.service = self.pressure = 0

    def observe(self, service, pressure=0):
        self.service = max(self.service, service)
        self.pressure = max(self.pressure, pressure)

    def window(self):
        return max(1, 4 * self.service, self.pressure)


class _ProgressSocket:
    def __init__(self, sock, adaptive=False, deadline=None, timing=None):
        self._socket = sock
        self._adaptive = adaptive
        self._deadline = deadline
        self._timing = timing
        self._read_progress, self._write_progress = Progress(), Progress()
        self._io_refs = 0
        self._closed = False

    def _set_timeout(self, budget):
        if self._timing is not None:
            budget = max(budget or 0, self._timing.window())
        if self._deadline is not None:
            remaining = self._deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("file transfer owner timeout")
            budget = remaining if budget is None else min(budget, remaining)
        self._socket.settimeout(budget)

    def _buffered(self, write):
        try:
            if not write and getattr(self._socket, "pending", lambda: 0)():
                return True
            ready = select.select([] if write else [self._socket], [self._socket] if write else [], [], 0)
            return bool(ready[1] if write else ready[0])
        except (TypeError, ValueError, AttributeError):
            return True  # Opaque hooks cannot establish wire service time.

    def _operation(self, operation, data, progress, *args):
        if self._adaptive and hasattr(self._socket, "settimeout"):
            self._set_timeout(progress.window(len(data)))
        waited = not self._buffered(progress is self._write_progress)
        started = time.monotonic()
        count = operation(data, *args)
        if self._adaptive:
            now = time.monotonic()
            progress.record(now, now - started, count if isinstance(count, int) else len(count), waited)
        return count

    def send(self, data, flags=0):
        return self._operation(self._socket.send, data, self._write_progress, flags)

    def recv(self, count, flags=0):
        # Avoid allocating count bytes just to describe the sample size.
        if self._adaptive:
            self._set_timeout(self._read_progress.window(count))
        waited = not self._buffered(False)
        started = time.monotonic()
        data = self._socket.recv(count, flags)
        if self._adaptive:
            now = time.monotonic()
            self._read_progress.record(now, now - started, len(data), waited)
        return data

    def recv_into(self, buffer, nbytes=0, flags=0):
        return self._operation(self._socket.recv_into, buffer, self._read_progress, nbytes, flags)

    makefile = socket.socket.makefile

    def _decref_socketios(self):
        self._io_refs -= 1
        if self._closed:
            self.close()

    def close(self):
        self._closed = True
        if not self._io_refs:
            self._socket.close()

    def __getattr__(self, name):
        return getattr(self._socket, name)

    def sendall(self, data, flags=0):
        # socket.sendall's timeout is cumulative, even while bytes progress.
        # send returns partial progress and starts a fresh socket-operation
        # timeout on the next call. TLS uses MemoryBIO below, so encrypted
        # output reaches this loop without a cumulative SSLSocket.write.
        remaining = memoryview(data)
        while remaining:
            sent = self.send(remaining, flags)
            if sent == 0:
                raise ConnectionError("file transfer connection closed during upload")
            remaining = remaining[sent:]


class _ProgressTLS:
    """Standard TLS engine with separately observable encrypted socket I/O."""

    def __init__(self, sock, context, server_hostname, adaptive=False, deadline=None, timing=None):
        self._raw = sock
        self._sender = _ProgressSocket(sock, adaptive, deadline, timing)
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl = context.wrap_bio(self._incoming, self._outgoing, server_hostname=server_hostname)
        self._io_refs = 0
        self._closed = False
        self._call(self._ssl.do_handshake)
        self._sender._read_progress, self._sender._write_progress = Progress(), Progress()

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
                data = self._sender.recv(64 * 1024)
                if data:
                    self._incoming.write(data)
                else:
                    self._incoming.write_eof()

    def pending(self):
        return self._ssl.pending()

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
    def __init__(self, context, adaptive=False, deadline=None, timing=None):
        self._context = context
        self._timing = timing
        self._adaptive = adaptive
        self._deadline = deadline

    def wrap_socket(self, sock, *, server_hostname):
        return _ProgressTLS(sock, self._context, server_hostname, self._adaptive, self._deadline, self._timing)


class ProgressConnection:
    def __init__(self, *args, progress_deadline=None, upload_timing=None, **kwargs):
        self._progress_deadline = progress_deadline
        self._upload_timing = upload_timing
        super().__init__(*args, **kwargs)

    def getresponse(self):
        # Upload rate cannot predict queue hashing/verification before headers.
        # Reset phase evidence; the owner bounds a server that never responds.
        sock = self.sock
        raw = getattr(sock, "_sender", sock)
        raw = getattr(raw, "_socket", raw)
        if isinstance(raw, _ProgressTLS):
            raw = raw._sender
        elif isinstance(sock, _ProgressSocket):
            raw = sock
        if isinstance(raw, _ProgressSocket) and raw._adaptive:
            raw._read_progress = Progress()
            raw._set_timeout(None)
        return super().getresponse()

    def _transfer_context(self, context):
        return context if isinstance(context, _TLSContext) else _TLSContext(context, self._progress_deadline is not None or self.timeout is None, self._progress_deadline, self._upload_timing)

    def connect(self):
        super().connect()
        self.sock = _ProgressSocket(self.sock, not isinstance(self.sock, _ProgressTLS) and (self.timeout is None or self._progress_deadline is not None), self._progress_deadline, self._upload_timing)


class _HTTPConnection(ProgressConnection, http.client.HTTPConnection):
    pass


class _HTTPSConnection(ProgressConnection, http.client.HTTPSConnection):
    def connect(self):
        self._context = self._transfer_context(self._context)
        super().connect()


class _HTTPHandler(HTTPHandler):
    def http_open(self, req):
        connection = _HTTPConnection if getattr(req, "_mycelium_file_transfer", False) else http.client.HTTPConnection
        return self.do_open(file_connection(connection, req), req)


class _HTTPSHandler(HTTPSHandler):
    def https_open(self, req):
        options = {"context": self._context}
        if hasattr(self, "_check_hostname"):  # Python 3.10/3.11
            options["check_hostname"] = self._check_hostname
        connection = _HTTPSConnection if getattr(req, "_mycelium_file_transfer", False) else http.client.HTTPSConnection
        return self.do_open(file_connection(connection, req), req, **options)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def file_handlers():
    return _HTTPHandler(), _HTTPSHandler()


def file_opener():
    return build_opener(*file_handlers(), _NoRedirect()).open


def file_connection(connection, request):
    if getattr(request, "_mycelium_file_transfer", False):
        return functools.partial(connection, progress_deadline=getattr(request, "_mycelium_transfer_deadline", None),
                                 upload_timing=getattr(request, "_mycelium_upload_timing", None))
    return connection
