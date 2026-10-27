"""Read the installation's private authority, with pinned HTTPS transport."""
import hashlib
import http.client
import json
from pathlib import Path
import re
import shlex
import ssl
import stat
from urllib.request import HTTPSHandler, HTTPRedirectHandler, build_opener


def _private(path):
    path = Path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ValueError("installed authority must be a private regular file (chmod 600)")
    return path.read_text(encoding="utf-8")


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _pinned_handler(pin):
    class Connection(http.client.HTTPSConnection):
        def connect(self):
            # Learn only certificate bytes on an unauthenticated connection.
            # Trust them only after matching the provisioned fingerprint.
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            wrap = getattr(self, "_transfer_context", lambda context: context)
            self._context = wrap(context)
            super().connect()
            cert = self.sock.getpeercert(binary_form=True)
            # Keep HTTPConnection's in-flight request state while reconnecting.
            self.sock.close()
            self.sock = None
            if hashlib.sha256(cert).hexdigest() != pin:
                raise ssl.SSLError("queue identity does not match installed authority")
            # Ordinary TLS verifies validity with the pinned leaf as trust anchor.
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.load_verify_locations(cadata=ssl.DER_cert_to_PEM_cert(cert))
            self._context = wrap(context)
            super().connect()
            if hashlib.sha256(self.sock.getpeercert(binary_form=True)).hexdigest() != pin:
                self.close()
                raise ssl.SSLError("queue identity does not match installed authority")

    class Handler(HTTPSHandler):
        def https_open(self, req):
            connection = Connection
            if getattr(req, "_mycelium_file_transfer", False):
                from .transfer import ProgressConnection

                class FileConnection(ProgressConnection, Connection):
                    pass

                connection = FileConnection
            return self.do_open(connection, req)

    return Handler()


def installed_authority(config=None):
    paths = [Path(config)] if config is not None else [
        Path.home() / ".local/share/mycelium" / role / "config.json"
        for role in ("worker", "queue")]
    for index, path in enumerate(paths):
        try:
            saved = json.loads(path.read_text(encoding="utf-8"))
            break
        except FileNotFoundError:
            if index == len(paths) - 1:
                raise ValueError("cannot read installed config") from None
    address = saved.get("queue")
    handlers = [_NoRedirect()]
    if saved.get("joining_file"):
        invitation = json.loads(_private(saved["joining_file"]))
        pin = invitation.get("fingerprint", "")
        token = invitation.get("token")
        address = address or invitation.get("address")
        if invitation.get("version") != 1 or not re.fullmatch(r"[0-9a-f]{64}", pin) or not token:
            raise ValueError("invalid installed joining authority")
        from urllib.parse import urlsplit
        parsed = urlsplit(address or "")
        if parsed.scheme != "https" or parsed.path not in {"", "/"}:
            raise ValueError("installed joining authority requires an HTTPS queue origin")
        handlers.append(_pinned_handler(pin))
    else:
        from .transfer import file_handlers
        handlers.extend(file_handlers())
        values = {}
        for line in _private(saved.get("environment_file") or saved.get("credentials", "")).splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            key = key.strip()
            words = shlex.split(value, comments=False)
            if not separator or key not in {"MYCE_QUEUE", "MYCE_TOKEN"} or len(words) != 1 or not words[0]:
                raise ValueError("invalid installed fleet environment")
            values[key] = words[0]
        token = values.get("MYCE_TOKEN")
        address = values.get("MYCE_QUEUE") if saved.get("environment_file") else address
        if not token or not address:
            raise ValueError("installed queue credentials are missing")
    return address, token, build_opener(*handlers).open
