"""Synchronous, standard-library client for the Mycelium HTTP API."""
from __future__ import annotations

import base64
import hashlib
import http.client
import io
from importlib.metadata import PackageNotFoundError, version
import tempfile
import re
import copy
import datetime as dt
import json
import os
import ssl
import shutil
import signal
import sys
from contextlib import contextmanager
import socket
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import Any, Callable, Iterator, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from .adaptive import RECOVERY_PERIOD, Pace, retry_after

try:
    CLIENT_VERSION = version("mycelium-client")
except PackageNotFoundError:
    CLIENT_VERSION = "development"
USER_AGENT = "mycelium-python/" + CLIENT_VERSION

TERMINAL = {"succeeded", "failed", "cancelled"}
# Labels the client's own readers send; any other mapping is labelled python.
_READER_ORIGINS = {"job_spec", "extension"}


class UnsupportedTLSRuntimeError(RuntimeError):
    """The Python TLS runtime cannot use installed queue authority."""


class IncompleteReplyError(ConnectionError):
    """A Job reply ended early, as during a queue restart; the read may be retried."""


class TransferStalledError(TimeoutError):
    """A file transfer received no data within its progress budget.

    code is "result_download_failed" for download_result, None otherwise.
    """

    def __init__(self, message: str, code: str | None = None):
        super().__init__(f"{code}: {message}" if code else message)
        self.code = code


class QueueStalledError(TimeoutError):
    """The queue stopped making progress while this call waited.

    The queue may still complete the request. Read-only calls and keyed
    submissions are safe to repeat. code is "queue_stalled".
    """
    code = "queue_stalled"

    def __init__(self, method: str, path: str, waited: float, window: float, cause: Exception | None = None):
        reason = (f"the queue did not answer progress checks ({cause})" if cause is not None
                  else "the queue held its lock with callers waiting")
        super().__init__(
            f"queue_stalled: {method} {path} waited {waited:.3f}s; {reason} for {window:.3f}s "
            "(four measured request periods); the request may still complete. Check the queue with "
            "`myce status` or its service log; read-only calls and keyed submissions are safe to repeat")
        self.method, self.path, self.waited, self.window, self.cause = method, path, waited, window, cause


class _UploadPendingError(ConnectionError):
    """A missing session whose blob is not published yet; retry only uploads."""


class APIError(RuntimeError):
    """The queue rejected a request; status and response body are retained."""

    def __init__(self, status: int, body: str, retry_after: float = 0):
        self.retry_after = retry_after
        self.status = status
        self.body = body
        self.code = None
        self.job_id = None
        self.current_revision = None
        message = f"Mycelium HTTP {status}: {body}"
        try:
            response = json.loads(body)
            if isinstance(response, dict):
                self.code = response.get("code")
                self.job_id = response.get("job_id")
                self.current_revision = response.get("current_revision")
                if self.code in {"job_failed", "job_cancelled", "job_spec_invalid", "queue_upgrade_required"}:
                    message = f"{self.code}: {response.get('error', body)}"
        except (ValueError, TypeError):
            pass
        super().__init__(message)


@dataclass
class EntryOutcome:
    """The finished trip of Client.run_entry.

    exit_code matches the CLI: 0 when the job succeeded and its outputs were
    downloaded and verified, the command's own exit status when it failed with
    one, otherwise 1. files maps each accepted output name to its local path.
    """
    job: dict[str, Any]
    receipt: dict[str, Any]
    exit_code: int
    files: dict[str, Path] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


class EntryPreparationError(RuntimeError):
    """myce run --entry refused or could not confirm the submission.

    exit_code and reason are the CLI's (64 invalid entry or input, 65 changed
    intent or rejected, 69 queue unavailable, 75 busy). receipt, when printed,
    keeps the key: rerunning with the same key resumes without duplicating work.
    """

    def __init__(self, exit_code: int, reason: str, receipt: dict[str, Any] | None):
        super().__init__(f"myce run --entry exited {exit_code}: {reason}")
        self.exit_code, self.reason, self.receipt = exit_code, reason, receipt


class EntryFailedError(RuntimeError):
    """A run_entry job ended failed or cancelled; outcome keeps job and exit_code."""

    def __init__(self, outcome: EntryOutcome):
        job, short = outcome.job, outcome.job["id"][:8]
        super().__init__(f"job {short} {job['state']}: {job.get('reason', '')}; "
                         f"run myce logs {short} for the traceback")
        self.outcome, self.exit_code = outcome, outcome.exit_code


class EntryInterrupted(KeyboardInterrupt):
    """The waiter detached; the job continues. exit_code matches CLI code 126."""

    exit_code = 126

    def __init__(self, job_id: str, project: str, key: str):
        super().__init__(f"detached; job {job_id} continues; re-attach: repeat run_entry with "
                         f"the same key={key!r} (project {project!r}); cancel explicitly: myce cancel {job_id}")
        self.job_id, self.project, self.key = job_id, project, key
        self.cancelled = False


@contextmanager
def _entry_signals():
    # Python only permits signal handlers on the main thread. Other threads
    # retain their owner's signal policy; neither path cancels the job.
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    def interrupt(signum, frame):
        raise KeyboardInterrupt
    previous = signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _exit_code(job: Mapping[str, Any]) -> int:
    if job["state"] == "succeeded":
        return 0
    cause = job.get("execution_cause") or {}
    code = cause.get("exit_code")
    if job["state"] == "failed" and cause.get("class") == "command_exit" and isinstance(code, int) and 0 < code <= 255:
        return code
    return 1


# Transport failures and typed HTTP refusals handled by the shared request policy.
REQUEST_ERRORS = (APIError, URLError, TimeoutError, ConnectionError, ssl.SSLError)


def _json_default(value: Any) -> str:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise ValueError("job timestamps must include a timezone")
        return value.isoformat()
    raise TypeError(f"cannot encode {type(value).__name__} as JSON")


def _job_reply(reply: Any) -> dict[str, Any]:
    if not isinstance(reply, dict) or any(
            not isinstance(reply.get(field), str) or not reply[field]
            for field in ("id", "state")):
        raise APIError(502, json.dumps({
            "code": "job_reply_invalid",
            "error": "queue returned an invalid Job reply: nonempty id and state are required",
        }))
    return reply


def _read_reply(response: Any) -> bytes:
    try:
        body = response.read()
    except http.client.IncompleteRead:
        raise IncompleteReplyError("queue reply ended early") from None
    length = response.headers.get("Content-Length")
    if length is not None and length.isdigit() and len(body) < int(length):
        raise IncompleteReplyError("queue reply ended early")
    return body


def _local_name(name: str) -> bool:
    return bool(name) and not name.startswith("/") and "\\" not in name and ":" not in name and all(
        part not in {"", ".", ".."} for part in name.split("/"))


def _normalize(spec: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(spec))
    needs = result.get("needs", {})
    platform = needs.get("platform")
    if isinstance(platform, list):
        needs["platform"] = {"required": platform}
    elif platform == "any":
        needs["platform"] = {"any": True}
    if isinstance(result.get("environment"), str):
        result["environment"] = {"name": result["environment"]}
    return result


def _openai_http_client(opener: Any) -> Any:
    """Keep installed TLS pins and redirect policy on the SDK's HTTP path."""
    import httpx

    class Stream(httpx.SyncByteStream):
        def __init__(self, response: Any):
            self.response = response

        def __iter__(self) -> Iterator[bytes]:
            read = getattr(self.response, "read1", self.response.read)
            while chunk := read(65536):
                yield chunk

        def close(self) -> None:
            self.response.close()

    class Transport(httpx.BaseTransport):
        def handle_request(self, request: Any) -> Any:
            outgoing = Request(str(request.url), data=request.read(),
                               method=request.method, headers=dict(request.headers))
            timeout = request.extensions.get("timeout", {}).get("read", 600)
            try:
                response = opener(outgoing, timeout=timeout)
            except HTTPError as error:
                response = error  # Preserve HTTP status and error/redirect bodies.
            except (TimeoutError, socket.timeout) as error:
                raise httpx.ReadTimeout("installed queue request timed out", request=request) from error
            except (URLError, OSError) as error:
                raise httpx.ConnectError("installed queue transport failed", request=request) from error
            return httpx.Response(response.status, headers=list(response.headers.items()),
                                  stream=Stream(response), request=request)

    return httpx.Client(transport=Transport(), follow_redirects=False)


class Client:
    """Connect using explicit settings or MYCE_QUEUE and MYCE_TOKEN."""

    def __init__(self, url: str | None = None, token: str | None = None,
                 *, timeout: float | None = None, poll_interval: float | None = None,
                 transfer_timeout: float | None = None):
        self.url = (url or os.environ.get("MYCE_QUEUE", "http://127.0.0.1:7331")).rstrip("/")
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("queue URL must be an HTTP(S) URL without credentials, query or fragment")
        self.token = os.environ.get("MYCE_TOKEN", "") if token is None else token
        # run_entry hands the CLI the same authority without passing the token:
        # "environment" (MYCE_TOKEN), ("installed", config) or None (explicit).
        self._authority: Any = "environment" if token is None else None
        if not self.token:
            raise ValueError("set MYCE_TOKEN or pass token explicitly")
        if (timeout is not None and timeout <= 0) or (poll_interval is not None and poll_interval <= 0):
            raise ValueError("timeout and poll_interval must be positive")
        if transfer_timeout is not None and transfer_timeout <= 0:
            raise ValueError("transfer_timeout must be positive")
        self.transfer_timeout = transfer_timeout
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._clock = time
        self._jitter = None
        self._urlopen = urlopen
        # Waits for an in-flight call; tests substitute their clock.
        self._await = lambda event, seconds: event.wait(seconds)
        # Tests shorten the file transfer budgets; None uses the defaults.
        self._transfer_idle: float | None = None

    @classmethod
    def from_installed(cls, config: str | Path | None = None, *,
                       timeout: float | None = None, poll_interval: float | None = None,
                       transfer_timeout: float | None = None) -> Client:
        """Use the installed worker/queue authority without exporting credentials.

        Reads worker config first, then queue config if no worker is installed.
        A joining authority retains its certificate pin and refuses redirects.
        Explicit Client(url, token) construction keeps its existing behavior.
        Inside `myce dev -- COMMAND`, the dev session's queue outranks the
        installed authority, and no installed file or fleet token is read.
        """
        if (config is None and os.environ.get("MYCE_DEV_SESSION_ROOT")
                and os.environ.get("MYCE_QUEUE") and os.environ.get("MYCE_TOKEN")):
            return cls(timeout=timeout, poll_interval=poll_interval, transfer_timeout=transfer_timeout)
        if ssl.OPENSSL_VERSION.startswith("LibreSSL"):
            raise UnsupportedTLSRuntimeError(
                "Client.from_installed() requires OpenSSL (3 recommended); "
                "macOS system LibreSSL is unsupported for the queue Ed25519 "
                "joining certificate. Detected "
                f"{ssl.OPENSSL_VERSION}. Use Homebrew or python.org Python.")
        from .installed import installed_authority
        address, token, opener = installed_authority(config)
        client = cls(address, token, timeout=timeout, poll_interval=poll_interval, transfer_timeout=transfer_timeout)
        client._urlopen = opener
        client._authority = ("installed", config)
        return client

    @staticmethod
    def _retryable(error: Exception, policy: str) -> bool:
        cause = error.reason if isinstance(error, URLError) else error
        if isinstance(cause, ssl.SSLError):
            # Only known broken-record/connection failures are recoverable.
            # Unknown TLS failures (including installed pin mismatches) fail closed.
            transient = isinstance(cause, ssl.SSLEOFError) or getattr(cause, "reason", None) in {
                "RECORD_LAYER_FAILURE", "UNEXPECTED_EOF_WHILE_READING"}
            if not transient:
                return False
        if policy == "none":
            return False
        if isinstance(error, APIError):
            if policy == "upload":
                return error.status in {408, 429, 500, 502, 503, 504} and error.code in {
                    None, "", "request_failed", "queue_stopping", "client_rate_limited",
                    "observation_shed_control_pressure"}
            if policy == "wait":
                return error.code != "job_reply_invalid" and (error.status in {408, 429} or error.status >= 500)
            return error.status in {429, 503} and error.code != "file_blob_unavailable"
        return policy in {"keyed", "upload", "wait"}

    def _pause(self, pace: Pace, *, end: float | None = None,
               event: float | None = None, error: Exception | None = None,
               held: bool = False) -> None:
        delay = pace.next(self._clock.monotonic(), event, error)
        if held and error is None:
            delay = 0
        # An explicit polling floor applies only to observations. Fresh pressure
        # must follow the current Retry-After rather than an older local floor.
        if error is None:
            delay = max(self.poll_interval or 0, delay)
        if end is not None:
            delay = min(delay, max(0, end - self._clock.monotonic()))
        self._clock.sleep(delay)

    def _request(self, operation: Callable[[float | None], Any], *,
                 policy: str = "none", end: float | None = None,
                 expired: Callable[[], Exception] = lambda: TimeoutError("queue request timed out")) -> Any:
        """One retry owner; operations that reconcile writes disable inner retries."""
        pace = Pace(jitter=self._jitter)
        while True:
            started = self._clock.monotonic()
            remaining = None if end is None else end - started
            if remaining is not None and remaining <= 0:
                raise expired()
            try:
                return operation(remaining)
            except REQUEST_ERRORS as error:
                if not self._retryable(error, policy):
                    raise
                pace.observe(self._clock.monotonic() - started)
                self._pause(pace, end=end, error=error)

    def _open_request(self, request: Request, *, timeout: float | None,
                      opener: Any = None) -> Any:
        try:
            return (opener or self._urlopen)(request, timeout=timeout)
        except HTTPError as error:
            with error:
                body = error.read(4096).decode("utf-8", errors="replace")
            failure = APIError(error.code, body, retry_after(error.headers.get("Retry-After"), self._clock.time()))
            failure.code = failure.code or error.headers.get("X-Myce-Error-Code")
            path = request.selector
            if (error.code == 404 and not failure.code and request.method == "GET"
                    and path.startswith("/api/jobs/") and "/" not in path[len("/api/jobs/"):]):
                failure = APIError(404, json.dumps({
                    "code": "queue_upgrade_required",
                    "error": "queue too old for this client; upgrade the queue first",
                }))
            raise failure from None

    def _call(self, method: str, path: str, body: Any = None,
              *, timeout: float | None = None, raw: bool = False, retry: bool = True) -> Any:
        data = None if body is None else json.dumps(body, default=_json_default, allow_nan=False).encode()
        request = Request(self.url + path, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json",
            "User-Agent": USER_AGENT})
        budget = self.timeout if timeout is None else timeout
        end = None if budget is None else self._clock.monotonic() + budget
        def attempt(remaining: float | None) -> Any:
            def run() -> Any:
                with self._open_request(request, timeout=remaining) as response:
                    return _read_reply(response) if raw else json.load(response)
            return self._watched(method, path, run)
        return self._request(attempt, policy="read" if retry and method in {"GET", "HEAD"} else "none", end=end)

    def _watched(self, method: str, path: str, run: Callable[[], Any]) -> Any:
        """Bound one call by queue progress instead of a fixed timeout.

        A call still waiting after one transport recovery period reads
        GET /api/ping. It keeps waiting while the queue releases its lock or
        nobody waits on it, and raises QueueStalledError once releases have not
        moved, with callers waiting, for four of the queue's own p99 request
        periods (at least four recovery periods), or ping gave no answer for that
        long. A queue without progress evidence leaves the call to the caller's
        timeout. Held batch waits read the wait index and are not watched.
        """
        if path.split("?", 1)[0] in {"/api/ping", "/api/wait"}:
            return run()
        outcome: dict[str, Any] = {}
        done = threading.Event()

        def worker() -> None:
            try:
                outcome["value"] = run()
            except BaseException as error:  # Re-raised in the caller's thread.
                outcome["error"] = error
            finally:
                done.set()

        threading.Thread(target=worker, name="mycelium-call", daemon=True).start()
        if self._watch_progress(method, path, done):
            done.wait()
        if "error" in outcome:
            raise outcome["error"]
        return outcome["value"]

    def _watch_progress(self, method: str, path: str, done: threading.Event) -> bool:
        """Return True when the call should simply finish; raise on a stall."""
        started = progress_at = self._clock.monotonic()
        window, wait = 4 * RECOVERY_PERIOD, RECOVERY_PERIOD  # fast calls never probe
        releases, seen, cause = None, False, None
        request = Request(self.url + "/api/ping", method="GET", headers={
            "Authorization": "Bearer " + self.token, "User-Agent": USER_AGENT})
        while not self._await(done, wait):
            probe_started = self._clock.monotonic()
            try:
                with self._open_request(request, timeout=window) as response:
                    progress = json.load(response).get("queue_progress")
            except APIError as error:
                if error.status == 404:
                    return True  # No evidence from this queue: the caller's timeout applies.
                progress, cause = None, error
            except (ValueError, AttributeError):
                return True
            except REQUEST_ERRORS as error:
                progress, cause = None, error
            else:
                if not isinstance(progress, dict):
                    return True
            now = self._clock.monotonic()
            if progress is not None:
                cause = None
                p99 = float(progress.get("p99_ms") or 0) / 1000
                window = max(4 * RECOVERY_PERIOD, 4 * (p99 + now - probe_started))
                if not seen or progress.get("releases") != releases or not progress.get("waiting"):
                    progress_at = now
                seen, releases = True, progress.get("releases")
            quiet = now - progress_at
            if quiet >= window:
                if done.is_set():
                    return True
                raise QueueStalledError(method, path, now - started, window, cause)
            # Probe about four times per window, and again just as it would close.
            wait = max(RECOVERY_PERIOD, min(window / 4, window - quiet))
        return True

    def submit(self, job: Mapping[str, Any] | str | Path, *, timeout: float | None = None) -> dict[str, Any]:
        """Submit a mapping, YAML text, or a pathlib.Path to a YAML file.

        after={"job": parent_id, "outputs": {local: "KEY/OUTPUT_NAME"}}
        waits for the whole fan-out parent. Set after.piece for early single-piece
        readiness on an ordinary job. fan_out may accompany the whole-parent form.
        """
        if not isinstance(job, Mapping):
            return self.submit_yaml(job, timeout=timeout)
        spec = _normalize(job)
        # A mapping built here is labelled python; the client's own YAML and
        # extension readers keep theirs. A label copied from a Job reply is replaced.
        if spec.get("authoring_origin") not in _READER_ORIGINS:
            spec["authoring_origin"] = "python"
        options = {} if timeout is None else {"timeout": timeout}
        try:
            return self._call("POST", "/api/jobs", spec, **options)
        except APIError as error:
            # A queue older than the python label rejects it before creating
            # anything; it records the submission as api instead.
            if (spec["authoring_origin"] != "python" or error.code != "job_execution_invalid"
                    or "authoring_origin" not in error.body):
                raise
            del spec["authoring_origin"]
            return self._call("POST", "/api/jobs", spec, **options)

    def submit_yaml(self, source: str | Path, *, timeout: float | None = None) -> dict[str, Any]:
        """Load YAML safely. Pass Path for a file, str for YAML text."""
        try:
            import yaml
        except ImportError:
            raise ImportError("YAML submission requires mycelium-client[yaml]") from None
        text = source.read_text(encoding="utf-8") if isinstance(source, Path) else source
        job = yaml.safe_load(text)
        if not isinstance(job, Mapping):
            raise ValueError("job YAML must contain one mapping")
        return self.submit({**job, "authoring_origin": "job_spec"}, timeout=timeout)

    def submit_and_wait(self, job: Mapping[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        """Retry this call with the same spec/key after an uncertain response.

        Transport/overload retries reuse the identical key/spec until the owner timeout.
        No automatic replay of unkeyed work or application side effects.
        Returns all terminal states; inspect state before consuming results.
        """
        if not job.get("submission_key"):
            raise ValueError("submit_and_wait requires submission_key")
        spec = _normalize(job)
        end = None if timeout is None else self._clock.monotonic() + timeout
        submitted = self._request(
            lambda remaining: self.submit(spec) if remaining is None else self.submit(spec, timeout=remaining),
            policy="keyed", end=end,
            expired=lambda: TimeoutError("timed out submitting keyed job; retry the same key"))
        remaining = None if end is None else max(0, end - self._clock.monotonic())
        return self.wait(submitted["id"], timeout=remaining)

    def run(self, extension: str, *, inputs: Mapping[str, str | Path],
            outputs: str | Path, key: str, project: str, class_: str = "",
            after: Mapping[str, Any] | None = None,
            timeout: float | None = None) -> dict[str, Any]:
        """Upload, submit/wait by key, and download the extension's declared outputs.

        Retry identical inputs/extension/project/class/key after a lost response.
        Returns the accepted job; failures include a short-ID log command.
        """
        if not extension or not key or not project or not str(outputs):
            raise ValueError("run requires extension, outputs directory, key and project")
        if not all(_local_name(name) for name in inputs):
            raise ValueError("input names must be clean relative paths")
        files = {name: self.upload_file(source, project=project, class_=class_) for name, source in inputs.items()}
        job = self.submit_and_wait({"name": extension, "project": project, "class": class_,
                                    "extension": extension, "submission_key": key, "authoring_origin": "extension",
                                    "files": files, **({"after": copy.deepcopy(dict(after))} if after is not None else {})}, timeout=timeout)
        if job.get("files_expired"):
            raise APIError(410, json.dumps({"code": "result_expired", "error": "stored job files expired after the retention window"}))
        if job["state"] != "succeeded":
            short = job["id"][:8]
            raise APIError(409, json.dumps({"code": "job_" + job["state"], "job_id": job["id"],
                "error": f"job {short} {job['state']}: {job.get('reason', '')}; run myce logs {short} for the traceback"}))
        self.download_results(job, outputs)
        return job

    def run_entry(self, name: str | None = None, *, out: str | Path | None = None,
                  wait: bool = True, args: Sequence[str] = (),
                  inputs: Mapping[str, str | Path] | None = None, key: str | None = None,
                  class_: str | None = None, root: str | Path | None = None,
                  adapter: str | Path | None = None, timeout: float | None = None,
                  check: bool = True, myce: str | Path | None = None) -> Any:
        """Run a .mycelium/adapter.yaml entry: submit, wait, download and verify.

        Submission is myce run --entry itself, so source freezing, the key,
        environment locks and the local receipt match the CLI exactly: the same
        entry and key from Python or the CLI is one job. name defaults to the
        adapter's default entry; args are appended to its command; inputs add
        per-submission files {DEST: PATH}. root is the project directory (default:
        the current directory); adapter overrides root/.mycelium/adapter.yaml.

        wait=False returns the receipt (job_id, key, project). Otherwise it waits,
        downloads every accepted output into out and returns an EntryOutcome. A
        failed or cancelled job raises EntryFailedError unless check=False. A
        timeout raises TimeoutError and leaves the job running. Ctrl-C or SIGTERM
        detaches and raises EntryInterrupted (exit_code=126); the job continues.
        The CLI (myce on PATH, or myce=) uses this client's authority: from_installed() or MYCE_TOKEN.
        """
        if wait and out is None:
            raise ValueError("run_entry needs out=DIR when wait=True")
        binary = str(myce) if myce is not None else shutil.which("myce")
        if not binary:
            raise FileNotFoundError("run_entry needs the installed myce CLI on PATH (or pass myce=)")
        if self._authority is None:
            raise ValueError("run_entry uses installed authority: construct with Client.from_installed() "
                             "or set MYCE_TOKEN in the environment")
        root = Path.cwd() if root is None else Path(root).absolute()
        adapter = root / ".mycelium" / "adapter.yaml" if adapter is None else Path(adapter).absolute()
        command = [binary, "run", "--queue", self.url, "--adapter", str(adapter)]
        environment = dict(os.environ)
        if self._authority != "environment":
            # Installed authority only: an exported credential must not override it.
            for variable in ("MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"):
                environment.pop(variable, None)
            if self._authority[1] is not None:
                command += ["--config", str(Path(self._authority[1]).absolute())]
        if name:
            command += ["--entry", name]
        if key:
            command += ["--key", key]
        if class_:
            command += ["--class", class_]
        for dest, local in (inputs or {}).items():
            command += ["--input", f"{dest}={local}"]
        command += ["--", *map(str, args)]
        prepared = subprocess.run(command, cwd=root, env=environment, stdin=subprocess.DEVNULL,
                                  capture_output=True, text=True)
        lines = [line for line in prepared.stdout.splitlines() if line.strip()]
        try:
            receipt = json.loads(lines[-1]) if lines else None
        except ValueError:
            receipt = None
        if prepared.returncode != 0 or not isinstance(receipt, dict) or not receipt.get("job_id"):
            reason = (prepared.stderr.strip().splitlines() or ["no receipt printed"])[-1]
            raise EntryPreparationError(prepared.returncode, reason, receipt if isinstance(receipt, dict) else None)
        if not wait:
            return receipt
        with _entry_signals():
            try:
                job = self.wait(receipt["job_id"], timeout=timeout)
                outcome = EntryOutcome(job=job, receipt=receipt, exit_code=_exit_code(job))
                if outcome.ok:
                    outcome.files = self.download_results(job, out)
                elif check:
                    raise EntryFailedError(outcome)
                return outcome
            except KeyboardInterrupt:
                detached = EntryInterrupted(receipt["job_id"], receipt["project"], receipt["key"])
                print(str(detached), file=sys.stderr)
                raise detached from None

    def _file_request(self, method, path, data=None, headers=None, size=0, *, timing=None, deadline=None, retry=True):
        end = deadline if deadline is not None else (None if self.transfer_timeout is None else self._clock.monotonic() + self.transfer_timeout)
        def attempt(remaining: float | None) -> Any:
            started = self._clock.monotonic()
            try:
                if timing is None:
                    return self._file_request_once(method, path, data, headers, size, remaining)
                response = self._file_request_once(method, path, data, headers, size, remaining, timing)
            except REQUEST_ERRORS as error:
                if timing is not None:
                    timing.observe(self._clock.monotonic() - started, getattr(error, "retry_after", 0))
                raise
            timing.observe(self._clock.monotonic() - started,
                           retry_after(response.headers.get("Retry-After"), self._clock.time()))
            return response
        return self._request(attempt, policy="read" if retry and method in {"GET", "HEAD"} else "none",
                             end=end, expired=lambda: TransferStalledError("file transfer owner timeout"))

    def _file_request_once(self, method: str, path: str, data: bytes | None = None,
                      headers: Mapping[str, str] | None = None, size: int = 0, owner_timeout=None, timing=None):
        """size is the blob the queue may verify before replying; 0 when none."""
        budget = self._transfer_idle
        headers = dict(headers or {})
        if data is not None:
            headers["Content-Length"] = str(len(data))
        # Bound memory used by http.client; the transfer connection also sends
        # partial buffers separately so no sendall has a cumulative timeout.
        request = Request(self.url + path, data=None if data is None else io.BytesIO(data),
                          method=method, headers={"Authorization": "Bearer " + self.token, **headers})
        request._mycelium_file_transfer = True
        request._mycelium_upload_timing = timing if method == "PATCH" else None
        if owner_timeout is not None:
            request._mycelium_transfer_deadline = time.monotonic() + owner_timeout
            budget = owner_timeout if budget is None else min(budget, owner_timeout)
        opener = self._urlopen
        if opener is urlopen:
            from .transfer import file_opener
            opener = file_opener()
        try:
            response = self._open_request(request, timeout=budget, opener=opener)
        except (TimeoutError, socket.timeout):
            raise TransferStalledError("file transfer made no network progress") from None
        except URLError as error:
            if isinstance(error.reason, (TimeoutError, socket.timeout)):
                raise TransferStalledError("file transfer made no network progress") from None
            raise
        return response

    def upload_file(self, source: str | Path, *, project: str = "", class_: str = "", recovery: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Upload a job input, eligible for retention if never submitted."""
        from .transfer import UploadTiming
        timing = UploadTiming()
        end = None if self.transfer_timeout is None else self._clock.monotonic() + self.transfer_timeout
        def request(*args, **kwargs):
            return self._file_request(*args, **kwargs, timing=timing, deadline=end, retry=False)
        source = Path(source)
        digest = hashlib.sha256()
        size = 0
        with source.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
                size += len(chunk)
        ref = {"sha256": digest.hexdigest(), "size_bytes": size}
        endpoint = "/api/files/uploads/" + ref["sha256"]
        headers = {"Tus-Resumable": "1.0.0"}
        if recovery is not None:
            headers["X-Myce-File-Attempt"] = json.dumps({**{k: recovery[k] for k in ("id", "attempt", "machine", "session")}, "recovery": True})
        elif project:
            claim = self._call("POST", "/api/files/input-claims", {"project": project, "class": class_, "file": ref})
            headers["X-Myce-Input-Claim"] = claim["id"]
        def verified_blob():
            with request("HEAD", "/api/files/blobs/" + ref["sha256"], headers=headers, size=size) as response:
                if (response.headers.get("Content-Length") != str(size) or
                        response.headers.get("ETag") != '"' + ref["sha256"] + '"'):
                    raise ValueError("stored file identity mismatch")
            return ref

        def attempt(read):
            try:
                return verified_blob()
            except APIError as error:
                if error.status not in (404, 410):
                    raise
            with request("POST", "/api/files/uploads/", b"", size=size, headers={
                    **headers, "Upload-Length": str(size),
                    "Upload-Metadata": "sha256 " + base64.b64encode(ref["sha256"].encode()).decode()
                    + ",purpose " + base64.b64encode(b"job-input").decode()}):
                pass
            # A lost final acknowledgement may already have published the blob.
            try:
                return verified_blob()
            except APIError as error:
                if error.status not in (404, 410):
                    raise
            with request("HEAD", endpoint, headers=headers) as response:
                offset = int(response.headers["Upload-Offset"])
                length = int(response.headers["Upload-Length"])
            if length != size or not 0 <= offset <= size:
                raise ValueError("invalid upload offset/length")
            prefix = offset
            while prefix:
                chunk = read(min(1024 * 1024, prefix))
                if not chunk:
                    raise ValueError("input changed during upload")
                prefix -= len(chunk)
            while offset < size:
                chunk = read(min(1024 * 1024, size - offset))
                if not chunk:
                    raise ValueError("input changed during upload")
                with request("PATCH", endpoint, chunk, {
                        **headers, "Content-Type": "application/offset+octet-stream",
                        "Upload-Offset": str(offset)}, size) as response:
                    if int(response.headers["Upload-Offset"]) != offset + len(chunk):
                        raise ValueError("invalid upload acknowledgement")
                offset += len(chunk)
            return verified_blob()

        def reconciled_attempt():
            # Hash the exact prefix/chunks read by this attempt. Reopening and
            # hashing after a 404 would miss a changed PATCH if the file had
            # already been restored to its original bytes by then.
            digest = hashlib.sha256()
            actual_size = 0
            short_source = False
            with source.open("rb") as stream:
                def read(count):
                    nonlocal actual_size, short_source
                    if end is not None and self._clock.monotonic() >= end:
                        raise TransferStalledError("file transfer owner timeout")
                    chunk = stream.read(count)
                    digest.update(chunk)
                    actual_size += len(chunk)
                    short_source = short_source or (len(chunk) < count and actual_size < size)
                    return chunk

                def verify_source():
                    while read(1024 * 1024):
                        pass
                    if short_source or actual_size != size or digest.hexdigest() != ref["sha256"]:
                        raise ValueError("upload source size or SHA-256 mismatch")

                try:
                    result = attempt(read)
                except REQUEST_ERRORS as error:
                    if not (isinstance(error, APIError) and error.status in (404, 410)) and not self._retryable(error, "upload"):
                        raise
                    verify_source()
                    if not isinstance(error, APIError) or error.status not in (404, 410):
                        raise
                    # A verified blob plus verified source proves completion.
                    try:
                        return verified_blob()
                    except APIError as missing:
                        if missing.status not in (404, 410):
                            raise
                        raise _UploadPendingError(str(error)) from error
                verify_source()
                return result

        return self._request(lambda remaining: reconciled_attempt(), policy="upload", end=end,
                             expired=lambda: TransferStalledError("file transfer owner timeout"))

    def recover_result(self, job_id: str, *, files: Mapping[str, str | Path],
                       key: str, requested_by: str, reason: str,
                       source: Mapping[str, str]) -> dict[str, Any]:
        """Upload and append recovery evidence under operator authority.

        source is {kind: upload|nas, location: relative-label}. requested_by is
        an operator assertion. The queue independently verifies every byte and
        fences the exact failed execution captured before uploads begin.
        """
        endpoint = f"/api/jobs/{quote(job_id, safe='')}"
        plan = self._call("GET", endpoint + "/recovery-plan")
        if not files or not set(files).issubset(plan["outputs"]):
            raise ValueError("recovery requires declared output names")
        for path in files.values():
            source_path = Path(path)
            if source_path.is_symlink() or not source_path.is_file():
                raise ValueError("recovery sources must be regular files, not symlinks or special files")
        refs = {name: self.upload_file(path, recovery=plan) for name, path in files.items()}
        request = {field: plan[field] for field in ("id", "attempt", "machine", "session")}
        request.update(key=key, requested_by=requested_by, reason=reason,
                       source=dict(source), files=refs)
        with self._file_request("POST", endpoint + "/recovered-results",
                                json.dumps(request).encode(),
                                {"Content-Type": "application/json"},
                                sum(ref["size_bytes"] for ref in refs.values())) as response:
            return json.load(response)

    def download_result(self, job_id: str, artifact: str, destination: str | Path,
                        *, recovered_result_id: str | None = None) -> Path:
        """Fetch an accepted artifact or an explicitly selected recovered result.

        artifact is a manifest key; destination is an explicit caller path.
        Queue/worker paths are never interpreted as local paths.
        """
        if recovered_result_id is not None:
            record = self._call("GET", f"/api/jobs/{quote(job_id, safe='')}/recovered-results/"
                                f"{quote(recovered_result_id, safe='')}?require_files=true")
            if record.get("kind") != "recovered":
                raise ValueError("expected recovered result evidence")
            ref = record.get("files", {}).get(artifact)
        else:
            job = self.get(job_id)
            if job.get("files_expired"):
                raise APIError(410, json.dumps({"code": "result_expired", "error": "stored job files expired after the retention window"}))
            if job["state"] != "succeeded":
                raise APIError(409, json.dumps({"code": "request_stale", "error": "job has no accepted result"}))
            ref = (job.get("output_refs") or {}).get(artifact)
        if ref is None:
            raise APIError(404, json.dumps({"code": "artifact_unknown", "error": "accepted artifact not found"}))
        return self._download_ref(ref, destination)

    def download_results(self, job: str | Mapping[str, Any], destination: str | Path) -> dict[str, Path]:
        """Download every accepted output of a succeeded job into a directory.

        job is an ID or a Job reply from get() or wait(). Each file lands at destination/NAME and is
        checked against its recorded size and SHA-256. Returns {name: path}.
        Raises APIError (result_expired, request_stale) when the job has no
        accepted result to fetch.
        """
        if isinstance(job, str):
            job = self.get(job)
        if job.get("files_expired"):
            raise APIError(410, json.dumps({"code": "result_expired", "error": "stored job files expired after the retention window"}))
        if job["state"] != "succeeded":
            raise APIError(409, json.dumps({"code": "request_stale", "error": "job has no accepted result"}))
        refs = job.get("output_refs") or {}
        if not all(_local_name(name) for name in refs):
            raise ValueError("result names must be clean relative paths")
        root = Path(destination).absolute()
        files = {}
        for name, ref in refs.items():
            target = root / name
            for parent in target.parents:
                if parent == root:
                    break
                if parent.is_symlink():
                    raise ValueError("result parent must be a directory, not a symlink")
            files[name] = self._download_ref(ref, target)
        return files

    def _download_ref(self, ref: Mapping[str, Any], destination: str | Path) -> Path:
        sha, size = ref["sha256"], ref["size_bytes"]
        if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha) or not isinstance(size, int) or size < 0:
            raise ValueError("invalid artifact identity")
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
                temporary = Path(stream.name)
                digest, received = hashlib.sha256(), 0
                try:
                    with self._file_request("GET", "/api/files/blobs/" + sha, size=size) as response:
                        for chunk in iter(lambda: response.read(1024 * 1024), b""):
                            received += len(chunk)
                            if received > size:
                                raise ValueError("artifact exceeds declared size")
                            digest.update(chunk)
                            stream.write(chunk)
                except TransferStalledError as error:
                    raise TransferStalledError(str(error), "result_download_failed") from None
                except (TimeoutError, socket.timeout):
                    raise TransferStalledError(
                        "file transfer stalled during download", "result_download_failed") from None
                if received != size or digest.hexdigest() != sha:
                    raise ValueError("artifact identity mismatch")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            return destination
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def status(self, *, full: bool = False, jobs: str | None = None,
               project: str | None = None, since: str | None = None,
               limit: int | None = None, before: str | None = None,
               sections: str | None = None, snapshot: bool = False) -> dict[str, Any]:
        """Return the compact status view: counts, alert_groups, hyphas, jobs, paused.

        The default is a few KB: running/waiting jobs with full IDs and reasons,
        paused jobs grouped by project and reason with their ids, one row per
        hypha. snapshot=True returns the summary snapshot shape (full current
        job and machine objects) for callers that need it; any of full, jobs,
        limit, before or sections also returns a snapshot. Job reasons are
        last committed facts; sections="jobs" opts into fresh diagnostic planning.
        sections accepts a comma-separated selection of jobs,machines,storage,
        work,health,efficiency,usage,diagnostics. full=True restores every existing
        field and all retained jobs. The queue streams one consistent document;
        this method materializes the returned dict in client memory.
        jobs="recent" adds a capacity-sized terminal
        page; jobs="all" pages history; jobs="active" keeps current/cleanup work.
        Absence from status does not prove completion: use get() or wait_many().
        project/since (queued-at RFC3339) filter jobs only.
        """
        query = {key: value for key, value in {
            "detail": "full" if full else None, "jobs": jobs, "project": project,
            "since": since, "limit": limit, "before": before, "sections": sections,
            "view": "snapshot" if snapshot else None,
        }.items() if value is not None}
        return self._call("GET", "/api/status" + ("?" + urlencode(query) if query else ""))

    def list(self, *, project: str | None = None) -> list[dict[str, Any]]:
        """Return all jobs, including terminal history, using bounded pages."""
        result = []
        before = None
        while True:
            snapshot = self.status(jobs="all", project=project, before=before)
            result.extend(snapshot.get("jobs") or [])
            before = (snapshot.get("jobs_page") or {}).get("next_before")
            if not before:
                return result

    def _read_job(self, job_id: str, *, timeout: float | None = None, wait: bool = False, retry: bool = True) -> dict[str, Any]:
        # A 2xx body cut short in transport is retryable; a complete body that
        # is not a UTF-8 JSON Job is the typed job_reply_invalid.
        query = "?view=wait" if wait else ""
        body = self._call("GET", f"/api/jobs/{quote(job_id, safe='')}{query}", timeout=timeout, raw=True, retry=retry and not wait)
        try:
            reply = json.loads(body.decode("utf-8"))
        except ValueError:
            reply = None
        return _job_reply(reply)

    def get(self, job_id: str) -> dict[str, Any]:
        """Return a Job reply with identity and per-attempt memory_attempts.

        spec is a privacy projection. spec_projection.omitted lists stripped
        paths relative to spec, regardless of submitted presence. In particular,
        absent spec.files does not mean that no files were submitted.
        environment_lock, when present, identifies the frozen lock of a
        lock_file environment: {"backend": ..., "sha256": {basename: hex}}
        (for Pixi, pixi.lock and pixi.toml), although spec.environment is
        projected away.

        Each entry contains attempt/machine/session, the observed memory_peak
        (including events and swap_gb when read), and memory_envelope. Missing
        readings are unknown; numeric zero is a measured zero. Constrained
        observations remain available even when excluded from learning.
        Retains the first and latest 31 rows; memory_attempts_dropped counts
        evictions (omitted zero). Peakless prelaunch attempts create no row.

        inputs_staged_at, when present, is the current attempt's worker-clock
        input-staging completion time, before launch RAM/checkpoint checks.
        It is diagnostic only; omission means unknown or no staging operation.
        Also includes cancellation acknowledgement, retained fan-out scope and
        ledger (complete/high_water); use ledger() for the paginated entries.
        """
        return self._read_job(job_id)

    def ledger(self, job_id: str, *, after: int = 0,
               high_water: int | None = None, limit: int = 100) -> dict[str, Any]:
        """Read one retained ledger page; reuse high_water and next to finish a snapshot.

        complete describes historical coverage, has_more describes pagination.
        HTTP 410 means the ID is unknown or its ledger has expired. get() includes
        the ledger completeness/high_water summary, without the entries.
        """
        query = f"after={after}&limit={limit}"
        if high_water is not None:
            query += f"&high_water={high_water}"
        return self._call("GET", f"/api/jobs/{quote(job_id, safe='')}/ledger?{query}")

    def cancel(self, job_id: str) -> dict[str, Any]:
        """Acknowledge no future execution; release may still be pending."""
        return self._call("POST", f"/api/jobs/{quote(job_id, safe='')}/cancel", {})

    def cancel_submission(self, project: str, submission_key: str) -> dict[str, Any]:
        """Cancel a project/key, reserving an unseen key against future execution."""
        return self._call("POST", "/api/jobs/cancel", {
            "project": project, "submission_key": submission_key,
        })

    def open_production_lease(self, project: str, key: str, ttl: str,
                              stages: list[Mapping[str, Any]] | None = None) -> dict[str, Any]:
        """Declare production stages not yet submitted.

        Submitted production jobs, including stages waiting on ``after:``,
        are already protected; a lease covers only stages you can't submit ahead.

        Planned host drains and fleet upgrades keep at least one enabled host
        that can start each stage (``{"needs": {...}, "allowed_machines": [...]}``,
        the same fields a job uses) until you close the lease or ``ttl`` (a
        duration such as ``"2h"``) passes. Reopen the same key to renew.
        Without stages it protects any host the project may use.
        """
        body: dict[str, Any] = {"project": project, "key": key, "ttl": ttl}
        if stages:
            body["stages"] = list(stages)
        return self._call("POST", "/api/production-leases", body)

    def close_production_lease(self, project: str, key: str) -> dict[str, Any]:
        """Close a production lease; closing an absent or expired lease succeeds."""
        return self._call("DELETE", f"/api/production-leases/{quote(project, safe='')}/{quote(key, safe='')}")

    def production_leases(self) -> list[dict[str, Any]]:
        """Open production leases across all projects."""
        return self._call("GET", "/api/production-leases").get("leases", [])

    def _wait_batch(self, job_ids, after, seconds, timeout):
        try:
            reply = self._call("POST", "/api/wait", {"ids": job_ids, "after": after,
                               "seconds": seconds}, timeout=timeout, retry=False, raw=True)
        except APIError as error:
            if error.status == 404 and not error.code or error.status == 405:
                raise APIError(error.status, json.dumps({
                    "code": "queue_upgrade_required",
                    "error": "queue too old for this client; upgrade the queue first",
                })) from None
            raise
        try:
            reply = json.loads(reply.decode("utf-8"))
        except (ValueError, AttributeError):
            raise APIError(502, '{"code":"job_reply_invalid","error":"invalid wait reply"}') from None
        rows = reply.get("jobs") if isinstance(reply, dict) else None
        if (not isinstance(rows, list) or len(rows) != len(job_ids)
                or any(not isinstance(row, dict) or not isinstance(row.get("id"), str)
                       or not (row["id"] == job_id or len(job_id) >= 8 and row["id"].startswith(job_id))
                       or type(row.get("revision")) is not int or row["revision"] <= 0
                       for job_id, row in zip(job_ids, rows))):
            raise APIError(502, '{"code":"job_reply_invalid","error":"invalid wait reply"}')
        return [_job_reply(row) for row in rows]

    def wait(self, job_id: str | list[str], *, timeout: float | None = None):
        """Wait for one ID, or batch a list of IDs; timeout never cancels work."""
        if isinstance(job_id, str):
            return self.wait_many([job_id], timeout=timeout)[0]
        return self.wait_many(job_id, timeout=timeout)

    def wait_many(self, job_ids, *, timeout: float | None = None) -> list[dict[str, Any]]:
        """Wait in one batch, returning full terminal Jobs in input order.

        Both supported releases provide scalar deadline metadata through the
        batch endpoint. Fetch full Jobs only when they reach a terminal state.
        At most 256 distinct IDs fit the batch-v1 protocol. No jobs are cancelled.
        """
        ids = list(job_ids)
        if any(not isinstance(job_id, str) or not job_id for job_id in ids):
            raise ValueError("job IDs must be nonempty strings")
        pending = dict.fromkeys(ids)
        if len(pending) > 256:
            raise ValueError("wait accepts at most 256 distinct job IDs")
        jobs, metadata, results = {}, {}, {}
        owner_end = None if timeout is None else self._clock.monotonic() + timeout
        end = owner_end
        after = {}
        pace = Pace(self.poll_interval or 0, self._jitter)
        event = None
        def timed_out():
            job_id = next(iter(pending))
            job = jobs.get(job_id)
            if job is None:
                return TimeoutError(f"timed out waiting for job {job_id}; status unavailable; job was not cancelled")
            fallback = "waiting for a hypha to take this job" if job["state"] == "queued" else "still in progress"
            reason = job.get("reason") or fallback
            return TimeoutError(f"timed out waiting for job {job['id']}; job is still {job['state']}: {reason}")
        def request_budget():
            remaining = None if end is None else end - self._clock.monotonic()
            if remaining is not None and remaining <= 0:
                raise timed_out()
            return remaining if self.timeout is None else (self.timeout if remaining is None else min(self.timeout, remaining))

        def observe(original, observed):
            nonlocal changed, ids, end, event
            job_id = observed["id"]
            job = jobs.get(job_id)
            changed = changed or job is None or any(observed.get(k) != job.get(k) for k in ("state", "reason", "revision"))
            jobs[job_id] = observed
            metadata.setdefault(job_id, dict(observed))
            saved = metadata[job_id]
            saved["state"] = observed["state"]
            if "spec" in observed:
                saved["spec"] = observed["spec"]
            if observed.get("started_at"):
                saved["started_at"] = observed["started_at"]
            if observed["state"] == "running" and not saved.get("started_at"):
                saved["started_at"] = dt.datetime.fromtimestamp(self._clock.time(), dt.timezone.utc).isoformat()
            if observed["state"] in TERMINAL or original != job_id:
                del pending[original]
            if observed["state"] in TERMINAL:
                results[job_id] = observed
            else:
                pending[job_id] = None
            # Preserve aliases for the input-order result below.
            if original != job_id:
                ids = [job_id if key == original else key for key in ids]
            refresh_times()

        def refresh_times():
            nonlocal end, event
            event = None
            end = owner_end
            for job_id in pending:
                saved = metadata.get(job_id, {})
                if saved.get("state") in TERMINAL:
                    continue
                spec = saved.get("spec") or {}
                if spec.get("deadline"):
                    until = dt.datetime.fromisoformat(spec["deadline"].replace("Z", "+00:00")).timestamp() - self._clock.time()
                    deadline = self._clock.monotonic() + until
                    end = deadline if end is None else min(end, deadline)
                if saved.get("started_at") and spec.get("expected_minutes"):
                    expected = dt.datetime.fromisoformat(saved["started_at"].replace("Z", "+00:00")).timestamp() + spec["expected_minutes"] * 60
                    next_event = self._clock.monotonic() + expected - self._clock.time()
                    if next_event > self._clock.monotonic():
                        event = next_event if event is None else min(event, next_event)

        while pending:
            started = self._clock.monotonic()
            remaining = None if end is None else end - started
            if remaining is not None and remaining <= 0:
                raise timed_out()
            budget = self.timeout
            if remaining is not None:
                budget = remaining if budget is None else min(budget, remaining)
            failure = None
            held = False
            changed = False
            try:
                seconds = min(20, int(pace.next(started, event)))
                if budget is not None:
                    seconds = min(seconds, int(budget / 2))
                observations = self._wait_batch(list(pending), after, seconds, budget)
                held = seconds > 0
                # Apply all available scalar metadata before any optional/full GET.
                # A slow payload read cannot delay learning a peer's deadline.
                for original, row in zip(list(pending), observations):
                    saved = metadata.setdefault(original, {})
                    saved["state"] = row["state"]
                    if "spec" in row:
                        saved["spec"] = row["spec"]
                    if row.get("started_at"):
                        saved["started_at"] = row["started_at"]
                refresh_times()
                for original, row in zip(list(pending), observations):
                    after[row["id"]] = row["revision"]
                    if row["state"] in TERMINAL:
                        try:
                            row = self._read_job(row["id"], timeout=request_budget(), wait=True)
                        except REQUEST_ERRORS as error:
                            if not self._retryable(error, "wait"):
                                raise
                            failure = error
                            continue
                    observe(original, row)
            except REQUEST_ERRORS as error:
                if not self._retryable(error, "wait"):
                    raise
                failure = error
            if not pending:
                return [results[job_id] for job_id in ids]
            if failure is None:
                pace.observe(self._clock.monotonic() - started, changed)
            if failure is not None:
                pace.observe(self._clock.monotonic() - started)
            self._pause(pace, end=end, event=event, error=failure, held=held)

        return []

    def logs(self, job_id: str, *, follow: bool = False,
             after: int = 0) -> Iterator[dict[str, Any]]:
        """Yield log chunks (seq, attempt, text); follow drains at completion.

        Retain seq and pass it as after to resume without repeating chunks.
        Network errors propagate; cancel a follow by closing its iterator.
        """
        if after < 0:
            raise ValueError("after must be nonnegative")
        path = f"/api/jobs/{quote(job_id, safe='')}/logs"
        pace = Pace(self.poll_interval or 0, self._jitter)
        while True:
            started = self._clock.monotonic()
            chunks = self._call("GET", f"{path}?after={after}") or []
            for chunk in chunks:
                after = chunk["seq"]
                yield chunk
            if len(chunks) == 256:
                continue
            if not follow:
                return
            if self._read_job(job_id, wait=True)["state"] in TERMINAL:
                follow = False
                continue
            pace.observe(self._clock.monotonic() - started, bool(chunks))
            self._pause(pace)

    def openai(self, *, project: str, priority: str = "experiment",
               deadline: str | None = None, **kwargs: Any) -> Any:
        """Return the standard OpenAI SDK with queue routing headers.

        Close the returned SDK client when done. Override its timeout via kwargs
        for long model loads; the default here is ten minutes.
        """
        if not project or priority not in {"production", "deadline", "experiment"}:
            raise ValueError("project and a valid priority are required")
        if priority == "deadline" and not deadline:
            raise ValueError("deadline priority requires a RFC3339 deadline")
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("OpenAI helper requires mycelium-client[openai]") from None
        headers = dict(kwargs.pop("default_headers", {}) or {})
        headers.update({"X-Myce-Project": project, "X-Myce-Priority": priority})
        if deadline:
            headers["X-Myce-Deadline"] = deadline
        kwargs.setdefault("timeout", 600)
        kwargs.setdefault("max_retries", 0)  # do not silently replay model calls
        if self._urlopen is not urlopen and "http_client" not in kwargs:
            kwargs["http_client"] = _openai_http_client(self._urlopen)
        return OpenAI(base_url=self.url + "/v1", api_key=self.token,
                      default_headers=headers, **kwargs)
