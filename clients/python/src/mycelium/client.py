"""Synchronous, standard-library client for the Mycelium HTTP API."""
from __future__ import annotations

import base64
import hashlib
import http.client
import io
import tempfile
import re
import copy
import datetime as dt
import json
import os
import ssl
from pathlib import Path
import time
from typing import Any, Iterator, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

TERMINAL = {"succeeded", "failed", "cancelled"}


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


# One per-operation budget, shared with Go: at least 60 s, or 30 s plus
# two full-blob hash passes at a conservative disk rate of 100 MiB/s (#218).
# There is no whole-request timeout, even while waiting for response headers.
FILE_RESPONSE_BUDGET = 30.0
FILE_VERIFY_BYTES_PER_SECOND = 100 * 1024 * 1024
FILE_TRANSFER_IDLE = 60.0


class APIError(RuntimeError):
    """The queue rejected a request; status and response body are retained."""

    def __init__(self, status: int, body: str):
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


class Client:
    """Connect using explicit settings or MYCE_QUEUE and MYCE_TOKEN."""

    def __init__(self, url: str | None = None, token: str | None = None,
                 *, timeout: float = 10, poll_interval: float = 0.3):
        self.url = (url or os.environ.get("MYCE_QUEUE", "http://127.0.0.1:7331")).rstrip("/")
        parsed = urlsplit(self.url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("queue URL must be an HTTP(S) URL without credentials, query or fragment")
        self.token = os.environ.get("MYCE_TOKEN", "") if token is None else token
        if not self.token:
            raise ValueError("set MYCE_TOKEN or pass token explicitly")
        if timeout <= 0 or poll_interval <= 0:
            raise ValueError("timeout and poll_interval must be positive")
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._urlopen = urlopen
        # Tests shorten the file transfer budgets; None uses the defaults.
        self._transfer_idle: float | None = None

    @classmethod
    def from_installed(cls, config: str | Path | None = None, *,
                       timeout: float = 10, poll_interval: float = 0.3) -> Client:
        """Use the installed worker/queue authority without exporting credentials.

        Reads worker config first, then queue config if no worker is installed.
        A joining authority retains its certificate pin and refuses redirects.
        Explicit Client(url, token) construction keeps its existing behavior.
        """
        if ssl.OPENSSL_VERSION.startswith("LibreSSL"):
            raise UnsupportedTLSRuntimeError(
                "Client.from_installed() requires OpenSSL (3 recommended); "
                "macOS system LibreSSL is unsupported for the queue Ed25519 "
                "joining certificate. Detected "
                f"{ssl.OPENSSL_VERSION}. Use Homebrew or python.org Python.")
        from .installed import installed_authority
        address, token, opener = installed_authority(config)
        client = cls(address, token, timeout=timeout, poll_interval=poll_interval)
        client._urlopen = opener
        return client

    def _call(self, method: str, path: str, body: Any = None,
              *, timeout: float | None = None, raw: bool = False) -> Any:
        data = None if body is None else json.dumps(body, default=_json_default, allow_nan=False).encode()
        request = Request(self.url + path, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with self._urlopen(request, timeout=self.timeout if timeout is None else timeout) as response:
                return _read_reply(response) if raw else json.load(response)
        except HTTPError as error:
            with error:
                body = error.read(4096).decode("utf-8", errors="replace")
            failure = APIError(error.code, body)
            if (error.code == 404 and not failure.code and method == "GET"
                    and path.startswith("/api/jobs/") and "/" not in path[len("/api/jobs/"):]):
                failure = APIError(404, json.dumps({
                    "code": "queue_upgrade_required",
                    "error": "queue too old for this client; upgrade the queue first",
                }))
            raise failure from None

    def submit(self, job: Mapping[str, Any] | str | Path) -> dict[str, Any]:
        """Submit a mapping, YAML text, or a pathlib.Path to a YAML file.

        after={"job": parent_id, "outputs": {local: "KEY/OUTPUT_NAME"}}
        waits for the whole fan-out parent. Set after.piece for early single-piece
        readiness on an ordinary job. fan_out may accompany the whole-parent form.
        """
        if not isinstance(job, Mapping):
            return self.submit_yaml(job)
        return self._call("POST", "/api/jobs", _normalize(job))

    def submit_yaml(self, source: str | Path) -> dict[str, Any]:
        """Load YAML safely. Pass Path for a file, str for YAML text."""
        try:
            import yaml
        except ImportError:
            raise ImportError("YAML submission requires mycelium-client[yaml]") from None
        text = source.read_text(encoding="utf-8") if isinstance(source, Path) else source
        job = yaml.safe_load(text)
        if not isinstance(job, Mapping):
            raise ValueError("job YAML must contain one mapping")
        return self.submit(job)

    def submit_and_wait(self, job: Mapping[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        """Retry this call with the same spec/key after an uncertain response.

        One transport retry reuses the identical key/spec.
        No automatic replay of unkeyed work or application side effects.
        Returns all terminal states; inspect state before consuming results.
        """
        if not job.get("submission_key"):
            raise ValueError("submit_and_wait requires submission_key")
        spec = _normalize(job)
        try:
            submitted = self.submit(spec)
        except (URLError, TimeoutError, ConnectionError):
            # One bounded retry of the identical keyed intent. HTTP rejections
            # and conflicts propagate; they are not transient transport errors.
            submitted = self.submit(spec)
        return self.wait(submitted["id"], timeout=timeout)

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
        def local_name(name: str) -> bool:
            return bool(name) and not name.startswith("/") and "\\" not in name and ":" not in name and all(
                part not in {"", ".", ".."} for part in name.split("/"))
        if not all(local_name(name) for name in inputs):
            raise ValueError("input names must be clean relative paths")
        files = {name: self.upload_file(source) for name, source in inputs.items()}
        job = self.submit_and_wait({"name": extension, "project": project, "class": class_,
                                    "extension": extension, "submission_key": key,
                                    "files": files, **({"after": copy.deepcopy(dict(after))} if after is not None else {})}, timeout=timeout)
        if job.get("files_expired"):
            raise APIError(410, json.dumps({"code": "result_expired", "error": "stored job files expired after the retention window"}))
        if job["state"] != "succeeded":
            short = job["id"][:8]
            raise APIError(409, json.dumps({"code": "job_" + job["state"], "job_id": job["id"],
                "error": f"job {short} {job['state']}: {job.get('reason', '')}; run myce logs {short} for the traceback"}))
        names = job.get("output_refs", {})
        if not all(local_name(name) for name in names):
            raise ValueError("result names must be clean relative paths")
        root = Path(outputs).absolute()
        for name in names:
            destination = root / name
            for parent in destination.parents:
                if parent == root:
                    break
                if parent.is_symlink():
                    raise ValueError("result parent must be a directory, not a symlink")
            self.download_result(job["id"], name, destination)
        return job

    def _file_request(self, method: str, path: str, data: bytes | None = None,
                      headers: Mapping[str, str] | None = None, size: int = 0):
        """size is the blob the queue may verify before replying; 0 when none."""
        budget = self._transfer_idle
        if budget is None:
            budget = max(FILE_TRANSFER_IDLE,
                         FILE_RESPONSE_BUDGET + 2 * max(size, 0) / FILE_VERIFY_BYTES_PER_SECOND)
        headers = dict(headers or {})
        if data is not None:
            headers["Content-Length"] = str(len(data))
        # Bound memory used by http.client; the transfer connection also sends
        # partial buffers separately so no sendall has a cumulative timeout.
        request = Request(self.url + path, data=None if data is None else io.BytesIO(data),
                          method=method, headers={"Authorization": "Bearer " + self.token, **headers})
        request._mycelium_file_transfer = True
        opener = self._urlopen
        if opener is urlopen:
            from .transfer import file_opener
            opener = file_opener()
        try:
            response = opener(request, timeout=budget)
        except HTTPError as error:
            with error:
                body = error.read(4096).decode("utf-8", errors="replace")
            raise APIError(error.code, body) from None
        except TimeoutError:
            raise TransferStalledError(f"no network progress for {budget:g}s") from None
        except URLError as error:
            if isinstance(error.reason, TimeoutError):
                raise TransferStalledError(f"no network progress for {budget:g}s") from None
            raise
        return response

    def upload_file(self, source: str | Path) -> dict[str, Any]:
        """Upload a job input, eligible for retention if never submitted."""
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
        try:
            with self._file_request("HEAD", "/api/files/blobs/" + ref["sha256"], size=size):
                return ref
        except APIError as error:
            if error.status not in (404, 410):
                raise
        with self._file_request("POST", "/api/files/uploads/", b"", size=size, headers={
                **headers, "Upload-Length": str(size),
                "Upload-Metadata": "sha256 " + base64.b64encode(ref["sha256"].encode()).decode()
                + ",purpose " + base64.b64encode(b"job-input").decode()}):
            pass
        # A repeated create may refer to an already published blob.
        try:
            with self._file_request("HEAD", "/api/files/blobs/" + ref["sha256"], size=size):
                return ref
        except APIError as error:
            if error.status not in (404, 410):
                raise
        with self._file_request("HEAD", endpoint, headers=headers) as response:
            offset = int(response.headers["Upload-Offset"])
            length = int(response.headers["Upload-Length"])
        if length != size or not 0 <= offset <= size:
            raise ValueError("invalid upload offset/length")
        with source.open("rb") as stream:
            stream.seek(offset)
            while offset < size:
                chunk = stream.read(min(1024 * 1024, size - offset))
                if not chunk:
                    raise ValueError("input changed during upload")
                with self._file_request("PATCH", endpoint, chunk, {
                        **headers, "Content-Type": "application/offset+octet-stream",
                        "Upload-Offset": str(offset)}, size) as response:
                    if int(response.headers["Upload-Offset"]) != offset + len(chunk):
                        raise ValueError("invalid upload acknowledgement")
                offset += len(chunk)
        with self._file_request("HEAD", "/api/files/blobs/" + ref["sha256"], size=size):
            pass
        return ref

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
        refs = {name: self.upload_file(path) for name, path in files.items()}
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
                except TimeoutError:
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
               limit: int | None = None, before: str | None = None) -> dict[str, Any]:
        """Return active jobs plus 100 recent terminal jobs and fleet health.

        Work curves remain included. full=True restores all jobs. jobs="all" pages history;
        jobs="active" includes nonterminal work and pending cleanup. Optional
        project/since (queued-at RFC3339) filters apply only to jobs.
        """
        query = {key: value for key, value in {
            "detail": "full" if full else None, "jobs": jobs, "project": project,
            "since": since, "limit": limit, "before": before,
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

    def _read_job(self, job_id: str, *, timeout: float | None = None) -> dict[str, Any]:
        # A 2xx body cut short in transport is retryable; a complete body that
        # is not a UTF-8 JSON Job is the typed job_reply_invalid.
        body = self._call("GET", f"/api/jobs/{quote(job_id, safe='')}", timeout=timeout, raw=True)
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

    def wait(self, job_id: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Wait for a terminal state; timeout leaves the job queued/running."""
        end = None if timeout is None else time.monotonic() + timeout
        job = None
        def timed_out() -> TimeoutError:
            if job is None:
                return TimeoutError(f"timed out waiting for job {job_id}; status unavailable; job was not cancelled")
            fallback = "waiting for a hypha to take this job" if job["state"] == "queued" else "still in progress"
            reason = job.get("reason") or fallback
            return TimeoutError(f"timed out waiting for job {job['id']}; job is still {job['state']}: {reason}")
        while True:
            remaining = None if end is None else end - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise timed_out()
            try:
                observed = self._read_job(job_id, timeout=self.timeout if remaining is None else min(self.timeout, remaining))
            except (TimeoutError, URLError, IncompleteReplyError) as error:
                if end is not None and time.monotonic() >= end:
                    raise timed_out() from None
                if not isinstance(error, IncompleteReplyError):
                    raise
            else:
                job = observed
                if job["state"] in TERMINAL:
                    return job
            time.sleep(self.poll_interval if remaining is None else min(self.poll_interval, max(0, end - time.monotonic())))

    def logs(self, job_id: str, *, follow: bool = False,
             after: int = 0) -> Iterator[dict[str, Any]]:
        """Yield log chunks (seq, attempt, text); follow drains at completion.

        Retain seq and pass it as after to resume without repeating chunks.
        Network errors propagate; cancel a follow by closing its iterator.
        """
        if after < 0:
            raise ValueError("after must be nonnegative")
        path = f"/api/jobs/{quote(job_id, safe='')}/logs"
        while True:
            chunks = self._call("GET", f"{path}?after={after}") or []
            for chunk in chunks:
                after = chunk["seq"]
                yield chunk
            if len(chunks) == 256:
                continue
            if not follow:
                return
            if self.get(job_id)["state"] in TERMINAL:
                follow = False
                continue
            time.sleep(self.poll_interval)

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
        return OpenAI(base_url=self.url + "/v1", api_key=self.token,
                      default_headers=headers, **kwargs)
