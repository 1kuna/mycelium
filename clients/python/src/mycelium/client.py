"""Synchronous, standard-library client for the Mycelium HTTP API."""
from __future__ import annotations

import copy
import datetime as dt
import json
import os
from pathlib import Path
import time
from typing import Any, Iterator, Mapping
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

TERMINAL = {"succeeded", "failed", "cancelled"}


class APIError(RuntimeError):
    """The queue rejected a request; status and response body are retained."""

    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body
        super().__init__(f"Mycelium HTTP {status}: {body}")


def _json_default(value: Any) -> str:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise ValueError("job timestamps must include a timezone")
        return value.isoformat()
    raise TypeError(f"cannot encode {type(value).__name__} as JSON")


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

    def _call(self, method: str, path: str, body: Any = None,
              *, timeout: float | None = None) -> Any:
        data = None if body is None else json.dumps(body, default=_json_default, allow_nan=False).encode()
        request = Request(self.url + path, data=data, method=method, headers={
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout if timeout is None else timeout) as response:
                return json.load(response)
        except HTTPError as error:
            with error:
                body = error.read(4096).decode("utf-8", errors="replace")
            raise APIError(error.code, body) from None

    def submit(self, job: Mapping[str, Any] | str | Path) -> dict[str, Any]:
        """Submit a mapping, YAML text, or a pathlib.Path to a YAML file."""
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

    def status(self) -> dict[str, Any]:
        """Return the queue snapshot, including waiting reasons and machines."""
        return self._call("GET", "/api/status")

    def list(self) -> list[dict[str, Any]]:
        """Return all jobs, including terminal jobs."""
        return self.status().get("jobs") or []

    def get(self, job_id: str) -> dict[str, Any]:
        """Find a job in the snapshot; raise KeyError if absent."""
        return self._find(self.list(), job_id)

    @staticmethod
    def _find(jobs: list[dict[str, Any]], job_id: str) -> dict[str, Any]:
        for job in jobs:
            if job["id"] == job_id:
                return job
        raise KeyError(job_id)

    def cancel(self, job_id: str) -> dict[str, Any]:
        """Request cancellation."""
        return self._call("POST", f"/api/jobs/{quote(job_id, safe='')}/cancel", {})

    def wait(self, job_id: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Wait for any terminal state, returning it (including failure)."""
        end = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = None if end is None else end - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"timed out waiting for job {job_id}")
            snapshot = self._call("GET", "/api/status", timeout=self.timeout if remaining is None else min(self.timeout, remaining))
            job = self._find(snapshot.get("jobs") or [], job_id)
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
