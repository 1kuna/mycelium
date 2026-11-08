import datetime as dt
import io
import json
import socket
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from mycelium import Client
from mycelium.transfer import _ProgressSocket


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
    def monotonic(self):
        return self.now
    def time(self):
        return 1793880000 + self.now  # 2026-11-05T12:00:00Z
    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Reply(io.BytesIO):
    def __init__(self, value, headers=None):
        if isinstance(value, dict) and isinstance(value.get("jobs"), list):
            for row in value["jobs"]:
                row.setdefault("spec", {})
        super().__init__(json.dumps(value).encode())
        self.headers = headers or {}


class AdaptiveTests(unittest.TestCase):
    def test_unchanged_waits_at_different_installation_latencies(self):
        for latency in (.002, 2):
            for batch in (False, True):
                with self.subTest(latency=latency, batch=batch):
                    clock = Clock()
                    client = Client(token="fixture")
                    client._clock = clock
                    calls, holds = [], []
                    def respond(request, **kwargs):
                        if request.selector == "/api/wait" and not batch:
                            raise HTTPError(request.full_url, 404, "unsupported", {}, Reply({}))
                        calls.append(request)
                        clock.now += latency
                        state = "succeeded" if len(calls) >= 7 else "queued"
                        job = {"id": "job", "state": state}
                        if request.selector == "/api/wait":
                            holds.append(json.loads(request.data)["seconds"])
                            job["revision"] = 1
                            job = {"jobs": [job]}
                        return Reply(job, {"X-Myce-Wait": "batch-v1"} if batch else {})
                    client._urlopen = respond
                    self.assertEqual(client.wait("job")["state"], "succeeded")
                    if not batch:
                        self.assertGreater(clock.sleeps[3], clock.sleeps[1])
                    if batch:
                        self.assertTrue(holds)
                        self.assertGreaterEqual(holds[-1], holds[0])

    def test_retry_after_no_retry_count_limit_and_owner_timeout(self):
        for status in (429, 503):
            for signal in ("7", "Thu, 05 Nov 2026 12:00:07 GMT"):
                clock = Clock()
                client = Client(token="fixture")
                client._clock, client._jitter = clock, lambda: .5
                attempts = []
                def respond(request, **kwargs):
                    attempts.append(clock.now)
                    clock.now += .001
                    if len(attempts) <= 4:
                        raise HTTPError(request.full_url, status, "busy", {"Retry-After": signal},
                                        io.BytesIO(b'{"code":"client_rate_limited"}'))
                    return Reply({"id": "job", "state": "succeeded"})
                client._urlopen = respond
                self.assertEqual(client.get("job")["state"], "succeeded")
                self.assertEqual(len(attempts), 5)
                self.assertGreaterEqual(clock.sleeps[0], 7)
                clock.now, attempts[:] = 0, []
                client._jitter = lambda: 0
                signal = "7"  # A fresh not-before signal on every owner-bounded retry.
                with self.assertRaises(TimeoutError):
                    client.wait("job", timeout=10)
                self.assertEqual(clock.now, 10)
                self.assertEqual(len(attempts), 2)


    def test_transfer_owner_timeout_bounds_progress(self):
        clock = Clock()
        class Socket:
            def settimeout(self, timeout):
                self.timeout = timeout
            def send(self, data, *args):
                clock.now += .1
                return 1
        transfer = _ProgressSocket(Socket(), adaptive=True, deadline=.5)
        with patch("mycelium.transfer.time", clock), patch.object(transfer, "_buffered", return_value=False), self.assertRaisesRegex(TimeoutError, "owner timeout"):
            transfer.sendall(b"long progressing upload")
        self.assertEqual(clock.now, .5)

    def test_transfer_includes_complete_service_period(self):
        for method in ("recv", "recv_into", "send"):
            with self.subTest(method=method):
                clock = Clock()
                class Socket:
                    period = .000001
                    timeout = None
                    def settimeout(self, timeout):
                        self.timeout = timeout
                    def advance(self):
                        if self.timeout is not None and self.period > self.timeout:
                            raise socket.timeout("brief readiness wait hid service period")
                        clock.now += self.period
                    def recv(self, count, flags=0):
                        self.advance()
                        return b"x" * count
                    def recv_into(self, data, *args):
                        self.advance()
                        return len(data)
                    send = recv_into
                source = Socket()
                transfer = _ProgressSocket(source, adaptive=True)
                data = 256 if method == "recv" else bytearray(256)
                operation = getattr(transfer, method)
                with patch("mycelium.transfer.time", clock), patch.object(transfer, "_buffered", return_value=False):
                    operation(data)
                    clock.now += 1  # Buffered draining/consumer work between readiness edges.
                    operation(data)
                    source.period = .05
                    operation(data)


class BufferedProgressTests(unittest.TestCase):
    def test_buffered_progress_survives_wire_gaps_and_then_stalls(self):
        for owner in (None, .8):
            with self.subTest(owner=owner):
                receiver, sender = socket.socketpair()
                stop = threading.Event()
                def produce():
                    try:
                        for _ in range(6):
                            sender.sendall(b"x" * 8192)
                            if stop.wait(.05):
                                return
                        stop.wait()
                    finally:
                        sender.close()
                thread = threading.Thread(target=produce, daemon=True)
                thread.start()
                deadline = None if owner is None else time.monotonic() + owner
                transfer = _ProgressSocket(receiver, adaptive=True, deadline=deadline)
                try:
                    total = 0
                    while total < 6 * 8192:
                        total += transfer.recv_into(bytearray(256))
                    with self.assertRaises(TimeoutError):
                        transfer.recv_into(bytearray(256))
                finally:
                    stop.set()
                    receiver.close()
                    thread.join()

class LongPollCompletionTests(unittest.TestCase):
    def test_completion_between_long_polls_is_observed_before_owner_timeout(self):
        clock = Clock()
        client = Client(token="fixture")
        client._clock = clock
        def respond(request, **kwargs):
            clock.now += .01
            state = 'succeeded' if clock.now >= 350 else 'running'
            job = {'id': 'job', 'state': state}
            if request.selector == '/api/wait':
                seconds = json.loads(request.data)['seconds']
                clock.now += min(seconds, max(0, 350 - clock.now))
                job.update(state='succeeded' if clock.now >= 350 else 'running', revision=1)
                job = {'jobs': [job]}
            return Reply(job, {'X-Myce-Wait': 'batch-v1'})
        client._urlopen = respond
        self.assertEqual(client.wait('job', timeout=600)['state'], 'succeeded')
        self.assertLess(clock.now, 371)
