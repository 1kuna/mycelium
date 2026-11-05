import datetime as dt
import io
import json
import socket
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from mycelium import Client
from mycelium.adaptive import Pace
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

    def test_near_expected_event(self):
        pace = Pace()
        for i in range(12):
            pace.observe(.001, i == 0)
        self.assertLess(pace.next(.99, 1), pace.next(0, 1))

    def test_throughput_stall_and_slow_progress(self):
        for period in (.001, 120):
            for size in (16, 1024 * 1024):
                clock = Clock()
                class Socket:
                    stalled = False
                    timeout = None
                    def settimeout(self, timeout):
                        self.timeout = timeout
                    def recv_into(self, buffer, *args):
                        if self.stalled:
                            clock.now += self.timeout
                            raise socket.timeout()
                        if self.timeout is not None and period > self.timeout:
                            raise AssertionError("slow progress killed")
                        clock.now += period
                        return len(buffer)
                source = Socket()
                transfer = _ProgressSocket(source, adaptive=True)
                with patch("mycelium.transfer.time", clock):
                    for _ in range(100):
                        self.assertEqual(transfer.recv_into(bytearray(size)), size)
                    self.assertAlmostEqual(clock.now, 100 * period)
                    source.stalled = True
                    with self.assertRaises(socket.timeout):
                        transfer.recv_into(bytearray(size))
                    self.assertGreater(source.timeout, period)
                    self.assertLess(source.timeout, 100 * period)

    def test_transfer_owner_timeout_bounds_progress(self):
        clock = Clock()
        class Socket:
            def settimeout(self, timeout):
                self.timeout = timeout
            def send(self, data, *args):
                clock.now += .1
                return 1
        transfer = _ProgressSocket(Socket(), adaptive=True, deadline=.5)
        with patch("mycelium.transfer.time", clock), self.assertRaisesRegex(TimeoutError, "owner timeout"):
            transfer.sendall(b"long progressing upload")
        self.assertEqual(clock.now, .5)
