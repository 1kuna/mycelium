import hashlib
import io
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError

from mycelium import APIError, Client
from mycelium.adaptive import Pace
from mycelium.client import TransferStalledError
from test_adaptive import Clock, Reply


class RetryPacingTests(unittest.TestCase):
    def test_fast_failures_allow_transport_recovery_and_owner_deadline(self):
        for elapsed in (0, .001):
            with self.subTest(elapsed=elapsed):
                clock = Clock()
                client = Client(token="fixture", timeout=3)
                client._clock, client._jitter = clock, lambda: 0
                attempts = []
                sleep = clock.sleep

                def pause(delay):
                    self.assertAlmostEqual(delay, min(1.0, 3 - clock.now))
                    sleep(delay)

                clock.sleep = pause

                def respond(request, **kwargs):
                    attempts.append(clock.now)
                    clock.now += elapsed
                    raise HTTPError(request.full_url, 503, "busy", {}, io.BytesIO(b'{}'))

                client._urlopen = respond
                with self.assertRaises(TimeoutError):
                    client.get("job")
                self.assertEqual(len(attempts), 3)
                self.assertEqual(clock.now, 3)
                pace = Pace(jitter=lambda: 0)
                pace.observe(elapsed)
                self.assertEqual(pace.next(0, error=ConnectionError()), 1.0)
                self.assertEqual(pace.next(0, error=APIError(503, "busy", .1)), .1)

    def test_get_and_download_follow_current_pressure(self):
        for download in (False, True):
            for signaled in (False, True):
                for bounded in (False, True):
                    with self.subTest(download=download, signaled=signaled, bounded=bounded):
                        clock = Clock()
                        client = Client(token="fixture", timeout=17 if bounded else None,
                                        transfer_timeout=17 if bounded else None)
                        client._clock, client._jitter = clock, lambda: .5
                        data = b"accepted bytes"
                        digest = hashlib.sha256(data).hexdigest()
                        job = {"id": "job", "state": "succeeded", "output_refs": {
                            "result": {"sha256": digest, "size_bytes": len(data)}}}
                        attempts, expected = [], []
                        not_before = 0

                        def respond(request, **kwargs):
                            nonlocal not_before
                            if download and not request.selector.startswith("/api/files/"):
                                return Reply(job)
                            self.assertGreaterEqual(clock.now + 1e-9, not_before)
                            attempts.append(clock.now)
                            clock.now += .008
                            if len(attempts) <= 19:
                                signal = (len(attempts) - 1) % 13 + 1 if signaled else 0
                                delay = (signal or 1.0) * 1.5  # RFC 6298 without a signal.
                                expected.append(min(delay, max(0, 17 - clock.now)) if bounded else delay)
                                not_before = clock.now + signal
                                raise HTTPError(request.full_url, 429, "busy",
                                                {"Retry-After": str(signal)} if signaled else {},
                                                io.BytesIO(b'{"code":"client_rate_limited"}'))
                            return io.BytesIO(data) if download else Reply(job)

                        client._urlopen = respond
                        with tempfile.TemporaryDirectory() as directory:
                            def run():
                                if download:
                                    path = client.download_result("job", "result", Path(directory) / "result")
                                    self.assertEqual(path.read_bytes(), data)
                                else:
                                    self.assertEqual(client.get("job"), job)
                            if bounded:
                                with self.assertRaises((TimeoutError, TransferStalledError)):
                                    run()
                                self.assertAlmostEqual(clock.now, 17)
                            else:
                                run()
                                self.assertEqual(len(attempts), 20)
                        self.assertEqual(len(clock.sleeps), len(expected))
                        for got, want in zip(clock.sleeps, expected):
                            self.assertAlmostEqual(got, want)

    def test_pressure_discards_unchanged_history_and_uses_measured_cost(self):
        pace = Pace(jitter=lambda: .5)
        for _ in range(100):
            pace.observe(.008)
        for signal in (13, 1, 0):
            self.assertAlmostEqual(pace.next(0, error=APIError(429, "busy", signal)),
                                   (signal or 1.0) * 1.5)
        # A slower completed attempt changes fallback pacing without a fixed cap.
        pace.observe(8)
        self.assertAlmostEqual(pace.next(0, error=ConnectionError()), 4.004 * 1.5)
