import hashlib
import io
import json
import ssl
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError, URLError

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

    def test_requests_follow_current_pressure(self):
        for operation in ("get", "download", "keyed", "wait", "upload"):
            for signaled in (False, True):
                for bounded in (False, True):
                    with self.subTest(operation=operation, signaled=signaled, bounded=bounded):
                        clock = Clock()
                        client = Client(token="fixture", timeout=17 if bounded else None,
                                        transfer_timeout=17 if bounded else None)
                        client._clock, client._jitter = clock, lambda: .5
                        if operation == "wait" and signaled:
                            client.poll_interval = 120  # Must not override fresh Retry-After.
                        data = b"accepted bytes"
                        digest = hashlib.sha256(data).hexdigest()
                        job = {"id": "job", "state": "succeeded", "output_refs": {
                            "result": {"sha256": digest, "size_bytes": len(data)}}}
                        attempts, expected = [], []
                        not_before = 0

                        def respond(request, **kwargs):
                            nonlocal not_before
                            target = (request.selector.startswith("/api/files/") if operation in {"download", "upload"}
                                      else request.selector == "/api/jobs" if operation == "keyed"
                                      else request.selector == "/api/wait" if operation == "wait"
                                      else request.selector == "/api/jobs/job")
                            if not target:
                                return Reply({"jobs": [{**job, "revision": 1}]} if request.selector == "/api/wait" else job)
                            if operation == "keyed":
                                self.assertEqual(json.loads(request.data), {"submission_key": "same-key"})
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
                            if operation == "download":
                                return io.BytesIO(data)
                            if operation == "upload":
                                return Reply({}, {"Content-Length": str(len(data)), "ETag": '"' + digest + '"'})
                            return Reply({"jobs": [{**job, "revision": 1}]} if operation == "wait" else job)

                        client._urlopen = respond
                        with tempfile.TemporaryDirectory() as directory:
                            def run():
                                if operation == "download":
                                    path = client.download_result("job", "result", Path(directory) / "result")
                                    self.assertEqual(path.read_bytes(), data)
                                elif operation == "upload":
                                    source = Path(directory) / "input"
                                    source.write_bytes(data)
                                    self.assertEqual(client.upload_file(source), {"sha256": digest, "size_bytes": len(data)})
                                elif operation == "keyed":
                                    self.assertEqual(client.submit_and_wait({"submission_key": "same-key"}, timeout=17 if bounded else None), job)
                                elif operation == "wait":
                                    self.assertEqual(client.wait("job", timeout=17 if bounded else None), job)
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

    def test_tls_failure_classification_for_wait_and_keyed_submit(self):
        record = ssl.SSLError(ssl.SSL_ERROR_SSL, "record failure")
        record.reason = "RECORD_LAYER_FAILURE"
        failures = [record, ssl.SSLEOFError("EOF"),
                    ssl.SSLCertVerificationError("certificate verification failed"),
                    ssl.SSLError("queue identity does not match installed authority"),
                    ssl.SSLError("unsupported protocol")]
        for failure in failures:
            for wrapped in (False, True):
                for keyed in (False, True):
                    with self.subTest(failure=failure, wrapped=wrapped, keyed=keyed):
                        client = Client(token="fixture")
                        clock = client._clock = Clock()
                        client._jitter = lambda: 0
                        requests = []
                        error = URLError(failure) if wrapped else failure
                        def respond(request, **kwargs):
                            requests.append(request)
                            if len(requests) == 1:
                                if wrapped:
                                    raise error
                                class BrokenReply(Reply):
                                    def read(self, *args):
                                        raise error
                                return BrokenReply({})  # Bare TLS failure while reading.
                            job = {"id": "job", "state": "succeeded", "revision": 1}
                            return Reply({"jobs": [job]} if request.selector == "/api/wait" else job)
                        client._urlopen = respond
                        def run():
                            return (client.submit_and_wait({"submission_key": "fixed"}, timeout=10)
                                    if keyed else client.wait_many(["job"], timeout=10)[0])
                        if failure in failures[:2]:
                            self.assertEqual(run()["state"], "succeeded")
                            self.assertEqual(clock.sleeps, [1])
                        else:
                            with self.assertRaises(type(error)):
                                run()
                            self.assertEqual(len(requests), 1)
                            self.assertEqual(clock.sleeps, [])

    def test_unkeyed_write_is_never_replayed(self):
        for action in (lambda client: client.submit({"name": "once"}),
                       lambda client: client.cancel("job")):
            client = Client(token="fixture")
            client._clock = Clock()
            calls = []
            def respond(request, **kwargs):
                calls.append(request)
                raise HTTPError(request.full_url, 503, "busy", {"Retry-After": "1"}, io.BytesIO(b'{}'))
            client._urlopen = respond
            with self.assertRaises(APIError):
                action(client)
            self.assertEqual(len(calls), 1)
            self.assertEqual(client._clock.sleeps, [])
