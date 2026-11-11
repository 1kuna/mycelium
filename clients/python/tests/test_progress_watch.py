"""#1016: calls wait while the queue progresses and fail when it stalls."""
import json
import threading
import unittest
from urllib.error import URLError

from mycelium import Client, QueueStalledError
from test_adaptive import Clock, Reply


class ProgressWatchTests(unittest.TestCase):
    def run_call(self, ping, finish_after=None, path="/api/status"):
        clock = Clock()
        client = Client(token="fixture")
        client._clock = clock
        finished = threading.Event()
        pings = []

        def wait(event, seconds):
            clock.sleep(seconds)
            return event.wait(0.05)  # real time only for the worker to return
        client._await = wait

        def respond(request, timeout=None):
            if request.selector == "/api/ping":
                pings.append(clock.now)
                if finish_after is not None and clock.now >= finish_after:
                    finished.set()
                reply = ping(len(pings), clock.now)
                if isinstance(reply, Exception):
                    raise reply
                return Reply(reply)
            if not finished.wait(5):
                raise AssertionError("call was never released")
            return Reply({"ok": True})
        client._urlopen = respond
        try:
            return client._call("GET", path, retry=False), clock.now, pings
        finally:
            finished.set()

    @staticmethod
    def progress(releases, waiting, p99):
        return {"version": 1, "queue_progress": {"releases": releases, "waiting": waiting, "p99_ms": p99 * 1000}}

    def test_slow_queue_that_progresses_is_not_cut_off(self):
        reply, waited, _ = self.run_call(lambda n, now: self.progress(n, 3, 2), finish_after=60)
        self.assertEqual(reply, {"ok": True})
        self.assertGreaterEqual(waited, 60)

    def test_idle_lock_is_not_a_stall(self):
        reply, _, _ = self.run_call(lambda n, now: self.progress(9, 0, .01), finish_after=30)
        self.assertEqual(reply, {"ok": True})

    def test_held_lock_with_waiters_stalls_within_four_periods(self):
        with self.assertRaises(QueueStalledError) as raised:
            self.run_call(lambda n, now: self.progress(7, 2, 3))
        stall = raised.exception
        self.assertEqual(stall.window, 12)
        self.assertGreaterEqual(stall.waited, 12)
        self.assertLess(stall.waited, 20)
        self.assertIsNone(stall.cause)
        self.assertEqual(stall.code, "queue_stalled")
        self.assertIn("myce status", str(stall))

    def test_unanswered_probes_stall_with_their_cause(self):
        cause = URLError("probe connection reset")
        with self.assertRaises(QueueStalledError) as raised:
            self.run_call(lambda n, now: cause)
        self.assertIs(raised.exception.cause, cause)
        self.assertGreaterEqual(raised.exception.waited, 4)

    def test_intermittent_progress_resets_the_window(self):
        reply, waited, _ = self.run_call(lambda n, now: self.progress(int(now // 10), 1, 3), finish_after=90)
        self.assertEqual(reply, {"ok": True})
        self.assertGreaterEqual(waited, 90)

    def test_queue_without_progress_evidence_leaves_the_caller_deadline(self):
        from urllib.error import HTTPError
        import io
        for reply in ({"version": 1}, [{"seq": 1}],
                      HTTPError("http://fixture/api/ping", 404, "missing", {}, io.BytesIO(b""))):
            with self.subTest(reply=reply):
                clock = Clock()
                client = Client(token="fixture")
                client._clock = clock
                release = threading.Event()
                pings = []

                def wait(event, seconds):
                    clock.sleep(seconds)
                    return event.wait(0.05)
                client._await = wait

                def respond(request, timeout=None, reply=reply):
                    if request.selector == "/api/ping":
                        pings.append(1)
                        # After the only probe, release the call in real time.
                        threading.Timer(0.1, release.set).start()
                        if isinstance(reply, Exception):
                            raise reply
                        return Reply(reply)
                    release.wait(5)
                    return Reply({"ok": True})
                client._urlopen = respond
                self.assertEqual(client._call("GET", "/api/status", retry=False), {"ok": True})
                self.assertEqual(len(pings), 1)

    def test_fast_calls_and_batch_waits_never_probe(self):
        client = Client(token="fixture")
        selectors = []

        def respond(request, timeout=None):
            selectors.append(request.selector)
            return Reply({"jobs": []} if request.selector == "/api/wait" else {"ok": True})
        client._urlopen = respond
        client._call("GET", "/api/status")
        client._call("POST", "/api/wait", {"ids": [], "after": {}, "seconds": 0}, retry=False)
        self.assertNotIn("/api/ping", selectors)


if __name__ == "__main__":
    unittest.main()
