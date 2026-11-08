"""Consumer batching and mixed-version wait journeys with an injected clock."""
import json
import unittest
from urllib.error import HTTPError

from mycelium import Client
from test_adaptive import Clock, Reply


class BatchWaitTests(unittest.TestCase):
    def test_single_and_many_unchanged_jobs_use_one_wait(self):
        for count in (1, 12):
            with self.subTest(count=count):
                clock = Clock()
                client = Client(token="fixture")
                client._clock = clock
                ids = [f"job-{n}" for n in range(count)]
                gets, batches = [], []

                def respond(request, **kwargs):
                    clock.now += .01
                    if request.selector == "/api/wait":
                        body = json.loads(request.data)
                        self.assertEqual(body["ids"], ids)
                        self.assertEqual(body["after"], {} if not batches else dict.fromkeys(ids, 1))
                        batches.append(body)
                        state = "succeeded" if len(batches) == 6 else "running"
                        return Reply({"jobs": [{"id": key, "state": state, "revision": 1 if state == "running" else 2} for key in ids]})
                    gets.append(request.selector)
                    key = request.selector.split("/api/jobs/")[1].split("?")[0]
                    return Reply({"id": key, "state": "succeeded" if len(batches) == 6 else "running",
                                  "output_refs": {"answer": {"size_bytes": 42}}}, {"X-Myce-Wait": "batch-v1"})

                client._urlopen = respond
                result = client.wait(ids[0] if count == 1 else ids)
                rows = [result] if count == 1 else result
                self.assertEqual([row["id"] for row in rows], ids)
                self.assertTrue(all(row["output_refs"] for row in rows))
                self.assertEqual(len(gets), 2 * count)  # Initial metadata + terminal payload only.
                self.assertEqual(len(batches), 6)

    def test_older_queue_and_endpoint_fallback(self):
        for refusal in (None, 404, 405):
            with self.subTest(refusal=refusal):
                client = Client(token="fixture")
                client._clock = Clock()
                calls, reads = [], {}

                def respond(request, **kwargs):
                    calls.append(request.selector)
                    if request.selector == "/api/wait":
                        raise HTTPError(request.full_url, refusal, "unsupported", {}, Reply({}))
                    key = request.selector.split("/api/jobs/")[1].split("?")[0]
                    reads[key] = reads.get(key, 0) + 1
                    return Reply({"id": key, "state": "running" if reads[key] <= 3 else "succeeded"},
                                 {"X-Myce-Wait": "batch-v1"} if refusal else {})

                client._urlopen = respond
                rows = client.wait_many(["a", "b"])
                self.assertEqual([row["state"] for row in rows], ["succeeded", "succeeded"])
                self.assertEqual(calls.count("/api/wait"), int(refusal is not None))
                self.assertIn("/api/jobs/a?view=wait", calls)

    def test_completed_job_deadline_does_not_bound_peer(self):
        client = Client(token="fixture")
        client._clock = Clock()
        calls = []

        def respond(request, **kwargs):
            calls.append(request.selector)
            key = request.selector.split("/api/jobs/")[1].split("?")[0]
            state = "succeeded" if key == "done" or len(calls) > 2 else "running"
            return Reply({"id": key, "state": state, "spec": {
                "deadline": "2026-11-05T11:00:00Z"} if key == "done" else {}})

        client._urlopen = respond
        self.assertEqual(len(client.wait_many(["done", "peer"], timeout=30)), 2)

    def test_partial_initial_reads_retain_deadlines_and_results(self):
        for slow in (False, True):
            with self.subTest(slow=slow):
                client = Client(token="fixture")
                clock = client._clock = Clock()
                reads = []
                budgets = []

                def respond(request, **kwargs):
                    reads.append(request.selector)
                    clock.now += .01
                    if request.selector == "/api/jobs/done":
                        return Reply({"id": "done", "state": "succeeded"})
                    if request.selector.startswith("/api/jobs/early"):
                        return Reply({"id": "early", "state": "running", "spec": {
                            "deadline": "2026-11-05T12:00:02Z"}})
                    budgets.append(kwargs["timeout"])
                    if slow:
                        clock.now += max(0, kwargs["timeout"] - .01)
                        raise TimeoutError("peer read timed out")
                    raise HTTPError(request.full_url, 503, "busy", {"Retry-After": "1"}, Reply({}))

                client._urlopen = respond
                with self.assertRaises(TimeoutError):
                    client.wait_many(["done", "early", "peer"])
                self.assertAlmostEqual(clock.now, 2, places=6)
                self.assertEqual(reads.count("/api/jobs/done"), 1)
                self.assertGreaterEqual(sum(path.startswith("/api/jobs/early") for path in reads), 1)
                self.assertTrue(all(0 < budget <= 2 for budget in budgets))

    def test_failing_peer_does_not_hide_completion_or_retain_deadline(self):
        client = Client(token="fixture")
        clock = client._clock = Clock()
        client._jitter = lambda: 0
        early_reads = []

        def respond(request, **kwargs):
            clock.now += .01
            if request.selector.startswith("/api/jobs/early"):
                early_reads.append(clock.now)
                return Reply({"id": "early", "state": "running" if clock.now < 1 else "succeeded",
                              "spec": {"deadline": "2026-11-05T12:00:02Z"}})
            if clock.now < 3:
                raise HTTPError(request.full_url, 503, "busy", {"Retry-After": "1"}, Reply({}))
            return Reply({"id": "peer", "state": "succeeded"})

        client._urlopen = respond
        rows = client.wait_many(["peer", "early"], timeout=10)
        self.assertEqual([row["id"] for row in rows], ["peer", "early"])
        self.assertGreater(len(early_reads), 1)
        self.assertGreaterEqual(clock.now, 3)

    def test_shed_bootstrap_enters_one_batch_for_all_jobs(self):
        client = Client(token="fixture")
        client._clock = Clock()
        ids = ["job-a", "job-b", "job-c"]
        gets, waits = [], []

        def respond(request, **kwargs):
            client._clock.now += .01
            if request.selector == "/api/wait":
                body = json.loads(request.data)
                self.assertEqual(body["ids"], ids)
                waits.append(body)
                return Reply({"jobs": [{"id": key, "state": "succeeded" if len(waits) == 4 else "running", "revision": len(waits)} for key in ids]})
            gets.append(request.selector)
            if request.selector.endswith("?view=wait"):
                key = request.selector.split("/api/jobs/")[1].split("?")[0]
                return Reply({"id": key, "state": "succeeded", "output_refs": {"answer": {"size_bytes": 42}}})
            raise HTTPError(request.full_url, 429, "shed", {"Retry-After": "1"}, Reply({"code": "client_rate_limited"}))

        client._urlopen = respond
        rows = client.wait_many(ids, timeout=30)
        self.assertEqual([row["id"] for row in rows], ids)
        self.assertTrue(all(row["output_refs"] for row in rows))
        self.assertEqual(len([path for path in gets if "?" not in path]), 1)
        self.assertEqual(len(waits), 4)

    def test_shed_bootstrap_resolves_prefix_like_queue_and_go_client(self):
        client = Client(token="fixture")
        client._clock = Clock()
        full_id = "12345678abcdef"
        batches = []

        def respond(request, **kwargs):
            client._clock.now += .01
            if request.selector == "/api/wait":
                body = json.loads(request.data)
                batches.append(body["ids"])
                return Reply({"jobs": [{"id": full_id, "state": "running" if len(batches) == 1 else "succeeded", "revision": len(batches)}]})
            if request.selector.endswith("?view=wait"):
                return Reply({"id": full_id, "state": "succeeded", "output_refs": {"answer": {"size_bytes": 42}}})
            raise HTTPError(request.full_url, 429, "shed", {"Retry-After": "1"}, Reply({"code": "client_rate_limited"}))

        client._urlopen = respond
        row = client.wait(full_id[:8], timeout=10)
        self.assertEqual(row["id"], full_id)
        self.assertTrue(row["output_refs"])
        self.assertEqual(batches, [[full_id[:8]], [full_id]])

    def test_failed_terminal_payload_does_not_discard_completed_peer(self):
        client = Client(token="fixture")
        clock = client._clock = Clock()
        client._jitter = lambda: 0
        batches, peer_payloads = [], []

        def respond(request, **kwargs):
            clock.now += .01
            if request.selector == "/api/wait":
                body = json.loads(request.data)
                batches.append(body["ids"])
                clock.now = max(clock.now, 1)
                return Reply({"jobs": [{"id": key, "state": "succeeded", "revision": 1} for key in body["ids"]]})
            key = request.selector.split("/api/jobs/")[1].split("?")[0]
            if request.selector.endswith("?view=wait"):
                if key == "a" and clock.now < 3:
                    raise HTTPError(request.full_url, 503, "busy", {"Retry-After": "1"}, Reply({}))
                if key == "b":
                    peer_payloads.append(clock.now)
                return Reply({"id": key, "state": "succeeded", "output_refs": {"answer": {"size_bytes": 42}}})
            return Reply({"id": key, "state": "running", "spec": {"deadline": "2026-11-05T12:00:02Z"} if key == "b" else {}},
                         {"X-Myce-Wait": "batch-v1"})

        client._urlopen = respond
        rows = client.wait_many(["a", "b"], timeout=10)
        self.assertEqual([row["id"] for row in rows], ["a", "b"])
        self.assertEqual(len(peer_payloads), 1)
        self.assertLess(peer_payloads[0], 2)
        self.assertTrue(all(batch == ["a"] for batch in batches[1:]))
