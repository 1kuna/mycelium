"""Consumer batching and mixed-version wait journeys with an injected clock."""
import json
import unittest
from urllib.error import HTTPError

from mycelium import Client, APIError
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
                self.assertEqual(len(gets), count)  # Terminal payloads only; no bootstrap GET.
                self.assertEqual(len(batches), 6)

    def test_retired_queue_does_not_trigger_scalar_polling(self):
        for status in (404, 405):
            with self.subTest(status=status):
                client = Client(token="fixture")
                calls = []
                def respond(request, **kwargs):
                    calls.append(request.selector)
                    raise HTTPError(request.full_url, status, "unsupported", {}, Reply({}))
                client._urlopen = respond
                with self.assertRaises(APIError) as error:
                    client.wait_many(["a", "b"])
                self.assertEqual(error.exception.status, status)
                self.assertEqual(calls, ["/api/wait"])

    def test_completed_job_deadline_does_not_bound_peer(self):
        client = Client(token="fixture")
        client._clock = Clock()
        batches = []
        def respond(request, **kwargs):
            if request.selector == "/api/wait":
                ids = json.loads(request.data)["ids"]
                batches.append(ids)
                return Reply({"jobs": [{"id": key, "state": "succeeded" if key == "done" or len(batches) > 1 else "running",
                                        "revision": len(batches), "spec": {"deadline": "2026-11-05T11:00:00Z"} if key == "done" else {}} for key in ids]})
            key = request.selector.split("/api/jobs/")[1].split("?")[0]
            return Reply({"id": key, "state": "succeeded"})
        client._urlopen = respond
        self.assertEqual(len(client.wait_many(["done", "peer"], timeout=30)), 2)
        self.assertEqual(batches, [["done", "peer"], ["peer"]])

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
                if len(batches) > 1:
                    clock.now = max(clock.now, 1)
                return Reply({"jobs": [{"id": key, "state": "running" if len(batches) == 1 else "succeeded", "revision": len(batches),
                                        "spec": {"deadline": "2026-11-05T12:00:02Z"} if key == "b" else {}} for key in body["ids"]]})
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
        self.assertTrue(all(batch == ["a"] for batch in batches[2:]))

    def test_batch_retains_deadline_during_outage(self):
        client = Client(token="fixture")
        clock = client._clock = Clock()
        calls, reads = [], []
        def batch(ids, after, seconds, timeout):
            calls.append(ids)
            clock.now += min(.01, timeout) if timeout is not None else .01
            if len(calls) > 1:
                raise HTTPError("http://fixture.invalid/api/wait", 503, "busy", {}, Reply({}))
            return [{"id": "job", "state": "running", "revision": 1, "spec": {"deadline": "2026-11-05T12:00:01Z"}}]
        def read(job_id, **kwargs):
            reads.append(job_id)
            return {"id": job_id, "state": "running", "spec": {"deadline": "2026-11-05T12:00:01Z"}}
        client._wait_batch, client._read_job = batch, read
        with self.assertRaises(TimeoutError):
            client.wait("job")
        self.assertEqual(reads, [])
        self.assertAlmostEqual(clock.now, 1, places=6)
