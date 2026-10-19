# Integrating a GPU caller

Use the [approved design](design-proposal.md) for the product contract,
[operations](operations.md) for host changes and [status](guide-status-v3.md)
for typed evidence. These examples use a local dev queue and synthetic values.
Keep the real address and token in private configuration. Model/server calls
use [their existing interfaces](guide-model-calls.md); they are not keyed batch
submissions. CPU preparation and publication stay in the consumer.

## Keyed submission and cancellation

Start the [laptop dev queue](../README.md#five-minute-laptop-start). Persist the
key and identical spec in the consumer before the first request. For this demo:

```sh
export MYCE_QUEUE=http://127.0.0.1:7331
export MYCE_TOKEN=dev-token
cat > job.json <<'JSON'
{"name":"keyed-demo","project":"demo","submission_key":"demo-operation-1","priority":"experiment","run":{"command":["sh","-c","echo done"]},"needs":{"memory_gb":1,"platform":{"any":true}},"resumable":false}
JSON
curl --fail-with-body "$MYCE_QUEUE/api/jobs" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @job.json > first.json
# Retry the stored identical request after a lost response, while intent is open:
curl --fail-with-body "$MYCE_QUEUE/api/jobs" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @job.json > retry.json
```

Both replies have the same `id`; identity echoes in `spec.project` and
`spec.submission_key`. A changed spec under that key is HTTP 409
`submission_key_conflict`. Terminal replies never rerun a job. This does not
promise exactly-once external effects; keep writes attempt-safe.

When the consumer closes its operation, fence late publication and **cancel**
its stored keys. Never recover a closed operation by resubmitting:

```sh
curl --fail-with-body "$MYCE_QUEUE/api/jobs/cancel" \
  -H "Authorization: Bearer $MYCE_TOKEN" -H 'Content-Type: application/json' \
  -d '{"project":"demo","submission_key":"demo-operation-1"}' > cancellation.json
```

Persist and verify the exact project/key echo and `will_not_run: true`.
Wait for `release: confirmed`, or `not_applicable` when never attempted.
`pending` means an already assigned attempt may still run briefly and remains
unreleased. Terminal state alone does not prove every attempt released.
Retry cancellation to recover a lost acknowledgement. An unseen key creates
an `absent` cancelled reservation: its first later valid submit can only bind a
minimal terminal cancelled job. It never executes or conflicts on spec.

An unbound reservation lasts 30 days. After expiry submit returns HTTP 410
`submission_key_expired`; cancel returns `unknown` and null release. Genuinely
unseen keys return `absent`. Unknown/expired coverage blocks the consumer's
reopen. Job-bound keys remain for the job lifetime, plus retention after removal.
For isolated dev expiry tests use `myce dev clock advance 30d`; it has no production
route. See the [full submission/cancellation contract](design-proposal.md#submission-identity-and-cancellation-80)
and [wire schemas](schemas/job-v1.schema.json).

## Owner switch

The consumer owns its operation intent, cancellation completion, output fence
and other-owner checks. Mycelium owns only its host admission/release obligations.
Do not put consumer state in the queue or infer completion from a host verdict.

1. Close consumer intent, reject late attempt publication, and cancel exact IDs/keys.
2. On the worker host, close Mycelium, preserving the returned accepted revision:

   ```sh
   myce host close --local --config WORKER_CONFIG --wait-released --timeout 30s --json
   myce host status --local --config WORKER_CONFIG --expect closed --verdict --timeout 10s --json
   ```

   Real Linux privileged release uses the [protected root copy](operations.md#protected-root-owned-copy)
   with `--worker-user WORKER_USER`. Local exclusion can pass offline while
   queue settlement remains pending; that is not full rest or remote cancellation proof.
3. Persist consumer cancellation completion independently and verify the other
   owner has released the host before reopening. Require confirmed previous-close
   settlement. Read status and use its current `desired.revision`:

   ```sh
   myce host status --local --config WORKER_CONFIG --json
   myce host open --local --config WORKER_CONFIG --expect-revision REVISION --timeout 30s --json
   myce host status --local --config WORKER_CONFIG --expect ready --wait --timeout 30s --json
   ```

4. Require ready PASS, then verify the application's own runtime/model identity
   and complete health/result. Acceptance is not readiness or application health.
   `revision_mismatch` rejects without acceptance; `revision_superseded` preserves
   the earlier acceptance and directs the caller to the newer intent. Never
   bypass an unsettled close, stale revision or UNKNOWN observation.

For a Python caller use [Client](../clients/python/README.md); resumable commands
use [the checkpoint contract](guide-resumable-jobs.md). Dependency recipes,
server-local named secrets and immutable model sources are in
[environments](environments.md) and [servers](guide-servers.md).
