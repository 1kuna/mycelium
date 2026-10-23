# Integrating a GPU caller

Use the [approved design](design-proposal.md) for the product contract,
[operations](operations.md) for host changes and [status](guide-status-v3.md)
for typed evidence. These examples use a local dev queue and synthetic values.
Keep the real address and token in private configuration. Model/server calls
use [their existing interfaces](guide-model-calls.md); they are not keyed batch
submissions. CPU preparation and publication stay in the consumer.

## Keyed submission and cancellation

Start the [laptop dev queue](try-it-locally.md#five-minute-start). Persist the
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

## Maintenance

Close consumer publication intent and persist exact cancellation acknowledgements
independently. Use `myce host drain HOST` or `host disable HOST --stop --wait` for
queue maintenance. Inspect exact pending cleanup and worker freshness, then use
`host resume HOST` when authorized. Queue outage leaves maintenance unavailable;
ordinary service tools have no queue release guarantee. Check the application's
own runtime/model identity and health before directing traffic to it. Integrate
through the submission API; no local host-control client is required.

For a Python caller use [Client](../clients/python/README.md); resumable commands
use [the checkpoint contract](guide-resumable-jobs.md). Dependency recipes,
server-local named secrets and immutable model sources are in
[environments](environments.md) and [servers](guide-servers.md).
