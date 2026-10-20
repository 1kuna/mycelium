# Wire schemas

These JSON Schema 2020-12 documents describe actual Go JSON output and worker
messages. The Job reply is not a new-job submission validator: a cancelled
reservation has a minimal spec. The worker reply's assignment `job` references
the same `job-v1.schema.json`; there is no parallel job format.

- `job-v1.schema.json`: stable Job identity/lifecycle and diagnostic projections.
- `cancel-ack-v1.schema.json`: cancellation acknowledgement and release status.
- `error-v1.schema.json`: typed error body, including `worker_protocol_invalid`.
- `worker-checkin-v1.schema.json`: current legacy and keyed check-ins.
- `worker-reply-v1.schema.json`: current legacy and keyed queue replies.
- `worker-result-v1.schema.json`: exact-attempt Started/result/release reports;
  log and checkpoint messages embed this same identity.
- `worker-event-v1.schema.json`: stop events with job/attempt/session/deadline;
  empty long-poll replies carry only their cursor and zero-valued fields.

Worker messages require `X-Myce-Start-Gate: host-launch-v3`. The optional
`attempt_protocol: keyed-attempt-v1` revision selects per-attempt check-in/reply
collections. Omission retains current one-attempt worker compatibility until
every fleet host runs machine workers and the rollback window is closed.
M1 limits queue admission to one attempt per worker, never wire collection size.
M2 adds machine.devices to this same keyed revision. Runtime validation
also enforces cross-field equality, forbids mixed forms and fences stale keys;
a schema match is not ownership or release proof. See
[the protocol contract](../current-worker-protocol.md) for negotiation, refusal,
stop handling and exact-ack/restart rules.

Regenerate and validate using the existing Go schema tools:

```sh
MYCE_UPDATE_SCHEMAS=1 go test ./internal/myce -run '^(TestWireSchemas|TestWorkerWireSchemas)$'
go test ./internal/myce -run '^(TestWireSchemas|TestWorkerWireSchemas|TestAttemptProtocolWireSchemas)$'
```

Tests regenerate from actual types and validate real queue messages, including
legacy and keyed assignments, heartbeats, results and empty/active events.
