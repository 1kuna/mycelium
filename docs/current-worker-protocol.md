# Current worker protocol

Queue and workers use `X-Myce-Start-Gate: host-launch-v3` for worker
requests, including completion, log and checkpoint reports and worker events. An old or missing protocol
gets HTTP 426 with `worker_protocol_mismatch`. Pre-v3 formats remain unsupported. Upgrade the queue before keyed workers;
current host-launch-v3 one-attempt workers remain supported as described below.

Every worker reports a capability profile. Profile version 0 is valid;
unknown capacity withholds admission. Dedicated participation and explicit
laptop admission-only operation remain supported. Enforced host allowances
require measured RAM and declared job RAM. Every model report names its engine
attempt. Local sources report `verified`, `invalid` or `unknown`; only explicit
UNKNOWN gets the existing bounded ready-routing grace.

Every batch command, including non-production work, obtains the durable Started
acknowledgement for its current execution before its payload executes. Current queue maintenance, placement, cancellation and exact attempt/session
bindings are checked at that handoff. Docker, engine and preparation plans use
queue maintenance and retain their exact cleanup custody. A rejected or stale report does not
release another attempt's custody.

New programmatic installs store literal `MYCE_TOKEN` and `MYCE_QUEUE`
assignments in their private credential file, using the same EnvironmentFile
parser as externally provisioned secrets. Old JSON credential files are not
converted; provision current credentials through the secret mechanism when
installing fresh. No token is passed in argv, logs, or job environments.

Old persisted formats return `legacy_state_present`; operators archive and
install fresh as described in [Archive plus fresh install](guide-fresh-install.md).

Environment-build and model-copy offers carry exact queue preparation attempts.
The queue retains issued offers before delivery; workers recheck `/api/workers/launch`
at launch, including after cache waits and at command supervisor gates. The same
queue admission check protects batch Started and engine launches. Drain cancels
unstarted offers and retains started custody until an exact terminal report.

## M1 keyed attempt wire revision

`attempt_protocol: keyed-attempt-v1` opts a check-in into keyed reporting. The
queue echoes that value. Without it, the current single `running` / `started`
request and `job` / `hold` / `stop` / `kill` / `grace_seconds` / `priority_stop`
reply fields retain their existing behavior. This is an extension of the
existing host-launch-v3 protocol, with the same authenticated endpoints.

A keyed heartbeat/progress report uses:

```json
{
  "attempt_protocol": "keyed-attempt-v1",
  "machine": {"id": "worker", "session": "session", "...": "ordinary machine facts"},
  "attempts": [{"id": "job", "attempt": 1, "session": "session", "progress": 7, "started": true}]
}
```

The abbreviated machine above stands for the usual valid capability and resource
report. Omit `attempts` when idle. A keyed reply uses the same key for each
assignment or control decision:

```json
{
  "attempt_protocol": "keyed-attempt-v1",
  "attempts": [{"id": "job", "attempt": 1, "session": "session", "stop": true, "grace_seconds": 10}],
  "hold": false, "stop": false, "kill": false, "leave": false,
  "grace_seconds": 0, "checkpoint_root": "checkpoints"
}
```

An assignment adds `job` to that attempt entry, using the unchanged job-v1
schema. Its ID, attempt, session and machine must match the entry and worker.
A stopping entry can also carry `kill` and P3 `priority_stop`. An exact attempt
can be held during checkpoint reconciliation. Global model/preparation plans,
maintenance policy, checkpoint configuration and leave remain worker-scoped.
The legacy control booleans remain false on keyed replies and `job` is omitted.
Workers apply controls only to the matching `(id, attempt, session)`. A stop
for an unheld key in the worker's own session receives an exact release-only
acknowledgement, including when the original assignment reply was lost or
discarded. Foreign-session controls and unheld holds are ignored. Stop events
add `session` to their existing ID/attempt/deadline fields and are fenced by all
three fields before delivery.

Results, release-only acknowledgements, logs, checkpoint publication and
`/api/workers/started` already use `id`, `attempt`, `session`, and `machine`.
Those shapes and ownership checks are unchanged. P3 stop provenance and deadline
semantics are unchanged. No omitted keyed attempt proves cleanup, releases a
charge, or requeues a stopping job. Exact acknowledgement or the existing
immediately previous-session cleanup rule is still required. Durable worker
spools and queue restart retain the same key for buffered results. An exact
non-pending native release-only acknowledgement advances its recorded stop to
requeue or cancellation, without waiting for an omitted attempt in a heartbeat.

Compatibility and refusal:

- Current host-launch-v3 one-attempt worker → new queue: unchanged legacy form.
- Keyed worker → new queue: collection form; M1 queue admission remains one attempt.
- Keyed worker → older unkeyed queue: fail with `worker_protocol_invalid` (HTTP 426)
  when the old queue returns HTTP 400 `request_invalid` for the keyed fields or
  lacks the revision echo. Other 400 errors retain their own status and code.
  Upgrade the queue first. Saved custody is retained; no automatic unkeyed downgrade.
- Unknown revision, mixed forms, duplicate keys or invalid key/session → `worker_protocol_invalid` (HTTP 426), before queue mutation.

Automatic downgrade would permit an unaddressed stop to affect a different
attempt, so clear refusal is the compatibility policy. Remove the legacy form
once every fleet host runs machine workers and the rollback window is closed.

`attempts` is a collection from M1, with no wire cardinality bound. Each report
is reconciled and answered independently, including stale-key stops; omitted
stopping keys still receive addressed controls. Duplicate keys are invalid.
M1 limits queue admission (including recovery) to one attempt per worker. The
M1 local supervisor selects only its owned control from the complete collection
and refuses conflicting assignments rather than dropping one. M2 adds
`machine.devices` and concurrent local custody/supervision to this same keyed
revision. M3 adds machine placement and concurrent admission on the shared
CPU/RAM ledger. No second keyed revision is planned.
