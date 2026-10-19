# Current worker protocol

Queue and workers use `X-Myce-Start-Gate: host-attempt-v3` for worker
requests, including completion, log and checkpoint reports and worker events. An old or missing protocol
gets HTTP 426 with `worker_protocol_mismatch`. Upgrade queue and workers
together while queue host admission is disabled. There is no preparation-only permit route.

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
