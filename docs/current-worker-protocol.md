# Current worker protocol

Queue and workers use `X-Myce-Start-Gate: host-attempt-v3` for worker
requests, including completion and log reports. An old or missing protocol
gets HTTP 426 with `worker_protocol_mismatch`. Upgrade queue and workers
together while the host is closed. There is no preparation-only permit route.

Every worker reports a capability profile. Profile version 0 is valid;
unknown capacity withholds admission. Dedicated participation and explicit
laptop admission-only operation remain supported. Enforced host allowances
require measured RAM and declared job RAM. Every model report names its engine
attempt. Local sources report `verified`, `invalid` or `unknown`; only explicit
UNKNOWN gets the existing bounded ready-routing grace.

Every batch command, including non-production work, obtains the durable Started
acknowledgement for its current execution before its payload executes. The host
revision and current placement are checked at that handoff. Docker and engine
launches retain their host revision gates. A rejected or stale report does not
release another attempt's custody.

New programmatic installs store literal `MYCE_TOKEN` and `MYCE_QUEUE`
assignments in their private credential file, using the same EnvironmentFile
parser as externally provisioned secrets. Old JSON credential files require
explicit offline conversion. No token is passed in argv, logs, or job environments.
