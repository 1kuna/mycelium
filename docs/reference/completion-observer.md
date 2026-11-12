# Completion observer upgrade and recovery (next release)

Managed observers adopt all armed registrations in their private user registry.
From the release containing #1160, caller-started shared owners have their own
session/process group and start from `/`, so starter exit and process-group
cleanup do not remove unrelated callers' observation. Their environment is an
allowlist: HOME/USER/LOGNAME, XDG config/cache/data paths, TLS certificate paths,
the starter's PATH and locale, and explicit dev/managed-attempt custody markers.
Runner tracking identities, job-scoped variables, inherited temporary directories,
unrelated secrets and MYCE_TOKEN are excluded. An allowlist also excludes future
CI providers' tracking variables without needing a provider-specific denylist.
Notifier delivery retains its separately saved caller environment with MYCE_TOKEN
removed; legacy registrations without that snapshot keep the owner's PATH/locale
fallback. Dev sessions and managed attempts retain their existing cleanup custody.

From the release containing #919's supervision fix, the detached starter owns a
separate observer child and retains its process handle. Child exit or silence
restarts observation automatically, without another keyed caller. One inherited
lifetime lock spans replacement; startup.lock spans socket setup/readiness and
idle retirement. Cooperative image handoff starts a replacement supervisor before
the previous supervisor retires. Dev/managed-attempt observers and supervisors
remain in their owner's original cleanup group/session.

From the release containing #919's handoff fix, a supervisor whose child is
handing off to a newer image waits for that child to exit. It no longer treats
the successor's status as its own child's silence. Through r6.14, every release
handoff was misread as a stall. The old supervisor SIGKILLed the handing-off
child, the successor's readiness write failed (`write ready: broken pipe`), and
arms went unobserved until some keyed command started a new owner, from 5 s to
9 min later. Unclassified read failures are now logged locally with the errno
(`EHOSTUNREACH`, `ENETUNREACH`, `ETIMEDOUT`, and so on), `local_network_access`,
`tls_identity`, or the Go error type, instead of a bare `io_error`.

Each outstanding authority read is checked independently of peer progress.
The initial read allowance is the batch protocol's 20-second maximum hold plus
one second (RFC 6298 initial transport recovery period). Completed read cost
extends it to at least two measured periods. A coordinator also gets the existing
30-second notifier deadline plus that transport recovery period; its announced
next wake protects intentional discovery/Retry-After/delivery waits. A missed
window doubles the allowance, without a cap. This can replay a slow healthy read,
trading that observation cost for detection of silent reads with no owner deadline.
No queue job, authority, submission key or original arm is replaced. Repeated
failed births/crashes back off from one transport recovery period, doubling
without a cap; successful reads restore that recovery pace.

`myce wait --observer-status` includes `supervision`: supervisor/child PIDs,
restart count, learned window and the last recovery's time, reason, child exit
code/signal when available, and final diagnostic snapshot. The separate private
`observer.supervision` record survives child status replacement; earlier recovery
snapshots remain in the existing two-file bounded structured log. Child stderr is
classified as process-output byte counts, never copied raw into structured logs.
Notifier intent is synced before execution and replayed after interrupted
acknowledgement with its original payload and delivery ID. Receivers deduplicate
that ID; a delivered/failed local acknowledgement still retires the registration.

[Canonical per-user CLI supervision](https://github.com/1kuna/mycelium/issues/1166)
remains separate: native launchd/systemd login/boot recovery, recovery after the
supervisor itself dies, and durable executable/registry install/upgrade/removal
binding are not implemented here. This release installs no observer unit/plist
and publishes no independent client image. Keep the starter executable available
while arms need it; a missing restart image is retained as `launch_failed` and
retried. Existing unsupervised/legacy owners need supported handoff or operator
recovery to gain supervision; installing source alone does not alter a process.
Ordinary startup and cooperative handoff hold startup.lock through socket
replacement/readiness; old-owner cleanup serializes its identity check and unlink
under the same lock. Broken owner startup records a bounded
`observer_start_failed` event in `observer.log` without raw errors or authority.

An ordinary keyed profile/adapter command starts one when the lifetime lock is
free. Updating a CLI symlink does not change the image of an existing process.
Supporting newer clients write a versioned upgrade request when arming or
adopting a receipt. The owner verifies the requesting image and handles it
between deliveries: it cancels and joins outstanding authority readers, releases
receipt locks, passes its lifetime lock to the new image and waits for readiness.
Socket cleanup holds the startup lock across its identity check and unlink, so
the old owner cannot race replacement startup. On startup failure it retains ownership and
reloads durable arms. A paused/stalled loop cannot perform that handoff; the
request itself is not proof of replacement. Unversioned development images do
not request automatic replacement.

Use `myce wait --observer-status` first. Privately preserve its safe metadata,
bounded `observer.log` files, a native process sample, exact process birth/image,
and receipt field names, states, job IDs and timestamps. Never print or publish
notifier targets, environment/authority values or private paths. Count bindings
without exposing values. Establish job terminal states with the installed CLI,
not the queue database. A stale phase is the last boundary reached; an unavailable
live frame remains unknown.

From the release containing #919’s recurrence fix, next-read spacing uses measured
observation service cost and the current Retry-After signal, with failure jitter
spread over one measured service period. Unchanged long-poll history is used only
to select the protocol hold; it cannot amplify the next-read timer. This trades
unbounded read suppression for continued observation at the measured service
rate; the queue can still shed reads with its explicit pressure signal.
Terminal-detail requests survive those pressure passes and resume before another
batch read, so learning a terminal state cannot repeatedly consume the credit
needed to retrieve its result reference.

A legacy owner has neither upgrade support nor diagnostics. Recovery of a shared
owner requires the owner's authorization and consideration of the wake volume:

1. Refresh the private receipt/registration snapshot and process identities.
2. Send SIGTERM to that exact observer. Verify its identity and descendants have
   exited and lifetime/receipt locks are free. Stop if a survivor or lock remains;
   do not substitute force-kill or edit registration state.
3. Compare every receipt against the snapshot. Older cancellation paths can
   disarm pending observations; report the exact affected job set before replay.
4. From the original consumer workspace, repeat its ordinary supported command,
   for example `myce run --profile PROFILE --key SAVED_KEY`. Keep the original
   arguments and notifier binding; omission inherits the saved binding. The
   replay starts a fresh owner and adopts registrations made by other callers.
   A new agent's arm would also start it, triggering the same backlog.
5. Verify a sample of terminal receipts has `delivery_status: delivered` and
   `observation: not_armed`, receiver delivery IDs are unique, same-key replay
   does not resend, and a new arm works. A receiver success does not prove useful
   agent work or accepted job results.

Delivery is serial. Supporting replacements pace stale terminal backlog per
binding using mean observed delivery service cost times remaining binding demand;
the first delivery per binding is immediately eligible. There is no legacy pacing
option. Distinct bindings and fresh completions can still wake together; the
measurement is notifier runtime, not downstream agent-turn runtime. Use a distinct
notifier executable path per thread, as required by the binding contract.
Receipt acknowledgement prevents ordinary replay duplicates; exactly-once
external effects across crashes require receiver deduplication by `delivery_id`.

### Completion and health delivery acceptance

Check component support on the actual installed CLI, running observer, queue
and workers; merged source alone is not rollout evidence. Preserve each original
job/key/arm and stable delivery identity. Verify computation and accepted result,
local terminal receipt, notifier invocation/result, transport acceptance,
supported durable receiving-thread arrival and owner disposition as separate
boundaries. An armed receipt, held owner lock, successful hook, HTTP acceptance
or wake alone does not prove the complete chain. Report an unsupported receiver
boundary to its runtime owner; do not invent a parallel receipt service.

Before cleanup, distinguish parent-bound foreground waits from intentional
managed shared observers. Reuse exact executable/birth/session and attempt or
fixture ownership evidence; a PID or old age is insufficient. Only its owner
stops a scoped helper, verifies exit and free locks, then removes disposable
fixture state. Preserve survivors, registrations and receipts and stop the
cleanup if custody is uncertain. A legacy shared owner follows the recovery
steps above; consumer scripts never kill all waits or edit registry files.
After a permitted lifecycle transition, verify the exact owned helpers are gone,
legitimate shared observers remain, and jobs/receipts are still recoverable.

Queue health delivery is independent of job completion. Confirm the running
queue's health hook is configured for the installation's intended private
receiver before claiming alert delivery. Diagnostic-only mode remains supported.
Keep receiver/executable configuration private. Trace one safe episode's stable
delivery ID through the real hook, durable queue acknowledgement, receiving
state and owner action; configuration presence or `--check` is insufficient.
Plan backlog delivery with its owner; keep all undelivered identities and receiver
deduplication across retries/restart. Never clear history or fake acknowledgement
to make status healthy. Respect current production-run and rollout authority.
