# One-command rolling fleet upgrade

Proposed amendment for [#244](https://github.com/1kuna/mycelium/issues/244),
2026-10-27. Reviewed source: `origin/main` at `0ce3bd30`.
**Not approved or implemented.** The [approved design](../design-proposal.md)
remains the only product spec. Its M4 installation section says: “No automatic
GPU driver, service or host changes are authorized by this spec.” Following the
worker brief's stop condition, this note requests the explicit exception for
signed runtime self-update; no runtime or fleet changes accompany it.

## Existing coverage and deployment lessons

Reuse [release approval](../../internal/myce/enrollment_release.go),
[maintenance](../../internal/myce/host_maintenance.go), the
[installer](../../internal/myce/install.go),
[enrollment](../../internal/myce/enrollment.go) and its
[drain ownership receipts](../../internal/myce/enrollment_upgrade.go).
Approval currently stores verified metadata, not the signed envelope; it neither
installs bytes nor commands workers. Enrollment reports version/session and
service health, but needs running-executable hash evidence for this operation.
The current upgrade helper refuses busy enabled hosts and restores installations
on failure. Those behaviors cannot simply be wrapped in a fleet loop.

Retained deployment scripts show separate signing/publishing, staging, drain
waits, saved-config installs, manager restarts, executable/hash checks, conditional
resume and default approval. Queue-only restarts preserved running batch attempts;
that evidence does not authorize restarting a busy worker. Some Darwin readiness
waits expired after installation, requiring direct reconciliation. Later NAS
refusals prompted manual binary swaps: those were operational workarounds, not
installer acceptance. Keep selected-mount and root-I/O protections; integrate the
independently reviewed fixes for [mount inventory #227](https://github.com/1kuna/mycelium/issues/227)
and [storage progress #235](https://github.com/1kuna/mycelium/issues/235), without
copying their bypasses or inventing a production window.

## Proposed contract

After publishing and `myce release approve`, an operator runs the illustrative,
currently unsupported command `myce fleet upgrade [--release VERSION]`. Omission
selects the current approval; an explicit version must match that approval, not
an arbitrary publisher directory. Freeze its signed envelope, publisher trust,
origin/CA, target hashes and host membership in the existing SQLite queue. Store
one active operation with per-host phases and receipts. An identical rerun
reconciles it; a different target conflicts until the stopped operation is
explicitly superseded by an approved fix-forward release. Approval changes never
silently retarget an operation. New hosts use normal onboarding separately.

The queue owns progress after the CLI disconnects. Extend authenticated check-ins
with an operation/host/session-fenced update instruction and acknowledgement.
Workers fetch bytes themselves over the approved HTTPS path, reject redirects,
verify the Ed25519 envelope against separately provisioned publisher trust, and
check size/SHA-256 before executing. Retain the signed envelope during approval
and provision worker trust through authenticated enrollment; neither a manifest
nor an update instruction may replace that trust. Existing approvals lacking the
envelope require identical verified reapproval. Local key rotation stays local.
The deployment permission boundary, including [#183](https://github.com/1kuna/mycelium/issues/183),
must be settled explicitly before implementation; a valid signature alone does
not authorize deployment. No ticket redemption, SSH, payload job or token-bearing
command is used for an installed hypha.

Upgrade one physical host at a time, then restore its admission before advancing.
Put hosts running production at the end; recheck production before selecting each
remaining host. Drain withdraws unstarted assignments and blocks new admission;
wait for all Started work to finish naturally and for exact preparation, engine,
call and release custody to settle. Report busy/offline/unknown-custody waits;
neither elapsed time nor an empty local inventory permits restart. Do not force
stop or impose a production completion deadline.

Place the queue service before any worker activation requiring its new protocol,
and before its colocated worker replacement. Preserve production-last ordering:
if the queue host is a production host, preceding workers must be compatible with
the existing queue. Refuse an incompatible ordering before mutation rather than
silently breaking either rule; protocol migrations need a separately approved
plan. At the queue step, pause new model-call admission and wait for in-flight
calls/streams, which ordinary queue restart would fail. Preserve running batch
attempts and buffered results through existing restart reconciliation, with no
new launches during outage; prove this continuity in the upgrade tests. Failure
to reconcile is a stopped operation, never evidence that work was released.

For each host, perform: **drain and wait → verify → install in place → restart →
health and running hash → conditional resume**. Reuse installer validation,
saved settings and publication, preserving config, authority, identity, spool,
state, catalogs, allowances and cache roots. Darwin retains its installed path,
valid signature and exact designated signing requirement; do not re-sign locally.
A finite helper of the same binary must run outside the service being restarted,
using the platform service manager so stopping that service cannot kill its
updater. Persist its local operation receipt before replacement, including the
queue's own restart intent. No additional daemon or database is needed.

Complete only after stable manager health, exact new PID/session, compatible
protocols, expected inventory/budgets, runner smoke and the running executable's
approved hash. A disk hash or version string alone is insufficient. Resume only
the operation's own drain with the existing owner/timestamp CAS; preserve prior
operator maintenance and stop on a conflicting edit. A rerun after replacement
rechecks these same postconditions instead of repeating a restart blindly.

`myce status` shows target, host order, current phase, wait reason, last verified
progress and failure. Stop at the first download, verification, install, restart
or health failure; keep that host maintained and leave later hosts untouched.
Report the host/phase and supported rerun or fix-forward action. In accordance
with MY-03, this path must not invoke the installer's existing automatic restore
or enrollment rollback: factor reuse explicitly so failed activation preserves
evidence and maintenance without reinstalling old bytes. No rollback snapshots,
old-version fallback, preflight bypass or automatic retry loop is added.
Use the injected Clock for decisions and existing bounded I/O/service probes
outside `q.mu`; long production waits remain progress-visible and cancellable.
Keep compact receipts and remove owned rebuildable staging after settlement.

## Acceptance and next owner

After explicit amendment approval, implement under MY-03/MY-08/MY-17/MY-20.
Fake-clock tests must prove sequential/production-last order (including the queue
host), natural production completion, interruption/restart reconciliation,
maintenance ownership and failure-stop. A `myce dev` end-to-end test must roll
multiple real command workers and the queue between two locally signed releases,
disconnect/re-run the operator, and verify hashes plus unchanged job identities,
attempts, starts, loss/retry counts and successful outputs. Include in-flight
model-call completion before queue cutover. Native signing and manager behavior
require separate authorized acceptance; laptop proof is not fleet qualification.

Current state: design note only. Next owner: orchestrator/maintainer to approve
the self-update exception, credential boundary and fix-forward installer change
in the product spec, then assign implementation against #244. The first release
containing that implementation still needs the existing authorized installation
path; only capable installed runtimes can receive later self-update instructions.
No live fleet upgrade is authorized by this proposal.
