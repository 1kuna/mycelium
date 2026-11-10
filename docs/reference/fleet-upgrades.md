# Fleet upgrade internals and failure codes

Details behind the [rolling fleet upgrade procedure](../operations.md#rolling-fleet-upgrades):
how hosts are chosen and verified, what each failure code means and how to
repair it, and how stale or failed operations recover.

## Idle moments and host order

From the release containing #900's idle-moment slice, normal updates leave busy
enabled hosts `pending`, with admission open. An hours-long command does not hold
the host's spare capacity for maintenance. Other idle compatible hosts can
progress. Fresh exact custody selects the next natural idle moment and acquires
the upgrade drain atomically before placement refills it. Unreleased attempts,
preparations, loading models, calls and unknown cleanup do not establish idle;
a ready idle model can enter drain but must confirm unload before restart.
After queue restart, each retained engine needs a fresh exact zero-call report
before it can count as idle, regardless of the host's production-hold policy.
Pending operations persist across queue restart without acquiring a drain.
Earlier releases drain a selected host while waiting for its work to finish.

Hosts update sequentially: eligible enabled/drained hosts first (production hosts
last within that group), then operator-disabled hosts. Started commands finish
naturally before their host restarts; there is no time-triggered cancellation.
Queue cutover waits for calls/streams while admission remains open, then pauses
new model calls at the idle selection. Batch attempts on other hosts reconnect unchanged.
The queue precedes its colocated worker. This implementation accepts the current
worker protocols only, so incompatible protocol migrations need a separate plan.

## Helper verification

Each enrolled service fetches approved bytes through the authenticated queue,
checks its locally pinned publisher key, signature, size and SHA-256, preserves
its installed path/settings and restarts its existing service. Completion needs
a new PID/session, running-image hash, manager and enrollment health. Publisher
trust is pinned once during authenticated service enrollment in the private state
root; rotation is a local provisioning operation, never an update instruction.
A foreground queue or worker without saved service settings/runtime proof cannot
participate; install the capable release normally before the first fleet update.

Fleet helpers also retain both running and installed identities in their
private receipt when they differ. A slot matching the frozen approved operation
or the current queue-approved release is verified against local publisher trust
and retained as the rollback identity; unknown bytes stop with
`fleet_slot_unapproved`. Recovery still needs an explicit operator rerun and all
existing operation/session, transient-helper absence, maintenance and custody
fences. The helper's installed receipt proves publication, never activation;
completion still requires a new healthy process with the approved image.

A storage preflight failure is reported as `storage_preflight_failed`, including
when recovered from the durable helper receipt; arbitrary helper error text is
never exposed. Failed acknowledgements also retain `failure_code` in each host
entry: the worker's exact allow-listed code, including after receipt replay and
queue restart. `failure` uses the same code except for the existing aliases
`fleet_storage_preflight_failed` → `storage_preflight_failed` and
`fleet_health_failed` → `service_health_failed`. Unknown strings are never
published, even when shaped like a code. Older stopped operations cannot recover
details already discarded by their old queue or worker.

## Failure codes and repairs

Fleet update failure codes and repair steps (apply repairs locally through the
authorized owner, then explicitly rerun `myce fleet upgrade`):

| Code | Next step |
| --- | --- |
| `fleet_slot_unapproved` | Compare running and installed slot identities in worker status and the private receipt. Verify and approve the installed release, or recover through the authorized installation owner; never bypass this refusal. |
| `fleet_instruction_stale` | Refresh queue status and current operation/session/revision and runtime proof; let exact custody settle. Before helper submission, refetch occurs at the next normal runtime poll. A pending receipt proves submission has not begun. A launched helper refetches its frozen instruction and retries only the refused control message once fresh proof/custody permits; publication and restart are not replayed. |
| `fleet_ack_stale` | The helper refetches before retrying the same control message. An obsolete operation/session or superseded helper stops locally without reporting a new terminal failure; its receipt retains the last durable phase. An ack after an operation stopped is expected to be stale. |
| `fleet_maintenance_changed`, `fleet_authorization_refused` | Reconcile maintenance ownership, exact custody, current runtime and queue authorization; never override the operator's drain. |
| `fleet_ack_failed` | Restore authenticated queue connectivity, then reconcile the durable receipt before rerunning. |
| `fleet_artifact_download_failed` | Restore authenticated artifact connectivity and queue/publisher availability. |
| `fleet_artifact_transfer_failed` | Check interrupted transfer and local disk write failures; repair connectivity or disk capacity/access. |
| `fleet_artifact_refused` | Check artifact availability, approved size/hash and signed release; repair publication or approve a corrected newer release. |
| `fleet_staging_exists` | Preserve the partial and receipt; prove the old helper absent. An explicit recovery revision removes only this operation's rebuildable partial. Never remove active staging. |
| `fleet_staging_write_failed`, `fleet_staging_cleanup_failed` | Repair binary-directory access, disk capacity and staging file type; preserve evidence and prove the old helper absent before cleanup. |
| `fleet_darwin_identity_refused` | Verify both installed and candidate signatures and exact designated requirements locally. The same candidate succeeding elsewhere does not prove compatibility with this installation. Use the matching Developer ID requirement; a legacy ad-hoc install needs a separately authorized identity migration. Never locally re-sign to bypass the refusal. |
| `fleet_candidate_invalid`, `fleet_version_invalid` | Verify the staged executable can run and reports the approved version, target, protocols and native sensing. Publish a corrected release when needed. |
| `fleet_version_refused` | Compare running, installed and approved versions/hashes; do not downgrade or replace a same-version binary with different bytes. |
| `fleet_signature_refused`, `fleet_publisher_mismatch`, `fleet_publisher_unavailable` | Verify signed release and locally provisioned publisher trust. Trust rotation requires separate local provisioning. |
| `fleet_instruction_invalid`, `fleet_target_invalid` | Check exact operation/host/service/session identity and approved OS/architecture target. |
| `fleet_config_invalid`, `fleet_service_invalid`, `fleet_install_invalid`, `fleet_state_invalid`, `fleet_local_file_invalid` | Repair saved service settings, file types/permissions and private state root through the installer owner; preserve identity and state. |
| `fleet_credentials_unavailable`, `fleet_queue_invalid` | Repair saved authentication and queue URL through the enrollment owner; never copy credentials into commands or diagnostics. |
| `fleet_binary_unavailable`, `fleet_existing_service_unavailable` | Restore readable installed bytes and the exact existing service-manager runtime before rerunning. |
| `fleet_helper_busy`, `fleet_helper_absence_unproven` | Inspect the existing helper/lock through its service manager; wait for owned work or prove absence. Never launch a duplicate or kill an unknown helper. |
| `fleet_helper_launch_failed` | Inspect the finite helper's launchd/systemd registration and saved executable/settings; repair the manager refusal. Older `helper_launch_failed` acks map here. |
| `fleet_helper_launch_unclassified` | Inspect the launch owner's local diagnostic and receipt; an unexpected launch error was sanitized. Upgrade the worker if its code is too old to classify it. |
| `fleet_receipt_invalid`, `fleet_receipt_conflict`, `fleet_receipt_write_failed`, `fleet_previous_failure` | Preserve and reconcile local receipt, operation identity and settings; repair storage permissions/capacity or the recorded earlier failure before explicit recovery. |
| `fleet_preflight_failed` | Repair saved install settings and policies using installer validation. |
| `fleet_storage_preflight_failed` (`failure: storage_preflight_failed`) | Repair the named state/NAS root or mount through the storage owner. Do not bypass storage checks. |
| `fleet_install_changed`, `fleet_recovery_runtime_changed` | Reconcile changed settings, installed bytes and manager PID/running image; never recover against a different runtime automatically. |
| `fleet_publish_failed` | Repair atomic replacement access/capacity in the installed binary directory, then reconcile the installed hash and receipt. |
| `fleet_restart_failed` | Inspect the existing service manager and restart intent; repair and explicitly recover without blindly repeating a restart. |
| `fleet_health_failed` (`failure: service_health_failed`), `runner_smoke_failed` | Inspect new runtime startup, manager health and runner smoke. Repair or fix forward with a corrected release; never automatically restore old bytes. |
| `fleet_failure_code_missing` | The worker sent no code. Ask the worker owner to inspect its receipt and update the reporter. |
| `fleet_failure_code_unrecognized` | The worker/receipt supplied an unknown diagnostic. Inspect it locally and add a reviewed sanitized classification; arbitrary text is withheld. |
| `fleet_failure_detail_unavailable` | An older reporter already collapsed the cause to `update_failed` or `fleet_update_failed`. Recover the original receipt locally if available; queue status cannot reconstruct it. |

## Stale authorization recovery

An initial authorization can become stale with an unchanged revision: a slow
instruction request may outlive the queue's runtime-proof freshness window or
custody may change before authorization. Cancellation/release alone does not
advance the fleet revision. Before manager submission, the next normal runtime
poll refetches and rechecks authority; a pending receipt can resume because no
helper has been submitted. Once submission begins, that revision cannot launch
again automatically, even after an ambiguous manager response.

A launched helper also refetches after stale authorization or acknowledgement.
An empty fenced response waits for fresh proof and exact custody; a matching
instruction permits retrying only that control request, with its original
operation, release, session and helper revision. A newer operator revision alone
does not replace an active helper. A durable replacement claim, changed session,
stopped operation or maintenance refusal ends recovery without authorizing any
mutation. Before restart, the helper rechecks the installed target hash, saved
configuration hash, locally provisioned publisher trust and original manager PID
after its last control wait. Obsolete control messages do not create failed
receipts or stop newer progress. Genuine validation/maintenance failures still retain their terminal
receipt and repair guidance. Durable restarting intent still prevents repeating
a manager restart after interruption. This recovery requires the updated queue
and helper binaries; it cannot repair an older helper already stopped by #739's
post-intent behavior.

## Failed hosts and stopped operations

A failed worker step on an operator-disabled host is retained as
per-host `phase: failed` with its failure code and repair guidance; the operation
continues to the remaining disabled hosts. `state: complete` means every host
was processed, so also inspect per-host failures; it does not claim a failed
disabled host was updated. No disabled host is enabled or resumed, even after a
successful update. An explicit identical rerun retries failed disabled workers
while preserving completed hosts' progress and the same operation ID.
Disabled workers with unavailable/unhealthy runtime proofs, missing inventory,
incompatible targets or refused downgrades also retain named per-host failures;
they cannot refuse the initial rollout for enabled hosts. No failed check permits
an update. Identical retries preserve the original maintenance receipt and
interrupted runtime phase: an operator policy change remains a conflict, and a
changed process with the wrong bytes remains a visible failure rather than a
wait on its obsolete session.

Enabled/drained-host failures, queue-service cutover failures (even on a disabled
host), and replaced maintenance receipts still stop the operation and preserve
maintenance; later hosts remain untouched. Queue cutover retains its production
admission fence until healthy queue bytes are verified. Repair the cause and
explicitly rerun `myce fleet upgrade` to reconcile
that same operation. A rerun advances its recovery revision and rechecks the exact
old runtime and maintenance receipt before recovering an interrupted helper.
An active helper continues to report its result across reruns. Services already
running the approved bytes receive the same stable health checks without a restart.
It never blindly repeats a restart or resumes an operator's drain. For a stopped
operation, `--release` may explicitly select a newly approved newer release to
fix forward. Approval changes alone never retarget a running operation.

If an operator has replaced a stopped operation's drain, an identical rerun
keeps the stale receipt as a visible `maintenance_changed` conflict. Explicitly
selecting an approved newer release uses the current drained/disabled policy,
including its budgets and placement, without acquiring permission to resume it.
Exact release custody and stable health are still required; already-current
services are verified without restarting. If admission was resumed instead,
the newer operation is refused until the operator explicitly drains or disables
the host. No receipt or database edits are needed.
