# Fleet upgrade internals and failure codes

Details behind the [rolling fleet upgrade procedure](../operations.md#rolling-fleet-upgrades):
how hosts are chosen, how a service verifies and installs a release, how health
is checked, and what each failure code means.

## Host order

The operation is part of the queue state; it survives the queue's own restart.
One host is active at a time. The next one is picked from the pending hosts:
hosts with no work, calls or cleanup now first, then busy hosts, then hosts
running production, and the queue's own host only once every other host is
done. Ties go by host name.

A normal update skips over a host, leaving it enabled, while draining it would
strand open production work: a production lease or a queued production job,
including a stage waiting on `after:`, with no other enabled host able to start
it. It uses the same predicate as `myce host drain` (`production_work_open`).
A critical update does not wait for that work (MY-39).

Each host goes through `draining`, then `worker_update`, `worker_health` and
`worker_start` for
its worker, then `queue_update` and `queue_health` for the queue on the queue's
host, then `complete`. `skipped` marks an out-of-service host without a usable
runtime proof. The queue issues the update instruction only while the host is
drained and quiet and the service's runtime proof is fresh; the service's
`restarting` report moves the phase to `*_health`.

## How a service installs a release

Each installed worker and queue reports a runtime proof: its version, the
SHA-256 of its running image, PID, session, service-manager health and runner
smoke. When the queue says an instruction is waiting (in the check-in reply),
the service fetches it and, in its own process:

1. checks the instruction against its locally pinned publisher key (provisioned
   once at enrollment; never rotated by an instruction) and the signed release;
2. asks the queue to authorize this operation for its exact session, and refuses
   a downgrade or a same-version release with other bytes;
3. checks the installed binary is the running one, or already the release (an
   earlier updater stopped before its restart); anything else is
   `fleet_install_changed`;
4. writes a private receipt and starts the same installed binary as a finite
   transient job (`systemd-run --user` or `launchctl submit`), outside the
   service's process group so the restart cannot kill it.

Authorization reads the operation, revision, maintenance, session and fresh
runtime proof in one queue read section; it does not run a write transaction
or persist accounting. Instruction discovery's deadline is not shared with
launch authorization. Before a receipt exists, a typed `queue_stalled` yields
to the next ordinary runtime poll, which refetches the instruction and rechecks
all fences. It sends no failed acknowledgement and launches no helper on that
attempt. Recovery uses the existing 15-minute service-step deadline below;
authorization attempts never renew it. There is no new retry count or delay.
Definitive refusals and invalid returned proofs keep their existing behavior;
stale-control responses still use the existing short freshness retry/refetch.
Once helper intent exists, a failure remains durable and never relaunches or
repeats a restart automatically.

The transient updater, holding the local update lock:

1. runs the installer's own preflight and storage checks and checks the existing
   service is active;
2. asks the queue again, then downloads the release through the queue's
   authenticated endpoint (no credential reaches the publisher), checking its
   size and SHA-256; on macOS checks the code-signing identity matches the
   installed one; runs the candidate's `version --json` and checks its version,
   target and protocols; rechecks publisher trust;
3. asks the queue once more, checks the installed bytes and settings did not
   change, and atomically replaces the installed binary;
4. checks the service still runs the original PID, records `restarting`, and
   restarts the existing service once;
5. waits up to one minute for the service manager to report a new healthy
   process.

It never restores old bytes, repeats a restart or resumes maintenance. A
failure is saved in the receipt and reported; a restarted service replays a
report the queue never received.

The **installed** binary runs these storage checks before fetching the candidate.
Publishing a newer candidate alone cannot change the helper doing this preflight.
The checks cover local state/cache format roots, cached mount inventory, NAS root
containment and real-directory metadata, and checkpoint/model/environment
write, sync, read-back and rename probes. Mount checks probe exact configured
paths; state-format checks inspect local state roots.

A storage failure records `storage_failure` in the private receipt. When the
queue advertises `storage_diagnostics` in its instruction, the live/replayed
acknowledgement and host entry in `myce status --json` carry it too; older queues
reject unknown acknowledgement fields. It contains
only a root role (`state`, a named cache/queue root, `nas`, `checkpoint`, `model`
or `environment`), fixed `operation`, numeric `errno`, `elapsed_ns`, and
`timed_out` when a filesystem call stalls. An unfinished syscall has no errno:
zero means no OS errno was returned, including logical refusals. The existing
stall bound and single outstanding sequence remain unchanged. Older installed
helpers report only the failure code; absent details cannot identify the cause.

### First upgrade of a worker with a missing NAS

A helper that supports NAS-optional updates records `storage_warning` with
`root: nas` and `operation: inventory` if the unchanged, previously selected
worker NAS is absent from mount inventory. The worker's configured checkpoint,
model and environment source roots must be inside that NAS and its state and
caches must be outside it. The remaining local state, signature and service
checks still have to pass. The queue's existing protocol and installed-slot
proof rules still apply to a preflight refusal.

An older installed helper may refuse before fetching the new release. For
that first update, keep the worker drained and use the publisher-verified
candidate with the [in-place worker installer](../operations.md#recover-a-worker-missing-fleet-runtime-proof).
Use the saved config with the same storage paths. After the replacement
reports a fresh session and the expected signed hash, the drain owner can
resume it and confirm `degraded` status with the missing root roles. The
ordinary worker start probe and compatible jobs can run in that state.

## Health and stopping

A runtime proof is fresh against its own process's measured proof cadence
(#1450). A worker sends the proof its runtime last collected, about one
heartbeat old, and the queue keeps it until the next check-in, so a healthy
proof reaches about two heartbeat cycles of age; host load stretches those
cycles. The queue learns, per process, how old each proof got before the next
one replaced it, and accepts a proof younger than that mean plus four
deviations (the estimator worker loss uses). It is never stricter than 5
seconds, a new process starts at 5 seconds, and no proof counts after 30
seconds (the measured filesystem stall backstop). A refusal names the proof's
age and the bound it was judged against.

A service is verified when the queue sees a fresh runtime proof from a new
process (new session and PID) with the release's version and exact bytes, a
healthy service manager and runner smoke, stable for 30 seconds (the enrollment
stability window, which catches a crash loop). The window passes only on a
proof the process collected at least 30 seconds after its first, so a process
that stops proving never passes. One late proof keeps the window; a missing or
unhealthy proof restarts it. A new process with other bytes
stops the operation at once. If no verified process appears within 15 minutes of
the update starting (the updater's 10-minute download bound, 3-minute manager
command bound, 1-minute readiness wait and the stability window, rounded up),
the update died or hung, and the operation stops.

A stopped operation keeps every drain it holds. A rerun is a new operation; it
adopts drains left by the stopped operation (and by operations from releases
before this path), verifies those hosts and then resumes them.

Worker health is followed by `worker_start`: a queue-owned CPU smoke must start,
succeed and release, or a newly assigned useful job must reach Started on the
verified replacement process/session/image. The queue snapshots existing attempt
identities when this check begins; retained attempts cannot pass it by replaying
Started. The snapshot and pending probe survive queue restart. A capacity-deferred
probe keeps its runtime identity and maintenance policy while the queue waits for
the worker to report again. The queue may verify other hosts during this wait.
Revisiting the deferred host and completing the rollout require fresh proof from
the same process, judged against its proof cadence. The existing worker-start
step deadline still applies while proof is unavailable. Ordinary admission
resumes during this stage; no resource/policy bypass or smoke-induced stop is
allowed. A failed check stops later hosts, cancels only its smoke and restores an
operation-owned drain when current policy and open production work allow it.
The stage reuses the 15-minute service-step budget. A same-release rerun retries
unaccepted current bytes before other pending workers. Successful status names
the proving assignment in `wait`. This is queue-side behavior.

## Local recovery when a worker sends no runtime proof

The [ordinary installer](../operations.md#recover-a-worker-missing-fleet-runtime-proof)
can recover an active, enrolled worker that still sends fresh check-ins. Fleet
upgrade authorization still requires its normal runtime proof. The local
installer uses these checks in preview and again just before it stops the
selected service:

- The queue's drain or disable and fresh quiescence account for attempts,
  releases, preparations, engines and calls. Locally retained attempts and
  detached owners can precede a check-in, so their spool records also block.
- The local enrollment receipt matches the queue's worker, session and
  reported release. The service manager's healthy process must match the
  receipt PID, and its process birth and manager identity must agree across
  the installer checks. This protects against stopping a replacement process.
- The installed file must match the service's kernel executable mapping and
  the installer's rollback snapshot. This detects an unrelated or recently
  swapped slot without executing it. If Linux credential protection hides
  the mapping, the saved slot must match a signature under the pinned publisher
  key, the queue-reported installed version, and the platform artifact's exact
  hash and size. The installer checks a retained managed update receipt or
  current queue approval, or accepts an explicit `--installed-release-manifest`
  for a prior release. The private `slot-reconciliation.json` records which
  evidence qualified the saved rollback slot.

The installer keeps the existing queue maintenance policy and serializes with
the local fleet helper. Keep the drain or disable in force through activation.
The queue's catalog-transition fence can supply fresh identity evidence from a
drained worker on the previous catalog. A queue that rejects those check-ins
cannot supply that evidence.

## Failure codes and repairs

Apply repairs locally through the authorized owner, then rerun
`myce fleet upgrade`. Queue-side failures:

| Failure | Next step |
| --- | --- |
| `worker_running_hash_mismatch`, `queue_running_hash_mismatch` | A new process runs other bytes. Compare the running and installed hashes in status; reinstall the release or approve a fixed one. |
| `worker_update_not_verified`, `queue_update_not_verified` | No healthy new process in 15 minutes. Check the service is online, its updater's receipt (`fleet-update.json` in the state directory) and the service manager. |
| `worker_start_not_verified` | The upgraded worker did not prove its CPU probe started, succeeded and released within 15 minutes after health passed. Inspect the job ID in `wait` and the `fleet-start-probe` job history, start/custody records and admission holds. Keep existing jobs and checkpoints; fix forward, then rerun. |
| `worker_start_proof_unavailable` | A deferred worker's healthy runtime proof did not return within the worker-start step budget. Check the missing or stale proof detail in `wait` and restore the worker's check-ins before rerunning. |
| `worker_start_probe_failed` | The probe failed, was cancelled, retried/lost or executed on another session. Inspect its exact attempt's finding and retained release obligations; rerun only after repair. |
| `worker_start_runtime_changed` | Runtime identity or health contradicted the start check. For a deferred host, `wait` names the failed predicate. Restore healthy runtime evidence and rerun. An old probe never accepts a replacement process. |
| `worker_start_maintenance_blocked` | An operator-owned drain or disable prevents ordinary probe admission. Keep that policy; its owner must resolve it before rerunning. |
| `maintenance_changed` | Someone changed the host's maintenance during the update; their policy is kept. Rerun when ready. |
| `drain_conflict`, `resume_conflict`, `maintenance_unavailable` | The host's maintenance policy changed between read and write. Check `myce host status HOST`, then rerun. |
| `worker_unmanaged`, `worker_target_incompatible`, `worker_downgrade_refused` | The worker has no fresh runtime proof, no artifact for its platform, or is newer than the release. |
| `service_health_failed`, `storage_preflight_failed` | Aliases of `fleet_health_failed` and `fleet_storage_preflight_failed` below. |

Local installer and service updater failures. Updater reports expose a fixed
`failure_code`.

| Code | Next step |
| --- | --- |
| `fleet_authorization_refused` | The authorization call failed, or its returned proof failed readiness/session checks. This code alone does not establish a queue refusal. Inspect the sanitized authorization diagnostics below. |
| `fleet_instruction_stale`, `fleet_ack_stale`, `fleet_maintenance_changed` | The queue no longer authorizes this operation or session. Check the operation's state and the host's maintenance. |
| `fleet_ack_failed` | Restore authenticated queue connectivity. |
| `fleet_artifact_download_failed`, `fleet_artifact_transfer_failed` | Restore connectivity to the queue and its publisher; check local disk space. |
| `fleet_artifact_refused` | The bytes did not match the signed size or hash, or the publisher refused. Repair publication or approve a corrected release. |
| `fleet_staging_write_failed`, `fleet_publish_failed` | Repair access or space in the installed binary's directory. |
| `fleet_disk_preflight_failed` | Restore disk space or quota for staging and publishing. The helper received `ENOSPC` or `EDQUOT` before replacing the installed binary. |
| `fleet_signature_refused`, `fleet_publisher_mismatch`, `fleet_publisher_unavailable` | Verify the signed release and the locally provisioned publisher key; rotating trust is local provisioning. |
| `fleet_darwin_identity_refused` | The candidate's signing identity differs from the installed one. Never re-sign locally to bypass it. |
| `fleet_candidate_invalid` | The candidate does not run or reports another version, target or protocol. Publish a corrected release. |
| `fleet_version_refused` | Downgrade or same-version different bytes; compare running, installed and approved versions. |
| `fleet_install_changed` | The installed bytes or settings are neither the running nor the release's. Reinstall through the installer owner. |
| `fleet_binary_unavailable`, `fleet_existing_service_unavailable` | Restore the installed binary and verify the running service. For an enrolled worker sending no runtime proof, follow the local recovery procedure above. |
| `fleet_recovery_not_quiescent` | Keep the host drained or disabled. Wait for queue and local custody to release, and confirm fresh worker check-ins. |
| `fleet_recovery_worker_unproven` | Verify the selected worker, its fresh enrollment session and the service-manager PID. |
| `fleet_recovery_slot_mismatch` | Verify the installed slot matches the active service's kernel executable mapping. Inspect an interrupted publication or unusable process observation before retrying. |
| `fleet_recovery_manifest_required` | Linux hides the executable mapping and the signed installed slot could not be verified. Supply the installed release's signed manifest with `--installed-release-manifest` and check the slot's version, hash and size. |
| `fleet_recovery_runtime_changed` | Check the selected service, session and host drain again. The operation's original evidence changed. |
| `fleet_config_invalid`, `fleet_service_invalid`, `fleet_install_invalid`, `fleet_state_invalid`, `fleet_local_file_invalid`, `fleet_preflight_failed` | Repair saved settings, file permissions or the private state directory through the installer. |
| `fleet_storage_preflight_failed` | Repair the named state or NAS root or mount through the storage owner. |
| `fleet_credentials_unavailable`, `fleet_queue_invalid` | Repair saved credentials or the queue address through the enrollment owner. |
| `fleet_instruction_invalid`, `fleet_target_invalid` | The instruction names another host, service or platform. |
| `fleet_helper_busy`, `fleet_helper_launch_failed`, `fleet_helper_launch_unclassified` | Inspect the transient updater's launchd or systemd registration and the local lock. |
| `fleet_receipt_write_failed` | Repair the state directory's space or permissions. |
| `fleet_restart_failed`, `fleet_health_failed` | The restart or the new process failed. Fix forward with a corrected release; old bytes are never restored automatically. |
| `fleet_failure_code_missing`, `fleet_failure_code_unrecognized` | The service sent no code, or one this queue does not know (for example from an older updater). Inspect its receipt locally. |

### Authorization diagnostics

Service logs record `event=fleet_authorization`. A worker's `cause` retains a
typed `HTTP STATUS code=CODE`, a classified transport/decode failure (for example
`timeout` or `response_decode_failed`), or the failed readiness/session predicate:
`proof_pid_missing`, `proof_session_missing`, `proof_sha256_invalid`,
`proof_service_unhealthy`, `proof_runner_smoke_failed`,
`proof_start_gate_mismatch`, `proof_attempt_protocol_mismatch`, or
`proof_session_mismatch`. The public failure code stays
`fleet_authorization_refused` for terminal failures; a pre-intent `queue_stalled`
is pending as described above. Stale-control codes and their retry policy stay
unchanged. Received values, server messages, credentials and raw bodies are not
logged.

HTTP codes use a fixed allowlist for this endpoint; an unknown code becomes
`request_failed` while retaining the numeric HTTP status.

A supporting queue independently logs `side=queue cause="ready"` or its
classified refusal, plus `caller=active`, `cancelled` or `timeout`. This is useful
when an older worker reports only the generic failure. A queue's ready decision
does not prove that the caller received its answer. Correlate the worker and
queue records at the failing call; a later request or a healthy status snapshot
cannot establish a historical failure's cause. Existing binaries do not acquire
these diagnostics when a candidate release is published.

The authenticated `/api/ping` response bypasses memory-capacity admission: its
fixed scalar liveness data must not wait for the OS memory probe that admits
growing requests. A completed control response, including an HTTP refusal, wins
a simultaneous client progress-probe failure. These queue and client changes
address separate boundaries: a queue upgrade removes avoidable ping and
authorization delays for existing workers, but cannot repair their installed
client's classification or recovery. The worker changes take effect only after
that worker upgrades. Publishing the release alone does not unblock a stopped
operation, and a queue-only update cannot guarantee an older worker's next
authorization succeeds. Leave live acceptance to the rollout owner.
