# Seeing your mycelium

Run `myce status` using your usual queue address and private token. `--full`
shows full job IDs. Open the queue address in a browser for the same information;
sign in as `myce`, using the token as the password. The page is read-only and
refreshes every five seconds. Keep tokens in private configuration, not URLs.

Each hypha shows its capability profile: CPU count, system memory, accelerator
memory, shared or separate memory, and local storage. Missing measurements say
unknown. Extension reports say which kind of work or program is qualified on
that hypha; having hardware alone does not make an extension qualified.

Availability explains owner use, battery power, heat, missing readings,
disabling, leaving, and loss of contact. An available hypha can still be busy.
Running jobs show their hypha and attempt. Every waiting job has an explanation,
including missing qualification, memory or resource requirements, unavailable
hyphae, software preparation, and work ahead in the queue.

The page and CLI use the queue's decisions. Reading status does not choose where
to run work. The status API returns job summaries without submitted commands,
environment values, program arguments, extension files, or local checkpoint
paths. Use the existing job output and log commands when you need their results.

For maintenance gates, every nonterminal job in redacted `GET /api/status`
retains `spec.allowed_machines` (machine names) and reports:

- `eligible_hosts`: sorted, unique host IDs of currently known machines allowed
  by both the job and project allow-lists. For running and stopping jobs, including
  in-flight model calls, this also includes the current machine's host and
  `production_host` when set, even if policy no longer allows them. A restricted
  job with no known eligible or occupied host reports an empty array.
- `eligible_unknown_machines`: sorted, unique machine names in the effective
  allow-list that are not currently known machines. The effective list is the intersection
  when both job and project lists are nonempty, or whichever list is nonempty.
  This field is omitted when there are no unknown names. A joining machine
  resolves its name into a host ID on subsequent status reads.
- `eligible_unrestricted: true`: neither allow-list restricts the job. Treat it
  as able to run anywhere, including future hosts. `eligible_hosts` is omitted
  unless the job currently occupies a host; listed hosts never limit this marker.

These fields cover queued, infra-requeued, running and stopping jobs. They
express placement policy and current occupancy, not dispatch readiness: busy,
disabled, offline or temporarily unavailable machines remain policy candidates.
Workers normally default `HostID` to the OS hostname. The queue uses a machine-ID
fallback only when a check-in has no host ID. Gates must match the exact `host_id`
strings workers report, rather than assuming host IDs equal machine names.

Refresh status before each decision. Count a job as the protected host's work if
any of these apply:

- `eligible_hosts` contains that host ID;
- `eligible_unrestricted` is true;
- `eligible_unknown_machines` is nonempty, unless the gate can map all those names
  to other hosts;
- `spec.allowed_machines` names a worker on that host.

Missing/invalid eligibility fields and unreadable status must fail closed. Also
check current assignments and release/hold state before maintenance: policy
eligibility alone proves no release of existing work. Loaded model servers
appear under `loaded_models`, not jobs, and must be checked separately. Terminal
jobs omit all three derived eligibility fields.

Host gates should read `holds_detail`, an array (empty when there are no holds)
whose entries have `host`, `kind`, `operator_action_required`, `text` and an
optional `clears_at` timestamp. Entries are sorted by host and kind; a host can
have several holds at once. Do not match English text to choose an action.

| `kind` | `operator_action_required` | `clears_at` | When emitted |
| --- | --- | --- | --- |
| `production_hold` | false | omitted | Active production demand, including work between stages. |
| `production_stopping` | false | omitted | Lower-priority work is within its stop grace. |
| `production_release_unknown` | conditional | omitted | Production reservation has an unconfirmed release. True only for a gone/missing worker, failed cleanup, or release still unknown at least two minutes after grace. Restart check-in grace suppresses the time-based flag. Draining calls, recovery and the short wait for a kill result are false. |
| `docker_transition` | false | omitted | Recognized Docker startup/stop operation or final release report in progress. |
| `docker_cleanup_waiting` | false | omitted | Retained Docker custody or a `cleanup unconfirmed:`-prefixed status awaits confirmation, before escalation. |
| `docker_cleanup_operator_action` | true | omitted | Docker custody reached the cleanup escalation bound (`myce resolve-docker`). |
| `startup_release` | conditional | omitted | Failed startup awaits exact execution release; true when any corresponding worker is gone/missing. |
| `host_disabled` | false | omitted | `DisabledHosts[host]` is set, or every known worker on the host is disabled. Does not clear by itself; not a fault. Partial unit disables are omitted from hold details; check each machine's disabled state. |
| `unclassified` | true | omitted | A production host blocker has no recognized explicit classification, or a worker reports an unrecognized Docker status (for example, `pull pending`). Never treated as self-clearing. |

There is no idle production cooldown. `clears_at` is optional; refresh before acting. A false
operator flag means no fault currently demands operator attention, not that
release is proven or that recovery has a deadline. In particular, deliberate
CLOSED admission persists until revision-bound open. Cleanup acknowledgements may clear an
escalated hold without an operator override; custody remains fenced until proven.

Docker summaries choose deterministically by severity: escalation, unclassified
status, reported cleanup error, waiting release, then recognized transition.
Equal severity uses lexical text order. A transition cannot hide waiting custody.
Exact current executions that are healthy and starting or running do not accrue cleanup time;
becoming live again clears stale timers. Stopped attempts escalate two minutes
**after stop grace expires**. Lost/unconfirmed custody without a stop deadline
starts its two-minute window when first observed. All decisions use the queue's
injected clock. Once escalated, a later stop request preserves operator attention
and does not emit a second escalation event.

Known limits: a recovering model slot without `StopAt` has no time-based
escalation deadline while its worker remains present. A deliberate disabled-host
hold stays non-fault (`operator_action_required: false`) even if its disabled
worker later dies. Check worker liveness and model release evidence separately;
these holds alone do not prove release or eventual recovery.

For run monitoring, distinguish routine reservations and transitions from
operator holds using these fields, including the target host. For maintenance,
a hold's automatic recovery does not make the host safe: wait for relevant holds
to clear and verify assignments, loaded models and attempt release evidence.
Treat unreadable status, missing/invalid details, or unknown kinds as unresolved.
An empty hold list alone does not prove a specific attempt has released custody.

The compatibility field `host_holds` remains a list of the existing strings and
is **omitted when empty**. `text` equals the corresponding existing string; the
additional startup-release and disabled-host entries use the queue's existing
fence messages and appear only in `holds_detail`.

The CLI and page label the effective host allowance **last reported**, including
stopped workers, with `memory_allowance_bytes` and UNQUALIFIED qualification.
The queue derives it from the host's worker reports and fences admission when
their policy values disagree. It is not a live read of an offline host's intent.
Check locally with `myce host-memory-budget show --host-state-dir DIR` for the
current value. Changes require explicit CLOSED host reconfiguration; open/close
preserve the allowance. Native measurement and qualification remain separate.

## Host-control envelope

`myce host close|open|status --local --json` emits the same schema-v1 envelope,
including on error. `reconcile --local` performs convergence; use read-only status
for this public envelope and rest verdict. See [operations](operations.md#close-reconcile-and-reopen-a-host)
for invocation and the [schema](schemas/host-envelope-v1.schema.json) for exact types.

| Fields | Interpretation |
| --- | --- |
| `schema_version`, `command` | Version 1; close, open or status. |
| `host_id`, `operation_id`, `boot_id` | Verified host, desired operation and current boot; nullable if unknown. |
| `accepted_revision` | This command's durably accepted revision; null for rejection and read-only status. |
| `desired`, `observed` | State/revision pairs. Desired is durable intent; observed applies to this boot. Unknown revisions stay null. |
| `last_close`, `host_settlement` | Last close's ID/revision, settlement and UTC settlement time. Open preserves it. Settlement equals the close history, or unknown when null. |
| `queue_observed_revision` | Queue acknowledgement, separate from previous-close settlement and current readiness. |
| `local_exclusion`, `restart_safe` | PASS/FAIL/UNKNOWN physical exclusion; nullable restart-policy evidence. |
| `counts` | Nullable active, preparing, stopping, unsettled, pending-daemon, running-container, GPU-holder, queued, fenced-queued and unresolved-placement counts. Null never means zero. |
| `units`, `attempts` | Exact unit/cgroup and execution/release evidence. Null means incomplete; empty arrays mean complete empty inventory. |
| `observed_at`, `source_observed_at` | UTC completion and per-source timestamps. Freshness is checked against the current boot/revision. |
| `errors` | Sanitized component/code/message objects; stable codes, not English parsing. |

Status never initializes or repairs state. `state_uninitialized`/`state_corrupt`
keep unverified fields null. A stale compare-and-set returns exit 3,
`revision_mismatch`, unchanged desired state and no accepted revision. An accepted
operation overtaken by another retains its accepted revision with
`revision_superseded` and the newer desired revision. Without a release wait
its acceptance exits 0; with a release wait it fails nonzero, never exit 3.
Open requires a selector matching the current confirmed close; otherwise it
rejects with mismatch or `previous_close_unsettled` without allocation.

Without wait/verdict, exit zero means acceptance or successful snapshot only.
`close --wait-released` needs local exclusion PASS, even if queue settlement is
pending. `status --expect ready --wait` needs ready PASS. With
`--expect closed|ready --verdict`, zero exit means the requested **rest PASS** only.

### Rest verdicts

A verdict adds `expectation`, `verdict`, unique stable `reasons`, nullable
`queue_reachable` and nullable `memory_allowance_bytes` together. The schema checks
shape, not physical truth. Use the product verdict rather than reconstructing it
from holds or external receipts.

| Verdict | Closed | Ready |
| --- | --- | --- |
| PASS | Current CLOSED fence, complete local exclusion, fresh queue acknowledgement/confirmed settlement and no relevant active or unresolved release/placement. | Current-boot open revision applied and queue-acknowledged, all installed required workers fresh/healthy at that revision, no unresolved release. |
| FAIL | Fresh proof of open/stale admission, owned live work/GPU holders, unsafe restart or unfenced eligible work. | Fresh proof of wrong required revision or required-worker health violation. |
| UNKNOWN | No proven violation, but required evidence missing, stale or ambiguous. Local PASS with queue pending is not rest PASS. | Required boot/revision/worker/queue/release evidence is missing, stale or ambiguous. |

FAIL takes precedence over UNKNOWN; retain both causes. Fenced queued jobs are
counted and allowed. Positively foreign-only work is outside the host; unknown
placement stays unresolved. Ready does not require empty cgroups and does not
certify a model or application. Consumer cancellation completion stays separate.

Evaluate after collection and the final revision fence at one instant. Age equal
to its freshness bound is fresh; just past it, missing time or a future timestamp
makes evidence absent. Default collection/queue freshness is 10 seconds.
Collection or fence error gives UNKNOWN with `collection_failed`. Printed verdict
and exit status use that same result; a timeout is non-PASS, never release proof.
