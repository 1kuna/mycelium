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
| `production_cooldown` | false | idle interval end | Idle production reservation; calculated with the queue's injected clock. New demand can extend it. |
| `docker_transition` | false | omitted | Ordinary Docker startup/stop operation or final release report in progress. |
| `docker_cleanup_waiting` | false | omitted | Docker custody or a cleanup error awaits confirmation, before escalation. |
| `docker_cleanup_operator_action` | true | omitted | Docker custody reached the cleanup escalation bound (`myce resolve-docker`). |
| `startup_release` | conditional | omitted | Failed startup awaits exact execution release; true when any corresponding worker is gone/missing. |
| `host_disabled` | false | omitted | `DisabledHosts[host]` is set, or every known worker on the host is disabled. Does not clear by itself; not a fault. Partial unit disables are omitted from hold details; check each machine's disabled state. |
| `unclassified` | true | omitted | A production host blocker has no recognized explicit classification. Never treated as self-clearing. |

Only `production_cooldown` supplies `clears_at`; refresh before acting. A false
operator flag means no fault currently demands operator attention, not that
release is proven or that recovery has a deadline. In particular, deliberate
disabling persists until re-enabled. Cleanup acknowledgements may clear an
escalated hold without an operator override; custody remains fenced until proven.

Docker summaries choose deterministically by severity: escalation, reported
cleanup error, waiting release, then ordinary transition. Equal severity uses
lexical text order. A transition cannot hide waiting custody. Exact current
executions that are healthy and starting or running do not accrue cleanup time;
becoming live again clears stale timers. Stopped attempts escalate two minutes
**after stop grace expires**. Lost/unconfirmed custody without a stop deadline
starts its two-minute window when first observed. All decisions use the queue's
injected clock.

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

The CLI and page label host ceiling budgets **last reported**, including stopped
workers. Both `host_memory_budget_gb` and `effective_host_memory_budget_gb` are
worker-reported queue state, not a live read of an offline host's budget file.
Check the host's setting locally with `myce host-memory-budget show` when a
current value is required; the queue cannot observe file changes while all of
that host's workers are stopped.
