# Wait and status

Detail for step 3 of [the guide](../SKILL.md). A field your installed component
doesn't return is unknown; see [capability evidence](release-behavior.md#capability-evidence).

## Waiting

Keep owned waits in the foreground: one `myce wait JOB_A JOB_B` covers at most
256 distinct IDs in the CLI. `--json` adds full diagnostics when available;
state/reason observation continues if optional detail reads are unavailable.
Batch waits carry revisions; queues below the current/previous release floor
must be upgraded, and clients do not fall back to per-job polling.

Python callers use `client.wait(JOB_ID)` or `client.wait_many([JOB_A, JOB_B])`;
a list returns terminal jobs in input order. Keep the installed pinned opener.
See [Python client](../../../clients/python/README.md).

`myce wait --timeout DURATION` and the Python timeout bound observation only;
timeout or interruption leaves the job alive. Without an owner or job deadline
there is no fixed observer lifetime. Follow current Retry-After and sanitized
retry causes. Pending work is not failure, and a notification is not accepted
output.

## Status reads

Use `myce status JOB_ID` and `myce logs JOB_ID` for a real blocker; keep full or
history status out of loops. Default fleet status is compact current work,
machines, holds and active alerts; absence is not proof of completion or loss.
Job reasons there are last committed facts, not a new placement plan.
Default `myce status --json`, `GET /api/status` and Python `client.status()`
return that compact view (a few KB): `counts`, `alert_groups`, `hyphas`,
running/waiting `jobs` with full IDs and reasons, and `paused` grouped by
project and reason with their `ids`. Full job objects need
`myce status JOB_ID --json`, `--sections jobs --json` or
`client.status(snapshot=True)`.

- `myce status --sections jobs,machines` adds detailed diagnostics;
  `--sections work,storage,health,efficiency,usage,diagnostics` selects retained
  collections. Python: `client.status(sections="work,storage")`.
- `myce status --jobs all --limit N --before CURSOR --json` pages history;
  Python `client.list()` walks the pages.
- `myce status --history`, `GET /api/status?detail=full` or
  `client.status(full=True)` restores every field for full inspection.

See [section coverage](../../../docs/status.md#reading-myce-status). Follow
Retry-After and typed recovery diagnostics without changing identity or trust to
evade them; permanent authority, TLS, integrity and configuration refusals go to
their owner.

A per-job GET completes from one published committed observation, cold and
parent reads included, without waiting on the live control mutex; treat it as a
point-in-time observation. Mutation acknowledgements and accepted-result checks
stay the authority for those actions. A cancelled read returns retryable 503
`status_unavailable`: back off and keep the same job and key. Status builds
derive worker-contact diagnostics and retained history outside the queue mutex;
slow-request mutex timing excludes response-cache build time, so older status/job
mutex figures that include that work need a direct queue-lock measurement for
attribution.

`myce efficiency` gives one read of reservation versus measured use, blocker
wait totals and wasted attempts; see
[queue efficiency](release-behavior.md#queue-efficiency-source-audited-verify-installed-support).

## Host status

`myce host status HOST` always prints its full JSON on stdout; parse that, not
the stderr line. Exit 0 means healthy. Exit 3 means degraded: every worker is
fresh and consistent, and the stderr line names each faulty device, root or
host-hygiene reason and what clears it (for a quarantined accelerator, `myce
device clear-quarantine` on that host after repair, or a reboot). A degraded
host is valid evidence for that clear. Exit 1 means missing, stale or
inconsistent worker evidence; do not act on device state from it.

## Recovery and trust

Newer clients recover interrupted transfers and queue restarts within the
caller's bound. Older clients or observers may need the installation owner to
verify unchanged trust before same-key replay. Queues with the
established-response shutdown repair close interrupted TLS sockets before another
encrypted record is sent; earlier queues can produce a real bad-record-MAC
refusal during shutdown even with unchanged trust. Keep integrity refusals
permanent and report them to the authority owner; never broaden the client TLS
classifier to recover a watch. Verify the running queue separately from CLI and
observer versions. See [transfer/recovery](../../../docs/files.md) and
[other contracts](release-behavior.md#other-release-dependent-contracts).

## Slow or stalled queue

Under load a control call (resolve, submit, preflight, status, cancel) can take
longer than 10 s and still be healthy, so the CLI and Python client set no
fixed request timeout. A call still waiting after one second reads
`GET /api/ping`, which takes no queue lock. The call keeps waiting while the
queue's lock releases advance or nobody waits on the lock. It fails with
`queue_stalled` (Python `QueueStalledError`, a `TimeoutError`) after four of the
queue's own p99 request periods with no release while callers wait (at least
4 s), or when ping gives no answer for that long.

- The queue may still finish the request. Read `myce status` (or the job) once,
  then repeat: resolve, status and other reads are safe, and submission is safe
  with the same key. Unkeyed writes are not repeated for you.
- An older queue whose ping lacks `queue_progress` gives no evidence. The call
  then waits for your own bound: `Client(timeout=SECONDS)`, a per-call
  `timeout=`, or interrupting the CLI.
- Resolve, file preflight and identical extension registration also retry
  429/503 after `Retry-After`.
- `submission unconfirmed` now names its cause (`queue_stalled`, overload or
  transport); rerun the same command with the same key.
- Held batch waits (`myce wait`, `wait_many`) are paced by the wait protocol and
  not watched. File transfers use their own byte-progress windows.

Needs a client and queue with #1016 (`queue_progress` on ping); see
[other contracts](release-behavior.md#other-release-dependent-contracts).

## Observers and detached completion

For detached completion, bind a notifier to your own thread with
[T3 binding](../t3-binding.md). Profile/adapter receipts use one managed observer
per user registry across worktrees; it survives submitter exit, while ordinary
foreground waits end with their parent. Never start `--observer-dir --detached`
yourself or create per-job detached waits. Inside managed attempts and dev
sessions, the observer stays in that session's cleanup custody.

`myce wait --observer-status` reads local build, phase, read, delivery, error and
lifetime-lock metadata without queue requests. A held lock proves ownership, not
advancement or delivery; a quiet long-poll can be healthy. Missing legacy
metadata is unknown, not a diagnosis. Shared owners run in their own session with
a clean allowlist environment that excludes runner/job tracking and `MYCE_TOKEN`
and keeps PATH/locale. The detached process supervises a separate observer
child: child death or silent coordinator/authority reads trigger automatic
adoption of durable arms. `--observer-status` includes `supervision` (restart
count, exit signal/code, pre-recovery diagnostic). A pending notifier payload
survives replacement with the same delivery ID, so receivers deduplicate it. No
compute is resubmitted. Recovery allowances come from protocol holds, notifier
deadlines and measured service cost.

Existing observers need a supported handoff or start to gain new behavior; a new
CLI binary does not replace a running observer. Native login/boot supervision and
recovery of the supervisor itself remain
[#1166](https://github.com/1kuna/mycelium/issues/1166). Legacy or unsupervised
shared-observer recovery belongs to its operator, who preserves all durable arms,
proves exact owner exit, then replays the original key and binding. Consumers
keep stable receipts and delivery IDs and never kill a shared observer or edit
its registry. See
[upgrade and recovery](../../../docs/reference/completion-observer.md#completion-observer-upgrade-and-recovery-next-release).

Local receipt `delivery_status: delivered` records notifier success; transport
acceptance, receiving-thread arrival and owner disposition are separate evidence
that a receipt or PID alone cannot show.

For a long detached job, set one stall check at about twice its expected
duration, bound to your thread, verifying one thing (state or log growth), and
delete it when done. If duration is unknown, record that and a next owner rather
than inventing an ETA; use the foreground wait while this session owns completion.

## Stage progress and honest ETA

Report current-stage work through `MYCE_PROGRESS_FILE`, without credentials or
log parsing. A two-stage command can publish at each batch and stage transition:

```python
import json
import os
from pathlib import Path

progress = Path(os.environ["MYCE_PROGRESS_FILE"])

def report(stage, completed, total, unit, *, total_work_known=False):
    temporary = progress.with_name(progress.name + ".tmp")
    temporary.write_text(json.dumps({
        "stage_index": stage, "stage_count": 2,
        "completed": completed, "total": total, "unit": unit,
        "total_work_known": total_work_known,
    }))
    os.replace(temporary, progress)

report(1, 10, 10, "records")       # Stage one finished; job is still running.
report(2, 0, 100, "iterations")    # Stage-two duration has no timing evidence.
```

Indices are one-based. Omit `stage_count` if later stages are unknown, and both
stage fields if stage identity is unknown. Set `total_work_known` true only when
the complete workload across all stages is known; counts stay stage-local even
then. Rewrite at least every 30 seconds to keep the evidence fresh.

`myce status JOB_ID` or `Client.get(JOB_ID)` shows `application_progress` (stage
index/count, work-known flag, exact attempt, age, `stale`) and, separately,
`runtime` (elapsed and remaining time, source, sample count, history age,
limitations). Missing timing stays unknown; finishing stage one never yields
zero whole-job remaining, and an exceeded estimate becomes unknown. See the
[report contract](../../../docs/status.md#execution-elapsed-time-remaining-estimates-and-application-progress).

Checkpoint `progress` is a durable generation number, not a percentage or ETA.
Neither stage completion nor a checkpoint means success or resource release:
keep the job ID, wait for success and verify results.

## Benchmark telemetry

`myce telemetry capture` runs a bounded local sampler with a declared run/host
reference and issues no queue requests; keep its versioned JSONL beside exact
attempt evidence. Replay permission and parity are separate. See
[benchmark telemetry](../../../docs/benchmark-telemetry.md).

## Project doctor

`myce doctor --project` (with `--entry`, `--adapter`, `--json`) is a read-only
check of authority, source, adapter, resolve, dependencies and completion
configuration that names a next owner and action. It submits, builds and uploads
nothing, and does not run a notifier or application result checks; use the
guide's journey and T3 binding for those.
