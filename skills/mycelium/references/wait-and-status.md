# Wait and status

Detail for "Long waits" in [the guide](../SKILL.md). A field your installed
queue or worker doesn't return is unknown, not false.

## Waiting

Keep owned waits in the foreground: one `myce wait JOB_A JOB_B` covers at most
256 distinct IDs in the CLI. `--json` adds full diagnostics when available;
state/reason observation continues if optional detail reads are unavailable.
Batch waits carry revisions, and clients never fall back to per-job polling.

Python callers use `client.wait(JOB_ID)` or `client.wait_many([JOB_A, JOB_B])`;
a list returns terminal jobs in input order. Keep the installed pinned opener.
See [Python client](https://github.com/1kuna/mycelium/blob/main/clients/python/README.md).

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
  collections. Python: `client.status(sections="work,storage")`. A machine's
  host RAM is `host_memory`: `capacity_gb`, `charged_gb`, `room_gb` (room =
  capacity − charged; negative is debt). `free_host_memory_gb`,
  `host_memory_in_use_gb`, `host_available_memory_gb`,
  `effective_host_memory_budget_gb` and `memory_allowance_bytes` are deprecated
  and leave status next release. Replaced worker identities appear only in
  `--history`.
- `myce status --jobs all --limit N --before CURSOR --json` pages history;
  Python `client.list()` walks the pages.
- `myce status --history`, `GET /api/status?detail=full` or
  `client.status(full=True)` restores every field for full inspection.

See [section coverage](https://github.com/1kuna/mycelium/blob/main/docs/status.md#reading-myce-status). Follow
Retry-After and typed recovery diagnostics without changing identity or trust to
evade them; permanent authority, TLS, integrity and configuration refusals go to
their owner.

Status reads and per-job GETs complete from the queue's last committed
publication, without waiting on the live control mutex; treat each as a
point-in-time observation. Their `freshness.captured_at` is when that state was
committed and `freshness.age_seconds` is its age when served (normally about a
second with an active fleet). Interpret contact and progress at that time;
re-read once rather than polling if it matters. Mutation acknowledgements and
accepted-result checks stay the authority for those actions. A cancelled read
returns retryable 503 `status_unavailable`: back off and keep the same job and
key.

`myce efficiency` gives one read of reservation versus measured use, blocker
wait totals and wasted attempts; missing samples are unknown, not zero.
`myce efficiency --project NAME` shows only your project's waits, the queue
hours your host lists and targets cost, typed sizing against learned peaks,
and one advice line per item. See
[queue efficiency](https://github.com/1kuna/mycelium/blob/main/docs/status.md).

## Host status

`myce host status HOST` always prints its full JSON on stdout; parse that, not
the stderr line. Exit 0 means healthy. Exit 3 means degraded: every worker is
fresh and consistent, and the stderr line names each faulty device, root or
host-hygiene reason and what clears it (for a quarantined accelerator, `myce
device clear-quarantine` on that host after repair, or a reboot). A degraded
host is valid evidence for that clear. Exit 1 means missing, stale or
inconsistent worker evidence; do not act on device state from it.

## Recovery and trust

Clients recover interrupted transfers and queue restarts within the caller's
bound. Keep integrity refusals permanent and report them to the authority owner;
never broaden the client TLS classifier to recover a watch. See
[transfer and recovery](https://github.com/1kuna/mycelium/blob/main/docs/files.md) and
[request budgets](https://github.com/1kuna/mycelium/blob/main/docs/request-budgets.md).

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
- To bound a call yourself, use `Client(timeout=SECONDS)`, a per-call
  `timeout=`, or interrupt the CLI.
- Resolve, file preflight and identical extension registration also retry
  429/503 after `Retry-After`.
- `submission unconfirmed` names its cause (`queue_stalled`, overload or
  transport); rerun the same command with the same key.
- Held batch waits (`myce wait`, `wait_many`) are paced by the wait protocol and
  not watched. File transfers use their own byte-progress windows.

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

A new CLI binary does not replace a running observer; it needs a supported
handoff or start. Native login/boot supervision and recovery of the supervisor
itself remain [#1166](https://github.com/1kuna/mycelium/issues/1166). Unsupervised
shared-observer recovery belongs to its operator, who preserves all durable arms,
proves exact owner exit, then replays the original key and binding. Consumers
keep stable receipts and delivery IDs and never kill a shared observer or edit
its registry. See
[upgrade and recovery](https://github.com/1kuna/mycelium/blob/main/docs/reference/completion-observer.md#completion-observer-upgrade-and-recovery-next-release).

Local receipt `delivery_status: delivered` records notifier success; transport
acceptance, receiving-thread arrival and owner disposition are separate evidence
that a receipt or PID alone cannot show.

For a long detached job, set one stall check at about twice its expected
duration (`runtime.last_accepted_seconds` when known), bound to your thread, verifying one thing (state or log growth), and
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
`runtime`: `elapsed_seconds` and `last_accepted_seconds`, the last accepted
duration of this kind of work, or a `reason` it is unknown. It is history, not
a remaining time or deadline; Mycelium no longer predicts completion (MY-49).
`remaining_seconds`, `samples`, `source` and `history_age_seconds` are gone, and
`timing.predicted_seconds` is a deprecated mirror for one release. See the
[report contract](https://github.com/1kuna/mycelium/blob/main/docs/status.md#execution-elapsed-time-last-accepted-duration-and-application-progress).

Checkpoint `progress` is a durable generation number, not a percentage or ETA.
Neither stage completion nor a checkpoint means success or resource release:
keep the job ID, wait for success and verify results.

## Benchmark telemetry

`myce telemetry capture` runs a bounded local sampler with a declared run/host
reference and issues no queue requests; keep its versioned JSONL beside exact
attempt evidence. Replay permission and parity are separate. See
[benchmark telemetry](https://github.com/1kuna/mycelium/blob/main/docs/benchmark-telemetry.md).

## Project doctor

`myce doctor --project` (with `--entry`, `--adapter`, `--json`) is a read-only
check of authority, source, adapter, resolve, dependencies and completion
configuration that names a next owner and action. It submits, builds and uploads
nothing, and does not run a notifier or application result checks; use the
guide's journey and T3 binding for those.
