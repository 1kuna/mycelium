# Owner priority policy

The queue stores one policy in SQLite. Tiers are ordered from highest to lowest;
a job matches the first selector in the first matching tier. Each selector is
`project/class`, with Go `path.Match` globs independently in both components.
Overlap is legal. Tier names must be unique and the final tier must include
`*/*`. Repeated selectors and selectors following a catchall produce warnings.

```yaml
version: 1
tiers:
  - name: urgent
    selectors: ["transcribe/production"]
  - name: shared
    selectors: ["sim/*", "transcribe/experiment"]
  - name: other
    selectors: ["*/*"]
projects:
  transcribe: {weight: 1}
  sim: {weight: 1, max_concurrent: 2}
```

The optional `projects` mapping uses the existing project settings, including
weights, allowed machines and experiment caps. Omitted weights default to one.
Omitting `projects` preserves existing settings; supplying it replaces the map.
An explicit `projects: {}` clears all project settings and is retained by exports.
There is no second project priority store. `queue --project-config` and
`dev --project-config` seed settings only when no policy has been stored. Once
stored, the live SQLite policy is authoritative: restarting or reinstalling with
the same or edited project config does not replace it or advance its revision.
To deliberately change settings, export the current policy, edit its `projects`
mapping, and import it with `myce policy set FILE --if-revision N` as below.

Within each tier, fitting projects take turns by weighted recent usage before
model/environment warmth. Usage has a 24-hour half-life and separate stable
`tier/resource-family/project` keys. GPU work uses normalized allocated device
seconds; CPU core accounting/admission is a later slice. Equal usage favors fewer
active allocations per weight, then the existing affinity/deadline/queue order.
Thus a warm project cannot monopolize newly available workers. Non-fitting jobs
do not block compatible backfill or consume a turn. Idle, unpinned same-tier model
residency can be reclaimed through the existing release planner; active calls
and loading attempts are not interrupted for same-tier fairness.

`weight` is positive and defaults to one. `max_concurrent` is an optional
nonnegative cap across all classes and tiers for that project (zero means no
cap). It counts running/stopping batch attempts and assigned model calls,
including preparation before Started. Existing `max_concurrent_experiments` and
`max_memory_share` retain their experiment-only scope. Both cap types apply
independently. Updates use the same policy revision CAS; caps remain hard limits
when a project is alone. Reducing a cap withholds new admissions and does not
stop existing work.

The accounting migration is versioned and durable. Old project totals lack
class metadata, so they are preserved once in the current catchall GPU tier.
New usage remains attached to stable tier names across reordering and restart;
removed tiers stay inactive without transferring their history to other names.
See [fairness and usage](guide-fairness-usage.md) for accounting details.

Use the queue owner credential (`MYCE_TOKEN`) and queue address (`MYCE_QUEUE`):

```sh
myce policy get
myce policy get --yaml > policy.yaml
myce policy set policy.yaml --if-revision 0
myce status
```

`get` returns JSON with `revision`, `policy` and optional `warnings`. `--yaml`
exports an importable policy and prints its revision to stderr. `set` validates
and atomically replaces the policy only if the revision matches, then returns
the new record. Read the current revision before editing; a conflict requires
reading and reconciling the newer policy. YAML is import/export, not live state.
Flags may precede the filename; the shown filename-before-flags form also works.

Authenticated HTTP clients use `GET /api/policy` and
`PUT /api/policy` with `{"expected_revision": 0, "policy": {...}}`. A stale
revision returns HTTP 409 with code `policy_revision_conflict`. Missing revision
and invalid policies return 400. This uses the existing queue owner/fleet token;
priority markers confer no authorization.

Jobs may declare `class: production`, `class: experiment` or any other marker.
The default class is `experiment`. Neither project nor class can contain `/`.
Legacy `priority` remains accepted and is normalized to class at admission; if
both are supplied they must agree. Legacy deadline validation and production
retry/start behavior remain unchanged. A custom high-tier class does not acquire
production reliability semantics.

Before any policy is configured, revision zero synthesizes these same tiers
through the same matcher: `*/production`, then `*/deadline`, then `*/*`.
The catchall tier is named `experiment` for compatibility and also accepts new
classes. Submitted classes and submission-key fingerprints remain unchanged by
policy updates. Existing jobs are ranked using the current policy at each
planning decision; changing policy does not immediately stop all lower work.

Status JSON includes `spec.class` and each job's `priority_policy` with `tier`,
zero-based `index`, `selector` and `revision`, plus the current policy record on
the snapshot. CLI and browser status show the tier, selector and revision.
Placement, preparation and existing reclamation use one tier comparator.

Placement, status and simulation use the same admission planner and comparator.
Higher tiers can reclaim batch attempts, native sidecar groups, model calls and
loads, idle engines (including pins), and environment builds. Each exact planned
stop records `priority_preemption`, demand/victim identity and policy revision.
Production victims keep their infrastructure retry budget; experiments keep
losses and checkpoint progress. A successful completion wins over preemption;
cancellation wins over both. Independent exit/OOM still fails under the shared
outcome classifier. A deadline authorizes stopping, never assumes cleanup.

Resumable victims get ten seconds by default, non-resumable commands stop
immediately. Deadline demand preserves non-resumable commands and keeps the
120-second grace for resumable commands. `queue --preemption-stop-grace`
configures the other priority-stop interval; the legacy
`--production-stop-grace` flag remains an alias. After two displacements, resumed
work gets at least that interval of runtime before another priority stop. Pins
are desired state and are restored when capacity permits. Model requests accept
`X-Myce-Class` (or legacy `X-Myce-Priority`); conflicts are rejected. Interrupted
OpenAI streams emit an SSE error with `type: interruption` and
`code: priority_preemption`; generic streams use the `X-Myce-Error-Code` trailer.
Partial output is never transparently replayed.

Equal-tier projects share separate available workers; multiple concurrent
batch attempts on one worker are a later slice. Same-tier work is not stopped
to rebalance usage: a single indefinite allocation can occupy a scarce device
indefinitely. Strict higher tiers can starve lower ones.
