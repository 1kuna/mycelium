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
    selectors: ["verso/production"]
  - name: shared
    selectors: ["atlas/*", "verso/experiment"]
  - name: other
    selectors: ["*/*"]
projects:
  verso: {weight: 1}
  atlas: {weight: 1}
```

The optional `projects` mapping uses the existing project settings, including
weights, allowed machines and experiment caps. Omitted weights default to one.
Omitting `projects` preserves existing settings; supplying it replaces the map.
There is no second project priority store. Existing project configuration remains
available and advances the same revision when it changes settings. Shared-tier fairness for arbitrary classes is a later slice; existing
experiment fairness and caps continue to apply.

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

This slice changes rank only. Existing resumability, stop-grace, retry, host
hold and pin rules still apply. Generalized within-tier fairness, expanded
preemption/retry causes and custom model-call classes are subsequent slices.
Equal-tier jobs can use separate available workers; they are not stopped to
manufacture fairness. Strict higher tiers can starve lower ones.
