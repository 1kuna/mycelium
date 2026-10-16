# Proposal: native Atlas server entries (#68)

Design only; [the approved design](design-proposal.md) remains the product spec.
Reuse `Model`, `engine: server`, `start_command`, environment recipes, health
checks, pins, memory admission and idle unloading for the chat LLM, GLM-OCR and
Chandra. Preserve each approved vLLM argv on CUDA arm64; qualify its exact
runtime/environment separately. No new server supervisor, dependency manager,
CPU-only jobs or automatic permanent reservations. `logs: none` already exists
on models and now also covers batch jobs; it does not control HTTP upload spools.

## 1. Entry ownership and placement

Add optional `Model.project` and `Model.allowed_machines`, matching `Spec`.
Omission retains shared entries and existing placement. A nonempty project owns
the entry: reject calls and pins from another project. Intersect the entry's
allow-list with `ProjectPolicy.AllowedMachines`, platform/device/environment
eligibility and any pin restriction; never let one widen another. Empty lists
mean unrestricted at that layer. An empty intersection waits with a clear reason.

Extend catalog validation in `models.go`, `modelFitsMachine` in `server.go`,
call selection/reclamation in `models.go`, `maintainPins` in `pins.go` and
pre-copy planning in `sync.go`. Snapshot restrictions into worker load plans;
recheck them on dispatch and reconnect, including already loaded copies. Status
keeps ownership and placement visible. Changing a fence must drain an excluded
copy before reuse, preserving cleanup custody and memory accounting.

Risk: missing any warmup/reuse path bypasses the fence. Project labels with the
shared fleet token are accounting controls, not independent tenant credentials;
plain commands retain worker-user permissions. This extends the approved
project allow-list rule without conflicting with AGENTS.md or scheduling order.
**Question for maintainer:** should `project` make an entry exclusive? Recommend yes
when set, retaining shared access only for entries that omit it.

## 2. Explicit environment and named secrets

Add `Model.env: map[string]string` for non-secret literal values and
`Model.secret_env: map[string]string` mapping environment variable to a local
secret name. Extend `serverCommand` in `server.go`; reuse environment acquisition
and launch rather than wrappers. Resolve approved names from operator-provisioned
worker-local secret storage only immediately before start; unavailable names
refuse load. Use the same process environment for the optional stop command.
Keep values out of catalog/SQLite/load plans, errors, command lines and status.

Reject overrides of `MYCE_*`, device selectors, framework offline controls and
worker environment paths; worker-owned values win. Never inherit, reference or
alias the fleet token into either process. Device selection remains
`device_env`, with its existing validation. Operators must keep `env` literals
non-secret; a validator cannot reliably identify arbitrary credentials.

Risk: a trusted payload can read its supplied secret or write its own telemetry;
`logs: none` only stops Mycelium output capture. No sandbox is implied. This is
compatible with plain-command execution and AGENTS.md's token prohibition; named
secret storage is a small new configuration contract.
**Question for maintainer:** include secret resolution now? Recommend the two maps
with only explicitly provisioned local names, no remote secret service; Atlas
can start with non-secret `env` alone if no runtime secret is needed.

## 3. Operator-approved local weights

Add `Model.local_source: APPROVAL_NAME`, mutually exclusive with `file_store`
and `direct_nas`. Keep `files`, `size_bytes` and `sha256` as the manifest identity.
An explicit worker-local approval binds the alias, physical host, immutable HF
snapshot revision, canonical snapshot root, permitted blob root and manifest.
Approval computes hashes once, following snapshot symlinks only into that blob
root; reject dangling links, traversal, special files and unlisted files. Cache
the manifest/approval durably in worker state. Catalogs and check-ins carry the
alias and identity, never the private source path.

Extend `ModelCache.Prepare` in `cache.go` and bundle validation in
`model_files.go` with this separate source mode; do not weaken the normal
no-symlink copy path. Later loads check the cached approval, link targets and
file metadata, then pass the original snapshot path to `{model_path}` without
copying or reading weight bytes again. Missing/changed metadata invalidates
approval and requires explicit reapproval; no silent NAS/download fallback.
Only workers advertising that approved identity are eligible. Sync reports
local readiness without transfers; Mycelium never evicts or deletes external
snapshots/blobs. The operator must protect them from HF cache cleanup/mutation
while approved, including while unloaded, and retain a recovery source.

Risk: metadata checks cannot prove unchanged bytes against a malicious writer.
This explicitly trusts operator-controlled immutable storage; read-only access
for the runtime alone is insufficient if another process can modify the blobs.
It conflicts with the approved NAS-master/local-verified-copy contract and
relaxes no-symlink custody for this mode only. AGENTS.md's laptop testability,
attempt custody and token rules remain intact.
**Question for maintainer:** approve this custody exception? Recommend opt-in per-host
approval only for operator-protected snapshots, with explicit reapproval after
changes, rather than hashing on every load or accepting arbitrary argv paths.

## 4. Header-less OpenAI clients

After parsing the model name in `API.modelCall` (`models.go`), use its nonempty
`project` when `X-Myce-Project` is absent. Add optional
`ProjectPolicy.default_call_priority` for a missing priority header; recommend
`experiment`, allowing explicit operator approval of `production`. Invalid
supplied headers still fail; explicit projects must satisfy entry ownership.
Deadline priority still needs a valid deadline. Shared models without an owner
still require a project header. The OpenAI JSON body and `/v1` routes stay intact.

Fleet bearer authentication remains mandatory and is stripped before forwarding
by `proxyHeaders`. A project default supplies scheduling context, never authority;
it cannot make a shared bearer token project-scoped. Standard OpenAI clients can
use their API-key setting for authentication. A client that cannot hold the
fleet token needs an operator-controlled local adapter or a separately approved
credential design. Do not expose the queue publicly or embed its token in YAML.

Risk: defaults can misattribute work or grant unintended production priority.
No AGENTS.md conflict; selecting a priority by configured default extends the
spec's “with a priority” contract and needs approval. Generic OCR routes still
spool uploads; header defaults and log opt-out do not satisfy #10's body-free
storage rule for those routes.
**Question for maintainer:** allow model-owned project fallback and per-project priority
defaults? Recommend yes, explicit opt-in with `experiment` as the safe default;
retain fleet authentication and keep project-scoped credentials separate.

Before any native proof: fake-clock placement/pin/reuse tests, laptop symlink
approval/invalidation tests, secret/token exclusion tests and HTTP default/auth
tests. Then the integration owner agrees capacity/pin hours, runtime identity
and body-storage privacy before a separately authorized hardware run.
