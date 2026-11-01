# Agents submit; Mycelium coordinates

Proposal for [#544](https://github.com/1kuna/mycelium/issues/544), including its
[app-packaging report](https://github.com/1kuna/mycelium/issues/544#issuecomment-6561804741).
The [approved design](design-proposal.md) remains the product spec. This document
proposes implementation slices; it claims no deployment or native qualification.
Source checked at `02ce0cc6`; links below name existing files and functions.

An agent declares software, qualified variants, resources, class/deadline and
immutable inputs. The queue owns placement and precedence. MY-18 prefers the
least valuable fitting host; MY-26/MY-27 require explaining portability gaps and
staging data rather than turning a missing local environment into a host pin.
MY-19 output parity still gates production variants. No project relays another
project's yield, approval or release request through an orchestrator.

## Order by coordination removed

Ranks measure recurring handoffs removed, rather than implementation size.
Keep the recommended first three together: they replace cross-project yield,
cancel/resubmit and blackout messages using existing priority machinery. Then
fix packaging before adding GPU throughput: a portable job that cannot find its
inputs or interpreter still requires a human to choose a machine.

| Rank | Manual step removed / slice | Tracking |
| --- | --- | --- |
| 1 | Ask another project to yield: deadline guidance | [#545](https://github.com/1kuna/mycelium/issues/545) |
| 2 | Cancel and resubmit to hold queued work | [#546](https://github.com/1kuna/mycelium/issues/546) |
| 3 | Relay blackout windows: scheduled local holds | [#547](https://github.com/1kuna/mycelium/issues/547) |
| 4 | Submit, fail, then pin: resolve before submission | [#548](https://github.com/1kuna/mycelium/issues/548) |
| 5 | Pin because data exists on one machine: staging advice | [#549](https://github.com/1kuna/mycelium/issues/549) |
| 6 | Discover where environment files belong: library + example | [#550](https://github.com/1kuna/mycelium/issues/550) |
| 7 | Repair an old system interpreter: platform base runtimes | [#551](https://github.com/1kuna/mycelium/issues/551) |
| 8 | Ask which side rejected a path: scoped path diagnostics | [#552](https://github.com/1kuna/mycelium/issues/552) |
| 9 | Ask why waiting: slot/capability/memory explanations | #537 |
| 10 | Maintain positive host lists to avoid heavy queue-host I/O | #543, #527 |
| 11 | Approve read-only cached model mounts per job | [#553](https://github.com/1kuna/mycelium/issues/553) |
| 12 | Approve each immutable image/model download | [#554](https://github.com/1kuna/mycelium/issues/554) |
| 13 | Drop a memory cap to request network-none | #542 |
| 14 | Hand-run third-party-code block/network probes | [#555](https://github.com/1kuna/mycelium/issues/555) |
| 15 | Diagnose repeated large tool-bundle uploads | #539 |
| 16 | Coordinate small jobs around one GPU batch slot | #538: decision first |
| 17 | Copy inline locks onto the queue host manually | #526; #251 decision hold |

Ranks are delivery preference, not a reason to serialize independent fixes.
Resolve consumes ranks 5–8 and 9 incrementally; #542 precedes Docker probe proof.
Rank 17 separates existing queue-side snapshots from the unapproved portable
submitter snapshot, so the release reference cannot imply the latter shipped.

## Reuse and state ownership

Existing mechanisms, inspected before proposing additions:

| Existing seam (`file:function`, under `internal/myce/` unless shown) | Reuse |
| --- | --- |
| `priority_policy.go:defaultPriorityPolicy`, `priorityMatch`, `comparePriority`, `SetPriorityPolicy` | Existing deadline tier, owner ordering, revision CAS and project settings |
| `types.go:Spec.Validate`; `schedule.go:beforeWithinTier` | Deadline validation and within-tier deadline ordering |
| `queue_persistence.go:Queue.persistState`; `queue.go:Queue.SubmitContext`, `Queue.Started` | One SQLite authority, immutable submission identity, final start fence |
| `reservation.go:Queue.CreateReservation`, `maintainReservations` | Existing lifecycle-bound reservations, not recurring calendars or queued-job pause |
| `host_maintenance.go:Queue.ChangeHostMaintenance`, `maintainHostObligation` | Admission withdrawal and exact cleanup; host drain is broader than job hold |
| `pins.go:ModelPin.active`; `production_admission.go:Queue.productionAdmissionReason`, `productionArrival` | Clock-based windows, measured setup and declared production dependency history |
| `environments.go:ReadEnvironments`, `snapshotEnvironment`, `EnvironmentCatalog.resolveEnvironment`, `EnvironmentCache.Acquire`, `ensure` | Catalog-relative inputs, frozen recipes, uv/Pixi builders and use locks |
| `environment_readback.go:Queue.EnvironmentEntries`; `portability.go:Queue.Portability` | Catalog identities and live target/recipe/qualification advice |
| `shared_input_cache.go:SharedInputCache.Acquire`; `shared_inputs.go:Worker.acquireSharedInputs` | Verified read-only inputs and managed paths |
| `local_models.go:ApproveLocalModel`, `ModelCache.prepareLocal`; `cache.go:ModelCache.Prepare`, `FetchModel` | Approved local snapshots, bounded symlinks, hashed models and verified fetches |
| `placement_advice.go:PlacementAdvisories`; `value_placement.go:Queue.assignmentPlan` | Existing pin warning and shared placement projection |
| `machine_placement.go:batchOnTarget`; `placement_plan.go:Queue.roomPlanWithBatch`; `schedule.go:waitingReason` | Current batch-slot rule, preemption and diagnostic gap |
| `docker.go:dockerRunArgs`; `docker_network.go:Spec.validateDockerGroup`; `extension_qualification.go:Queue.extensionQualificationPlan` | Digest-only Docker, memory/network coupling and first-demand checks |

New fields below are proposals, not accepted syntax today. Runtime hold records,
host roles, blackout selectors/windows, root mappings and acquisition limits live
only in local queue policy/SQLite, configured through CLI/API. Extend the existing
`myce policy get/set --if-revision` and `GET/PUT /api/policy`, preserving unrelated
fields and revision conflicts; do not add another policy service. Existing
catalog startup configuration stays on its supported queue CLI, not in Git.
Expose library identity through policy metadata; absolute root mappings stay local.
Repository examples contain generic relative paths and mechanisms only.

All scheduling uses injected Clock. Snapshot state, do filesystem/network/hash
work outside `q.mu`, then recheck policy/session/attempt fences before admission.
All slices preserve cancellation precedence, exact release custody, submission
keys and existing deadline/loss/retry contracts. Priority labels grant no new
authority; use existing authenticated control access, never payload credentials.

Discrete GPU fit is bounded by **per-device VRAM**, plus a separate host RAM
charge. Spare host DRAM cannot satisfy a discrete GPU's VRAM need. Only reported
unified-memory hosts share device and host allocations in one physical pool;
reuse `host_memory.go:hostNeed` and existing unified admission, never sum it twice.

## Slices and laptop acceptance

### 1. Deadline guidance

Reuse owner tiers, not a new urgent priority. In submit/API job replies and
`myce submit`, `wait` and job GET, return advisory code, effective tier/selector/
revision, blockers and next action. A deadline on experiment work should explain
`class: deadline` plus `deadline`, its actual policy match and whether it changes
precedence. With custom policy, never promise promotion. Do not mutate accepted
class/keyed intent; changing an accepted job's class needs a new submission.
Deadline is an ordering hint, not a completion guarantee or production start_by.
Only suggest supported preemption: deadline demand preserves non-resumable work.
Values live in submitted spec and current SQLite policy; observations inform ETA,
with unknown/overrun explicit. Fake-clock checks cover default/custom ordering,
deadline validation and an unpreemptible blocker. Replaces asking a peer to yield.

### 2. Queued-job pause

Propose `myce pause JOB --reason TEXT [--until TIME] --if-revision N`, `resume`,
and `POST /api/jobs/{id}/hold|resume`. Store a revisioned admission hold on the
logical job, outside immutable Spec; keep state queued, ID, age, key and inputs.
An indefinite hold requires explicit resume; an optional absolute expiry uses
Clock. Running/assigned jobs return a specific conflict rather than pretend to
pause execution. A parent hold gates new fan-out children; running children finish.
Check hold at planning, preparation authorization and Started; resumed work goes
through ordinary fairness. Pause does not freeze start_by/give_up clocks or erase
another hold. Fake-clock/reopen tests cover expiry, cancellation, replay, stale
resume and assignment racing pause. Replaces cancel/resubmit and lost queue intent.

### 3. Scheduled local holds

Extend revisioned policy with rules: stable ID, beneficiary project/class,
component/role selector, reason, timezone and one-shot or weekly windows.
Configure through policy CLI/API; no cron, chat reminders or repository timetable.
Reuse `ModelPin.active` start-inclusive/end-exclusive semantics; split overnight
windows explicitly. Test timezone/DST boundaries and overlapping rules with Clock.
Rules gate competing placement/preparation/Started across the selected resources;
other hosts remain eligible. Existing Started work drains; beneficiary demand uses
existing tier preemption. Report draining until exact release, never promise
exclusive hardware merely because a window opened. Maintain independent drains,
reservations and cleanup fences when a window ends or a rule is removed.
Use existing measured setup/dependency forecast first; learn candidate windows
from accepted production timing locally, showing evidence and uncertainty. History
cannot authorize an ownership change: owner enables a recurring rule through CLI.
Cost: held capacity while enabled; no extra cooldown. Fake-clock restart/release
tests replay several projects around a window. Replaces repeated blackout messages.

### 4–8. Resolve packaging before choosing a host

**4 — Resolve.** Propose `myce resolve FILE [--target OS/ARCH/PLATFORM | --host ID]
[--json]` and authenticated `POST /api/resolve`. Reuse strict decoding, catalog
snapshots, `Portability`, shared-input manifests and planner predicates. Read-only:
no job, upload, reservation, build, download or payload run. Return separate
submitter/queue/worker input visibility, recipe/runtime/qualification coverage,
needs-staging/build status, capability and current admission per target. Host is
an optional diagnostic filter, never inserted into the spec. Fresh worker evidence
or a non-executing worker inspection may establish visibility; otherwise unknown,
not ready. Cache absence is build-needed, not unsupported. Submit revalidates.
Fake-clock/inventory tests make a missing input/env fail before payload, preserve
unknown stale evidence and leave the queue unchanged. Replaces submit/fail/pin.

**5 — Stage, don't pin.** Extend `PlacementAdvisories` and resolve/submit responses
with input-specific actions: small private bytes via `files`/`myce file put`, large
immutable trees via `shared_inputs` (#221), source via its snapshot. Values live
as content identities in the job/store; workers resolve managed input variables,
not submitter paths. Warn on an absolute command/workdir path without a portable
input contract; don't guess/rewrite shell arguments or widen authored constraints.
Fake workers with different roots must run the same staged CPU input without an
allowlist; an unstaged path gets guidance. Replaces sharing mount recipes and pins.

**6 — Environment library.** Document the queue-readable catalog directory as the
library; `ReadEnvironments` resolves relative companions there and workers receive
immutable bytes. A common logical environment name does not require common mount
paths. Readback stays `myce env list|get`, `GET /api/environments[/{name}]`.
Example layout, relative to a privately configured queue library root:

```text
library/catalog.yaml
library/cpu-tools/pixi.toml
library/cpu-tools/pixi.lock
```

```yaml
# catalog.yaml; pixi.lock pins Python 3.12 and ffmpeg for declared targets
environments:
  cpu-tools:
    cpu: {backend: pixi, lock_file: cpu-tools/pixi.lock, environment: default}
```

Select with existing queue `--environment-config library/catalog.yaml`, alongside
normal options; catalog changes currently require reload by queue restart. The job
uses `environment: cpu-tools`, CPU needs, staged source/inputs and `[python, task.py]`.
The detailed [CPU tools example](environments.md) supplies the manifest/lock steps.
Record the library's logical identity in local policy and return resolution base/
side through authenticated diagnostics. Test different queue/worker roots and
missing companion bytes with small fixtures. Replaces asking where locks belong;
it does not introduce dynamic catalog registration or bypass #251/#252.

**7 — Base runtimes.** Build on existing `python: '3.12'`, uv managed Python and
locked Pixi Python/ffmpeg; do not install dependencies into system Python. Expose
exact OS/arch/backend runtime availability/readiness through env readback/resolve.
Recipes/locks own versions; local policy owns permitted acquisition and preparation
preferences, DB/cache owns measured build state. Reuse download caches; add shared
base interpreter reuse only if measured repeated downloads justify it. Test with
an old system interpreter and fake uv/Pixi builders across synthetic targets;
missing runtime says prepare-needed/blocked. Replaces per-host interpreter repair.

**8 — Path errors.** Extend existing typed errors with field, side (submitter,
queue, worker), author-supplied path/logical label, resolution base and cause:
missing, unreadable, companion absent or wrong root. Preserve job_environment_invalid.
Authenticated diagnostics may echo the caller's path; hidden queue/worker roots
remain logical IDs with private local detail. Public-safe advice omits raw paths.
Use errors from each actual read, not a new existence guess. Fixture tests cover
all three sides and sanitized output; Clock tests expire worker observations.
Replaces relayed investigations of which machine could see the same spelling.

### 9–10. Explain and avoid placement friction

**9 — #537.** Extend shared planner diagnostics and `waitingReason`, not a parallel
scheduler. Job GET/submit/wait expose actual candidate failures: GPU slot holder/
tier/device, Docker or other capability, platform, VRAM, host RAM, environment,
hold and cleanup. Store facts in existing attempts/inventory; derive explanations
per snapshot. ETA uses comparable history only. Fake-clock cases separately
block on slot, missing Docker and memory; clearing one cannot hide the next.
Include field/valid-ID detail for reservation selector errors. Replaces status asks.

**10 — #543/#527.** Use `excluded_machines: [ID]` for a genuine exclusion,
intersecting existing policy/allowed_machines; no positive complement list.
Validate through submit/spec check/resolve and apply at every admission gate.
Queue-host identity/roles and measured I/O sensitivity live in local policy/DB;
placement prefers another eligible host for heavy I/O. Reuse learned storage
accounting and standard I/O controls; IOWeight alone is not qualified protection.
Clock tests retain eligibility of newly joined alternatives and withheld reasons;
#527 still requires separate native latency proof. Replaces per-project avoidance asks.

### 11–14. Prepare and qualify without per-job approvals

**11 — Model mounts.** Declare a catalog model identity/digest as a read-only
input, not arbitrary host bind paths. Reuse local approval, manifest verification,
model/shared-input caches and use locks through exact cleanup. Resolve HF snapshot
symlinks plus referenced blobs within the locally approved root; expose only that
closure via managed input paths/native grants or worker-owned Docker mounts.
Approval/root policy stays local; immutable manifest stays with job/model identity.
New declarations surface through spec/resolve/submit; no model engine is implied.
Small-file tests reject traversal, changed blobs and writes, and prevent eviction
until release across restart. Replaces repeated mount approval without broad home access.

**12 — Downloads.** Extend local policy with permitted sources, digest/hash and
byte/unpacked-size limits; preparation uses standard Docker pull and existing
verified model fetch, not a downloader service. Runtime remains `--pull=never`
and offline unless explicitly allowed. Resolve/submit show cached, permitted
prepare-needed or blocked, plus bytes/free-space evidence. Reserve remaining disk
before download; validate expanded image layers and final identity before publish.
Learn actual sizes/transfer cost locally; unknown size must fit an explicit owner
limit. Cost is bounded cache/disk and cold-start time, preventing unbounded pulls.
Fake-clock/probe tests cover policy changes, no-space and interrupted preparation;
digest failure never launches. Replaces individual image/model download approvals.

**13 — #542.** Decouple Docker memory from network selection in validateDockerGroup;
keep needs.host_memory_gb for admission and document effective --memory per route.
Spec/resolve accept docker_memory_gb with default network none. Values stay in
Spec/attempt ledger. Fake-daemon plus Clock/custody checks cover cap + no network;
native inspect proves enforcement separately. Replaces omitting the ceiling.

**14 — Safety probes.** Add `myce probe FILE` / `POST /api/probes` that submits a
normal first-demand check using existing qualification, environment and runner.
Bind result to frozen code/model/environment, target and isolation fingerprint in
SQLite. Test forbidden sibling/credential/cache writes, control access and outbound
network with worker-created canaries; include actual custom-code loading offline.
No real secrets or internet endpoint is needed. Reuse standard sandbox/Docker
boundaries; #542 enables capped network-none proof. Unknown/failed probes withhold
the requested safe route and explain repair; unsandboxed work never gains a false
pass. Fake-clock CPU tests cover stale reports, expiry by changed fingerprint and
cleanup. Native denial and MY-19 parity remain separate. Replaces manual block probes.

### 15–17. Reuse input transport; keep the throughput decision separate

**15 — #539.** Extend `cmd/myce/profile_source.go:freezeProfileSource` diagnostics:
submit/spec-check name repeated large members and suggest immutable shared inputs.
Compare member digests during existing archive work; history stays local, thresholds
are policy/explained, no duplicate bulk scan under q.mu. Directory + paths is
currently rejected (paths require Git); document that explicitly. Tiny fixtures
test repeat warnings and filtering semantics. Replaces manual archive-budget advice.

**16 — #538.** Retain one batch per GPU pending maintainer approval. A later opt-in
would use per-device learned VRAM envelopes, separate host RAM and unified-only
shared accounting, qualification and measured contention/OOM blast radius. Policy
and measurements stay local; spec/resolve expose sharing eligibility. Fake-clock
admission/cleanup tests precede separate native timing/parity proof. Replaces slot
handoffs only after that decision, not a prerequisite for the first slice.

**17 — Snapshots.** Existing snapshotEnvironment reads queue-visible locks at
admission and freezes bytes. #526 does not establish submitter-side path transport.
Reuse catalog or bundled extension environments now; #251/#252 own the conditional
project-carried recipe change. Test unchanged keyed snapshots after source edits/
restart, and resolve the actual reading side. Replaces manual lock copying only
when the supported portable route is available; never silently fall back to a pin.

## Implementation tickets and proof

The eleven named new tickets above each carry outcome and acceptance criteria;
reuse the numbered existing issues rather than duplicating their implementations.
First-slice proof: fake Clock, SQLite reopen and real small CPU commands with fake
GPU inventory replay deadline demand, held peers and scheduled exclusion, with
no cancellation/resubmission or orchestrator messages and no start before cleanup.
Packaging proof: two synthetic roots, one staged media input, pinned environment
and an old system Python; resolve explains missing pieces and both qualified
targets execute without a data-driven host pin. No heavy gates for this document.
The reported generic registration 400 has an exact error and replay context, but
the serialized request is missing. Next owner: support worker obtains a sanitized
request before diagnosing the defect; no speculative fix ticket.
