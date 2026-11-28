# Project-carried resident engines

> **Awaiting maintainer approval** for [#1775](https://github.com/1kuna/mycelium/issues/1775).
> Design proposal, 2026-11-20. No implementation or deployment is authorized by
> this document. The [approved design](design-proposal.md) remains the only
> product spec; its existing text is unchanged.

## Outcome and recommendation

An agent includes a foreground model server, immutable weights manifest and
locked environment in its project's existing instruction file, registers it
through `myce extension add`, and submits ordinary stages bound to that frozen
engine. The queue and workers keep running. Two independent inputs for the same
stage can use one warm engine; only the engine outlives an attempt.

Extend the immutable extension snapshot and `freezeResident`, rather than
making the operator catalog mutable. Keep `ModelConfigHash` checking exactly as
it is for the operator catalog. Project engines travel in authenticated,
attempt-fenced resident load plans and never enter that catalog or its hash.
Use the existing environment builders, file store, model cache, model slots,
admission planner and attempt pipe. There is one binary and one SQLite queue.
No new engine registry service, daemon or execution runtime is proposed.

The reported 2026-11-20 incident required draining five worker hosts and
reinstalling six services to add one restricted engine for the paying production
consumer (task brief; not independently measured here). The coupling has a
[public issue receipt](https://github.com/1kuna/mycelium/issues/1775#issuecomment-7032072813).
This is a general extension contract, not a consumer-specific registration path.

## Binding design and rulings

The approved [resident-stage contract](services.md#resident-engines-for-independent-stage-jobs)
already freezes catalog engines into jobs. This proposal changes the source of
that frozen definition. It also proposes target-aware server recipes and
extending the existing payload isolation launcher to those servers; neither is
claimed to exist today.

| Ruling | Constraint on this proposal |
| --- | --- |
| [MY-01 / MY-11](DECISIONS.md#my-11), [MY-26 / MY-27](DECISIONS.md#my-26) | General software on every compatible machine. Declare exact software targets and variants; a host restriction needs a real requirement, not a preferred box. |
| [MY-12 / MY-17 / MY-20 / MY-22](DECISIONS.md#my-17) | One agent-written adapter; existing software assets and locks are not additional instruction files. No curated applications or operator catalog edits for normal consumer setup. |
| [MY-18 / MY-19 / MY-31 / MY-51](DECISIONS.md#my-19) | Least-valuable fit, measured resources, output parity before production variants or splitting. Qualification attaches to exact software and machine kind, never grants production approval to a named host. |
| [MY-34 / MY-44](DECISIONS.md#my-34) | No queue-owner software approval or arbitrary rate, count, size, idle, build-time or readiness limits. Physical capacity, measured pressure and secret/device isolation still bind. |
| [MY-23 / MY-24 / MY-25](DECISIONS.md#my-23) | Preserve token filtering, assigned devices, launch gates and release custody. State the difference between kernel isolation, an explicit unsandboxed exception and trusted preparation. |
| [MY-36](DECISIONS.md#my-36) | Stage weights locally with verification; the self-service path does not introduce direct NAS execution. |
| [MY-30 / MY-50 / MY-56](DECISIONS.md#my-50) | Existing tiers and exact-release preemption; warm engines do not reserve a whole machine or exempt themselves from reclamation. |
| [MY-03 / MY-42](DECISIONS.md#my-42), [MY-35 / MY-55](DECISIONS.md#my-35) | Adjacent-release interoperability only; no silent downgrade fallback. Registration itself requires no service replacement or interruption of running work. |
| [MY-09](DECISIONS.md#my-09), [MY-41 / MY-53](DECISIONS.md#my-41) | Laptop proof first, meaningful targeted checks and quick per implementation change, full once per release batch; no independent-review gate. This design still needs the maintainer's product decision. |

**Environment decision checked on 2026-11-20:**
[#251](https://github.com/1kuna/mycelium/issues/251) is **closed**; its
[closing ruling](https://github.com/1kuna/mycelium/issues/251#issuecomment-6792083565)
applies MY-44 to project-carried locks. Its old title/body's decision hold is
superseded. [#192](https://github.com/1kuna/mycelium/issues/192) is **open** for
the larger fresh-project acceptance, with a
[current-state correction](https://github.com/1kuna/mycelium/issues/192#issuecomment-6905694390)
confirming that unified adapters and project-carried locks are implemented.
The approved [project-environment contract](design-proposal.md#project-supplied-environments)
already covers immutable uv/Pixi recipes and worker-specific preparation
failures. The remaining seam is using those recipes for a resident server;
this proposal does not reopen environment approval.

## What the current code proves

Source inspection is pinned to main
[`03ddc496dd3f1eaaeda39ac405b058670ae4e7c5`](https://github.com/1kuna/mycelium/commit/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5).
These are source facts, not native qualification or live deployment evidence.

| Mechanism | Current behavior and seam |
| --- | --- |
| [Model validation][models] | `ReadModels` strictly decodes YAML; `validateModels`, [`validateServer`][server] and [`ModelFilesIdentity`][files] validate engines, memory, commands, health endpoints and manifests. [`ConfigureModels`][configure] resolves environments from the queue library. Reuse validation, not startup configuration mutation. |
| [Catalog handshake][register] | `Queue.Register` rejects a differing hash when the worker has `ModelURL` and the queue catalog is nonempty. [Worker check-in][worker] sets that URL even without catalog entries. [`modelConfigHash`][catalog-hash] hashes sorted decoded `Model` values, not the original YAML bytes; comments/formatting and catalog Idle/Wait are outside it. Operationally all proxies must agree on that normalized catalog. |
| [Resident freeze][resident] | `freezeResident` strips author authority over the internal engine, resolves a catalog name, deep-copies its model/environment and hashes stage/source/extension identity. Inputs and per-item arguments do not split residency. `residentPlanningJob` admits the cold engine and driver together; `reserveResident` owns a single charged slot. |
| [Resident load plan][plan], [worker application][engine] | `modelPlan` already sends `ModelSlot.Resident` and a frozen `EnvironmentRecipe`. The worker uses `s.Resident.Model` instead of looking it up in its local catalog. **Inference:** dynamic definitions need not change worker catalog bytes. Arbitrary project recipe support is not proved by this path. |
| [Extension registration][extensions], [CLI][cli] | `myce extension add` posts the bundle to the queue. `AddExtension` reconstructs/validates it, freezes it, and rejects different bytes for an existing name/version. The extension registry already lives in SQLite. |
| [Recipe snapshots][recipes], [bundle store][bundles] | Extension environments bind lock/build bytes and exact targets; the existing file store turns bundle maps into verified `FileRef`s outside the queue mutex. No new package manager is needed. |
| [Weights identity][files], [file-store model copying][stored-model] | `ModelFilesIdentity` binds every relative path, size and digest; `copyStoredModel` verifies the individual blobs and complete cached directory. A manifest hash alone is not evidence that bytes were uploaded or retained. |
| [Retention][retention] | Existing resident slots retain model and environment references. New server source and recipes must also be reachable from registrations, accepted/replayable jobs and slots; adding a manifest field alone would leave a collection gap. |
| [Isolation specification](design-proposal.md#native-job-isolation), [server launch][server] | Stage payloads default to a kernel boundary. Standalone servers and environment builds are explicitly outside that guarantee today. Moving server code into an extension does not automatically sandbox it. |
| [Dev][dev] | `myce dev` uses real API/worker execution with fake GPU inventory, but currently rejects non-fake startup catalog engines. A CPU foreground-server fixture needs an explicit dev implementation seam, not an assertion that fake replies exercised server launch. |

## Authoring and registration

**Proposed authoring:** `resident` becomes an exclusive choice between the
existing `{model: catalog-name}` and a project-carried `{server: ...}`. Keep
`ResidentBinding.Engine` queue-owned; never accept that internal field as an
author override. A server uses existing `Model` fields and validation. Its
`environment` names an existing recipe in the containing exact extension
variant, not the queue environment library. Add this choice to variants so
different OS/architecture/backend combinations can carry their own complete
server recipe and budget; do not merge partial variants.

Illustrative **new syntax**, not a runnable existing example (measurement and
manifest placeholders must be replaced with evidence):

```yaml
name: example-infer
version: '1'
purpose: Independent inference stages sharing a warm server
kinds: [example-infer]
variants:
  - target: {os: linux, arch: amd64, platform: cuda}
    environments:
      runtime: {backend: uv, python: '3.12', lock_file: runtime/uv.lock}
    resident:
      server:
        name: inference
        engine: server
        environment: runtime
        file_store: true
        files: [{path: weights.bin, size_bytes: EXACT_BYTES, sha256: FILE_DIGEST}]
        size_bytes: EXACT_TOTAL_BYTES
        sha256: MANIFEST_DIGEST
        memory_gb: MEASURED_DEVICE_ENVELOPE
        host_memory_gb: MEASURED_HOST_ENVELOPE
        start_command: [python, '{extension}/serve.py', --weights, '{model_path}', --port, '{port}']
        health_url: LOOPBACK_HEALTH_URL
    needs: {cpu_cores: 1, memory: auto}
    execution:
      main:
        command: [python, client.py]
        environment: runtime
    # Existing checks/baselines/references and outputs complete this extension.
```

`{extension}` in server argv is a proposed addition to the existing literal
server placeholder expansion: the engine's read-only, digest-verified copy of
the registered source, held for its whole lifetime, never a submitting checkout
or an attempt's disposable source directory. `{model_path}` remains the worker's
verified local weights directory. A locked installed server executable is also
valid; in that case no source placeholder is necessary.

For a unified adapter, put the same declaration in its existing complete
variant (flat entries use `advanced.resident.server` plus the entry's ordinary
environment). The adapter lowers to the same immutable selected-entry snapshot;
there is no second authored catalog file. The CLI can upload/register that
snapshot through the existing extension/file-store route as part of normal
submission. Explicit `myce extension add` remains the reusable-kind interface.
Concrete JSON/schema details are implementation work within this exclusive
choice and identity contract; the decision is not to expose another catalog.

Registration performs static validation and durable freezing, not a model load
or production qualification:

1. Snapshot source, locked recipes, referenced build inputs and weights
   manifests with the existing client upload/preflight. Large weights remain
   separate verified blobs, not inline extension YAML or source bundles. Normal
   model fetch/upload mechanisms may supply those identities beforehand.
2. Reconstruct the authoritative bundle on the queue; reject unknown fields,
   conflicting `model`/`server`, malformed targets, missing lock companions,
   invalid commands/health URLs, invalid memory and manifest mismatches. Reuse
   catalog validation through a common entry validator plus validation of the
   project recipe references. Do not call `ConfigureModels` or modify `q.models`.
3. Freeze every candidate's full `EnvironmentRecipe` and source `FileRef`.
   Verify file-store weight availability/size/digests using the existing upload
   and publication fences. For an existing model-root source, freeze the
   declared manifest now and verify actual bytes at the eligible worker before
   load; the queue does not walk remote weight directories at registration.
4. Commit the immutable registration and all retaining references atomically.
   Same name/version and same digest is idempotent; changed content conflicts
   and requires a new version. No queue restart, YAML write, service install or
   worker broadcast occurs. A refused upload never leaves a usable registration.

Hashing, bulk verification, source hydration, package checks and network I/O
happen outside `q.mu`. Recheck file-store generation/publication fences and
registration identity inside the small commit, as `AddExtension` already does.

## Frozen binding and catalog safety

Preserve **two distinct identities**, with explicit origin in internal plans:

| Origin | Authoritative identity and worker requirement |
| --- | --- |
| Operator catalog | Existing normalized whole-catalog hash. Preserve the exact registration mismatch refusal, including its current empty-catalog condition. Project registration cannot add, replace or shadow a catalog name. |
| Project snapshot | Frozen extension/entry digest, exact candidate target, server definition, weights manifest, recipe key and source reference. An eligible worker receives the full frozen definition in its resident plan and verifies those identities. No worker advertises a project catalog hash. |

Keep origin explicit rather than inferring authority from a model-name prefix.
The human `server.name` is local to its frozen entry, not a fleet-global model
alias. The existing derived resident name remains an opaque execution identity;
do not publish project engines in `/v1/models`, permit arbitrary direct model
calls, or share them between projects through this change. The project enters
the residency hash, as it does today. Broader shared serving remains the operator
catalog's existing contract. This is stage binding, not a new public serving API.

After keyed replay lookup and ordinary extension/environment resolution,
`freezeResident` freezes the complete eligible candidate set from the immutable
entry, rather than re-reading mutable author files or catalog entries later.
That is an internal extension of today's single-engine frozen binding. The
existing planner chooses one exact stage/server target pair; each candidate has
its own resolved model budget, source and environment. Persist the chosen
concrete `ResidentEngine` in the slot and assigned job. `ModelSlot.Resident` stays
the worker-facing concrete engine, not an instruction to resolve a name later.
Never flatten environments to backend-only keys: two CUDA variants may differ
in OS/architecture or toolchain. The chosen exact target binds preparation,
qualification, admission and the load plan. Requeue may select another already
frozen, qualified candidate through ordinary placement; a running attempt never
changes target.

Reuse identity retains the current project/stage/source/environment/extension
inputs, plus the concrete server candidate's immutable definition and target.
Inputs, per-item argv and job IDs stay outside residency identity. Edited server
code, weights, runtime flags, locks or isolation policy change identity. A changed
entry may conservatively get a new copy even if some server bytes match; this
proposal adds no cross-entry deduplication contract.

New capability negotiation (for example `resident_specs_v1`) gates project
engines. Current `resident_jobs_v1` alone is insufficient evidence of support for
new source, target and isolation fields. Send no new strict-wire fields to an
older worker. It remains registered and can run existing work; a missing
capability is a visible eligibility blocker, never a reason to restart unrelated
workers or weaken catalog checking. After the one-time binary deployment that
implements this feature, subsequent engine registrations require no upgrades.
Keep only the adjacent release interoperability required by MY-42.

Before launch the worker checks the authenticated queue plan, content/target
identity, current placement, device binding, maintenance and exact session/load
attempt authorization. A model-runtime cache key is never launch authority.
Keep immutable source and environment use locks through exact unload. After
queue restart reconcile durable slots and accepted snapshots with worker
reports; do not recover from a newly edited adapter. A stale load/call/unload
reply cannot authorize another session or release another engine's charge.

The host-restricted case is now local: registration alters only SQLite and the
file store; a compatible worker receives a resident plan on demand. Other
workers retain their unchanged operator hash, continue checking in and need
neither the new project definition nor its files. Existing caller/owner project
policy, entry fences, exact target/device eligibility and qualification all
intersect. A host allow-list can narrow an actual requirement; it never widens
owner policy or substitutes for MY-19/MY-51 approval by machine kind.

## Trust and operator responsibilities

**Recommendation:** give the existing authenticated submission/extension
registration authority this operation. Today's API authentication and
caller-supplied project labels are not a tenant identity system
([authentication middleware][auth], [shared-entry contract](services.md#shared-entries-and-placement)).
Do not claim that a project string prevents a trusted fleet client from naming
another project. No unauthenticated registration, new credentials, per-engine
approval gate or general multi-tenant ACL system is part of this proposal.

The project supplies software, locks, immutable data, target requirements and
non-secret literals. The queue validates and freezes; the worker supplies local
roots, actual devices, ports, scratch and launch authority. Registration grants
no root, driver, package-manager service, host-service edit or filesystem grant.

- The self-service path accepts uploaded verified weights or safe relative
  model-root sources. It cannot set an absolute root, traverse outside it,
  create a local-source approval, or opt into `direct_nas`. Existing operator
  local-source/direct-NAS catalog entries remain reachable via `resident.model`.
  Root provisioning and approved in-place sources stay operator responsibilities;
  another ordinary model under an existing root needs no approval.
- Device requirements intersect worker inventory and owner policy; queue-owned
  selectors win. No author-provided environment can override `MYCE_*`, worker
  executable paths, offline controls or device masks. Do not turn catalog
  validation's acceptance of `device_env` into arbitrary device access.
- Reject `secret_env` on a project-carried server in this initial contract.
  A reference by name is not a per-project secret grant: the existing worker
  secret directory has no such ACL. Provisioning secrets or delegating access
  needs operator action outside this feature. A consumer requiring the existing
  operator-provisioned secret contract uses `resident.model`; names and values
  are never copied into project snapshots. Future delegated secret references
  need an explicit access design, not permission inferred from a model name.
- Project-carried servers default to the registering extension/entry's existing
  isolation policy, including the explicit MY-24 unsandboxed reason. Reuse the
  native sandbox launcher with an engine-owned work/temp directory, read-only
  frozen source/environment/weights and the exact worker-reserved service port.
  Do not include an attempt's input/output tree. Keep environment allowlisting,
  token scrubbing, cgroups and release custody for either isolation choice.
  Unsupported required isolation withholds that candidate; it does not silently
  launch an unsandboxed server. Stage status and engine status must separately
  disclose their actual boundaries. This launcher plumbing is new work, not a
  claim that `LlamaEngine` already provides it.
- Environment builds retain the approved trusted-code boundary. A lock digest
  establishes identity, not safety of arbitrary installers. Existing sandbox
  limitations and explicit unsandboxed exceptions remain visible; no hostile
  multi-tenant security guarantee follows from self-service registration.

For a private Linux network namespace, reserve the engine port inside the
engine's own namespace and reuse the namespace helper for worker-owned health
probes and HTTP byte forwarding. Keep fleet authentication in the parent worker,
outside the helper and payload; do not join an attempt's namespace or open the
control API to the engine. The existing [namespace probe helper][namespace]
establishes a reusable entry point; streaming forwarding and engine-lifetime
custody still need implementation. On other supported isolation backends use
their existing exact-port policy and retain their documented network limitations.
This is an internal worker helper, not another service or a payload-held fleet
credential. Qualification must exercise the real transport, not just process
startup.

Payloads never inherit the fleet token. The existing worker-owned attempt pipe
keeps fleet authentication out of the client and binds requests to a live
job/session/attempt; a payload cannot select another engine/project/tier.
Credentials, secrets and private installation paths stay out of stored authoring
examples, logs and public diagnostics. Managed engines retain the current
`worker_data: ephemeral` refusal: a shared engine cannot promise per-attempt
erasure of its memory or request spool.

## Memory, admission and lifecycle

Registration alone reserves nothing. Keep the existing cold engine-plus-driver
admission transaction, model slot accounting, device/physical-host ledgers and
memory charge until exact unload acknowledgement. Charge an engine once per
host/device copy; charge each stage's staging, CPU and host RAM separately.
Unified memory is one pool, not host capacity plus device capacity.

Reuse measured [memory envelopes](reference/memory-mechanisms.md), worker
resource probes and [model readiness observations][readiness]. A server budget
must cover initialization peak, idle weights/runtime and the peak of **one
representative request**, including host allocations and measured launch/stop
overshoot. Record evidence against the frozen server/source/weights/recipe,
exact target, relevant device fingerprint and request shape. Stage-driver peaks
alone cannot measure a separate server process, and healthy readiness alone
cannot measure request activations. Extending engine-owned sampling through the
qualification request is required implementation work.

Initially keep `Model`'s positive finite envelopes, backed by recorded
measurements; do not infer them from weight bytes or copy the illustration's
placeholders. An engine without request-peak evidence can register and run
experimental/internal qualification under the existing bounded first-run and
physical admission rules, using an explicitly declared exploration envelope.
That is a ceiling for the check, not a measured production budget. If the
engine's memory is not contained/observable on a target, retain conservative
envelope accounting and disclose that limitation. Production eligibility needs
the complete engine/client check, measured envelope evidence and the existing
output-parity requirements; no manual queue-owner approval is added. Failed,
constrained or stale observations cannot lower a budget. Changed request shapes
or limits require appropriate evidence/requalification before spending an
insufficient envelope. Generic automatic resident sizing is not assumed to
exist merely because driver jobs support `memory: auto`.

Keep the existing one-in-flight resident request contract. This is the physical
meaning of a single-activation envelope, not an arbitrary throughput cap.
Concurrent activations would need a separately measured envelope and admission
design. Separate compatible copies remain possible through normal placement.
No fixed engine count, per-project registration quota, warm lifetime, extra
headroom percentage or rollout delay is introduced (MY-34).

Pending/running/dependent stage demand keeps its exact engine warm through the
existing residency policy. Once demand ends, measured load cost/interarrival
history determines retention; pressure and higher-tier work can reclaim sooner.
Do not introduce a pin or an idle cooldown. Keep existing cancellation,
non-resumable/single-device/no-CPU-fallback restrictions, retry classification,
result checks, accepted-output dependencies and no replay of interrupted
requests. Engine death fails/interrupts the affected call under those contracts;
successful sibling results remain accepted. Source and recipe preparation
failure excludes the exact worker/target identity, not unrelated compatible
workers. Apply the approved hold-versus-dead-end behavior, without idle rebuild
loops. Every release remains attempt/session-specific.

## Migration and laptop proof

Existing `resident.model`, operator models, pins, public model calls and their
catalog hash check remain valid. Registration does not change existing accepted
jobs or adopt an old warm process under a different identity.

1. Ship the new queue/worker capability through the normal separately owned
   release. Old workers still run their supported jobs. Feature eligibility,
   not a global catalog mismatch, identifies workers needing that capability.
2. Copy an operator engine's immutable manifest, source and locked environment
   into a new project entry version; preserve commands, real requirements and
   measured budgets. Complete engine/client checks and MY-19 parity. New jobs
   opt into the new snapshot; retained jobs finish under their frozen binding.
3. Leave the old operator entry while calls, pins, legacy bindings or custody
   need it. Its eventual removal is a separate normal catalog change with the
   existing hash-match rollout requirement. Initial adoption requires no
   catalog cleanup and no change to unrelated worker files.
4. A failed new registration/load leaves the old entry intact. Do not silently
   fall back to a different model, stale version, CPU or old protocol. Publish
   corrective versions and use existing cancellation/requeue where appropriate.

All control semantics remain laptop-testable. Extend `myce dev` to permit a
declared local CPU foreground-server fixture on fake accelerator inventory,
through the **real** server process/health/HTTP/stop path; never replace the new
server with `FakeEngine` for the decisive journey. Use tiny hashed weight blobs
and a project-carried locked environment with a locally available fixture/tool
cache. `myce dev` remains explicit and isolated; it grants no fleet authority.
Native GPU memory, kernel isolation and model quality still need their own
platform evidence.

Required integrated journey: start a dev queue with an unrelated operator model
and two already-connected workers. Register one project engine/recipe, restrict
its eligible target using synthetic inventory/policy, and run two independent
bound jobs. Prove a single server startup, distinct accepted outputs and eventual
exact unload; every queue/worker session and operator hash stays unchanged.
The other worker keeps serving unrelated work without receiving the project
definition. Re-run unrestricted with two qualified targets to prove the
restricted fixture is not a product placement default.

Fake-clock and fault-injection cases cover keyed replay, edited-source version
conflicts, corrupt/missing/collected blobs, target mismatch, failed preparation
on one worker, stale load/call/unload identities, cancellation during load,
queue restart with cold local caches, pending dependencies, memory contention
and preemption. Kernel tests exercise sibling/control denial, exact service
port access, default engine isolation and the visible unsandboxed exception on
supported platforms. Prove an actual operator-catalog mismatch still refuses
registration. These checks are proposed acceptance, **not run in this design
task**; no local test gates ran on this Mac.

## Alternatives rejected

1. **Hot-edit and broadcast the operator catalog.** This preserves a single
   global namespace but makes every project registration a coordinated fleet
   catalog transaction. An unrelated/offline worker would need an updated
   hash before reconnecting, and immutable accepted binding/secret authority
   still need design. It retains exactly the coupling #1775 must remove.
2. **Drop or scope away the operator hash check for all models.** This allows
   a queue catalog name to resolve to different weights, argv or environments
   on a worker. Per-load content identity could replace that entire contract,
   but that broad migration is unnecessary for project-bound resident engines.
   Keeping the existing check and using frozen plans is the smaller change.
3. **Use an ordinary per-job sidecar or an externally managed URL.** A sidecar
   restarts initialization per attempt; an external URL bypasses queue-owned
   memory, placement and exact unload custody. Neither meets the warm-engine
   outcome. An arbitrary finite command still cannot become resident without
   the author's foreground server and explicit client boundary.
4. **Create another dynamic model/recipe registry.** The immutable extension
   registry and file store already own version conflicts, transport and
   retention. Another mutable name lookup adds a competing authority and still
   needs frozen job identities. Embed the definition in the existing snapshot.

## Implementation slices after approval

These are proposed slices, not dispatched work or implementation authorization.
The implementation owner records their issues/acceptance after approval.

| Slice | Change | Acceptance |
| --- | --- | --- |
| 1. Immutable authoring and registration | Exclusive catalog/server binding; reuse common model validation, exact variants, recipe snapshots and blob references. Extend reference tracing for source/weights/recipes. | API registration and replay work with an unrelated catalog and no restart; same-version changes conflict; invalid/missing assets and secret/root escape attempts refuse; collection cannot remove registered or accepted engine inputs. |
| 2. Frozen candidate and capability protocol | Extend `freezeResident` to frozen target pairs; send a concrete engine only to a capable selected worker. Keep operator hash and existing catalog calls unchanged. | Two targets with the same backend and different OS/arch/locks cannot cross-resolve; older worker still accepts existing work; stale plans fail; catalog mismatch still rejects; accepted replay/restart reads no mutable author files. |
| 3. Engine execution, isolation and measured admission | Stage engine-owned source, acquire frozen environment, reuse server supervision and native sandbox launcher, extend request-peak observations and exact resource custody. | Real foreground fixture starts, serves and unloads; token/device/root boundaries hold; engine/client budgets charge once correctly on dedicated/unified fixtures; bad preparation can move to another qualified worker; cancellation/preemption cannot release memory early. Unsupported required isolation is visible. |
| 4. Consumer journey and migration | Adapter lowering, registered-extension path, actionable status, dev CPU-server support and guides. Update `skills/mycelium/SKILL.md` and `docs/services.md` in the same consumer-contract change. | Integrated two-job/one-load dev proof above, restricted and portable cases, old catalog path preserved, measured production-parity evidence contract explicit. Quick plus targeted checks per change; full once for the release batch (MY-41). Native proof/deployment are separate and do not interrupt running production work. |

## Maintainer decisions and remaining risks

Approve or reject these proposed contract choices; they are recommendations,
not new rulings:

1. **Use project-carried frozen server definitions in the existing extension
   path**, with an exclusive catalog-name alternative, target-aware candidates
   and unchanged operator catalog hash enforcement.
2. **Authorize current trusted submitters to register them without operator
   approval**, but keep privileged roots/device grants outside registration
   and reject project-carried named secrets until delegated secret access has
   a design. Secret-dependent consumers can keep the operator binding.
3. **Apply the existing extension isolation policy to the new engine process**,
   reusing its launcher and explicit exception, and require complete measured
   engine/client qualification before production. Environment preparation
   retains its existing trusted-code limitation.

Open engineering risks: target-pair projection changes a core frozen identity;
all admission, fan-out materialization and replay paths must use the same
candidate. Reachability must include queued and retained jobs as well as warm
slots or collection can strand recovery. A sandbox must permit the exact local
inference transport without exposing the control API; platform limitations need
visible withholding. Engine activation memory is distinct from stage memory,
and measurements must not turn missing evidence into capacity. The existing
shared fleet credential is not a project ACL. None of these risks is resolved
by source inspection or a fake GPU result alone.

Current state: design only, awaiting maintainer approval. Next owner: the
orchestrator brings the open design PR to the maintainer. Revisit when that
decision is recorded; implementation and native qualification follow only the
accepted contract.

<!-- Evidence links pin the inspected source, not a moving branch. -->
[models]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/models.go#L80-L195
[configure]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/models.go#L296-L337
[register]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/queue.go#L1712-L1714
[catalog-hash]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/models.go#L1276-L1281
[worker]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/worker_attempts.go#L1397-L1405
[resident]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/resident_jobs.go#L26-L197
[plan]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/models.go#L712-L741
[engine]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/engine.go#L773-L867
[extensions]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/extensions.go#L239-L385
[cli]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/cmd/myce/extension.go#L100-L125
[recipes]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/extension_composition.go#L109-L185
[bundles]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/extension_bundle_store.go#L15-L145
[files]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/model_files.go#L19-L88
[stored-model]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/model_store.go#L9-L40
[retention]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/file_retention.go#L488-L528
[server]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/server.go#L38-L168
[dev]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/dev.go#L123-L130
[auth]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/http.go#L510-L528
[readiness]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/model_readiness_resources.go#L12-L70
[namespace]: https://github.com/1kuna/mycelium/blob/03ddc496dd3f1eaaeda39ac405b058670ae4e7c5/internal/myce/native_namespace_linux.go#L317-L358
