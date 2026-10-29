# Run anything anywhere

Implementation proposal, 2026-10-29, for [MY-26](DECISIONS.md#current-rulings).
The [approved design](design-proposal.md) remains the only product spec;
the interfaces and scheduling change below await orchestrator plan acceptance.
This is a docs-only plan, with no native qualification or fleet authorization.

## Starting point

Source inspection of main `ef4c03c5` finds unified adapter entries and first-demand
native qualification (#250), complete extension variants, uv/Pixi builders,
least-valuable-fit placement (#193), priority preemption and durable checkpoint
transport. Reuse them, one binary, one SQLite queue and the shared planner.
Neither live portability advice nor same-tier scarcity yield exists today.
Current agent guides still describe separate profiles/extensions; their update
belongs with the new guidance, coordinated with #197/#198 and #254.

MY-17/MY-22 keep one project instruction file; MY-20 makes the agent its author.
MY-26 adds a live answer to “how can this software run across my cluster?” and
safe relocation when only one machine can satisfy other work. It adds neither
implicit emulation nor automatic application rewrites. #251/#252 still require
their separate maintainer decision about project-carried environments; this plan
can use today's catalog and bundled variant environments without them.

## Portability guidance

Propose `myce portability --adapter FILE --entry NAME [--json]`, also reached by
the installed skill before authoring and after inventory or requirements change.
It reads authenticated inventory plus the selected entry/extension metadata.
An inventory-only invocation helps an agent starting a new project. Text and JSON
share one read-only projection; no model service or package solver is required.
Known target/backend rules render concrete steps using the actual inventory.

Return one row per observed OS/architecture/backend, plus declared targets absent
from inventory. Include inventory observation time/freshness, available CPU and
accelerators, matched variant/digest, environment/lock target coverage and build
state, qualification/fingerprint state, hard requirements and remaining steps.
Separate “target exists”, “recipe declared”, “prepared”, “qualified” and “can admit
now”; a busy or offline host is not an unsupported platform. Missing facts stay
unknown. Never infer a hard software requirement from a missing variant alone.
Optional authored requirement reasons explain constraints; absent reasons prompt
investigation, not an invented claim that a backend is impossible.

For example, **if** inventory contains these targets, generate these actions:

| Observed target | Concrete advice for custom software |
| --- | --- |
| linux/amd64, XPU and CPU | Author separate XPU/CPU variants; lock dependencies for linux-64, check XPU runtime/package support, and use the assigned `xpu:N` or `cpu` device. |
| linux/arm64, CUDA and CPU | Lock for linux-aarch64; verify arm64 wheels/toolchain and CUDA compatibility; build missing native dependencies for that target. Use the assigned `cuda:N` or `cpu` device. |
| darwin/arm64, Metal and CPU | Lock for osx-arm64; use `mps` for Metal or an explicit CPU variant, and replace Linux-only shell, library and filesystem assumptions. |

The rows are illustrative, never a fleet allowlist. Enumerate the user's actual
inventory, identify unsupported Mycelium runtime targets separately, and explain
the missing runtime port instead of promising execution there. Use uv project
locks with their manifests, or Pixi locks covering each exact target; verify
frozen installation rather than re-solving on workers. Reuse one multi-target
lock where it covers the targets; split locks/recipes where packages differ.
Include a CPU implementation where feasible, with its own declared resources and
checks. The queue backend is `metal`; the application device is `mps`.
Use `MYCE_DEVICE` and assigned visibility, not a hardcoded GPU index. GPU checks
must exercise the selected backend; a CPU variant is an explicit alternative,
never a silent fallback inside GPU qualification.

For each uncovered target, explain whether to add a variant, lock/build input,
portable path/API, environment preparation or qualification. Qualify each native
variant against its recorded baseline; plain commands retain their existing
result checks. MY-19 still requires owner-accepted output parity before splitting
work across variants. CUDA-only third-party software gets a documented hard
requirement, not advice to pretend it supports XPU or Metal. Authors own packages,
algorithms, accuracy and checkpoint compatibility (MY-12).

On entry submission/registration and extension registration, emit a non-blocking
portability warning when only one platform/backend is covered while inventory
offers alternatives, or when declared coverage leaves observed targets out.
Show covered/missing targets, stated hard constraints and the next useful action.
Distinguish absent recipes from unqualified recipes. A recorded hard requirement
explains the limitation without rejecting legitimate single-platform work.
Registration remains non-executing; warnings neither widen targets nor change
frozen identity. An unavailable inventory produces an explicit unknown-coverage
notice rather than a false all-platforms pass. Advice contains no credentials,
raw host paths or private source; agents sanitize it before public issue filing.

## Scarce-machine yield

Keep MY-18 placement: prefer compatible free capacity and the least valuable fit;
an otherwise idle scarce host still runs portable work. Add one narrow relocation
case to the shared planner for finite batch commands, including native sidecars:

1. A ready queued demand has a documented hard requirement and exactly one
   structurally eligible **physical host H** after target/backend/toolchain,
   capacity, qualification, checkpoint and
   owner placement constraints. Determine this ignoring transient occupancy;
   another compatible host being busy, stale or maintained does not manufacture
   uniqueness. Unknown compatibility cannot prove uniqueness. A missing variant,
   convenience host pin, speed preference or host-value score is not evidence
   of a hard software requirement.
2. Portable work occupies resources H needs. “Portable” means the same frozen
   job has another eligible host, using either the same qualified variant or
   another qualified variant with equivalent outputs and compatible checkpoints.
   For plain commands, require another proven target/environment and result-check
   contract. A target label alone, CPU hardware alone or installed packages alone
   proves neither portability nor resumability.
3. Require an alternate host that can admit the victim now under resources,
   environment readiness, owner policy, freshness, maintenance and fairness.
   Demand must be at least as high in configured owner tiers as the victim;
   experiment demand never displaces production, even under a custom policy.
   A higher tier continues through existing priority preemption first. The new
   exception permits safe same-tier yield; it does not reorder unrelated demand.
4. Choose only enough victims to make H fit, retaining existing stop-count and
   lost-work comparisons. Resumable work must have a durable, destination-readable
   checkpoint with author-declared cross-target resume compatibility. Preserve
   every committed generation. For non-resumable experiment work only, propose an
   explicit `preemptible: true` opt-in meaning disposable execution can restart;
   this is a new field, not permission inferred from the class. Non-resumable
   production and non-opted-in experiments wait. Prefer cooperative movement;
   checkpoint incompatibility or missing transport yields a visible hold.
5. Persist demand/victim/attempt/destination intent and reason `scarce_host_yield`,
   then reuse stop, checkpoint, exact cleanup and same-job requeue mechanisms.
   Preserve job ID, submission key, frozen variant choices, accepted outputs and
   retry budgets; increment the existing displacement accounting. Route the next
   attempt to a revalidated qualified alternative, excluding H while that demand
   needs it. This is sequential stop/resume, not live process or GPU-state transfer.

Same-tier yield must not turn the priority stop's forced-kill deadline into a
claim of successful migration: resume only a verified complete generation. A
cooperative victim that cannot finish its final save is held with its checkpoint
state visible; only an opted-in disposable experiment may restart from scratch.
Already committed progress is never discarded. Existing stop/cleanup escalation
still applies, and uncertain release blocks admission on H. Save duration and
resume cost are measured under the checkpoint contract, not a new fixed timeout.
The checkpoint prerequisite prevents lost durable work at the cost of holding
demand when portability cannot be established.

Recheck destination admission and all exact-attempt/cancellation fences before
stopping and before dispatch: no advisory projection reserves capacity. If the
destination disappears after stop, keep the victim durably queued with a reason;
never launch it on an unqualified host or lose its saved work. Demand cancellation
removes its routing exclusion; a stop already delivered still completes cleanup.
Completion observed before stop delivery wins. Persist/recover intent through
queue/worker restart and reject stale outputs. Reuse the existing grace of runtime
after two displacements to prevent immediate repeated stops; no new cooldown.
Status explains the hard requirement, chosen alternative, checkpoint readiness,
stop/release state or why yield cannot happen.

This adds no separate migration daemon or host takeover. Existing higher-tier
preemption, including its established non-resumable restart rules, remains intact;
the new same-tier exception cannot broaden it. Model calls, pinned engines and
environment builds retain existing priority handling; replaying streaming output
or migrating service state is outside these finite-command phases. All I/O and
checkpoint verification stays outside `q.mu`, followed by fence rechecks; queue
decisions use the injected Clock.

## Small PRs and acceptance

Parent: [#324](https://github.com/1kuna/mycelium/issues/324). Each phase gets a
focused behavior check and quick gate; full runs once per integration batch.
Use fake inventory/Clock and real CPU commands in `myce dev`; native accelerator
qualification and consumer cutover remain separately authorized.

1. **[Live portability report (#325)](https://github.com/1kuna/mycelium/issues/325).** Add the shared inventory/entry projection and
   text/JSON command. Accept when changing synthetic inventory changes advice
   without a code/config fleet list; target/lock/build/qualification gaps are
   distinct, missing inventory stays unknown, and hard CUDA constraints are
   separated from absent variants. No execution or implicit registration.
2. **[Warnings and agent journey (#326)](https://github.com/1kuna/mycelium/issues/326).** Use that report at registration/submission;
   update skill/setup/extension guidance and a short user-facing prompt under
   #197/#198, coordinating #254's reader migration. Accept when a one-platform
   fixture warns with concrete steps, a genuine hard constraint is explained,
   registration executes nothing, and a fresh CPU fixture reaches a checked
   artifact using explicit alternatives. No unsupported qualification claim or
   no-operator environment claim while #251/#252 remain pending.
3. **[Yield planning and explanation (#327)](https://github.com/1kuna/mycelium/issues/327).** Add a read-only candidate projection to
   the existing planner/status. Accept when fake-clock tests identify unique H
   and qualified alternate capacity; reject busy-only uniqueness, unqualified or
   checkpoint-incompatible alternatives and production victims for experiments.
   Show blocked reasons and deterministic minimum useful victims without stops.
4. **[Safe yield and resume (#328)](https://github.com/1kuna/mycelium/issues/328).** Wire accepted plans into existing stop/requeue,
   add the explicit experiment opt-in and resume-compatibility metadata with
   strict validation, freeze them in intent, and update the approved spec.
   Accept when a real CPU dev command on simulated scarce H saves a generation,
   resumes on another host and produces one verified accepted result while the
   hard-bound demand starts on H only after exact release. Cover restart/stale
   reports, demand cancellation, a lost destination, failed save/transport holds,
   an opted-in experiment restart, production protection, and unchanged ordinary
   preemption/retry accounting. No saved generation or accepted result is lost.

Phases 1 → 2 and 3 → 4 can progress as independent tracks after plan acceptance.
Phase 4 publishes the metadata consumed by phase 3's internal evidence seam;
until then unknown resume compatibility is a hold. Reuse #250 rather than
reimplementing entries/qualification, and leave #251/#252's conditional scope
with those issues.

**Current state:** ruling and implementation proposal only; source, not fleet
behavior, inspected. **Next owner:** orchestrator accepts the phased plan and
assigns the two tracks; each phase remains open until its behavior criteria pass.
