# One adapter and value-aware placement

Design proposal for [#192](https://github.com/1kuna/mycelium/issues/192) and
[#193](https://github.com/1kuna/mycelium/issues/193), 2026-10-25.
**Pending decisions, not an approved spec amendment.** The
[approved design](design-proposal.md) remains authoritative. [MY-17](DECISIONS.md)
at `2567caf6` establishes the one-instruction-file vision only; merging profiles
and extensions and removing operator registration are recommendations below.
MY-18 requires least-value fitting placement; MY-19 requires accuracy parity
before speed-oriented splitting. Questions about job files/toolsets add no ruling.

## Today: 20 actions, 20 concepts

This is a count of the documented journey, not a timed fleet experiment. Scope:
an existing repository's first finite CPU command with locked dependencies and a
T3 completion wake, on an already enrolled fleet with installed client authority.
Machine enrollment and ordinary software development are prerequisites on both
sides. No retries, optional rehearsal, or later agent/CI cutover are counted.
An action is a separately performed author, operator, or receiver operation;
generating a hash during the handoff is included in that operation.

From [agent offload](agent-offload.md), [environments](environments.md),
[extensions](write-an-extension.md), [servers](guide-servers.md), and the
[offload skill at b4ea77c8](https://github.com/1kuna/mycelium/tree/b4ea77c8/skills/mycelium-offload)
(202-line skill + 140-line setup guide + 56-line binding guide = **398 lines**):

1. Inventory command, prerequisites, inputs, outputs, and evidence of success.
2. Choose profile, qualified extension, or long-lived server entry.
3. Write the profile: source, argv, exact targets, resources, class, I/O, check.
4. Write the prerequisite/result wrapper when the command has no suitable one.
5. Write the environment manifest.
6. Generate and retain its lock.
7. Hand manifest, lock, hashes, and catalog entry to the operator.
8. Operator stages the recipe files beside the catalog.
9. Operator adds the catalog entry, preserving existing entries.
10. Operator restarts the queue with that catalog.
11. Gitignore receipts.
12. Commit the intended source and new setup files.
13. Create this thread's completion webhook schedule.
14. Store its private binding and implement its notifier executable.
15. Verify notifier configuration with `--check`.
16. Submit `myce run --profile NAME --notify NOTIFIER`.
17. Read terminal/armed/delivery state; retain the key and job ID for recovery.
18. Receive/deduplicate the wake and reread authoritative job state.
19. Download accepted outputs with size/hash verification.
20. Inspect logs and the actual artifact against the project's acceptance check.

**20 actions**, including **3 operator actions** (8–10), one handoff (7), and
three wake-setup actions (13–15). Action 4 is conditional: a command with an
existing suitable wrapper needs **19**. The extension/server alternatives in
action 2 are branches, not additional mandatory CPU steps: extensions add
frozen-folder registration and per-target qualification; servers need a model
entry, weight identities, health/lifecycle configuration and endpoint integration.
They cannot honestly be included in one universal serial step count.

Concept ledger, at the same contract granularity before and after:

- **10 work contracts retained:** source snapshot; exact OS/arch/backend targets;
  resource envelope; project/class; environment manifest; environment lock;
  input/output mapping; result check; accepted-attempt/hash verification; retention.
- **3 competing descriptions today:** profile; extension; server catalog entry.
- **7 setup/recovery contracts today:** environment catalog; registration/restart;
  submission key; receipt; observer/delivery state; thread webhook binding; notifier.

Total **20**. This counts concepts the agent must learn, including the route
choice, rather than every YAML field or existing Git/shell skill. The proposed
adapter replaces the three descriptions with **one**, and hides the seven setup
contracts: **11 author-facing concepts**, of which only **one** is a Mycelium
description to author. These counts do not claim that locking or correctness
stops needing thought.

## Recommended adapter, pending Zach's choice

**Description options.** (A) Merge the useful profile fields into the composed
extension contract and call its project-facing file the adapter. (B) Keep both
contracts and generate a profile from an extension, or an extension from a
profile. B offers a smaller first CLI change, but generates a second artifact
with ownership/drift rules; profile-to-extension generation must additionally
invent or request targets, checks and qualification. **Recommend A:** one
authored source of truth, lowered directly into the existing extension snapshot
and `RunSpec`, with no second executor or generated editable profile.

The proposed `.mycelium/adapter.yaml` is an agent-written, machine-readable
instruction file, not prose interpreted by a model at execution time. It carries
run argv, locked environment, source and explicit inputs, required outputs and
their check. Exact per-target variants, resource needs and optional sidecars,
references, serving lifecycle or split declarations appear only when needed.
One software package can name several commands/stages in that file; adding a
stage must not require another profile or catalog. Existing software scripts and
lock files are reusable application assets, not additional Mycelium setup files.
For a genuinely single-file package, embed small manifest/lock bytes; referenced
files are frozen by the submitting client with the adapter. Never require a
submitter's path to exist on the queue or a worker.

**Environment options.** (A) Retain operator catalog approval, automating the
request/handoff. This keeps centralized package selection but cannot guarantee
one file plus one command without an operator wait. (B) Authenticated submission
carries a content-addressed locked recipe; workers prepare it on demand.
**Recommend B:** remove per-project registration/restart while retaining fleet
enrollment, permissions, resource policy and storage provisioning. This extends
the existing trust in submitted commands to their builds; it is not a sandbox
or permission to install drivers, services or solve mutable dependencies.
Builds, jobs and notifiers never receive the fleet token.

The queue verifies the submitted bundle's bytes, digest, supported recipe/target
and complete lock/manifest identity, then persists the immutable snapshot with
the job. Workers use existing uv/pixi/build backends, frozen/locked installation,
content-keyed caches, single-flight preparation and ready publication. Workers
verify fetched bytes; pixi rejects manifest/lock disagreement without re-solving.
Other backends need equivalent strictness or a clear unsupported-recipe error.
The queue verifies worker readiness/check evidence against that exact recipe,
target and hardware fingerprint; hashing a lock does not prove semantic quality
or that arbitrary build commands are reproducible. Missing network/packages,
disk space or prerequisites produce visible preparation failures, never a local
fallback. Preparation alone cannot qualify a variant. Run its qualification
check under normal admission on first compatible demand, before useful work;
persist existing exact qualification evidence. Finite plain commands retain
their per-run result check; merging descriptions must not require a GPU-style
native baseline for an ordinary CPU test suite. Native extension variants retain
their baseline requirements. No operator SSH/check step.

**Migration, fix forward (MY-03).** Convert maintained profiles into adapters,
moving `source`, `run`, `needs`, `targets`, I/O and `check` without changing their
meaning; snapshot named catalog recipes into the adapter's environment inputs.
Existing extensions supply the variants, environments, assets and checks; expose
them through the same file rather than reimplement them. Port server lifecycle
declarations into an adapter serving section that uses existing model slots,
endpoints, pins, weight manifests and no-replay rules. Keep separate qualification
checks and per-run result checks explicit internally; a profile success marker
does not manufacture a native baseline. Convert active callers/docs/skill in
each slice, then remove the replaced authoring path. Preserve accepted job/key
snapshots and history unchanged. No runtime format fallback, alias registry,
dual-write period or automatic rollback layer.

Keep internal: SQLite submission keys/cancellation, private receipts and exact
attempt fencing; source freezing, file transport/retention and verified result
publication; environment caches/locks; qualification; the host ledger, planner,
tiers, fairness, preemption and participation; native groups and server engines.
Receipt state belongs in private client state, keyed by repository and operation,
so each repository need not add a gitignore rule. No peer network, leases,
replicated registry, new scheduler or required Docker. Snapshot under `q.mu`,
perform hashing/transfers/checks outside it, then recheck identity and fences
before the small SQLite commit. The same rule applies to environment admission.

## One command and its defaults

Proposed `myce run` discovers the adapter and runs its default entry. With an
adapter, `myce run -- CMD` supplies literal argv under its declared environment,
source, target and result contract; a command incompatible with that contract
must fail, not borrow a qualification or silently append/duplicate argv.

Without an adapter, recommend retaining the plain-command escape hatch:
CPU-only, one logical core, `memory: auto` under existing exploratory admission,
class `experiment`, project `default`, no resume, no GPU selection, installed
tools, command exit as the check, no implicit source/input/output upload.
It runs on the least-value currently fitting admitted worker and waits in the
foreground, streaming status/logs. Missing software fails clearly; this does not
promise portable dependencies or qualify a package. Users wanting repo files or
verified artifacts need the adapter. Alternative: refuse every adapter-less
command until an agent writes one; stronger reproducibility, weaker ad-hoc use.
The recommendation changes today's fixed 1 GiB default to existing auto memory,
not an invented universal budget. An adapter can declare its actual resources.

The adapter path freezes itself plus the chosen source/inputs, prepares/checks,
runs and returns the verified accepted result in one invocation. Default source
is the committed Git tree; refuse dirty application source with an actionable
message rather than silently uploading it. The new adapter itself need not be
committed before freezing. An explicit directory/input selection covers non-Git
projects. Default foreground execution retrieves declared outputs to private
client result storage and reports their location. It exits successfully only
after command, semantic check, accepted-attempt publication and downloaded size/
hash verification. Large outputs may explicitly remain remote with verified
references; that is a distinct reported result, not a claim of local download.

An explicit repeat creates a new operation, so identical tests can run again;
reconnecting/recovering an interrupted invocation reuses its saved internal key.
Expose a resume/repeat action, not key generation. Keep existing cancellation
and ambiguous-submit reconciliation. Async is optional: the harness owns its
thread binding and supplies it privately, reusing the existing observer/notifier
protocol; no T3 dependency in Mycelium and no per-project webhook recipe. Never
claim a wake is armed without delivery configuration. Without a supported
harness binding, wait in the foreground or return an explicit hold. This makes
the two-action CLI journey independent of future harness integration.

## Value-aware placement

Recommend a **scarcity vector, not hand-set machine prices**. Inventory already
measures accelerator backend/usable memory, host RAM, topology and allowances.
Build capability bands at distinct measured usable capacities: each accelerator
backend and its memory tiers, and host-memory tiers above the fleet minimum.
Count physical hosts supplying each band, not devices or worker rows. Use fresh,
known inventory and owner allowances; unknown capabilities confer no invented
capacity or cheap ranking. Prefer a known-value fitting alternative; if the
only fitting host has unknown value, explain it and retain normal admission.
Qualification still determines whether this particular adapter fits.
Unified host/device RAM remains one physical pool, never summed as two supplies.

For each host, count its bands supplied by only one host, then two, and so on.
Compare those counts lexicographically, **fewer rare bands first**; equal vectors
prefer smaller usable RAM, then existing measured completion/affinity and stable
target identity. This is an explicit proposed ordering, not a measured dollar
value or an exchange rate between GiB and accelerators. It is cheap to compute
once per inventory/policy snapshot. Temporary occupancy does not redefine value;
it removes a candidate from immediate admission. A 16 GiB CPU host beats an idle
large-memory accelerator host for a fitting CPU command; accelerator-only work
still needs its qualified accelerator, and large-memory work still needs RAM.

Order of decisions: eligibility/accuracy/owner policy first; existing priority
tier and per-resource-family project deficit next; existing free-capacity-before-
preemption and useful-victim/lost-work comparisons next; value ranks otherwise
admissible targets for the selected work. It must not select another project to
improve a value score, change fair-share charges, or preempt equal-tier work.
Use the existing shared planner across pull requests, status and simulation:
an expensive worker cannot win merely by checking in first while a cheaper
currently admissible target exists. Do not reserve an idle cheap host or wait
for a busy/offline one. If only the scarce host is admissible, use it; there is
no speculative reservation for work that might arrive later.

Daily-use idle/power/heat checks, maintenance, unknown telemetry and cleanup
custody remain hard eligibility fences. Owner activity removes capacity even
when it would otherwise be cheapest; no extra cooldown or owner interruption.
Memory forecasts grant no capacity and cannot release an old attempt's charge.

**Cost versus speed is a product choice.** Recommend default preservation for
ordinary/CI commands; an adapter's explicit speed goal uses #107's measured
completion horizon over all eligible machines, with value breaking equal-cost
plans. Always-value-first would preserve accelerators but defeat MY-19's speed
goal; always-fastest would send CI to scarce hardware. No guessed percentage
trade-off or fixed batch size. Actual deadlines may require a feasible faster
plan; explain that choice. The optional goal and deadline behavior need approval.

Reuse #107's exact recipe/shape/component concurrency curves, safe measured
counts, memory high-water marks, wave retry and keyed gather. Stage inputs become
runnable through accepted-output dependencies; generic workflow expansion stays
with #107 rather than introducing another orchestrator here. Splits are authored,
never inferred from arbitrary programs. **MY-19 adds an eligibility condition:**
every participating variant must match the same owner-approved accuracy reference
for that stage, model, precision and shape. Separate platform-native baseline
passes alone do not establish cross-machine parity. Bind parity evidence to
adapter/environment/backend/hardware/check identities and invalidate it when
those change. An agent resolves discrepancies, rather than widening tolerances
or substituting a different model to gain speed. Exclude a failing variant and
report why; an accepted reference variant can continue unsplit. Use every
eligible parity-qualified machine where measured curves reduce wall time,
including smaller tails; non-beneficial parallelism is not a speed improvement.
Missing curves retain existing serial behavior and visible unknowns. Native
accuracy/speed claims require native evidence, not fake inventory.

## New path and delivery

Assuming the recommended options are accepted, the same-granularity author
procedure becomes **6 actions**:

1. Inventory command, prerequisites, inputs, outputs, and evidence of success.
2. Write the adapter and its result contract, reusing application scripts.
3. Write/reuse the application environment manifest in or referenced by it.
4. Generate/reuse the immutable lock and include its bytes in the snapshot.
5. Commit the intended application source, or explicitly select directory inputs.
6. Run `myce run`; freeze, prepare, qualify where required, run, check, retrieve
   and verify happen inside that invocation. Return success or an actionable
   failure, with no host choice or manual key/receipt/notifier work.

**20 → 6 author actions; 20 → 11 author-facing concepts; 3 → 0 operator actions.**
The intended **user surface is 2 steps: have the agent write one adapter, then
run one Mycelium command**. Inventory, lock creation and source commits remain
the agent's authoring work (1–5); existing locks/committed source shorten both
journeys. One additional Mycelium file is required; do not count an operator
task as hidden authoring. A dirty source, absent lock, missing required native
reference, unsupported backend, offline dependency or missing authority is an
honest failed prerequisite, not a completed first result.
The skill can then teach author/run, interpret success/hold, resume/repeat and
retrieve optional large artifacts; remove project catalog/key/webhook tutorials.

Small independently releasable slices (no fleet access implied):

1. **Portable locked recipes.** Ship client-frozen recipe submission independent
   of adapter unification. Acceptance: `myce dev` accepts a submitter-only lock,
   with an unrelated catalog loaded and no queue restart; first demand builds,
   second demand reuses; corrupt bytes or mismatched locks fail; a fake-clock
   cancel/maintenance transition during preparation prevents launch. Risk:
   trusted builds widen package/network activity; retain budgets and strict locks.
2. **One adapter for finite work.** Lower merged fields through existing runner
   and on-demand qualification. Acceptance: a tiny CPU app with existing locks
   and a real output check gets a verified result via one file/one foreground
   command in dev; convert a profile and extension fixture, prove equal behavior,
   failed check/stale result rejection and unchanged fake-clock retry decisions.
   Risk: confusing baseline qualification with per-run success, or copying app
   files that drift; snapshot referenced source rather than maintaining copies.
3. **Finish foreground run.** Hide receipts/keys, add resume/repeat and automatic
   result retrieval. Acceptance: dev lost-reply/reconnect adopts once, deliberate
   repeat runs twice, corrupt download fails; plain-command defaults and dirty-
   source refusal match this note. Fake-clock cancellation still fences retries.
   Risk: duplicate side effects; retain existing identity/custody protocols.
4. **Optional harness binding.** Reuse observer/notifier interfaces with private
   harness-owned binding. Acceptance: two fake harness threads receive only their
   own completion, terminal replay needs no wake, failed arming cannot report
   success, and unbound execution waits. Risk: false wake promises; exercise
   observer loss and same-operation recovery through a dev command.
5. **Serving from the adapter.** Lower its serving section into existing server
   slots. Acceptance: a dev HTTP fixture retains readiness, model identity, pins,
   cancellation/cleanup and no-stream-replay behavior; fake-clock health loss
   retains custody until cleanup. Risk: endpoint/owner identity collisions;
   preserve existing immutable model configuration and policy intersections.
6. **Preserve scarce machines.** Add the proposed value ordering independently
   of adapter migration. Acceptance: dev/fake inventory puts fitting CPU work on
   the small host and GPU work on its accelerator; fake-clock cases cover cheap
   host busy/lost, owner return, fairness, higher-tier preemption, restart custody,
   and no artificial waiting. Compare existing planner decisions outside ties.
   Risk: fragmented capacity or added latency; expose ranking reasons and compare
   paired `myce sim` workloads before integration.
7. **Parity-gated speed plans.** Bind cross-platform parity to existing variants
   and let approved speed goals use current measured fan-out planning. Acceptance:
   fake variants with individually passing native checks but discrepant outputs
   cannot split; matched variants/curves fan out and retry only affected waves;
   changing fingerprints invalidates evidence. Fake-clock owner/loss/retry checks
   and paired simulations must improve makespan with unchanged production p95,
   zero added failures/unfinished work and complete keyed output coverage. Risk:
   sparse curves or unrepresentative references; report coverage, and keep native
   parity and measured full-fleet speedup as separately owned acceptance.

Dependencies: 1 → 2 → 3; 4 follows 3 and is optional; 5 uses 1–2; 6 is
independent; 7 uses 6 and existing #107 measurements/planning. Each slice is
usable without the later ones. Use focused laptop regressions and the repository
quick gate per implementation slice, one full gate per integration batch.
Fake inventory and injected `Clock`
prove decisions; real CPU commands in `myce dev` prove transfer/run/check behavior.
They do not qualify native hardware, package availability or application accuracy.

**Decisions for Zach:** approve merge versus generation (recommend merge), and
demand-carried locks versus operator registration (recommend demand-carried).
Also approve preservation-by-default with explicit speed/deadline selection
versus always preserving (recommend the former), and the adapter-less installed-
tools escape hatch versus mandatory adapters (recommend retaining the escape).
Only the workload owner can supply accuracy criteria/reference evidence when
none exists; agents can investigate discrepancies but cannot invent tolerances.

**Current state:** design only, based on rulings `2567caf6`; no behavior changed
or native evidence claimed. **Next owner:** orchestrator gathers these decisions,
records approved amendments in the sole spec, and scopes the above slices on
#192/#193 and parity/workflow follow-up on #107. Revisit at those decisions;
MY-18's independent placement slice need not wait for the adapter choice.
