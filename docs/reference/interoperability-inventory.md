# Interoperability inventory

[Issue #1849](https://github.com/1kuna/mycelium/issues/1849) asks what Mycelium can take off the agent's hands for each job. Count target lists, pins, locks, variant declarations and manual recovery steps the agent has to add or change. Reused entries and locks amortize authoring across submissions, so job counts measure exposure. Actual edits, agent minutes, unique authored entries, the full extension catalog, reasons hidden by status redaction and historical Darwin slot availability are unknown.

## Job and extension constraints

**Measurement.** One successful installed-CLI `myce status --history --json` read at 21:57 UTC on 2026-11-20 returned 12,648 retained records, including 109 with `spec.extension_check`. The table uses the other 12,539 records, including 6,815 submitted in the preceding seven days. It counts unique target tuples per record from `placement_intent.targets`. The 18 records with unknown intent are shown separately. See [placement intent and requirement reason fields](../status.md#generated-field-inventory).

| Authored intent | Retained | Last seven days |
| --- | ---: | ---: |
| One exact OS/architecture/backend target | 4,748 | 2,539 |
| One OS, any architecture explicitly allowed by a partial target | 1 | 0 |
| One OS, multiple listed architectures | 3,914 | 2,533 |
| Multiple OSes | 1,746 | 457 |
| No target list, known intent | 2,112 | 1,268 |
| Unknown intent | 18 | 18 |

- Of 8,663 one-OS records, 7,200 used CPU targets, 1,186 XPU, 267 CUDA and 10 Metal. Among the 7,200 CPU records, 7,112 were outside extension snapshots. In the last seven days, 4,214 such CPU records had one OS: 2,909 with submission-sourced environment coverage, 1,227 with catalog coverage and 78 with no required environment. The snapshot exposed no retained `command_discovery` projection. These counts bound the audit for removable lists. Lock coverage, application dependencies and parity must be checked before widening any entry.
- There were 593 named-host pins, 100 other allowlists and 1,233 exclusions. Of the pins, 474 had at least one authored host reason and 119 had none. Status exposes reason presence, so the specific rationale and whether it was necessary are unknown. Of the jobs with known intent and no target list, 86 still required a single accelerator backend (43 CUDA, 29 Metal and 14 XPU). Source: `placement_intent.host_constraint`, `required_backends` and `requirement_reasons`.
- The 2,371 non-check jobs with extension-snapshot environment coverage all recorded one OS. Of these, 597 had a single exact environment target and 1,774 had multiple targets within that OS. Workers exposed 320 extension report rows across 118 digests. Report targets may include missing variants, and report diagnostics can redact the reason, so the count of distinct registered single-platform extensions is unknown. Source: `placement_intent.environment`, `machines[].extension_reports` and [local extension report construction](../../internal/myce/extension_local.go).

**Documented reasons.** Native Darwin CGO and an iOS Simulator SDK are hard platform requirements for the respective work. Accelerator-specific software requires its declared backend until another implementation passes the applicable checks. Conversely, two reported CPU entries listed Linux on both architectures and excluded Darwin at the authored predicate. Their actual Darwin support and the author's reason were unknown. Constraints elsewhere in the snapshot do not establish the same causes. [Development gate](../development.md#laptop-test-gates) · [#1833](https://github.com/1kuna/mycelium/issues/1833) · [MY-26/MY-27](../DECISIONS.md#my-26) · [#853](https://github.com/1kuna/mycelium/issues/853)

## Native qualification

**Measurement.** Of 109 retained qualification job records, 38 succeeded, 57 failed and 14 were cancelled. Count each job once by terminal `command_started` and `execution_cause`. These counts exclude offers refused before a job record existed and do not identify repeat attempts. At snapshot time, three queued parent jobs had a `native_qualification` hold. Source: same history snapshot.

| Final check outcome | Cause | Count |
| --- | --- | ---: |
| Failed before command start | Staging (one memory-capacity predicate, 32 without a finer cause) | 33 |
| Failed before command start | Launch authorization (two launch refusals, one admission change) | 3 |
| Failed before command start | Resource limit | 3 |
| Failed before command start | Unknown cause | 7 |
| Failed after command start | Command exit, exact payload cause unknown in status | 8 |
| Failed after command start | Resource limit | 2 |
| Failed after command start | Unknown cause | 1 |
| Cancelled | Eight without a typed cause, three resource limits, one I/O error, one admission change, one command cancellation | 14 |

**Incident evidence.** #1839 describes two checks for one Darwin demand, one executed failure with Command Line Tools selected and one prelaunch authorization refusal. A third worker correctly refused the class, even though the hold incorrectly promised a check there. #1833 also documents a consumer app build failing because a PATH presence probe accepted the Xcode shim, a separate failed attempt assuming a fixed application path and one reported client exit 69 with cause unknown. These reports overlap and cannot be added to the snapshot totals. The fixes merged in #1837 and #1843. Deployment and recovery for the affected demand were not verified by this inventory. [#1833](https://github.com/1kuna/mycelium/issues/1833) · [#1839](https://github.com/1kuna/mycelium/issues/1839) · [#1837](https://github.com/1kuna/mycelium/pull/1837) · [#1843](https://github.com/1kuna/mycelium/pull/1843)

## Platform paths and test capacity

**Source measurement.** At `f94d936fa`, compare repository Go files selected by `CGO_ENABLED=1 go list -e -json ./...` under Darwin arm64 and Linux amd64/arm64, using `GoFiles`, `CgoFiles`, `TestGoFiles` and `XTestGoFiles` across eight packages. Both Linux architectures selected identical file sets. This checks source selection. Native compilation and execution are separate checks.

| Runtime target | Production Go files | OS-exclusive production files | Test files | OS-exclusive test files / top-level test declarations |
| --- | ---: | ---: | ---: | ---: |
| Darwin arm64 | 516 | 22 | 866 | 16 / 26 |
| Linux amd64 and arm64, per target | 518 | 24 | 872 | 22 / 35 |

Darwin paths cover libproc RSS, process ownership and signals, Seatbelt isolation, listeners, APFS cloning, storage and service lifecycle. Linux paths cover cgroup/request memory, namespaces, process and custodian handles, socket inventory, filesystems and accelerator activity. The tree also has 43 production files and 69 test files referencing `runtime.GOOS` or `runtime.GOARCH`, including code already counted above. The native matrix also covers runtime branches in common files, so 26 Darwin-exclusive declarations are a lower bound on native coverage. Current wall and CPU time for those 26 tests alone are unknown. A paired shared model-journey test retained its assertions and reduced mean root wall from 5.770 s to 1.735 s on Darwin and from 3.785 s to 1.005 s on Linux (two samples per condition per OS). That measures one shared fixture. [#1840](https://github.com/1kuna/mycelium/pull/1840) · [Gate tooling](../development.md#laptop-test-gates)

**Queue measurement.** The gate names below select the ordinary native profiles. The cohort is records submitted in the preceding seven days. Job wall is finish minus Started, summed over records with both timestamps, including failed or cancelled runs. Start delay uses `start_latency_seconds` among started records, with the p90 taken at the lower observed index. Retained history held 329 Darwin quicks and 271 Darwin fulls in total. This does not include on-laptop gates or arbitrary consumer suites. [Gate profiles](../../.mycelium/profiles) · [Main full gate](../../.github/workflows/main-full.yml)

| Native gate | Submitted | Timed finished / job wall hours | Median finished job wall | Started / median and p90 start delay |
| --- | ---: | ---: | ---: | ---: |
| Darwin quick | 205 | 125 / 8.84 | 2.3 min | 126 / 1.8 and 76.6 min |
| Darwin full | 72 | 63 / 64.96 | 55.0 min | 64 / 3.5 and 205.5 min |
| Linux quick | 219 | 189 / 16.85 | 2.8 min | 189 / 0.25 and 11.8 min |
| Linux full | 76 | 75 / 41.34 | 27.9 min | 76 / 0.27 and 30.8 min |

Of three healthy Darwin inventory records at capture, two were non-interactive (18 effective logical cores, six free in aggregate) and one was shared-interactive and excludes test admission. At that instant three Darwin quick jobs were held for capacity, and one quick and one full were running. The 73.8 Darwin job-wall hours in the cohort overlapped and omit attempts without both timestamps. They cannot yield historical host utilization or compatible slot-hours. CPU is only one admission dimension. Earlier evidence records seven Darwin quicks blocked by memory and a separate single full that held its then-only eligible executor for two hours. The Darwin delay tail warrants preserving native capacity, but its causes cannot be apportioned from this sample. [Shared-host admission](../design-proposal.md#shared-interactive-host-profile) · [#1419](https://github.com/1kuna/mycelium/issues/1419) · [#991](https://github.com/1kuna/mycelium/issues/991)

## Consumer reports and relays

**Documented sample.** Seven cases across the seven linked issues below, grouped into six rows. This is a targeted issue-history sample. Unique consumers, unreported workarounds and their frequency are unknown.

| Cases | Per-job work or compatibility limit reported |
| ---: | --- |
| 1 | Two CPU entries from a consumer project enumerated Linux targets by architecture. Actual Darwin support was unknown. [#853](https://github.com/1kuna/mycelium/issues/853) |
| 1 | A consumer project had to express full Xcode and simulator SDK health beyond executable names. A guessed fixed application path failed. The existing reusable entry baseline is the documented way to express the SDK requirement. [#1833](https://github.com/1kuna/mycelium/issues/1833) · [#1839](https://github.com/1kuna/mycelium/issues/1839) |
| 2 | A consumer project reported a macOS packaged binary linked against host-specific libraries and, separately, a Linux packaged binary missing a codec. The packaging fixes were not measured. [#866](https://github.com/1kuna/mycelium/issues/866) |
| 1 | A consumer project's locked CPU speech recognizer produced blank output on one amd64 worker while arm64 and macOS produced normal transcripts. Cause unknown. This requires output qualification even when a lock installs. [#913](https://github.com/1kuna/mycelium/issues/913) |
| 1 | A consumer project waited after read-only resolve promised a native check that needed submitted demand. That handoff sent the agent to the wrong next action. [#1678](https://github.com/1kuna/mycelium/issues/1678) |
| 1 | A release worker historically listed both Linux architectures after an OS-only target was refused. Current history contains a partial OS target. [#480](https://github.com/1kuna/mycelium/issues/480) |

## Ranked plan

**Inference.** Rank by fields and manual steps removed per applicable job, weighted by observed exposure and divided qualitatively by implementation and operating cost. This sample does not establish agent-hours saved. Reuse the approved native runtime, keep hard restrictions binding, and require MY-19 parity before widening production. [Approved portability design](../design-proposal.md#portability-advice-and-variant-eligibility)

| Rank | Candidate | Effort removed per job | Cost and risk |
| ---: | --- | --- | --- |
| 1 | Finish live adoption of #853, then assess project-carried lock support | Removes an OS/architecture list for each opted-in portable CPU entry using an eligible lock. The project still owns dependencies and asserts whole-payload portability. #856 and #861 already merged named managed recipe support. | Low for a readback/adoption slice, medium to extend to project-carried locks. A lock describes dependencies. Payloads, sidecars and frozen outputs can still be incompatible. Keep unknown coverage explicit and preparation failures local to their target. [#853](https://github.com/1kuna/mycelium/issues/853) · [Discovery implementation](../../internal/myce/command_discovery.go) |
| 2 | Put MY-26's existing guidance at the job's next action | Saves manual inventory inspection and wrong retries or pins. Show exactly one next authoring step for a target restriction, missing lock coverage, missing variant, build failure or check result. | Low to medium. Reuse existing `myce portability` and resolve evidence. An apparent alternative is only a candidate until the applicable checks pass. Distinguish capacity and infrastructure from compatibility. [#325](https://github.com/1kuna/mycelium/issues/325) · [#1678](https://github.com/1kuna/mycelium/issues/1678) |
| 3 | Prove one reusable command across existing native variants | Could remove duplicated commands/entries when only device selection and package builds differ. Keep exact platform locks and output baselines. | Medium. Reuse assigned-device selection and measure the authoring diff on one payload. Backend behavior can diverge, as a shared lock already did for CPU outputs. [#1491](https://github.com/1kuna/mycelium/issues/1491) · [#913](https://github.com/1kuna/mycelium/issues/913) |
| 4 | Reuse build or activation hooks for one repeated packaging problem | Converts a repeated per-platform bootstrap or packaging workaround into one maintained recipe. Start with a concrete case such as bundled native libraries. | Medium. Existing Pixi hooks and bundled inputs may suffice. Trusted hooks add cache-identity, reproducibility and failure-attribution work. Measure the packaging change before adding a generic interface. [Project-supplied environments](../design-proposal.md#project-supplied-environments) · [#866](https://github.com/1kuna/mycelium/issues/866) |
| 5 | Linux VM trial on one spare, non-agents Mac | A Linux CPU job can keep its existing Linux configuration and reach more CPU capacity. There is no gain in native Darwin coverage. A VM could delay native tests if it competes for their RAM or CPU. | Medium recurring cost. The existing Lima test leg proves a local path. With separate approval, run an identical frozen Linux CPU job and native Darwin control with the guest idle and active. Compare completed output, runtime, start wait, CPU/RAM/disk and native queue delay. Verify isolation, avoid counting parent and guest capacity twice, and remove the trial guest if the gain disappears under contention. [Local Linux VM](../development.md#local-linux-vm) |
| 6 | WASI experiment for a small self-contained CPU payload | A single artifact could replace native builds and OS lists for that payload class. No measured eligible workload in this sample. | Medium to high per workload. Experiment through a native command wrapper with one immutable input and an output parity check on each OS. Count author changes and runtime overhead. Stop if dependencies need unsupported native APIs, libraries or accelerators. Keep it an experiment under the current [runtime design](../design-proposal.md#product-decisions). |

### First two implementation issues to propose

1. **Prove and extend CPU target discovery from frozen locks.** Use #853 as the parent. Reconcile its merged named-recipe path with one representative opted-in portable CPU entry and record the before/after authoring fields. If the observed gap is a project-carried Pixi lock, add a scoped slice to feed its selected immutable member coverage to the existing planner. Test fake Darwin and both Linux architectures, explicit restriction, sidecar intersection, unknown lock coverage, a failed preparation followed by a viable target, replay and the production parity boundary. Success means the eligible entry ships one lock and zero authored target rows, with a clear resolve result on an unsupported target. Deployment and live execution require separate authorization.
2. **Show one compatibility action at resolve and status.** Extend existing MY-26 surfaces and reconcile #1678 and the merged #1837/#1843 behavior before coding. Test a hard Darwin SDK requirement, a provably environment-covered CPU alternative, unknown coverage, a released prelaunch refusal and a real failed check. Success means each case gives the agent one evidence-backed action and a source without a host-specific recipe, manual inventory comparison or misleading resubmission. Record before/after authoring or recovery steps for those fixtures.
