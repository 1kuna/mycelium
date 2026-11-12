# Decisions

Owner: project maintainer (Zach). [The approved design](design-proposal.md) is the only product spec; this file records Zach's rulings as twelve principles. Each section states the current rule first, then Zach's words and dates as the source. Old MY numbers keep their anchors on the section that now holds them; the [index](#old-numbers) maps every number. Consolidated 2026-11-11 from MY-01 to MY-45 without changing any ruling. Agent defaults are kept apart and labelled. Exact source records, host details and deployment receipts stay in private local history; dates identify the original rulings, not new qualification.

The goal behind all of them, as relayed from Zach on 2026-11-11 when asking for this consolidation (paraphrase, not a quote): one general cluster in which the compute of any of Zach's machines is simply given to whatever work needs it.

## Current rulings

### 1. One open, general cluster
<a id="my-06"></a><a id="my-11"></a><a id="my-01"></a><a id="my-29"></a><a id="my-13"></a>

**Mycelium is one open, general-purpose cluster of machines. It serves any project and any kind of compute; nothing in the public product is specific to one deployment or consumer.**

- One mycelium is the cluster; a hypha is one machine, and its CPU, RAM and accelerators are components. A GPU is not a hypha, and a worker is not a separate mycelium. (MY-06)
- Mycelium is an open, general compute platform: general compute and third-party extensions across machine types, including serving, joining and daily-use participation. Any one consumer is an example, never the product definition. Check the vision before cutting a feature. (MY-11)
- Public docs and graphics describe generic machines and job kinds, not one private deployment. (MY-01)
- Mechanisms are generic and reusable. Deployment-specific production windows, host roles and project holds live in the local queue's durable policy or database, never hardcoded in this public repository; prefer learned or derived values where supported. Discrete accelerator memory and host memory are distinct needs; a unified-memory system shares one physical pool. (MY-29)
- Installation and normal operation work without Docker. Native processes, environments and sidecars are the one runtime. Managed Docker, which MY-13 kept as an explicit option for a concrete need, was retired on 2026-11-12 under [MY-49](#my-49) (an agent decision) after the last consumer moved its model jobs to native execution ([#1456](https://github.com/1kuna/mycelium/issues/1456)). (MY-13, MY-49)

<details><summary>Sources</summary>

MY-06 terminology ruling, 2026-10-21. MY-11 scope ruling, 2026-10-19 (#16, #17, #22, #69); it superseded job-only simplification. MY-01 general-not-consumer-specific ruling, 2026-10-23. MY-29, 2026-11-01, policy intent (not implemented learning or holds). MY-13 runtime ruling, 2026-10-19: Docker is optional; installation and normal operation must work without it, native processes, environments and sidecars are the default, and managed Docker commands remain an explicit supported option for a concrete need. Retirement of that option, 2026-11-12: orchestrator decision under MY-49, not a maintainer ruling.

</details>

### 2. Any software plugs in with one agent-written file; Mycelium does the porting work
<a id="my-17"></a><a id="my-22"></a><a id="my-12"></a><a id="my-27"></a><a id="my-26"></a><a id="my-20"></a>

**Any software joins when an agent writes one instruction file describing what it needs and which variants exist. Mycelium does as much of the porting and setup work as it can, so agents don't have to.**

- All software is unsupported by default. An agent plugs any of it in by writing one simple instruction file in the project; keep user-facing setup that simple. Profiles and extensions are one kind of file. (MY-17, MY-22)
- Extensions wrap existing software. The extension's author owns package versions and platform variants; Mycelium provides environments, build hooks, qualification and placement. No application rewrites, project-specific source changes, or curated application packages in Mycelium. (MY-12)
- A job or extension describes what the software needs and which variants exist (for example CUDA, MLX, XPU, CPU), never hosts. It names a host or platform only for a hard requirement, such as CUDA-only software or qualifying a specific device. Mycelium places work wherever a qualified variant fits (section 3). Every new variant passes output parity (MY-19) before production use. (MY-27)
- Agents can run any software on any cluster machine, except under hard requirements. Mycelium gives dynamic, general portability guidance derived from live inventory and extension/environment metadata, not a fixed fleet recipe, and says when adding a variant would widen placement or relieve a scarce machine (for example porting a pipeline to macOS, or running an LLM on MLX instead of CUDA). A single-platform implementation is a portability gap to explain and address. Mycelium takes on as much of this as possible so agents don't have to. (MY-26, MY-27)
- Agent-first. A user's agent installs the Mycelium skills, makes its software compatible and manages the setup as needs change. On a problem it files a sanitized issue with a provenance marker, and a triage bot sorts issues into Mycelium defects, extensibility blind spots and agent-side mistakes. Once Mycelium is stable, other projects' agents file issues instead of messaging the maintainer's orchestrator; until then direct messages are fine. In idle time the cluster studies its own telemetry to improve routing and to alert on cross-machine problems, poorly configured extensions and Mycelium-caused failures. Core functionality comes first; the maintainer's own workload is the test bed. (MY-20)

<details><summary>Sources</summary>

MY-17 vision statement, 2026-10-25 (#192). MY-22, 2026-10-27: “if you recommend merging them then i'm fine with it”. Agent reading (orchestrator, 2026-11-12): MY-44 covers project-carried lock files (#1163, #1297); operator registration is not required. MY-12 extension ruling, 2026-10-19. MY-27, 2026-10-31: “yes i like the variants model. whatever can simplify mycelium to handle all the work so agents don’t necessarily need to. and so things can run on as many machines as possible.” MY-26, 2026-10-29; records intent, not implemented migration or new platforms ([implementation proposal](https://github.com/1kuna/mycelium/blob/12bb6b7826153c38d356555b711c9ba4a11f856a/docs/design-run-anywhere.md)). MY-20 vision ruling, 2026-10-25 (#194).

</details>

### 3. Mycelium owns placement and learns it from its own measurements
<a id="my-31"></a><a id="my-07"></a><a id="my-18"></a><a id="my-19"></a><a id="my-30"></a>

**Mycelium alone decides where work runs, from its own measurements. It uses the least valuable machine that fits, keeps scarce machines for work only they can do, and moves work only through checkpoint/requeue.**

- Telemetry is integral. Mycelium learns from its jobs, devices, host setup, storage and memory topology (unified versus dedicated), and it alone decides which machine runs work, which and how many devices it uses, and when work moves. It improves those decisions from its own measurements. Agents state needs and variants, never placement. Host and device pins are a supported optional feature for situational needs (for example a dedicated box for CI runners or builds); the default is no pin. (MY-31)
- Place work on the least valuable machine that fits: work needing only CPU and time, such as CI, goes to an idle general-purpose machine; scarce machines (large accelerator memory, a unique accelerator) stay free for work only they can do. (MY-18) When queued work can run only on a scarce machine, portable work occupying it moves to another compatible, qualified host where possible through supported checkpoint/requeue; experiment-tier work may be preempted under its restart contract. Durable progress is preserved; stopping a process or losing a host is not migration. (MY-26)
- Use measured per-work-type capability to place and split compatible work across the shared pool. Where the owner wants speed, split each pipeline stage across the machines that can run it, in parallel. A machine or variant joins a split only after its outputs match the reference within the owner's accuracy criteria; agents resolve discrepancies before splitting is accepted. Multi-machine/gang execution stays deferred until its design and qualification land. The vision is not evidence that every workload or mode is implemented. (MY-07, MY-19)
- A multi-GPU host is one machine. Mycelium uses its GPUs separately (one job per card, experiments filling idle cards) or together (one job sharded across cards), whichever benefits the running projects most. This supersedes the earlier one-GPU-per-execution scope limit and, where it conflicts, the approved design's device-selection scope (an implementation task). (MY-30)
- Production priority uses the existing priority tiers and preemption, not host-wide exclusion, blackout holds or orchestrator-negotiated special cases. Existing consumer customer-window records remain unless Zach changes them. (MY-30)
- None of this relaxes MY-19 parity, exact-release custody before reuse, or the deploy guardrails in section 6; work moves only through supported checkpoint/requeue. MY-31 records intent, not implemented learning.

<details><summary>Sources</summary>

MY-31, 2026-11-02: “yes telemetry needs to be integral so mycelium can learn about jobs, cards, system setups, storage, memory, unified vs dedicated, etc. and picking the system, cards, amount of cards to use, when to transfer jobs, etc. is all on mycelium to figure out. truly a self improving, load balancing, local server cluster.” Pins: “i think pins are okay as a feature but definitely i wanna keep them as optional and situational like a single box for CI runners and builds or something for example”. MY-18 placement ruling, 2026-10-25 (#193). MY-07 vision ruling, 2026-10-21 (#107). MY-19 vision ruling, 2026-10-25 (#107). MY-30, 2026-11-02: “[the two-GPU host] is meant to be a dual card system. 64gb of vram. this whole "split the cards" thing is nonsense to me. we can either split the cards or shard the models across both cards. mycelium should understand that and handle it, whichever way is most beneficial to the running projects, that's the point. we need to stop with this whole [the production consumer] production paranoia stuff and get mycelium to a place where it can actually do its job for all my projects.” See [the dual-GPU research](research/dual-b70-and-xpu-variants.md).

</details>

### 4. Automatic and smart: measured limits, no approval gates
<a id="my-34"></a><a id="my-44"></a>

**Mycelium runs whatever the hosts can run, automatically. There are no approval gates and no arbitrary limits; the limits that remain are measured or physical.**

- Mycelium is fully unmanaged, automatic and smart. As of 2026-11-02 the next stretch is adopting and enforcing existing mechanisms; add new ones only when adoption needs them, and fix bugs forward as they surface. (MY-30)
- No arbitrary fixed limits (rates, caps, timeouts, size defaults). Act on measured pressure and learned telemetry, and refuse, shed or yield work only under real contention, ordered by cost and class. A remaining limit states the failure it prevents, its cost, and a measured or physical basis. An emergency fixed cap is a stopgap with a named adaptive follow-up. Mycelium is released publicly, so it must work for any system combination and workload: thresholds derive from the running system's own measurements, never from values tuned to one deployment. This applies to every rollout wait, drain and health bound too. (MY-34)
- Mycelium runs whatever can run on the hosts. Consumers need no queue-owner approval to run work their hosts can run. Physical and safety protections stay: disk floors and size limits, memory and device admission, secret and token isolation. (MY-44)

<details><summary>Sources</summary>

MY-30, 2026-11-02: “i agree on the next stretch being adoption rather than new stuff unless adoption requires new things to be added. i want it to be fully unmanaged and automatic and smart. if we run into bugs then we'll fix them. it's all part of the process.” MY-34, 2026-11-05: “i don't want us to just have a bunch of arbitrary limits on things. i'd rather it be smart then try to be blindly safe ya know” and “it needs to be smart and dynamic rather than hardcoded because we're releasing this publicly so i needs to work for any system combination and any workload”. MY-44, 2026-11-10, about the `models.sources` allowlist: “i don't like that things need to be approved by you. mycelium needs to be able to run whatever can run on my systems. this is just excess friction”. Applied that day: queue policy revision 105 permits all repositories (`*/*`) and keeps a 100 GiB model-copy free floor; the code default follows.

</details>

### 5. Fleet roles and the queue
<a id="my-45"></a><a id="my-08"></a><a id="my-40"></a><a id="my-48"></a>

**One always-on host carries the persistent services and the queue; every other machine is an on-demand node. A shared queue is a direction, not yet a design.**

- The queue/GPU host is the only machine running work 24/7: every project's always-on services, kept light so its GPUs stay schedulable in the fleet. Every other machine except the agents' host is a managed on-demand node with nothing persistent on it, so Mycelium sizes its capacity from measured OS needs alone. The agents' host stays a Mycelium node under stricter limits. (MY-45)
- Adding a supported machine type takes one command, using per-platform environments and qualification, not per-machine images. (MY-08)
- The dedicated CUDA node keeps a 4 GB owner memory reserve. (MY-48)
- One SQLite queue, as the approved design says. Direction (not yet a design): the queue becomes shared and load-balanced across machines so nothing is lost when a machine goes down. This authorizes no peer network or replicated queue until Zach approves a design with laptop-testable migration steps (#1176). (MY-40)

<details><summary>Sources</summary>

MY-48, 2026-11-12, maintainer, answering the orchestrator's question about the CUDA node's 16 GB owner reserve: “[the CUDA node] let’s do 4gb”. Received by the Mycelium orchestrator.
MY-45, 2026-11-11: “the only machine that should be running... work, 24/7, and in the background, is the [dual-GPU queue host]. And then everything else should just be, like, an on demand node. I still want the 2 [queue-host] GPUs to be part of the Mycelium Fleet ... it should be reserved effectively just for... the persistent projects, so things that have to constantly be online. Best case scenario is that... it would run something lightweight on the [queue host], all of the projects would. so that they're not taking up as much RAM, so that we're still able to use the [queue-host] GPUs with no problem.” And: “The host is still part of Mycelium, but it's got... harsher limits set just because that's where everything with the agents is hosted from, and if that machine goes down, then nothing works. So that one's gotta be coveted. But the rest of them are effectively just managed nodes as part of the cluster.” At the time Zach was moving one project's persistent service off a node himself. MY-08 onboarding ruling, 2026-10-21 (#108). MY-40, 2026-11-09: “i want the queue to be something the machines share rather than just running on one machine. just like we load balance the jobs, i think we should load balance the queue as well. ... so mycelium truly works as a biological mycelium could. where information is part of the collective in a way. that way if a machine goes down, nothing is truly lost.”

</details>

### 6. Tested fixes deploy without asking; critical updates anytime, ordinary ones between runs
<a id="my-28"></a><a id="my-33"></a><a id="my-39"></a><a id="my-35"></a><a id="my-21"></a><a id="my-32"></a>

**Tested fixes ship without asking, one release at a time, with rollback if the queue is unhealthy. Critical updates may deploy during a production run; ordinary ones wait until a successful run ends.**

- Tested fixes deploy to the fleet without separate authorization, bundled one release at a time. If the queue is not healthy afterwards, the operator rolls back. Fleet hardware, OS, driver and package changes are outside this authority. (MY-28)
- No pre-production freeze, customer-window blackout, consumer canary gate, or asking Zach about deploy timing. Before each scheduled production window (twice weekly; schedule in private operator notes) the fleet is on a healthy, tested release. (MY-32, MY-33)
- Critical updates may deploy during a production run so other work does not stall. Patch time is weighed when choosing how to roll out, and making it quick is a priority (#900), but it never defers a necessary critical update. Ordinary updates wait while a production run is succeeding; fixing a run that fails because of Mycelium is expected. (MY-39)
- Updates roll out automatically with minimal downtime, for example between jobs; a critical update may stop work to apply immediately. “Automatic” means a release already signed and approved with `myce release approve` is applied without an operator stepping through each host. It is never silent: the rollout reports each step. It adds no automatic rollback machinery (MY-03): an unhealthy rollout stops advancing, and the operator rolls back if the queue is unhealthy, otherwise fix forward. (MY-35)
- `myce fleet upgrade` is an explicit operator command that replaces only Mycelium's own binary and restarts only its existing service unit on enrolled hosts, for a signed, approved release. Workers fetch and verify the bytes themselves through the authenticated control path; no SSH. No silent updates or downgrades, and no GPU driver, OS, package or other host changes. #183's downgrade credential scope is separate. This is a scoped exception to the design's M4 service/host-change prohibition; its native acceptance still needs separate authorization. (MY-21)
- Agent defaults (orchestrator, 2026-11-06 and 2026-11-09; not Zach's words): hosts upgrade one at a time as their running work finishes; a critical update checkpoints and requeues resumable work and reports what it interrupted; a rollout verifies health, preservation and custody and stops advancing when a result is unhealthy; use supported rollout paths that preserve running attempts and resumable checkpoints, announce rollouts to consumer orchestrators, and prefer quick rolling updates. This is deployment authority, not a claim that every desired rolling-update mechanism exists.

<details><summary>Sources</summary>

MY-28, 2026-11-01, relayed by the guidance-review orchestrator; it replaced MY-21's separate live-rollout authorization. MY-32, 2026-11-02, answering whether to drop the customer-window clause: “i agree with your recommendation for the production rule removal”. MY-33, 2026-11-03, sanitized: “Be ready for production [on the scheduled production mornings] and don't patch during a successful production run.” MY-39, 2026-11-09: “i think depending on patch time (which we should work on making quick) critical updates should be able to run during [the production consumer's] production runs. because i don't wanna have the rest of my work stall on the fact that we're waiting on a [production consumer] run that takes several hours.” MY-35, 2026-11-06: “we need a way to do automatic update deployments that run without needing to take an entire system down i think. or for it to have minimal downtime like between jobs unless it’s critical and needs immediate deployment (stopping everything and applying the update)”. MY-21, 2026-10-27: “make a fast and easy way to update the version on all hyphae seamlessly” (#244).

</details>

### 7. One runtime; only the adjacent release interoperates; fix forward
<a id="my-03"></a><a id="my-42"></a>

**There is one runtime. Only the previous release stays compatible, for rolling upgrades; everything older lives in tagged history, and problems are fixed forward.**

- Maintain one v3 runtime. Earlier versions live only in tagged source history: no compatibility layers, old-version fallback or automatic rollback machinery. Fix forward. The approved design's checkpoint/retry contracts stay. (MY-03)
- The one exception is the adjacent release pair: the current release interoperates with the previous one during rolling upgrades (MY-35). Older protocols, importers, converters and persistence formats belong in tagged history. Before deleting their runtime paths, verify every active installation and the production database meet the floor. (MY-42)
- Release-specific: on 2026-11-10 Zach required candidate r6.9 to support queue rollback to the live r6.8, preferably by converting the v3 database to v2 without losing jobs, custody or decisions; see [the rollback procedure as shipped](https://github.com/1kuna/mycelium/blob/302620cacaace8a16e7ed424e1183dbb10f46414/docs/operations.md#queue-only-rollback-from-r69-to-r68). Agent note (2026-11-11, not a ruling): this applied to the r6.8/r6.9 pair; with r6.10 live everywhere the converter left main under MY-42 and lives in source history at that commit (no r6.x tags exist yet).

<details><summary>Sources</summary>

MY-03, 2026-10-22 (#80). MY-42, 2026-11-10, Zach answering refactor review finding 10, which proposed “current and previous release only”: “yes drop old migration code”. At r6.8 (`v3.1.0-rc.20261110.s076r6.8`) the floor was r6.7.

</details>

### 8. Iterate fast: local first, quick gates, changeable tooling
<a id="my-41"></a><a id="my-38"></a><a id="my-37"></a><a id="my-04"></a><a id="my-02"></a><a id="my-16"></a>

**Iteration speed comes first: a faster test suite, then faster iteration. Develop locally, merge on quick plus targeted tests, run the full suite once per release batch, and change any tooling that blocks work.**

- No independent review before merging. A PR merges when quick plus its targeted tests pass; the full suite runs once per release batch, before deployment, and breakage is fixed forward. Authorized hotfixes may run quick, deploy, then run full. Development and test tooling (gates, custody, admission, guards) may be changed, reset or cleared when it blocks work. Process rules stay only where they protect production customers or secrets; tooling authority never permits breaking running production customer jobs or exposing secrets. (MY-41, MY-04)
- Tests cover behaviour, security invariants and real incidents; no repeated full suites per fix, speculative matrices or assertion-count preservation. (MY-04)
- Priority 1 is a faster test suite; priority 2 is faster iteration toward stability. Pausing the queue or fleet is authorized when it lets that work be applied. (MY-38)
- Most, if not all, development runs on the 64 GB development host and never depends on fleet availability. Quick and full gates run natively on Darwin and on Linux in a directly invoked local VM, with persistent caches, idle cache trimming, disk floors and measured capacity admission; fleet gates are optional extra evidence. When the host can't run gates (for example under memory pressure), use the fleet instead of waiting. (MY-37)
- Share normal Go build and module caches; remove owned rebuildable per-run scratch when done (no routine Trash retention of reproducible caches) and keep the evidence. (MY-02)
- Agent defaults for fleet pauses (orchestrator, not Zach's words): hold new placements while running jobs finish or reach a checkpoint, never kill them, announce each pause to affected consumer orchestrators (notify the orchestrator before pausing a production host), then resume. Local never-admitted gate waiters may be cancelled and restarted with current helpers; an admitted gate is cancelled only through supported cancellation and its replacement restarts immediately. Release-critical work keeps priority; post each replacement receipt or session on its PR.

<details><summary>Sources</summary>

MY-41, 2026-11-09, lightly redacted: “get rid of the fresh independent review [requirement] honestly. get rid of whatever rule is telling you our test tooling is untouchable. and get rid of whatever other bureaucratic [process] you've got. how ... do you expect to change anything if you've got a rule against changing everything? ... i work fast and messy. if something breaks then [so be it], we just fix it.” It superseded review rounds, exact-head full gates per PR, gate-exception procedures, “never clear” custody rules and MY-16's concurrency cap. MY-38, 2026-11-09: “if you need to pause the queue to update and make iteration faster i'd prefer you did that rather than just constantly waiting. us being able to iterate fast matters much more”; clarified: “i meant pausing the fleet if it means you can apply whatever you need to so we can iterate quicker. but i want you to make the test suite faster too so we can iterate quicker. top priority is faster test suite and second priority is faster iteration so we can reach better stability quicker which can't be done without a faster test suite” ([#1086](https://github.com/1kuna/mycelium/issues/1086#issuecomment-6744698723)); it superseded the orchestrator's local-gate-only reading and its default of draining older waiters naturally. MY-37, 2026-11-09: “Most, if not all, mycelium development should be able to occur” on that host; “And iteration should not be dependent on the availability of the nodes. It should just be dependent on the host machine that's being used for development.” (#1086); it superseded the 2026-11-03 fleet-only gate location and exact-head fleet-gate merge requirement. The fleet fallback is Zach's 2026-11-10 approval of ledger C9. MY-04 test ruling, 2026-10-22. MY-02 cache-storage incident ruling, 2026-10-23. See [development](development.md).

</details>

### 9. Jobs are isolated, and secrets never reach them
<a id="my-23"></a><a id="my-24"></a><a id="my-25"></a>

**Jobs never see the fleet token. Extensions run in a kernel sandbox unless they declare an exception with a reason; plain native jobs stay unsandboxed in v1 unless they opt in; status and warnings show both.**

- [AGENTS.md](../AGENTS.md)'s token rule applies: jobs never inherit `MYCE_TOKEN`, and the fleet token is never exposed. Same-user native jobs need a kernel boundary; `0700` alone is not enough. Keep #219's environment allowlist, managed per-attempt work/temp, private extension source, shared caches without payload content writes, assigned GPU access, and the cgroup, launch and cleanup gates. (MY-23)
- Linux prefers rootless bubblewrap user/mount/PID/IPC/network namespaces with one anchor shared by sidecars. Only a namespace policy refusal permits the Landlock ABI >= 6 plus seccomp fallback. Both Linux modes are offline-only and withhold `native_network` until namespace egress is qualified. The fallback allows exact worker-reserved sidecar TCP ports but cannot restrict destination or bind address (off-host same-port access is possible; metadata is not isolated). Per the explicit v1 Option B ruling, the fallback allows only AF_UNIX/SOCK_SEQPACKET creation (protocol zero, normal CLOEXEC/NONBLOCK), keeping Landlock abstract/signal scopes and every other denial; same-UID pathname SEQPACKET endpoints stay reachable (partial). Startup inventories warn about unexpected non-root/non-CUDA listening seqpacket endpoints, unknown ownership or inventory failure; that snapshot grants no stronger isolation. Minimal-root namespaces plus a per-host AppArmor userns exception are the full-isolation upgrade, pending the operator and non-blocking; that exception is an explicit privileged option, not authorization to apply it or to disable a global sysctl. (MY-23)
- macOS uses fail-closed (deprecated) Seatbelt for targeted sibling, credential and control protection, with metadata visible and Metal kept; online jobs still deny the queue TCP port at all addresses. General payload outbound needs `needs.network: true` and a capable host, or submission is refused immediately. Preparation stays network-capable. (MY-23)
- New extension registrations and adapter entries default to the #177 sandbox. One may declare top-level `isolation: unsandboxed` with a required nonempty `reason`; its main/check payload and sidecars then omit only the kernel sandbox and keep token/environment filtering, managed work/input staging, device binding, cgroups, launch/Started authorization, stale-attempt fencing and cleanup custody. Status and job GET show `unsandboxed: <reason>` and the worker warns once per attempt. Pre-policy persisted and frozen registrations stay unsandboxed unless their author opts in through new authored bytes/version; replaying an identical digest cannot flip policy. (MY-24)
- Plain native jobs stay unsandboxed by default in v1, because live consumers prepare dependencies and use home caches inside plain commands; `isolation: sandboxed` opts in. Status, job GET and per-attempt warnings explain the default. (MY-25)

<details><summary>Sources</summary>

MY-23, 2026-10-27: [#177](https://github.com/1kuna/mycelium/issues/177), [#219](https://github.com/1kuna/mycelium/issues/219), [#248 minimum](https://github.com/1kuna/mycelium/issues/248#issuecomment-6447873336), [network clarification](https://github.com/1kuna/mycelium/issues/248#issuecomment-6448371815), [v1 seqpacket ruling](https://github.com/1kuna/mycelium/issues/177#issuecomment-6453575275); it replaced #248's blanket deferral only for this minimum. The [design amendment](design-proposal.md#amendment-minimum-native-job-isolation-177-219-248) and [guarantee table](native-isolation.md) record limits, not qualification or rollout. MY-24, 2026-10-27, orchestrator implementation assignment for batch/s047 under #177; it does not qualify hardware or add Office/Docker helper work (#272). MY-25, 2026-10-28, maintainer's release-worker assignment, conditional fallback after the s047 inventory (#177).

</details>

### 10. Inputs are staged on the machine that processes them
<a id="my-36"></a>

**Work processes a local copy of its inputs, so the network is never the bottleneck.**

Every NAS input, whatever its size, is transferred to the executing machine's local disk, and processing uses that copy, so the bottleneck is local compute and local benchmarks stay meaningful. No size threshold selects local staging; MY-34 governs headroom, eviction and recovery. Mutable checkpoint saves keep their own durability and custody contract and are never treated as immutable cache entries. Agent boundary: declared shared and job-file inputs stage automatically; raw paths embedded in payload code need an input declaration or local path.

<details><summary>Sources</summary>

MY-36, 2026-11-08 (#1061): “i think any files should transfer to the machines it's being processed on from the nas because processing bottleneck shouldn't be network because then local benchmarks become entirely irrelevant.”

</details>

- <a id="my-47"></a>Finished jobs' files are kept for 3 days, then reclaimed, and files already past that window are cleared now to make space. (MY-47) Agent note: applied as the queue's file retention, which also governs worker scratch held for finished jobs.

<details><summary>Sources</summary>

MY-47, 2026-11-11, maintainer, answering the orchestrator's question after the queue host reached 97% disk with job files kept until explicit release: “keep finished job files for 3 days but clear out whatever’s stale right now so we can make space”. Received by the Mycelium orchestrator.

</details>

### 11. Compute and token efficient
<a id="my-43"></a>

**Mycelium spends as little compute and as few tokens as it can, in the product and in how its agents report.**

Mycelium's product keeps control-plane cost per check-in low, default status output small, and gives consumer agents one batched wait (#1120, #1053). Mycelium's agents report only on a state change, a fault or a needed decision, plus one final summary.

<a id="my-49"></a>Zach left the five slimming choices from the 2026-11-11 audit to the orchestrator (MY-49). The orchestrator chose the following. They are agent decisions, not his words, and can be changed on evidence:
- Delete the simulator, the fleet viewer and the Windows host bridge. Git history keeps them, and the bridge comes back when a Windows machine joins.
- Retire managed Docker (MY-13's supported option) once the one project still using it runs its model jobs natively. Done 2026-11-12, after that project's native run matched its Docker run ([#1456](https://github.com/1kuna/mycelium/issues/1456)).
- Cut curves, benchmarks, time prediction and batch planning down to the last accepted duration and peak per kind of work. Placement keeps learning from its own measurements (MY-31).
- Make `myce fleet upgrade` the one rollout path (MY-21, MY-35), reduced to what host-by-host rollouts have proven: drain the host, swap the verified binary, restart, check health. Private per-release rollout scripts go.
- Keep finished job records for 14 days after de-duplication, plus a one-line summary and what was learned. Files stay at 3 days (MY-47).
- Send health problems to the Mycelium inbox as one opening and one closing per host-level problem. Efficiency and recipe findings stay in status only.
- Consolidate the approved design into its current form, without changing what it says.

<details><summary>Sources</summary>

MY-43, 2026-11-10: “i want our program to be both compute and token efficient”, approving the orchestrator's reporting proposal.

MY-49, 2026-11-12, maintainer, answering the orchestrator's five slimming questions: “the 5 slimming decisions do whatever is best idk”. Received by the Mycelium orchestrator.

</details>

### 12. How work on Mycelium proceeds
<a id="my-05"></a><a id="my-09"></a><a id="my-10"></a><a id="my-14"></a><a id="my-15"></a>

**Blocked work blocks only itself, a pause stops the orchestrator, features are proven on Mycelium's own workloads first, and verifiers judge product evidence.**

- A blocked track blocks only itself. A hardware hold constrains that hardware; authorized off-hardware fixes and independent tracks continue without overlapping ownership. This never overrides an explicit pause of the agent or expands authorization. (MY-05)
- An explicit orchestration pause applies to the orchestrator: record a self-contained worker queue and next owner, then stop (no reviews or dispatches through background notifications) until resumed. (MY-15)
- Prove features on Mycelium's own workloads before integrating a consumer, then integrate through its extension and a small submit/wait or endpoint seam, not step-by-step coupling or consumer rewrites. (MY-09)
- Delivery sequence does not delete scope: prove accelerator execution and initial consumer integrations before expanding delivery commitments. CPU-only scheduling is implemented; broader resource vision and multi-machine work keep their own gates. No native acceptance is inferred. (MY-10)
- Verifier checklists judge product evidence: code, behaviour and acceptance. Process self-audits do not block working packages; explicit authorization, privacy and task boundaries still apply. (MY-14)

<details><summary>Sources</summary>

MY-05 coordination ruling, 2026-10-16/22. MY-15 pause clarification, 2026-10-17. MY-09 integration ruling, 2026-10-21. MY-10 sequencing ruling, 2026-10-20 (#98). MY-14 runner ruling after #93, 2026-10-19.

</details>

## Agent defaults

These are agents' decisions, not Zach's rulings. Change them through their issues.

- **Measured local gate admission** (orchestrator, 2026-11-09, under MY-37). Admit full gates from measured per-leg CPU/RAM and live host capacity, not a fixed N=1, in head-of-line FIFO order; explicitly exclusive timing runs reserve all local capacity. CPU admission reserves a static fleet-worker share in local gate config (initially 4 cores, matching the admitted worker allowance), updated by the operator when that host's worker CPU budget changes; drift only overcommits CPU, an accepted tradeoff. RAM admission uses live available memory with no fleet allowance. Normal-pressure shortages keep their FIFO position with a visible memory reason; pressure, sustained-swap and disk stops remain, and a requirement above physical RAM is refused. Admission never depends on the fleet and never changes fleet-worker custody. Basis: four-core native peak 7.01 GiB, Linux/VM peak 10.57 GiB, measured headroom. It replaced the per-admission status-read, cache and generation-fencing design after reproduced races and starvation.
- **Device-fault cascade protection and measured recovery** (#1142, implementation worker, 2026-11-09, from a relayed consumer request, applied generically). One faulting job must not consume a host's last healthy accelerator of the same backend. Placement uses attempt-specific fault history and current healthy capacity. Recovery needs positive vendor evidence and a quiet window derived from observed fault-to-normal latency, local probe gaps and prior recovery evidence (MY-34; no fixed duration or host policy). Repeated-teardown escalation stays with the operator. See [the mechanism](reference/accelerator-quarantine.md#accelerator-driver-fault-quarantine).
- **Capability-based XPU recovery proof** (#1185, orchestrator, 2026-11-09, under MY-34). A required component reported `Unknown` on both the quarantined device and a healthy same-model sibling in the same health batch is excluded from that proof and recorded `unsupported_on_device`. Every other required component must report `OK`; missing, malformed or failed components block. Without a healthy same-model sibling, Unknown keeps quarantine. The comparison repeats per batch and is not a permanent allowlist; the kernel observation window and escalation policy are unchanged. The installer grants narrowly scoped GPU MEI/PMT access through the supported install path, with no manual host edits. Native rollout and automatic clear remain to be verified after release; fake-probe evidence is laptop proof only.

## Old numbers

Each old number, its original date and title, and where it lives now. All are current unless marked superseded.

| Old | Date | Original title | Now |
|---|---|---|---|
| MY-01 | 2026-10-23 | general public presentation | §1 |
| MY-02 | 2026-10-23 | shared build caches | §8 |
| MY-03 | 2026-10-22 | v3 and fix forward | §7; narrowed by MY-42 for the adjacent pair |
| MY-04 | 2026-10-22 | proportionate test gates | §8; merge rule refined by MY-41 |
| MY-05 | 2026-10-22 | independent work continues across a blocked track | §12 |
| MY-06 | 2026-10-21 | one cluster, machine hyphae | §1 |
| MY-07 | 2026-10-21 | measured multiplexing | §3 |
| MY-08 | 2026-10-21 | one-command machine onboarding | §5 |
| MY-09 | 2026-10-21 | standalone proof before consumer integration | §12 |
| MY-10 | 2026-10-20 | delivery sequence does not delete scope | §12 |
| MY-11 | 2026-10-19 | open general compute platform | §1 |
| MY-12 | 2026-10-19 | extensions wrap existing software | §2 |
| MY-13 | 2026-10-19 | Docker is optional | §1 |
| MY-14 | 2026-10-19 | verifier criteria judge product evidence | §12 |
| MY-15 | 2026-10-17 | orchestration pauses apply to the orchestrator | §12 |
| MY-16 | 2026-10-15 | bound concurrent full race gates | superseded by MY-41 (§8) |
| MY-17 | 2026-10-25 | one agent-written instruction file | §2 |
| MY-18 | 2026-10-25 | least valuable machine that fits | §3 |
| MY-19 | 2026-10-25 | split for speed only after accuracy parity | §3 |
| MY-20 | 2026-10-25 | agent-first, set up once, improves itself | §2 |
| MY-21 | 2026-10-27 | one-command fleet upgrade | §6; live-rollout authorization superseded by MY-28 |
| MY-22 | 2026-10-27 | merge profiles and extensions | §2 |
| MY-23 | 2026-10-27 | minimum native isolation | §9; defaults narrowed by MY-24/25 |
| MY-24 | 2026-10-27 | declared isolation exceptions | §9 |
| MY-25 | 2026-10-28 | plain native v1 default | §9 |
| MY-26 | 2026-10-29 | run anything anywhere; scarce machines yield | §2 and §3 |
| MY-27 | 2026-10-31 | variants | §2 |
| MY-28 | 2026-11-01 | tested fixes deploy without separate authorization | §6; its timing clauses superseded by MY-32, MY-33 and MY-39 |
| MY-29 | 2026-11-01 | fleet policy local, mechanisms generic | §1 |
| MY-30 | 2026-11-02 | unmanaged and automatic; multi-GPU hosts; production tiers | §3 and §4 |
| MY-31 | 2026-11-02 | telemetry integral; Mycelium owns placement | §3 |
| MY-32 | 2026-11-02 | no customer-window blackout | §6; active-run clause superseded by MY-33/MY-39 |
| MY-33 | 2026-11-03 | production readiness and deploy timing | §6; critical-update timing superseded by MY-39 |
| MY-34 | 2026-11-05 | smart adaptive limits | §4 |
| MY-35 | 2026-11-06 | automatic rolling updates | §6; critical-update wait superseded by MY-39 |
| MY-36 | 2026-11-08 | stage NAS inputs locally | §10 |
| MY-37 | 2026-11-09 | local development independent of the fleet | §8 |
| MY-38 | 2026-11-09 | faster tests first, faster iteration second | §8 |
| MY-39 | 2026-11-09 | critical updates during production runs | §6 |
| MY-40 | 2026-11-09 | direction | shared fault-tolerant queue: §5 |
| MY-41 | 2026-11-09 | fast iteration, changeable tooling | §8 |
| MY-42 | 2026-11-10 | current and previous release only | §7 |
| MY-43 | 2026-11-10 | compute and token efficient | §11 |
| MY-44 | 2026-11-10 | run whatever can run, no approval gates | §4 |
| MY-45 | 2026-11-11 | fleet roles | §5 |
| MY-46 | 2026-11-11 | withdrawn (restated MY-11/MY-26) | §1 |
| MY-47 | 2026-11-11 | finished-job files kept 3 days | §10 |
| MY-48 | 2026-11-12 | CUDA node owner reserve 4 GB | §5 |
| MY-49 | 2026-11-12 | slimming choices left to the orchestrator | §11 |

Superseded agent defaults, both by MY-41: the 2026-11-07 orchestrator default that known, owned main failures do not block unrelated merges, and the 2026-11-08 weekly-review proposal on evidence for gate exceptions. Earlier version pins, private topology, temporary windows, routing recipes and receipts are in private local history and are not current fleet evidence.
