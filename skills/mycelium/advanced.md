# Mycelium advanced contracts

Start with [the first-run guide](SKILL.md). Read the sections below before using
the corresponding feature. Release qualifiers describe feature availability,
not proof that your installed CLI, queue and workers contain it; check `myce version`
and installed usage before relying on a newer contract.

Mycelium runs CPU and GPU work for every project through one queue. It places each job among the currently eligible, qualified hosts using its implemented scheduling policy. You describe what the work **needs** and its **class**; you don't pick machines or negotiate with other projects for room.

## Submit

In short: run `myce spec check --resolve` first; the job's class is the only urgency lever; pin a host or device only for a hard requirement recorded in `requirement_reasons`; memory is optional.

1. Use the project's `.mycelium/adapter.yaml` entry (`myce run --entry NAME --key KEY`) or an existing profile (`myce run --profile NAME --key KEY`). For a one-off command, `myce run --cpu N -- CMD` (add `--gpu cuda|xpu|metal`; `--mem` is then accelerator memory and `--host-mem` host memory). Leave out `--mem` and Mycelium learns it. A one-off takes no `--key`, `--notify` or `--input`, so use an adapter entry when you need those. If nothing fits, author an adapter with [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md); [setup-project.md](setup-project.md) covers profiles.
2. **Resolve before submitting.** `myce spec check --resolve FILE` (a job spec with `--files-from DIR`, a profile, or `.mycelium/adapter.yaml` with `--entry NAME`) asks the queue, read-only, which machines can run the job as written. For each missing input or environment it names the field, the side that can't see it (submitter, queue or worker) and the fix. It also flags absolute data paths, system-Python commands, a host pin that only exists for data locality, and large tool bundles in the source. Apply those fixes instead of adding a pin. Nothing is submitted, uploaded, built or reserved, and submit revalidates. **From the next release:** command targets expose immutable environment keys and ready/prepare-needed/blocked state; cold uv/Pixi builders can supply a locked newer Python without changing system Python (use managed `python3` via PATH). Native variant adapters return metadata/lock gaps, with eligibility unknown until frozen registration and qualification; Git/snapshot lock coverage stays unknown until the selected source is inspected, rather than borrowing working-tree files. File-store inputs expose `ready` after bounded read-only SHA-256/size verification, `needs-staging` for missing/corrupt bytes or future uploads, and `unknown` when observation cannot finish; submission verifies again. NAS reads remain unverified by default. Add `--inspect-nas` and repeat after worker heartbeats for cached existence-only signals on declared `shared_inputs.nas`: missing is flagged before submission; present never proves hashes. Pending, stale or absent evidence stays unknown; samples expire with their worker session freshness fence from the next release. The signal requires upgraded workers as well as the queue. Prefer `local:` or `myce file put` and immutable hash/size when data is outside the checkpoint root.

   **NAS visibility remains unverified by resolve.** `shared_inputs.NAME.nas` is relative to each worker's `--checkpoint-root`, which can sit below the NAS mount. A successful resolve does not prove the file exists there. For bytes elsewhere, use profile `local: ACTUAL_SUBMITTER_PATH`, or `myce file put ACTUAL_PATH` followed by `{sha256: HASH, size_bytes: N}` without `nas`. A genuinely missing NAS source fails before launch with `input_staging: not_found`; do not repeatedly resubmit the same wrong path or clear caches. From the next release, resolve labels this unknown visibility and the failure's `execution_cause.path_diagnostic` names the input and logical worker root without exposing expanded paths.
   For a profile `local:` shared input, an omitted `size_bytes` is derived from the file; a declared `sha256` is checked at submission. Resolve verifies a declared hash and checks the size before submission. A mismatch names the input and the differing field.

3. **State needs, not machines:** platform, accelerator, cores, environment. **Memory is optional:** omit it and Mycelium treats the job as `memory: auto`. **From the next release:** a cold run reserves measured available physical headroom, retaining explicit allowances, reserves, containment and OOM retry floors. With no demand evidence, an arbitrary command has no finite known upper bound: the initial probe holds that headroom. On supported Linux CPU attempts, fresh complete cgroup footprint counters and a measured growth horizon let its admission charge track live use plus growth, retaining known need, member caps and retry floors. Its kernel ceiling stays fixed; stale or missing telemetry restores the full charge. Protected work withholds its charged capacity rather than blocking every new probe. A clean accepted completion teaches a reusable envelope; other hosts remain independent. Discrete GPU probes keep the card exclusive. On unified memory, unknown host/device overlap reserves the shared pool and keeps the card exclusive; the displayed device marker is not a measured device need. Comparable accepted peaks add observed variability or sampling growth, whichever is larger. Cross-target estimates require paired accepted measurements and their observed transfer error; unknown transfer error remains exploratory. Constrained completions, missing pressure and incomplete component counters cannot justify a smaller bound. A genuine constrained failure or OOM retains prior requirements and re-explores physical headroom, without inventing an upper bound by multiplying a constrained peak. Infrastructure-interrupted attempts do not invalidate accepted peaks. Existing explicit measured host/device figures remain useful for containment and sharing. `myce status JOB_ID` and job GET `memory_envelope` show the figure used, its `basis` (declared, learned, hint or exploratory) and its `evidence` count. Job GET `resources` puts your declared figure, the learned one and the one used side by side for host memory, device memory, CPU and storage; once learned evidence covers a declared number, you can drop the number. For repeatedly measured CPU verification profiles, compare job GET `resources` (learned figure and evidence count) with `memory_envelope` before replacing a conservative declaration with `memory: auto`; retain commands, concurrency and input identity so the evidence remains comparable. Keep an explicit number only for a measured bound or a real containment need; Mycelium uses it as written. On a release with corrected discrete-XPU host attribution, a new accepted sample can lower an older inflated learned envelope: status then uses the maximum per-run host footprint, excluding sibling Mycelium growth. When accepted evidence needs an uncalibrated cross-OS or discrete-GPU transfer, Mycelium permits one bounded measurement on that target family’s largest eligible allowance, provided the retained measured lower bound and any host hint fit. The cgroup total remains the conservative basis when complete evidence cannot separate reclaimable cache. The probe uses the allowance less launch slack and drift headroom, keeps live admission and release checks, and is consumed durably at assignment. Its next accepted target-family sample replaces the transferred prior. An OOM gets no probe retry; a constrained or otherwise ineligible result cannot reopen it, including after restart or resubmission. Reliable oversized peaks and OOM floors keep the actionable capacity refusal. A cold run is exploratory, not proof that a smaller reservation is safe. `myce portability` shows target, environment and qualification gaps; adding a variant still needs output parity before production use.

   **Learning needs a stable memory identity.** One eligible accepted sample can teach the next run of the same plain command; there is no multi-run bootstrap threshold. Project, name, exact command, environment and inputs identify that work. Changing names, literal input arguments or input references can create separate identities; `learning_reason: eligible` does not make them comparable. For one program processing comparable items, use a stable adapter entry/profile base command, append item arguments after `--`, and stage item files with `--input`. Leave `memory_class` omitted to reuse the profile's base memory class; set `memory_class: per_item` when arguments or item bytes change the memory need. Do not reuse a class across differently sized work just to escape exploration. From the next release, resolve gives raw auto commands this guidance as a hint. Accepted telemetry still has to meet pressure, hardware and freshness rules; two complete local samples are needed only for the narrower component-range estimator.

   **Tools (available in the next release):** declare native command prerequisites in the profile, job or adapter variant with `needs.tools: [python3.12, node, npm, npx, git]`. These are executable names on the effective runtime PATH, not package names or version ranges. Use a versioned executable for Python 3.12 and a locked environment recipe to pin Node and other package versions. `environment: {}` means worker-installed tools; a named catalog environment supplies portable tools. Resolve and placement reject missing inventory with `tools_missing`, listing the tools and worker; older workers without inventory cannot run explicit tool requirements. Cold managed environments may resolve as prepare-needed: their inventory is checked after preparation and again on the effective launch PATH. A stale inventory or a job PATH override cannot bypass the launch check. Requirements describe the main native runtime; Docker images, model servers and sidecar environments own their dependencies separately. Keep wrapper checks diagnostic: print every missing prerequisite to stderr and exit 127; Mycelium cannot infer dependencies hidden in arbitrary shell scripts.

   Keep a host/device pin only for a hardware requirement, parity or validation, local-only data you can't yet stage, or a shared host that can run only an already-prepared environment. Record the reason in the adapter entry's or legacy profile's `requirement_reasons` (available from the next release). For hardware or parity pins, qualify portable variants and verify output parity; for device validation, reserve only the device under test. Replace filesystem affinity with managed `source`, `files` or immutable `shared_inputs`; use the [queue-visible environment library](https://github.com/1kuna/mycelium/blob/main/docs/environments.md#queue-visible-environment-library) (`myce env list`, `myce env get NAME`) for runtime dependencies. Verify preparation and qualification before removing a pin. **From the next release**, the map is frozen with your submitted job and private receipt. Status/job GET `requirement_reasons` show sanitized constraint, authored/missing state and the source field; raw explanation text stays omitted under the existing privacy policy. Keep evidence in your authored source/private receipt. Missing metadata on older jobs does not prove a pin unnecessary.

   **Declare implementations once:** a native adapter entry or extension uses the existing `variants` list, each with an exact OS/arch/backend `target`, its own `needs`, `execution`, environments/lock inputs, and native qualification check/baselines. Use backend `metal` for MLX/MPS; declare and check a CPU implementation explicitly. Do not mix flat recipes with variants or assume an implicit fallback. Run `myce portability` before authoring and after inventory or requirements change. Registration warnings distinguish missing recipes from declared coverage still needing qualification; unavailable inventory means unknown coverage. A useful request to an agent is: “Make this entry portable across the targets in my live portability report, preserving its output criteria and documenting genuine requirements.” [Setup](setup-project.md) and the [variant example](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#write-an-extension) describe the existing catalog and bundled-environment options; project environment submission/build eligibility remain separate work (#251/#252).

   **Drop convenience pins in future submissions:** stage local data, declare alternatives, prepare them and verify exact native qualification and owner-required parity, then remove `allowed_machines` and convenience device selectors from the authored entry/profile. Resolve again and let Mycelium choose. Keep justified hardware/validation restrictions. Do not cancel accepted work to move it. **From the next release**, status and job GET `variant_eligibility` explain each declared variant on each observed host/device; `variant_advice` identifies missing implementations that could relieve scarce-host contention. Advice is a porting investigation, conditional on actual CPU/RAM/device needs and qualification; safe relocation has its own checkpoint/requeue contract.

   **From the next release**, newly registered multi-variant extensions require measured MY-19 parity or explicit approval on production candidate hosts, in addition to native qualification. Experiments and checks can produce the evidence. Compare outputs with the reference within the owner's accuracy criteria, then ask the queue owner to approve physical hosts for this project/work through the existing `production_parity_hosts` policy (`myce policy get/set`). Approval makes hosts eligible; it is not a job pin. Native baselines alone do not establish cross-variant parity. Historical registrations and accepted jobs retain their admission contract when no quality criteria are declared. Measured failures cannot be bypassed by host approval.

   For a situational hardware or locality constraint, a raw job spec can set `excluded_machines: [worker-a]` instead of enumerating every permitted worker. It removes machine IDs from eligibility without widening job or project allow-lists; unlisted future workers stay eligible. Resolve first and use `require_target: true` when submission must refuse without a compatible known target. This is optional authored intent, never a queue-safety or urgency mechanism.

   **Several GPUs:** write one entry with the job's total accelerator memory (`--mem`, `memory_gb` or `memory: auto`); Mycelium chooses the cards. Work one card holds gets one card; otherwise it gets the fewest cards of one host that hold it, all visible. Read the count from `MYCE_GPU_COUNT` (vLLM `--tensor-parallel-size $MYCE_GPU_COUNT`; Hugging Face `device_map="auto"` uses every visible card). Software limited to one device needs a job that fits one card. `require_target: true` refuses a need larger than any host's cards together.

   **Shared GPUs:** a GPU runs several jobs at once when their memory fits side by side, so an accurate `--mem`/`memory_gb` or an accepted `memory: auto` figure lets a small job start beside another instead of waiting. Managed Docker batch jobs can also share when their device figure is declared or learned and each container has an enforced host-memory cap. `run.docker_memory_gb` sets the main container cap independently of networking; without it the resolved host reservation supplies `--memory`. Sidecar caps or the shared parent still apply. Model-server, extension-check, fan-out, exploratory first runs and multi-card jobs keep their cards to themselves, as does a GPU job with `--host-mem` but no `--mem`. A newcomer never joins a higher class's attempt. A higher class stops the fewest co-tenants it needs (every co-tenant on its cards for a multi-card job) and starts after they release; the same class waits, and `myce status JOB_ID` says `device N shared with K attempts`.

   On XPU cards, a higher class stops every lower-class co-tenant and waits for each exact release before starting, even when memory fits: lower-tier teardown can reset shared engines. **Available from the next queue release:** the same stop-then-start rule applies to CUDA, including unified-memory CUDA. Without reliable fresh per-process growth evidence, a lower-class co-tenant can exceed its reservation and starve a higher-class model load. The cost is mixed-class packing on these cards; equal-class packing, including experiments sharing with experiments, and other platforms are unchanged.

   **From the next release:** a learned host-memory envelope without a device measurement keeps the GPU exclusive for native jobs too. Its `memory_envelope.source` says `GPU recipe default`; a host peak alone does not establish safe GPU sharing. A later accepted device measurement can enable sharing.

   **From the next release:** unified CUDA jobs also share when the driver lists PIDs with `N/A` memory, provided fresh per-attempt RSS/cgroup estimates reconcile with host `MemAvailable` deltas. These labelled upper estimates include CPU/shared pages and can overstate device use. Unexplained host growth, foreign GPU processes or incomplete evidence keep execution exclusive; missing or stale samples also prevent newcomers and shed existing co-tenants to exclusive use. A newly started tenant gets the existing sensor freshness window to supply its first sample, while another newcomer still waits. Declare a measured device figure or reuse accepted learning; cold probes still keep the card.

   Fresh counters or reconciled unified estimates drive the shared-card guard. On unified CUDA, observed growth is projected across the sensor horizon plus existing stop grace, and checked against physical headroom as well as the card budget. The lowest tier yields first; among equals the job most over its reservation yields. This planned stop requeues without a failure, retaining the measured peak plus observed growth as the next device floor. Checkpoint grace and exact release still apply. Sampling cannot guarantee protection from unseen bursts. Single tenants keep their existing containment; status explains uncertain attribution.

   Shared interactive hosts admit only native CPU commands with already-prepared environments: no Docker, extension jobs or new environment preparation. Use a plain command via `myce run --cpu N --mem SIZE -- CMD` or a native command adapter entry with a prepared environment; work needing other execution modes waits for an eligible target.

   **Available on a guard-capable macOS worker:** native CPU attempts with a positive resolved host-memory reservation use it (plus configured slack) as a sampled RSS threshold. Three over-limit samples spanning two seconds stop the exact attempt with `execution_cause.class: rss_limit_exceeded`; its `rss_limit` evidence reports observed/limit bytes and sample quality. Brief spikes and unknown samples do not trigger it. `memory: auto` follows bounded OOM escalation; a plain declared-limit failure is terminal. Correct a measured reservation before resubmitting. This reactive guard can miss spikes and overcount shared pages; it is not a kernel ceiling. Cleanup/partial diagnostics and independent host-pressure yield remain authoritative. See [native memory guard](https://github.com/1kuna/mycelium/blob/main/docs/native-isolation.md#macos-sampled-memory-guard-770).
   **External downloads:** declare `needs.network: true` on a CPU download job. Plain native commands default to unsandboxed in v1 and can use outbound networking without `native_network`; that capability qualifies protected outbound access for `isolation: sandboxed` jobs. A frozen extension retains its own isolation policy. Environment preparation and host CPU/RAM admission still apply.
   **Models:** declare the exact snapshot rather than asking for a mount or a download approval. In the adapter entry or batch YAML add `model_snapshots: {base: {repo: OWNER/NAME, revision: FULL_40_CHAR_COMMIT}}` (optional `files: [...]` allow-list) and read the read-only directory at `$MYCE_MODEL_BASE`, the same for native and Docker jobs. Mycelium verifies the snapshot from the fleet's shared hub cache and refuses with a named reason (`model_snapshot_missing`, `_revision`, `_mismatch`, `_escape`, `_too_large`, or `model_snapshot_not_permitted` at submit). For a missing snapshot, run a CPU job with `needs.network: true` that runs `hf download OWNER/NAME --revision COMMIT` into the shared hub cache, then resubmit. A `not_permitted` refusal is local queue policy: propose the source to the Mycelium thread. Concurrent jobs reuse one verified worker-cache entry without a per-job copy. Sandboxed native jobs deny model content writes (Linux Landlock or read-only namespaces; macOS Seatbelt); managed Docker uses a read-only bind. Plain native jobs default unsandboxed (MY-25): removed write bits refuse ordinary writes, but a same-user job can undo those permissions and mutate the cache. For enforced payload write denial, declare `isolation: sandboxed` or use managed Docker. Keep generated indexes and mutable library caches in `$MYCE_SCRATCH_DIR`. These protections ship with the next release; they do not qualify unsandboxed access as a security boundary. Pass offline flags (`HF_HUB_OFFLINE=1`) and load from the path; see [declared model snapshots](https://github.com/1kuna/mycelium/blob/main/docs/files.md#declared-model-snapshots-553).
   **Quality tolerance (next release):** set `quality_tolerance: {wer: 0.15}` in the job YAML or native adapter entry (a project can reuse the entry as its default). Values are maximum **absolute differences** in the metric's units: if WER values use percentages, this is percentage points. An explicit zero requires exact agreement. Every reported metric without an explicit tolerance defaults to parity within its measured margin; tolerances are never widened automatically.

   Extension publishers add `quality` to each complete variant, keyed by exact work type and then metric (the same field works in an adapter variant):

   ```yaml
   quality:
     extension:transcribe:
       wer:
         reference: corpus-v1-cuda-recipe-v3  # frozen input, method and reference recipe
         value: 6.19
         reference_value: 6.29
         margin_of_error: 0.12              # synthetic example; measured uncertainty of the difference
   ```

   Report comparable runs against the named frozen reference using the same metric units. `margin_of_error` is the measured uncertainty of the **difference**, not an invented zero or a single-run delta; use `null` for unknown values/uncertainty. The example numbers illustrate the schema, not native qualification. Evidence is frozen with the exact extension digest and work/target recipe; changed evidence needs a new version and native qualification. Mycelium checks every metric reported for that work across the extension's variants plus every job-requested metric before ranking. A complete in-tolerance map satisfies MY-19 without manual approval. Missing metrics or unknown uncertainty fall back to the existing work-scoped host approval for production, and experiments/checks can gather evidence. Measured out-of-tolerance variants are excluded for jobs of any class; checks bypass the filter. Read `variant_eligibility[].reasons` in job/status replies. Existing production spillover uses the same evidence; it still requires exact qualification, current capacity and release custody.

4. **Choose a class:**

   On a release with profile urgency overrides, `myce run --profile NAME --class CLASS --deadline RFC3339` (also for adapter entries) overrides those top-level fields; omit either flag to retain the authored value. A changed submission needs a new key.

   - `production`: work your project's rulings call production. Under the current policy it ranks first and can stop lower-tier work, including non-resumable commands.
   - `deadline` with `--deadline <RFC3339>`: work with a real date that the owner or a ruling set. It can stop lower-tier resumable work after a grace period; non-resumable commands keep running.
   - `experiment` (the default): everything else.

   Compatible idle capacity is preferred before stopping work elsewhere. When a batch demand causes preemption, it keeps the selected host and cards through exact release and starts there; losing the worker/session permits replanning. Equal-tier work normally keeps running; the qualified scarce-host yield described below is the narrow exception. Eligibility, resources and locks still apply, so a higher class doesn't guarantee immediate capacity. A higher class than the work deserves takes capacity from someone else's real deadline.
5. `source: {git: HEAD}` sends committed source bytes only: commit the selected source paths first. The adapter is frozen separately and may have uncommitted edits; the clean check excludes receipts and is scoped to `source.paths` when present. Directory sources upload deliberately prepared regular files without requiring Git; ignored files other than `.git` and `.mycelium/receipts` are included. Ignore `.mycelium/receipts/`. Save the receipt's `key` and `job_id`. The same key recovers the same job after a lost reply; changed intent needs a new key. Receipt blob references expire under normal retention; prefer the normal upload path when submitting new work. On a release with typed input verification, `input_blob_missing` or `input_blob_corrupt` means upload the original bytes again and retry the same unaccepted key; `input_blob_unavailable` means restore queue storage and retry.
6. **Scarce-host yield (available with the next release):** when ready work has a documented hard requirement that only one physical host satisfies, Mycelium can checkpoint portable batch work there, release its exact processes/sidecars, requeue the same job and resume on a qualified alternative. Preserve your key and use normal wait/result calls. A release or stop deadline does not prove successful movement: failed final save/transport holds the job with `scarce_host_yield`, retaining committed progress; its owner repairs the checkpoint or cancels the job. An alternative disappearing after stop leaves durable progress queued. Demand cancellation clears its scarce-host routing exclusion. Ordinary production/higher-tier preemption retains its existing contract.

   Declare tested compatibility with `resume_compatibility: {checkpoint_format: FORMAT, result_contract: CONTRACT, targets: [{target: {os: linux, arch: amd64, platform: cpu}, environment: none, result_check: TEST_ID}]}`. List every proven exact target; use its frozen resolved environment key, `none` without an environment, or `qualified` for native extension variants. The evidence identifier names your actual resume/result check: labels alone never prove portability, and native qualification and MY-19 parity still apply. See [scarce-host yield](https://github.com/1kuna/mycelium/blob/main/docs/design-proposal.md#amendment-scarce-host-yield-327-328-my-26).

   **`preemptible: true`** explicitly makes a **non-resumable experiment** disposable for this path: it may be stopped and restarted from scratch on a qualified alternative. Declare compatibility evidence too. Use it only when repeating execution is safe and losing unsaved work is acceptable. It cannot accompany `resumable: true` or another class; the experiment class alone grants no permission. It does not discard any committed checkpoint or accepted output.
7. **Bound execution when needed (next release):** add top-level `wall_time: 30m` to a spec, profile or adapter entry, choosing the duration from measured runs or a real consumer bound. Omission is unlimited. The allowance starts at accepted Started, excludes preparation and queue wait, and survives queue/worker restart; the owned worker supervisor enforces it during queue outages. Status shows `wall_time.limit`, `expires_at` and `remaining_seconds`, separately from completion estimates. Expiry is typed `execution/wall_time_limit`, stops the exact whole tree and follows ordinary command-failure policy (no automatic retry, no infrastructure-loss charge). Cleanup and partial-output custody remain pending until confirmed release. It is separate from priority `deadline`, `expected_minutes` and `myce wait` timeouts.
8. Make long experiments resumable: implement [atomic checkpoints](https://github.com/1kuna/mycelium/blob/main/docs/files.md) through `MYCE_CHECKPOINT_DIR`, restore progress on restart, then declare `resumable: true`. The flag alone does not preserve progress.

Existing profiles using inline `environment: {lock_file: LOCK, python: '3.13', platforms: [cpu]}` can convert to adapter entries without changing the lock contract. The queue reads and snapshots the lock, so `LOCK` must remain queue-readable; conversion does not upload a submitter-local environment recipe.

## Wait and check status

**Staged fan-out (from the next release):** a native adapter entry can combine
`fan_out` and `workload` with
`after: {job: PARENT_ID, outputs: {first/input: first/result}}`.
Omit `after.piece`: a fan-out consumer waits for the entire parent to succeed,
then receives the explicitly mapped accepted `KEY/OUTPUT_NAME` artifacts.
Keep a durable `--key` for each stage; upstream cancellation stops unfinished
dependents and their children transitively. Sidecars, qualification and
production `start_window` retain their queue contracts. Dependent result
checks and piece-selected fan-out consumers remain unsupported; see the
[adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md).

On releases with #775, job GET and status retain `spec.authoring_origin`: `profile`, `adapter`, `job_spec`, `command`, `extension` or `api`. Generated `extension_check` and `fan_out_child` records count separately from independent submissions. Historical `unknown` and unmarked older clients (`api` for new jobs) cannot establish zero legacy-reader use; verify caller upgrades and cutover too. Replay preserves the first accepted origin.

- Wait in the foreground with `myce wait JOB_ID` for adapter, profile and one-off jobs; it exits 0 only on success. It retries queue outages and reports prolonged read failures separately from job failure. A wait timeout leaves the job running: keep the same ID. For detached completion, optionally bind a notifier to **your own** thread using [t3-binding.md](t3-binding.md) and pass `--notify`; one binding per submitting thread, never an orchestrator's or a shared one. Check the installed CLI's usage before relying on a flag.
- **Available from the next release:** host headroom credits fresh exact-attempt nonreclaimable cgroup memory from managed Docker containers, including jobs without a shared cgroup parent. Missing or five-second-old samples keep the full reservation, and unified CUDA keeps its device-growth floor. External-memory history excludes this sampled container memory; a running container without a fresh sample sets `memory_observation.attribution_unknown: true` and skips external-history recording while retaining physical-memory admission evidence.
- **Model occupancy proofs:** authenticated `GET /api/models/intervals?limit=100` retains assignment, first-authorized load start, stop intent, exact release and class/tier-change events after slots disappear. Follow `next` as `after` with the first page's `high_water` (limit 1..256); keep `coverage_since` (the current uninterrupted coverage period, restarted after an older-binary rollback) and treat missing legacy times or release evidence as incomplete. Tier changes include effective production pins. See the [interval contract](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#durable-model-engine-interval-history-663).
- Read one job at a time: `myce status JOB_ID` (with its attempt history), `myce logs JOB_ID`. On a release with detailed staging causes, prelaunch failures are also in job GET `execution_cause.staging` and attempt history: declared input/blob, operation, OS/storage error and HTTP status. Infrastructure failures use the existing retry budget; `input_unavailable` (`blob_missing`/`blob_expired`) and `invalid_input` need corrected or freshly uploaded inputs. `logs: none` still suppresses logs. The full `myce status` snapshot is large and slows the queue for everyone, so keep it out of loops, checks and briefs.
- **Large shared inputs (s072 onward):** resolve and submit compare the sum of distinct declared cache identities (file/tree sizes, plus cold archive staging bytes; aliases share one entry) with enabled matching workers' reported cache capacities. `shared_input_cache_capacity` refuses impossible work only when every enabled worker under consideration reports capacity and no matching target fits, naming required/capacity bytes; split the simultaneous inputs or make a fitting target eligible. With the mixed-version correction, a worker without the optional report retains staging-time admission and its local cache enforces actual capacity; the queue applies capacity checks to reporting workers. With no matching worker yet, the job waits with `diagnostic_code: shared_input_cache_unreported`. Earlier s072 queues required reports before placement. A fitting but occupied cache waits with `diagnostic_code: shared_input_cache` and required/free/evictable/capacity bytes; Mycelium routes a mixed fleet to a fitting worker. Leave placement automatic. Omitted cache limits use physical free disk (the environment policy: quarter free, 4–80 GiB, 20 GiB left free); explicit saved limits win. Older releases still default to 10 GiB. Worker staging retains `resource_limit` / `shared_input_admission` / `cache_space` diagnostics if disk changes after placement. Re-uploading identical bytes cannot fix an oversized declaration.
- **From the next release:** use **one wait per worker with several IDs**, for example `myce wait JOB_A JOB_B JOB_C` (up to 256). Keep waits in the foreground, batch IDs rather than starting one wait process per job, and keep them owned by the agent session. Foreground waits exit when their parent dies; jobs continue. Prefer the thread-bound completion notifier for detached submissions. Profile/adapter receipts share one managed observer per host/user, batching across worktrees; do not start receipt observers yourself.
- CLI, Go and Python waits preserve an initial diagnostic read, then negotiate batched long-polls against the cheap committed-state index. Legacy `?view=wait` reads use that same index while pending. Use `myce status JOB_ID` for fresh placement/deadline advice. Old clients keep working, and new clients fall back on older queues. A full terminal payload is fetched once by result/notification clients; CLI waits print scalar terminal state and sanitized reason.
- **Available from the next release (#810):** CLI, Go, Python and managed receipt waits use measured response cost, revisions and expected events instead of a fixed poll interval. An explicit owner timeout or the job deadline bounds observation, with no fixed 24-hour lifetime; observation timeout leaves the job running and is recorded separately from job failure. Re-run the original keyed profile/adapter command to rearm. Read outages back off; receipt observers reopen installed authority on retries. Notifier delivery has a 30-second bound and records failure for same-key recovery.
- On queues with #816, observation admission follows measured queue pressure rather than a per-source bucket; older queues retain the fixed-rate stopgap. HTTP 429/503 replies carry `Retry-After` as seconds or an HTTP date; #810 clients honour that minimum with jittered adaptive backoff. Worker control, check-ins and launches are exempt. Do not evade the limit by changing client identity. Avoid status/full-job loops; custom pending checks should use the wait view or `POST /api/wait` (prior per-job revisions; on supporting queues, fitting ID count and transport duration follow [capacity negotiation](https://github.com/1kuna/mycelium/blob/main/docs/request-budgets.md); older queues retain up to 256 IDs and a 0..20-second transport wait).
- **Available from the next queue release:** job GET `copies` retains per-attempt `started_at`, `stopped_at` and exact `released_at`; assignment time remains in `myce ledger JOB_ID`. Use exact release, not a stop request or grace expiry, to prove card custody ended. Missing old timestamps mean unknown. See [execution intervals](https://github.com/1kuna/mycelium/blob/main/docs/status.md#per-copy-execution-intervals).
- **From the next fleet release:** running job/status reads include `liveness` on the job and its exact `copies` attempt: `advancing`, `idle` with `idle_seconds`, or `unknown`, plus CPU-time and work-directory byte values and each signal's age. This works without application progress or logs. Unknown includes older workers, missing/incomplete measurements and samples older than 30 seconds; never read it as idle. “Alive and advancing” is activity, not useful-progress or output proof; “alive but idle” alone does not justify killing/retrying a job.
- For a long detached job, give it one stall check at about twice its expected duration, bound to your thread, verifying one thing (state or log growth), and delete it when done. If duration is unknown, record that uncertainty and a next owner for follow-up rather than inventing an ETA; use the foreground wait while this session owns completion.

## Stage progress and honest ETA

**Available from the next release:** report current-stage work through the existing
`MYCE_PROGRESS_FILE` contract, without credentials or log parsing. For example,
a two-stage CPU command can publish at each batch and stage transition:

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
# Run refinement, reporting batches; write outputs and exit normally afterward.
```

Indices are one-based. Omit `stage_count` if later stages are not yet known;
omit both stage fields if stage identity is unknown. `total_work_known` defaults
false; a known stage total alone never makes the entire workload known. Set it
true only when the complete workload across all stages is known. Counts remain
stage-local, even then: unequal stages cannot be averaged into a job percentage.
Rewrite at least every 30 seconds to keep reporting evidence fresh.

Read `myce status JOB_ID` or Python `Client.get(JOB_ID)`: `application_progress`
has stage index/count, work-known declaration, exact attempt, age and `stale`.
`runtime` separately has elapsed and remaining time, source, sample count,
history age and limitations. Missing timing stays unknown; completing stage one
never yields zero whole-job remaining. Historical ETA must describe the complete
pipeline under comparable workload/reference identities, not just its first
stage. An exceeded estimate becomes unknown. See the
[report contract](https://github.com/1kuna/mycelium/blob/main/docs/status.md#execution-elapsed-time-remaining-estimates-and-application-progress).

Checkpoint `progress` is a durable generation number, not a percentage or ETA.
Its existence does not prove that all stages or outputs are recoverable. Neither
stage completion nor a checkpoint means job success or resource release: keep
the same job ID, wait for success, and verify the usual results.

## Mycelium repository merge gates

When working on Mycelium itself, require independent review and a passing fleet
full gate for the PR's exact head (fleet quick is sufficient for docs-only).
Before merging, inspect the latest completed `main-full` run and fix any red
failure forward first; a reviewed fix with its own passing gate can merge while
main is red. If already fixed, cite the failing test and merged fix. Repair a
merge with no workflow run. Queued or running main runs do not block, and
completed proof need not match current main. Keep one self-hosted run per merge,
without workflow concurrency grouping; do not wait on, poll or wake for main CI.
See [the repository merge procedure](https://github.com/1kuna/mycelium/blob/main/docs/development.md#automatic-main-full-gates).

## Results

**Input upload attribution (next release):** profile submissions, files-from and
Go/Python `Run` obtain queue-issued project/file claims before uploading. For a
direct upload use `myce file put --project P FILE`, Python
`upload_file(path, project="P", class_="experiment")`, or Go
`WithFileStoreSubmission(ctx, spec)`. If local quotas/reservations are configured,
an unidentified upload returns `file_input_claim_required`; upgrade the client or
obtain `POST /api/files/input-claims` and carry `X-Myce-Input-Claim` through the
transfer. Project `reserved_file_bytes` protects unused capacity from other
projects' inputs; claims commit quota before bytes arrive. Production and
completed worker results remain exempt from these project checks. The shared
operator credential still chooses the project; claims do not create project
accounts. See [input attribution](https://github.com/1kuna/mycelium/blob/main/docs/files.md#project-input-attribution-and-capacity-reservations-464).

`myce result JOB_ID --out DIR` verifies each output's SHA-256 and size; then run your project's own acceptance check, since a wake or a zero exit proves no result. Download within the retention window (default seven days). The queue store is shared: write large outputs (hundreds of MB and up) to your project's own storage inside the job and return a small receipt (path, size, SHA-256) as the result. Once you have collected and verified results, release them with `myce file release plan --project P JOB... > PLAN.json`, review the plan, then `myce file release apply --accept-expiry PLAN.json`; on `file_release_stale`, plan and review again, then apply promptly. A project may have store quotas: an over-quota non-production submission is refused with `project_file_store_quota` and a reason giving usage and limit, and resubmitting the same key works after a release or once the 24-hour allowance frees. Production work is never refused by quota. Failed jobs have no accepted outputs; `myce result JOB_ID --attempt N --out DIR` fetches failed-attempt diagnostics. Report placement as "Mycelium placed it on X because Y", from the job's placement rationale.

**Release results early** to free shared store space once your project is completely done with them, rather than waiting out retention. Select only your own project's terminal jobs, after every downstream use has finished and any copy you must keep is saved and verified; include fan-out parents, children and keyed answers you want expired. The project name is attribution, not a permission check, so never select another project's jobs. Plan, review, then apply:

```sh
myce file release plan --project P JOB_ID... > release-plan.json   # read-only snapshot
myce file release apply --accept-expiry release-plan.json          # irreversible
```

The plan lists each job's files, what still protects them and `reclaimable_bytes` (it can be zero while still expiring your references). Apply rechecks the plan: `file_release_stale` means re-plan and review again, nothing was expired; `file_release_protected` means a selected job is still live, has unresolved custody or lacks a verified delivery, so drop it from the next plan. Success schedules asynchronous collection, so freed space isn't immediate, and the released results then return `result_expired`. Keep the plan and apply response as your audit record. API: `POST /api/files/release/plan` and `/api/files/release/apply`; see [operations](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#project-owner-early-file-release-465).

## Urgency and pausing

- **Urgency is the class.** When your work must go first, submit it with the class it truly has and read its waiting reason. A message asking another project to stop can't change queue order, and their queued jobs would take the freed slot anyway.
- **Class is the only urgency lever.** Production class preempts lower tiers; nothing else reserves machines except a job-bound validation reservation for a qualification check. There are no customer windows or holds. Pins, holds and messages to other projects are not ways to go first.
- **Isolated checks:** use a job-bound reservation, not a higher class or a chat-negotiated hold. `myce extension check --reserve` reserves its selected device (the whole host for a CPU check); for an existing single validation job, use `myce reserve add SELECTOR --job JOB_ID --owner OWNER --reason TEXT`. For GPU extension checks, `reserve add host=HOST` defaults to the check's explicitly selected device; use `--whole-host` only when the check needs host-wide isolation and explain why in `--reason`. Other validation jobs retain their authored scope. Prefer `host=HOST;devices=DEVICE` when only one device needs isolation, leaving siblings eligible; quote the selector in the shell. Whole-host CPU validation can become ready on shared interactive hosts even though their GPUs remain excluded. A never-ready reservation expires its admission fence at max lifetime if its bound job is queued without retained custody; others can run, while its bound job remains queued until explicit release (then reserve again). Once ready, reservations withhold new conflicting work while started work finishes, and release after the bound job finishes and cleanup is confirmed; they do not guarantee immediate capacity. See [validation reservations](https://github.com/1kuna/mycelium/blob/main/docs/operations.md#reserve-a-host-or-its-devices-for-an-existing-validation-job). Broader policy changes stay with the queue owner.
- **Pausing your own queued job:** `myce pause JOB_ID --reason TEXT` makes it non-runnable (add `--until RFC3339` for an automatic resume time); `myce resume JOB_ID` restores it with the same key and queue age. Assigned/running attempts cannot be paused; leave running jobs alone. Don't cancel and resubmit paused work.
- Queue policy (`myce policy`) is shared state: propose a change to the Mycelium thread rather than setting it.

## When a job waits

From the next release, `host_capacity_accumulation` names the ready CPU or
accelerator job whose RAM/cores are being protected from new backfill on one
selected host. Running attempts finish normally; earlier fair turns, higher
tiers, nonconflicting capacity and other hosts remain eligible. Unready
environments, occupied GPU cards or
impossible needs do not create this wait. Do not cancel/resubmit to bypass it.

From s072, missing environments can prepare beside unrelated CPU commands on
the same worker, including an accelerator worker, when one additional core is
available. Mycelium reserves that builder core separately and preserves running
commands. Status distinguishes an offered preparation from a started build;
an offer alone is not proof of progress. CPU-scoped builds may also run beside device commands; device-scoped builds
wait for those commands, and active model work still withholds a new build, and preparation preference considers only peers
that can start it.

From s071, `myce status JOB_ID` and job GET show deterministic preparation errors
as a failed job with `environment failed`, a build-log excerpt and instructions
to fix the recipe and resubmit. Read `myce logs JOB_ID` for the full build output.
Correct the dependency/lock/index/build inputs and submit changed intent with a
new submission key; changing only the submission key does not retry the same
failed environment. `logs: none` omits the excerpt and saved log. A transient
network, disk or builder infrastructure failure stays queued with `environment
retry pending` and a retry time (30-second exponential backoff, capped at five
minutes). Let it retry, address a continuing infrastructure hold with its owner,
and keep independent work moving; other environments remain eligible to build.

From the next release, shared-host `myce host status` health and fleet
`host_states` use the same pressure and bounded check-in contact evidence.
Compare fleet snapshot `now` with a separate host-status read: public status
can be cached for one second. Missing/stale pressure still withholds new work;
queue-delayed check-ins alone do not renew a failed probe.

1. Read its waiting reason: `myce status JOB_ID`. From the next release, reasons name reservations only on compatible targets, give the resolved RAM envelope against each eligible host's free memory, and explain missing protected network support or disallowed environment preparation. Device reservations permit CPU work when the host's RAM and cores fit. Repeated owner-latency preemptions on a shared host trigger a job-specific cooldown there; other eligible hosts remain available. A missing configured NAS mount names the affected root role and holds only work depending on it; healthy local/cached work can continue, and admission recovers automatically when the mount returns. Every waiting job has one: missing qualification, memory or resource needs, no available host, software preparation, or work ahead in the queue. **From the next release**, GPU contention says `device N shared with K attempts` or `card busy: waiting for GPU batch slot`, with holding job IDs and tiers or pending exact release; RAM figures on another eligible target no longer hide the card blocker. Owner pressure alone lets an otherwise qualified `require_target` submission queue with `shared host temporarily unavailable` until pressure clears; structural restrictions and impossible capacity still refuse.
   From the next release, an attempt refused by the queue's start fence records the predicate in its ledger and requeue history. That refusal spends no loss or infrastructure retry budget even if the fence clears before its failure report; a real transport or execution failure still uses the normal budget.
   From s070, a preparation blocker names the other job and environment, and distinguishes an offer awaiting a builder report from a started build or pending exact release. Ready work can fill an unready winner's slot until the existing priority/preparation rules take custody; after a priority stop, the selected target stays committed so preparation can progress. Report a persistent offer with no builder report on the Mycelium issue, with job and attempt identities; do not clear custody or retry a failed build blindly.
2. Fix what's yours, in order: right-size needs using comparable accepted evidence, add portable targets, make the job resumable, and choose `deadline` plus a real date only when warranted. Nonterminal submission/status and the initial CLI wait read may include deadline guidance explaining policy precedence, blockers and ETA uncertainty. This is advice, not a deadline guarantee or an edit to accepted job intent; for an existing job's urgency, ask the queue owner rather than cancelling and resubmitting it.
3. Otherwise it's a hold with a next owner: record it on your ticket and keep other work moving. Running heavy work locally on a shared host instead isn't a fallback.
4. If the reason looks wrong, or Mycelium itself failed, search the Mycelium issues for a duplicate, then update or file a sanitized one (expected versus observed, version, job and attempt id, minimal command). The repo is public: no secrets, private paths, host details or webhook URLs.

## Credentials

For catalog image chat, install the repository's current Python client with
its `openai` extra and use `Client.from_installed().openai(project=PROJECT,
priority="experiment")`. The installed opener preserves queue TLS authority;
send images as OpenAI `image_url` parts with base64 data URLs. The model must
already be in the matching queue/worker catalog. See the
[image-chat example](https://github.com/1kuna/mycelium/blob/main/examples/image-chat.py)
for two-image requests and timings; its example model name is not a claim that
your installation has that model. Use the reviewed client commit containing
the installed-transport fix before calling a queue with a private TLS pin.

`myce submit FILE` also uses installed credentials when `MYCE_TOKEN` is unset, including the installed queue address and TLS pin.

Never open, print, copy or pass fleet credentials, and never set the fleet token yourself. Use the installed-authority CLI or `Client.from_installed()`, which resolve credentials internally; prefer the CLI for read-only status and result calls. A missing CLI or failed authority is a setup hold, not a reason to look for the token.

## Diagnostic reference: one blocker

With a client and queue containing #793's diagnostic changes, resolve, status and
wait lead with one dependency, next owner and action. `unprepared` means no builder
has started; `waiting_for_builder` means an offer or preparation capacity is still
pending; only a fresh builder report says `building`. A deterministic failure keeps
its recipe error: fix immutable inputs rather than retrying unchanged bytes.
`resource_pressure` retains measured resource/reservation evidence;
`unknown_observation` is a queue-owner hold to restore observations. Keep other
work moving on a hold. CPU resolve shows CPU views, and successful waits omit
urgency tutorials. Source placeholders marked `needs-staging` are uploaded by run.

## Benchmark telemetry receipts

For benchmark memory/activity receipts, candidate builds with
`myce telemetry capture` provide a bounded host-local sampler with no queue
requests. Attach its versioned JSONL beside retained exact-attempt memory and
CPU/I/O facts; [benchmark telemetry](https://github.com/1kuna/mycelium/blob/main/docs/benchmark-telemetry.md)
defines timestamps, coverage, units, missing readings and attribution. Earlier
installed releases do not imply this command exists. Native replay permission
and output-parity requirements still apply.

## Diagnostic reference: project doctor

On a supporting release, run `myce doctor --project` from the project root for
one read-only diagnosis. Select an entry with `--entry NAME`, another adapter
with `--adapter FILE`, or structured output with `--json`. It checks installed
authority without exposing it, client/server support, adapter/source validity,
dependency readability, resolve findings, target coverage and completion setup.
It prints one next owner/action and exits nonzero for a blocker. Fix consumer
findings; record installation, queue or storage-owner holds on your issue.

No notifier is required for foreground `myce wait`. If using detached completion,
pass this thread's executable with `--notify PATH` (or `MYCE_NOTIFY`); a missing or
non-executable supplied notifier blocks; executable lookup follows run, including
PATH lookup. Doctor never runs it, tests delivery or
verifies thread ownership. Follow [T3 binding](t3-binding.md) for that proof.
It also leaves application result checks unexecuted; verify downloaded results.

Doctor uses existing read-only resolve and source checks. It uploads/builds
nothing and changes no installation or receipts. Cold environments with a builder
are normal submit-time preparation; failed recipes, unavailable builders, pressure
and unknown observations retain resolve's action. NAS dependency reads remain an
explicit storage-owner verification hold; doctor does not inspect worker NAS.
An unsupported queue or missing installed authority is an owner hold, never a
reason to find credentials or run heavy work locally. Ordinary `myce doctor`
continues to diagnose an installation.
