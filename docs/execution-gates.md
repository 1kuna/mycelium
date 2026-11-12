# Execution gate audit (MY-44)

Implementation audit against [MY-44](DECISIONS.md#my-44), 2026-11-10. This
records source behavior for the next release, not a deployment claim. The
implementation worker received MY-44 through its task brief. Ordinary native
commands already run without extension registration, qualification or sandbox
opt-in; declaring needs is sufficient, subject to the protections below.

| Boundary | Disposition and reason |
| --- | --- |
| Declared model repository permission | Removed the implicit deny-all default. No model policy, empty policy and limits-only policy permit every repository. Optional `sources` restrictions remain an owner's deliberate deployment setting; `!` denies can exclude repositories without maintaining a permission list. |
| Master download source | Removed the built-in Hugging Face hostname allowlist from `myce model fetch`. Any HTTPS file URL can supply size/SHA-256 verified bytes; path containment, resumable staging and immutable publication remain. |
| Model copy size, cache budget and disk reserve | Kept: cold copies must fit real storage and leave room for metadata/control state. `max_bytes` remains optional; omitted/zero `free_floor_bytes` derives the existing physical floor from the cache volume, rather than requiring owner configuration. |
| Project `allowed_machines`, authored host/device pins and exclusions | Kept as optional explicit restrictions, absent by default. They encode placement requirements or deliberately partition shared hardware; no project registration or permission list is needed for ordinary work. Clear unnecessary restrictions at their source rather than silently overriding configured intent. |
| Extension/adapter registration | Kept immutable recipe registration, not owner approval. Consumers can register through the ordinary authenticated client. Frozen digests prevent a later edit replacing accepted code. Plain commands bypass this mechanism entirely. |
| Extension native qualification | Kept automatic first-demand checks against consumer-authored native baselines. Failures mean the declared implementation has not produced the required result on this device/environment; they cannot be treated as operator approval. Smoke checks are insufficient to claim output equivalence. Plain commands need no such qualification. |
| Project quotas and fair sharing | Kept optional explicit memory/concurrency/file-store quotas to protect other active workloads and shared storage; absent quotas do not require a project approval. Production priority and physical headroom remain authoritative. |
| Production parity and quality tolerances | Kept: MY-19/MY-27 require output parity for new production variants. New multi-variant registrations still require measured parity or explicit project/work machine-kind approval (MY-51) when measurements are unknown; an explicit approval list bounds production even with measurements. This is a remaining approval path with a correctness purpose: avoid silently changing production output. Consumer-requested parity approvals, result checks and tolerance limits are the consumer's own quality gates and are unchanged. Experiments/checks can collect comparison evidence. |
| Automatic plain-command production target discovery | Kept the MY-19 parity hold for inferred targets: immutable locks and a portable declaration show runnable software, not equivalent production output. Explicit targets or native variant/parity evidence remain available; experiments can discover targets automatically. |
| `unsandboxed: legacy extension registration` | No execution gate: this is a diagnostic describing retained registrations. New extensions default to sandboxed; the author may declare `isolation: unsandboxed` with a reason without owner approval. Explicit sandbox requests still fail closed if unavailable; plain native commands already default to unsandboxed. Token/environment scrubbing and launch custody apply in every mode. |
| In-place local model `approve-local` | Kept as an explicit local filesystem grant and integrity record, not a repository allowlist. It binds an exact folder, symlink root, host and manifest; a remote submitter cannot point a model server at arbitrary worker-private files. Changed bytes invalidate the record. Declared snapshots and verified packaged/file-store models avoid this local-path grant. |
| Model/server/environment catalog writes | Kept authenticated mutation of shared definitions and installed authority. These writes can alter other projects' serving configuration; ordinary submitted commands, portable adapter preparation and network-enabled download jobs do not require a catalog-owner approval. Frozen recipes/locks and hash verification protect accepted bytes. |
| Host enrollment, signed release approval and OS permissions | Kept: these establish trust in worker/control-plane code and authorize access to devices or mounts. They are installation boundaries, not permission to approve each workload. OS-required mount permissions and driver configuration cannot be supplied by accepting a queue job. |
| Memory, device admission and quarantine, disk health, maintenance and owner presence | Kept: prevent physical exhaustion, reuse of faulting devices, conflicting maintenance and interference with an actively used personal machine. Exact attempt fencing and process-release custody prevent stale work reclaiming resources. |
| Secrets, environment allowlists and `MYCE_TOKEN` isolation | Kept: consumer work must not inherit fleet/control credentials. These are environment boundaries, not executable allowlists. |
| Production tiers, preemption, checkpoint/resume compatibility and holds | Kept: prioritize intended production work and preserve active execution/progress before resource reuse. Resumability and disposable restart remain explicit consumer contracts. |

Audited submission/resolve (`model_snapshots.go`, `resolve.go`), priority/project
policy (`priority_policy.go`, `fairness.go`), registration and automatic checks
(`extensions.go`, `extension_qualification.go`), production eligibility
(`variant_status.go`, `quality.go`, `production_hold.go`), native isolation and
network capability (`declared_isolation.go`, `native_network.go`), local model
identity (`local_models.go`), master acquisition (`model_fetch.go`), portable
command discovery (`command_discovery.go`), catalog/owner operations, enrollment/release trust,
and worker environment/admission. Source names refer to `internal/myce` unless
otherwise linked. No production policy or running job was changed by this audit.
