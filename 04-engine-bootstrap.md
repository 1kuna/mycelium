# Mycelium - Engine Bootstrap Spec (Document 4)

> Status: future addition / extension spec. The current MVP and the current fleet benchmark use inference engines that are already installed and configured. This document defines the missing future layer that makes "one command joins a useful compute peer" true without manual engine setup on every machine.

This document does not replace the three current SSOT documents. It extends them with a host-readiness layer that sits between peer onboarding and backend launch.

## 1. What this adds

Mycelium already owns backend lifecycle once an engine exists: launch, ready-gate, stop, reaper, leases, scheduler fit, telemetry, and gateway routing. Engine bootstrap is the missing step before that:

1. Detect the host OS, accelerator stack, installed engines, package managers, disk state, and safety limits.
2. Decide which engine profiles are suitable for this machine.
3. Prefer engines already installed and usable.
4. When asked to apply changes, install or configure the selected engine profile.
5. Write durable peer config so the node can run as a compute peer without hand-editing `peer.json`.
6. Prove the engine profile with a tiny readiness smoke, not a large model load.

The north-star flow becomes:

```bash
mycelium bootstrap --join <mycjoin://...> --compute auto --apply
mycelium run
```

After bootstrap, the normal product path remains unchanged: apps submit to any peer, the coordinator places jobs, and the selected owner launches a backend adapter from an explicit profile.

## 2. Non-goals

- Mycelium still does not become an inference engine.
- Bootstrap does not download large models. Model artifact install stays in `myce add-model`.
- Bootstrap does not remotely install software on another peer. No SSH product transport. The command runs locally on the machine being prepared.
- Bootstrap does not bypass `max_util`, `oom_severity`, disk floors, owner admission, or scheduler fit.
- Bootstrap does not hide package-manager failures. A missing package manager, missing GPU runtime, checksum mismatch, unsafe vLLM cap, or unsupported OS fails loudly.
- Bootstrap does not select model quality winners. It may record engine facts and benchmark facts; judgment stays outside Mycelium.

## 3. Relationship to existing docs

- Document 1 says Mycelium conducts existing engines and supervises them as executable packages. This document defines how those executable packages can be discovered or installed.
- Document 1 Phase 3 model install remains model/preset install. Engine bootstrap is separate from catalog/model install.
- Document 1 Phase 4 onboarding remains join-token + LAN discovery. Engine bootstrap can be composed with onboarding but does not change peer membership authority.
- Document 3's current MVP gates do not require this layer. Future gates in this document are additive.
- The current fleet benchmark must use currently installed/configured engines unless the operator explicitly runs bootstrap first.

## 4. User-facing commands

### 4.1 Local bootstrap

```bash
mycelium bootstrap \
  --join <mycjoin://host:port?token=...&rpc_token=...> \
  --compute auto \
  --engines auto \
  --config ~/.mycelium/peer.json \
  --apply
```

Behavior:

- Without `--apply`, bootstrap is a dry-run doctor. It prints and writes an install plan but changes nothing.
- With `--apply`, bootstrap executes only the explicit plan it printed.
- `--join` persists membership/RPC token state through the same token manager used by normal peer join.
- `--compute auto` enables compute only if at least one engine profile becomes ready.
- `--engines auto` chooses supported profiles for the host. `--engines llamacpp,mlx` narrows the set.
- The command writes `~/.mycelium/bootstrap/<job_id>/plan.json`, `events.jsonl`, and `result.json`.
- If a peer config already exists, bootstrap updates only engine/profile fields it owns. It does not rewrite unrelated fleet, project, token, or policy settings.

### 4.2 Engine doctor

```bash
mycelium bootstrap --doctor --config ~/.mycelium/peer.json
myce engines doctor --peer local
myce engines list
```

Behavior:

- Reports installed engines, detected hardware, usable profiles, missing prerequisites, unsafe settings, and config drift.
- `myce engines doctor --peer <id>` reads remote peer-reported readiness facts over authenticated peer RPC. It does not install remotely.
- Doctor output is machine-readable with `--json`.

### 4.3 Explicit engine setup

```bash
mycelium bootstrap --engines llamacpp --apply
mycelium bootstrap --engines mlx --apply
mycelium bootstrap --engines vllm --apply
mycelium bootstrap --engines custom --backend-binary /path/to/wrapper --apply
```

Explicit setup is still profile-driven. Unknown engines fail. Custom profiles require a binary/wrapper and a health path.

## 5. Architecture

Engine bootstrap is a local subsystem:

```text
CLI bootstrap
  -> HostDetector
  -> EngineDetector
  -> BootstrapPlanner
  -> Package/Runtime Installer
  -> EngineVerifier
  -> EngineRegistry + PeerConfig writer
```

Runtime launch continues through existing backend adapters:

```text
Gateway request
  -> scheduler chooses preset/node/unit
  -> owner admission commits lease
  -> node agent launches BackendAdapter using EngineProfile
```

Bootstrap produces durable readiness data. The scheduler never shells out to package managers and never installs anything on a placement path.

## 6. New ports

Add these interfaces under `internal/ports` when this document is implemented.

```go
type HostDetector interface {
    DetectHost(ctx context.Context) (domain.HostFacts, error)
}

type EngineDetector interface {
    DetectEngines(ctx context.Context, host domain.HostFacts) ([]domain.EngineDetection, error)
}

type BootstrapPlanner interface {
    PlanBootstrap(ctx context.Context, req domain.BootstrapRequest, host domain.HostFacts, detections []domain.EngineDetection) (domain.BootstrapPlan, error)
}

type EngineInstaller interface {
    ApplyBootstrapPlan(ctx context.Context, plan domain.BootstrapPlan, progress func(domain.BootstrapEvent)) (domain.BootstrapResult, error)
}

type EngineVerifier interface {
    VerifyEngine(ctx context.Context, profile domain.EngineProfile) (domain.EngineVerification, error)
}

type EngineRegistry interface {
    SaveEngineProfile(ctx context.Context, profile domain.EngineProfile) error
    ListEngineProfiles(ctx context.Context) ([]domain.EngineProfile, error)
    MarkEngineProfileUnready(ctx context.Context, profileID, reason string) error
}
```

Every implementation gets a hand-written mock, compile-time assertion, and conformance suite. Fast tests use mocks only.

## 7. Domain shapes

The final field names can change when implemented, but the persisted shape should preserve these concepts.

```go
type HostFacts struct {
    NodeID            string
    OS                string // darwin, linux
    Arch              string // arm64, amd64
    Platform          string // os/arch, for example linux/arm64 or darwin/arm64
    Kernel            string
    LibC              string // glibc, musl, none/unknown
    PackageManagers   []string // brew, apt, dnf, pacman, docker, uv, pipx
    ContainerRuntime  string
    Accelerators      []Accelerator
    DriverFacts       map[string]string // cuda, rocm, level-zero, metal, driver versions
    TotalMemoryMB     int
    DiskFreeMB        int
    DiskTotalMB       int
    DiskMinFreeRatio  float64
    OOMSeverity       OOMSeverity
}

type EngineProfile struct {
    ID                string
    Backend           Backend
    DisplayName       string
    ManagedBy         string // system, mycelium, custom
    BinaryPath        string
    Args              []string
    Env               map[string]string
    HealthPath        string
    Version           string
    Source            EngineSource
    SupportedModels   []ModelFormat // gguf, hf-transformers, mlx
    RequiredLabels    map[string]string
    SupportedPlatforms []string // exact os/arch keys this profile can run on
    ArtifactPlatform  string // package/image/wheel platform actually selected
    MaxUtilDefault    float64
    DiskMinFreeRatio  float64
    Safety            EngineSafety
    VerifiedAt        time.Time
    Ready             bool
    UnreadyReason     string
}

type BootstrapPlan struct {
    ID                string
    CreatedAt         time.Time
    Host              HostFacts
    RequestedEngines  []Backend
    Actions           []BootstrapAction
    ResultingProfiles []EngineProfile
    Warnings          []string
}

type BootstrapAction struct {
    ID                string
    Kind              string // detect, install_package, pull_image, create_venv, write_wrapper, verify, write_config
    EngineProfileID   string
    CommandPreview    []string
    RequiresPrivilege bool
    EstimatedBytes    int64
    SourceURL         string
    Checksum          string
    Platform          string // exact package/image/wheel platform for this action
}
```

Persist bootstrap jobs in the same durable job/store surface used by catalog installs. Bootstrap progress is observable in `myce jobs list`.

## 8. Platform matrix

### 8.0 Compatibility key: OS + CPU architecture + accelerator runtime

Bootstrap must treat platform compatibility as a first-class constraint. "Linux NVIDIA" is not specific enough: the DGX Spark is Linux on ARM64, while most CUDA/vLLM examples assume Linux on x86_64. The same distinction applies across the fleet: macOS Apple Silicon is `darwin/arm64`, an Intel Mac is `darwin/amd64`, most desktop GPU boxes are `linux/amd64`, and specialty systems may be `linux/arm64`.

The compatibility key is:

```text
os + cpu_arch + accelerator_vendor + accelerator_runtime + driver_version + engine_distribution
```

Examples:

- `darwin/arm64 + apple/metal + llama.cpp-homebrew`
- `darwin/arm64 + apple/metal + mlx-python-wheel`
- `darwin/amd64 + cpu-or-amd/metal-unsupported + llama.cpp-homebrew`
- `linux/arm64 + nvidia/cuda + nvcr.io/nvidia/vllm arm64 image`
- `linux/amd64 + nvidia/cuda + vllm x86_64 wheel/image`
- `linux/amd64 + intel/level-zero + sycl llama.cpp wrapper`

Rules:

- A profile is schedulable only when its `SupportedPlatforms` contains the host platform and its accelerator runtime checks pass.
- Bootstrap must never infer that a `linux/amd64` binary, wheel, or image is valid on `linux/arm64`.
- Docker images must be checked for the selected platform, not just by repository/tag name. Multi-arch tags are accepted only when the manifest contains the host platform.
- Python wheels must be checked against host arch, Python ABI, OS, and accelerator stack. A wheel name or package install that works on x86_64 is not evidence it works on ARM64.
- Package-manager plans must record the package source and platform resolved by the package manager.
- Existing binaries/wrappers must be probed on the local host; a path copied from another architecture is not adopted unless it executes and reports a compatible version.
- Cross-compiled Mycelium itself is allowed, but backend engines are host-native processes/containers and must match the host platform.
- Unknown arch/runtime combinations fail with doctor evidence and no fallback.

### 8.1 macOS Apple Silicon

Supported profiles:

- llama.cpp Metal via native subprocess.
- MLX via `mlx_lm.server` in a Mycelium-managed Python environment.
- Custom native process.

Rules:

- Prefer an existing usable `llama-server` on `PATH` or configured in `peer.json`.
- If installing llama.cpp, prefer Homebrew when present.
- If installing MLX, create an isolated venv under `~/.mycelium/engines/mlx-lm/`.
- Never use Docker as the default Mac compute path.
- Treat unified memory as one pressure domain.
- Verify with a tiny local model or a no-model/version probe where the engine supports it. Do not download a large model during engine bootstrap.

Default safety:

- `max_util`: conservative host-specific default, never above the configured peer ceiling.
- `disk_min_free_ratio`: 0.25 unless the user config says stricter.
- `oom_severity`: `soft` unless detector identifies a catastrophic host profile.

### 8.1b macOS Intel

Intel Macs are not a primary LLM target, but bootstrap must classify them correctly.

Supported profiles:

- llama.cpp CPU or any detected discrete-GPU path that proves support.
- Custom native process.

Rules:

- Do not install or select MLX on `darwin/amd64`; MLX is Apple Silicon only for this product's supported profile.
- Do not apply Apple Silicon/Homebrew assumptions to Intel paths.
- Do not silently degrade an intended Metal/MLX profile to CPU. CPU fallback requires explicit opt-in.
- Doctor should report `darwin/amd64` as supported-for-detection and usually unready-for-compute unless a valid profile is explicitly configured.

### 8.2 Linux NVIDIA / DGX Spark

Supported profiles:

- vLLM OpenAI server through an existing binary or container wrapper.
- SGLang through an existing binary or container wrapper.
- llama.cpp CUDA where available.
- Custom process/container wrapper.

Rules:

- Split Linux NVIDIA into at least `linux/arm64` and `linux/amd64` during planning.
- DGX Spark is `linux/arm64`; it requires ARM64-compatible vLLM/SGLang/llama.cpp builds or multi-arch container images with a matching ARM64 manifest.
- Do not use generic x86_64 vLLM wheels/images on Spark. A tag named `latest`, `nightly`, or `cuda` is not enough evidence.
- For Spark, prefer the known NVIDIA-provided or already-installed ARM64-compatible vLLM image/wrapper that passes doctor.
- Prefer an already installed NVIDIA/vLLM image or binary that passes doctor.
- If installing container support, require Docker or compatible runtime plus NVIDIA Container Toolkit.
- DGX Spark defaults to `oom_severity=catastrophic`.
- For catastrophic NVIDIA hosts, bootstrap must refuse a vLLM profile whose default `--gpu-memory-utilization` is above the safe cap.
- Default Spark cap should be <= 0.85 unless the user explicitly configures a lower value; never silently raise it.
- Bootstrap must not start the 122B-class production path as a readiness check. Use a tiny cached model or version/health probe.

Default safety:

- `max_util`: <= 0.90 for Spark-class catastrophic hosts.
- vLLM `--gpu-memory-utilization`: <= 0.85 for Spark-class catastrophic hosts.
- no concurrent cold-load smoke on catastrophic hosts.

### 8.2b Linux NVIDIA x86_64

Supported profiles:

- vLLM/SGLang wheels or containers that explicitly support `linux/amd64` and the detected CUDA driver/runtime.
- llama.cpp CUDA binaries built for `linux/amd64`.
- Custom process/container wrapper.

Rules:

- Match CUDA major/minor requirements before adopting a wheel/image.
- Do not assume an ARM64 Spark-safe image has an x86_64 manifest or equivalent behavior.
- Keep x86_64 package/image resolution separate from Spark's ARM64 path so fixes for one platform cannot silently alter the other.

### 8.3 Linux Intel Arc / B70

Supported profiles:

- SYCL llama.cpp through an existing binary or container wrapper.
- Intel XPU/vLLM-compatible wrapper if already installed and verified.
- Custom process/container wrapper.

Rules:

- B70-class machines are expected to be `linux/amd64` unless host detection proves otherwise.
- Detect Intel GPUs through `clinfo`, Level Zero, or the available runtime.
- Prefer existing local images/wrappers before installing anything.
- If disk free is below the configured floor, bootstrap may report installed engines but must mark compute readiness blocked.
- Do not assume NVIDIA-style vLLM flags apply to Intel profiles.
- Engine verification should check the actual backend health path and a minimal generation only when a safe tiny model is configured.
- SYCL/XPU profiles must record the selected architecture and runtime version; a container image tag is not enough without platform/runtime proof.

### 8.4 Linux AMD

Supported profiles:

- ROCm-compatible llama.cpp or vLLM profiles where the host proves support.
- Custom process/container wrapper.

Rules:

- Treat AMD as supported-by-contract but not MVP smoke unless hardware is available.
- Unknown ROCm/vLLM combinations fail with actionable doctor evidence, not generic fallback.

### 8.5 CPU fallback

CPU-only inference is allowed only with an explicit opt-in profile.

Rules:

- Never auto-select CPU fallback for a compute peer with accelerators unless the user asks for it.
- Mark CPU profiles low speed and low priority in scheduler facts.
- Keep CPU fallback useful for tiny smoke/debug, not as a silent production degradation.

## 9. Planning rules

Bootstrap planning is deterministic and conservative:

1. Read host facts.
2. Read existing peer config and engine registry.
3. Detect usable existing engines.
4. Reject the host if disk free is below `disk_min_free_ratio`.
5. Reject engine candidates whose package, binary, wheel, or image platform does not exactly match the detected host platform.
6. Generate install actions only for requested engines that are supported on the detected host.
7. Prefer no-op adoption of an existing engine over install.
8. Prefer native Mac engines over containers on macOS.
9. Prefer explicit configured wrappers over generic package install on specialty hardware.
10. Produce one plan with exact actions. No hidden fallback actions.
11. Require `--apply` to mutate the host.

If multiple profiles are ready, bootstrap may either:

- write the best profile as the active `compute_config`, preserving current single-backend runtime behavior; or
- when multi-profile runtime support exists, register all profiles and advertise all compatible backend capabilities.

Until multi-profile runtime support exists, the first implementation should choose one active profile and record the others as detected-but-inactive.

## 10. Safety and trust

Bootstrap is allowed to install software, so it must be stricter than runtime placement.

Hard requirements:

- Dry-run by default.
- Every network source, package/image name, version, estimated bytes, and checksum/digest is shown in the plan.
- Public dataset/model downloads are never bundled into engine setup.
- Package manager commands are explicit actions, not generated shell strings.
- Privileged actions are marked before apply.
- Existing user-managed binaries are never overwritten.
- Managed installs live under `~/.mycelium/engines/` where practical.
- Config writes use atomic replace.
- Failed apply leaves the prior peer config usable.
- Apply is resumable through the bootstrap job record.
- A ready profile is saved only after install/config/verification all complete.
- Any profile whose verification fails is recorded as unready with evidence.

## 11. Disk and memory policy

Engine bootstrap must respect the same safety posture as scheduler placement.

Disk:

- Default `disk_min_free_ratio` is 0.25.
- Bootstrap refuses installs that would leave the host below the disk floor.
- Bootstrap writes the disk floor into peer config if none exists.
- Model install and scheduler placement continue to enforce disk headroom independently.

Memory:

- Bootstrap records host-level `max_util` and `oom_severity`.
- Spark-class catastrophic hosts default to `max_util <= 0.90`.
- Engine-specific memory flags must be bounded by the host cap.
- vLLM/SGLang reservation flags become estimator metadata; if missing, placement fails loudly.

## 12. Peer config integration

Bootstrap writes durable config that normal `mycelium run` can consume.

Minimum current-runtime output:

```json
{
  "compute": true,
  "join_token": "...",
  "rpc_token": "...",
  "seed_peers": ["192.0.2.10:52091"],
  "compute_config": {
    "id": "spark-peer",
    "name": "DGX Spark",
    "backend": "vllm",
    "backend_binary": "/opt/mycelium/engines/vllm/run-vllm.sh",
    "backend_listen": "0.0.0.0:52074",
    "max_util": 0.9,
    "disk_min_free_ratio": 0.25,
    "oom_severity": "catastrophic"
  }
}
```

Future multi-profile output can add:

```json
{
  "engine_profiles": [
    {
      "id": "spark-vllm-ngc",
      "backend": "vllm",
      "ready": true,
      "binary_path": "/opt/mycelium/engines/vllm/run-vllm.sh"
    }
  ]
}
```

The scheduler should use only ready profiles. Unknown or unverified profiles are not schedulable.

## 13. Catalog/model install integration

Engine bootstrap and model install are separate jobs but can inform each other:

- Bootstrap tells the catalog which model formats this host can serve.
- Catalog install checks disk floor before staging artifacts.
- Catalog install may choose a target node based on ready engine profiles and model format.
- Model provenance records the engine format assumptions, but not a specific engine binary.
- A model is not registered as usable on a node unless at least one ready engine profile can serve it.

## 14. Optimizer and benchmark integration

The optimizer may use bootstrap facts as inputs:

- engine version
- profile source
- launch args
- memory cap
- disk floor
- model format support
- measured smoke performance

The fleet benchmark should:

- default to currently installed/configured engines;
- include engine readiness facts in `manifest.json`;
- fail loudly if a required profile is missing;
- never run bootstrap implicitly unless a future explicit `--bootstrap` flag is added;
- use simulation/preflight to prove the scenario only against ready profiles.

## 15. Observability

Every bootstrap run emits:

- `plan.json`: host facts, requested engines, exact actions, safety checks.
- `events.jsonl`: detect/install/config/verify progress.
- `result.json`: ready profiles, unready profiles, config changes, evidence.
- `doctor.json`: latest host readiness summary.

Peer health should include an engine-readiness summary:

```json
{
  "engines": [
    {
      "profile_id": "macbook-llamacpp-metal",
      "backend": "llamacpp",
      "platform": "darwin/arm64",
      "artifact_platform": "darwin/arm64",
      "ready": true,
      "version": "llama-server ...",
      "source": "homebrew"
    }
  ]
}
```

Do not expose secrets, tokens, full environment variables, or private paths beyond what is necessary for local diagnostics.

## 16. Testing plan

### Fast tests

Fast tests use no real package managers, no network, no subprocesses, and no hardware.

Required coverage:

- Host detector mocks for macOS Apple Silicon, Linux NVIDIA/Spark, Linux Intel/B70, Linux AMD, and CPU-only.
- Host detector distinguishes `darwin/arm64`, `darwin/amd64`, `linux/arm64`, and `linux/amd64`.
- Engine detector mock records existing engine binaries/images and injected failures.
- Planner prefers existing usable engines over installs.
- Planner rejects unsupported OS/backend combinations.
- Planner rejects architecture-mismatched binaries, Python wheels, and container images.
- Planner accepts a multi-arch container tag only when its manifest includes the detected host platform.
- Planner proves DGX Spark vLLM setup uses an ARM64-compatible image/wrapper.
- Planner refuses to apply Apple Silicon MLX setup on Intel macOS.
- Planner refuses Spark vLLM caps above the safe default.
- Planner refuses installs that violate `disk_min_free_ratio`.
- Planner marks B70-style low disk as blocked before engine install.
- Planner never chooses Docker as default Mac compute.
- Installer applies actions in order and is resumable.
- Failed install does not save a ready profile.
- Failed verification saves an unready profile with evidence.
- Config writer preserves unrelated peer config fields.
- `myce jobs list` shows bootstrap progress.
- Conformance suites for `HostDetector`, `EngineDetector`, `BootstrapPlanner`, `EngineInstaller`, `EngineVerifier`, and `EngineRegistry`.

### E2E simulated tests

Synthetic fleet:

- Apple Silicon dev Mac: `darwin/arm64`, existing Homebrew llama.cpp and optional MLX venv.
- Intel Mac example: `darwin/amd64`, Homebrew present, MLX unsupported, CPU fallback only with explicit opt-in.
- second Apple Silicon Mac: `darwin/arm64`, no engine installed, Homebrew available.
- DGX Spark: `linux/arm64`, existing ARM64-compatible NVIDIA vLLM image/wrapper, catastrophic OOM, cap required.
- x86_64 NVIDIA box: `linux/amd64`, x86_64 CUDA/vLLM image or wheel.
- B70: `linux/amd64`, existing SYCL wrapper, disk below 25% free.

Expected behavior:

- local dev Mac adopts existing llama.cpp.
- Intel Mac reports MLX unsupported and does not silently select CPU.
- second peer plans Homebrew llama.cpp install but does not mutate without `--apply`.
- Spark adopts existing ARM64 vLLM image/wrapper only if cap <= 0.85.
- Spark rejects x86_64-only vLLM images/wheels even when the engine name and CUDA version look plausible.
- x86_64 NVIDIA accepts only x86_64-compatible CUDA/vLLM artifacts.
- B70 reports engine candidates but compute readiness blocked by disk floor.
- Resulting benchmark preflight uses only ready profiles.

### Smoke tests

Smoke tests are phase-boundary only and require explicit environment/config.

Targets:

```bash
make smoke-bootstrap-macos
make smoke-bootstrap-mlx
make smoke-bootstrap-spark-vllm
make smoke-bootstrap-b70
```

Rules:

- Required skips fail.
- Hardware-unavailable smoke is recorded in `BLOCKERS.md`.
- Smoke uses tiny/safe verification workloads.
- Spark smoke must not launch a 122B-class model or exceed the configured memory cap.
- B70 smoke must not run if disk floor is violated unless the test is specifically proving the violation path.

## 17. Implementation phases

### Bootstrap Phase A - contracts and dry-run doctor

Build domain types, ports, mocks, conformance suites, and `mycelium bootstrap --doctor`.

Gate:

```bash
go test ./internal/bootstrap/... -race
go test ./cmd/mycelium/... -race
go test ./... -race
```

### Bootstrap Phase B - macOS llama.cpp

Implement macOS host detection, Homebrew llama.cpp detection/install plan, config write, and verification.

Gate:

```bash
go test ./internal/bootstrap/... -race
make smoke-bootstrap-macos
```

### Bootstrap Phase C - macOS MLX

Implement MLX venv detection/install plan and `mlx_lm.server` profile verification.

Gate:

```bash
go test ./internal/bootstrap/... -race
make smoke-bootstrap-mlx
```

### Bootstrap Phase D - Linux NVIDIA / Spark

Implement NVIDIA host detection, Docker/NVIDIA runtime detection, vLLM profile planning, safe cap enforcement, and Spark smoke.

Gate:

```bash
go test ./internal/bootstrap/... -race
make smoke-bootstrap-spark-vllm
```

### Bootstrap Phase E - Linux Intel / B70

Implement Intel GPU detection, SYCL/custom-wrapper profile planning, disk-floor blocking, and B70 smoke.

Gate:

```bash
go test ./internal/bootstrap/... -race
make smoke-bootstrap-b70
```

### Bootstrap Phase F - multi-profile runtime

Allow one peer to advertise multiple ready engine profiles and let the scheduler choose among compatible profiles for a preset.

Gate:

```bash
go test ./internal/scheduler/... ./internal/node/... ./internal/bootstrap/... -race
make smoke-local
make smoke-fleet
make smoke-benchmark-fleet
```

### Future Phase G - explicit installer apply

Installer apply is future work. The current `engines install-plan` surface is advisory-only: it validates the path, planned actions, approval requirements, dry-run/manual boundaries, risks, and rollback notes, but it does not mutate the host.

The apply implementation must preserve a hard approval boundary from plan to mutation:

- an operator must select a specific saved install plan or freshly generated plan and pass an explicit apply flag;
- the command must show the exact actions, network/package/image sources, expected files, expected config/profile writes, required privileges, and rollback state before mutation;
- the applied plan must be the plan that was approved, not a silently regenerated different plan.

Per-engine apply executors should stay profile-driven:

- llama.cpp: Homebrew/native Metal on macOS Apple Silicon, CUDA build/container/wrapper on Linux NVIDIA, SYCL/oneAPI wrapper on Linux Intel/B70, ROCm path on Linux AMD when verified, CPU only with explicit opt-in;
- vLLM: Linux NVIDIA arm64/amd64 wheel or container strategy with platform/CUDA/toolkit checks and safe memory caps;
- SGLang: Linux NVIDIA wheel/container strategy only after the backend adapter/profile contract exists;
- MLX: macOS Apple Silicon managed venv under `~/.mycelium/engines/mlx-lm/` with `mlx_lm.server`;
- OpenVINO: Linux Intel/B70 OpenVINO GenAI or OVMS path selected by model-family support, with the existing B70 proof treated as evidence but not a generic shortcut;
- custom/native: no generic installer; apply only validates and records an explicit user-provided binary/profile/health contract.

Before any mutation, apply must re-run preflight checks and stop loudly on:

- host compatibility-key mismatch or incomplete host facts;
- disk free below `disk_min_free_ratio` after projected install size;
- missing or incompatible package manager, Python ABI, container runtime, GPU driver/runtime, CUDA/ROCm/Level Zero/SYCL/Metal/OpenVINO facts, or container manifest platform;
- unsafe vLLM/SGLang memory caps on catastrophic hosts;
- peer/service ownership ambiguity, running instances that would be disrupted, or config ownership conflicts.

Allowed mutation shapes are limited to the approved plan:

- create managed venvs/directories under `~/.mycelium/engines/` where practical;
- install packages, build/pull platform-checked images, or write wrappers only through explicit plan actions;
- write peer config/profile changes atomically with backups;
- register engine profiles only after verification succeeds.

Post-install verification must be cheap and evidence-bearing:

- binary/version checks for every installed or adopted runtime;
- engine health probe where the backend supports it without loading a large model;
- optional tiny/local model smoke only when a safe local test asset is configured;
- no production model loads as readiness checks.

Every apply run must preserve an evidence ledger:

- commands planned and commands executed;
- package/image/wheel/source versions and digests where available;
- files/directories/configs changed and backups written;
- engine versions detected after install;
- verification and smoke results;
- failure logs and manual recovery notes.

Rollback is a requirement for enabling broad apply, but the full rollback system is not designed in this phase. The apply implementation must at least record whether rollback is known, manual, unsupported, or unknown for each action, and broad automatic apply must remain disabled for paths whose rollback story has not been validated.

Hardware validation must cover at minimum:

- macOS Apple Silicon llama.cpp Metal and MLX;
- Intel macOS classification with CPU fallback opt-in only;
- DGX Spark/Linux NVIDIA arm64 vLLM/SGLang/llama.cpp with platform-safe images/wheels and memory caps;
- Linux NVIDIA x86_64 independently from Spark;
- B70/Linux Intel SYCL llama.cpp and OpenVINO;
- Linux AMD contract paths without claiming live smoke until hardware exists.

Stop rules:

- if an action fails, stop the apply sequence and preserve the evidence ledger;
- if verification fails, do not mark the engine profile ready;
- if config/profile write validation fails, restore the previous config where possible and require manual intervention;
- never hide a failed install behind a fallback engine.

### Future Phase H - runtime updates and rollback-aware upgrades

Runtime update availability checks, automatic runtime upgrades, and rollback orchestration are intentionally deferred until after the install/adoption path has repeated hardware-specific validation.

This future phase must be designed and proven separately from bootstrap install planning. It must cover:

- update metadata sources for each runtime family, including package managers, Python wheels, container images, and user-managed wrappers;
- installed-version detection that does not require launching production backends or model loads;
- platform-specific update safety for DGX Spark, B70, macOS Apple Silicon, Intel macOS, Linux NVIDIA x86_64, and Linux AMD;
- version pinning and compatibility with CUDA, ROCm, Level Zero/SYCL, Metal, OpenVINO, Python ABI, and container manifests;
- rollback strategy per install source, with explicit evidence when rollback is manual, unsupported, or unknown;
- repeated dry-run and smoke validation before any automatic deployment or upgrade behavior exists;
- operator approval boundaries for every network lookup, package/image selection, service restart, config change, and rollback action.

Until that phase is implemented, Mycelium may report that update/rollback support is future work, but it must not query package indexes on a schedule, auto-upgrade engines, restart services, or claim rollback safety.

## 18. Acceptance criteria

Engine bootstrap is complete when:

- A clean Mac can run one command that writes a compute peer config using native llama.cpp.
- A Mac with MLX support can add an MLX profile without Docker.
- Intel macOS is classified separately from Apple Silicon and never receives Apple Silicon-only MLX setup.
- Spark can expose a safe ARM64-compatible vLLM profile with memory caps that prevent the known OOM crash class.
- Linux x86_64 NVIDIA paths are selected independently from Spark's Linux ARM64 path.
- B70 can expose its existing `linux/amd64` SYCL/custom engine when disk headroom is safe and loudly refuse compute readiness when it is not.
- Existing installed engines are adopted without reinstall.
- No model download occurs during engine setup.
- All changes are durable, resumable, and visible through jobs/doctor output.
- The fleet benchmark can consume bootstrap readiness facts but still runs only through normal gateway/scheduler/runtime paths.
