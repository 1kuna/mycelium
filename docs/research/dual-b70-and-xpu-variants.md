# Two B70 GPUs: model sharding and PyTorch XPU variants

Research date: 2026-11-01. Refs #105. This is a proposal and source review,
not native qualification or a change to the approved product scope.

## Answer and evidence limits

**There is no fundamental prohibition on sharding a model across two Intel Arc
Pro B70s.** vLLM documents XPU tensor parallelism, and Intel's LLM Scaler supplies
multi-Arc serving, including relevant Qwen and Gemma models. The immediate
Mycelium blocker is deliberate: one execution may reserve only one GPU.
NVFP4 artifacts require a different, XPU-compatible implementation/artifact;
changing the device string is insufficient. A robot trainer using ordinary
PyTorch is a better first portability candidate than a custom CUDA VLM.

Unverified here: native two-card collectives, topology/P2P, model loading peaks,
video/audio correctness, training parity, speed, and coexistence with production.
No fleet commands, installations, jobs, service changes or benchmarks were run.
Hardware capacities below are supplied reference facts, not new inventory.
Public upstream documentation and repository sources were fetched; private
consumer code was inspected read-only. Private paths, datasets, operational
timings and credentials are intentionally excluded.

The reference configuration has two discrete 32 GiB VRAM devices and roughly
31 GiB host RAM. **Model capacity comes from VRAM, not host RAM.** The host does
not need a resident CPU copy of all GPU weights. Safetensors mmap, incremental
loading and suitable prequantized artifacts can load a model larger than host
RAM. Host loading peaks must be measured separately. This is different from a
GB10 system, where CPU and GPU share physical memory.
Intel publishes the B70's memory and interface specifications on its
[product page](https://www.intel.com/content/www/us/en/products/sku/245797/intel-arc-pro-b70-graphics/specifications.html);
two cards still require software sharding rather than becoming one flat pool.

The controlling local sources are [MY-26, MY-27 and MY-19](../DECISIONS.md), the
[approved device-selection scope](../design-proposal.md), and
[#105](https://github.com/1kuna/mycelium/issues/105). The latest
[#303 closeout](https://github.com/1kuna/mycelium/issues/303#issuecomment-6543464276)
does **not** certify two-card coexistence; it records incomplete qualification
and memory-learning defects. [#497](https://github.com/1kuna/mycelium/issues/497)
tracks the separately owned follow-up. An earlier successful single attempt is
not a concurrency certificate. Consult current local queue policy/DB before any
future native work; no calendar date or host-specific hold is encoded here.

## 1. Smallest Mycelium design for count=2

### Extend the existing planner and custody

Use the current machine worker, SQLite queue, resource ledger and plain command
runner. Do not add a second scheduler, distributed launcher service, leases or
cross-host execution. The application already has a collective launcher:
`torchrun` for training or vLLM multiprocessing for serving.

Current code confirms multiple enforcement points, not just an admission check:
[worker inventory/attempt binding](../../internal/myce/worker_inventory.go)
rejects several allocated devices; [model binding](../../internal/myce/server.go)
rejects multi-card masks; [assignment](../../internal/myce/queue.go) currently
records one selected component. The existing
[placement planner](../../internal/myce/placement_plan.go) projects releases
before admitting replacements. Lift these restrictions together with #105's
contract, rather than allowing a command to override a one-card reservation.

The smallest first implementation should qualify **homogeneous, discrete GPUs
on one host**. Reject unsupported mixed-backend/unified combinations explicitly;
do not accidentally double-count shared memory when later extending the scope.

1. Resolve `needs.gpu.count: 2` to two compatible, qualified, healthy devices on
   one machine. Keep `devices` an optional candidate set: it is not an allocation
   list. Intersect it with platform, extension and project requirements. Consider
   all eligible pairs within the existing planner and apply MY-18 fit/value
   ranking. Require qualification of the pair's collective path, not merely two
   individually healthy cards. Record device identities and usable transport in
   inventory/qualification data; don't require P2P if a measured host-staged
   collective satisfies the workload.
2. Persist **one attempt and one complete `allocated_devices` vector** with all
   device and host charges in one queue transaction. Reserve both or neither;
   never hold the first while waiting for the second. Retain the vector in every
   attempt copy, preparation/sidecar authorization, release and recovery record.
   Launch only after the worker rechecks both identities and Started authority.
3. Run one parent process/cgroup with all collective children and sidecars in its
   existing custody. A rank failure, device loss, worker loss or cancellation
   stops the entire attempt. A rank cannot succeed independently or leave a
   sibling rank holding a device. Use existing attempt/session fences to reject
   stale success and release reports. Restart recovery reconstructs the complete
   vector before allowing new work on either card.
4. Permit the same count/vector in model-server and extension paths, or fail
   those paths explicitly until implemented. Model pins and warm servers consume
   both devices; unloading must acknowledge the entire process tree. First
   qualifying a plain-command batch avoids building a new serving abstraction.

### Visibility and launcher contract

[Level Zero](https://oneapi-src.github.io/level-zero-spec/level-zero/1.16.24/core/PROG.html)
defines affinity masks and device renumbering;
[oneAPI's selector documentation](https://intel.github.io/llvm/EnvironmentVariables.html)
defines the second filtering layer. A two-card environment is conceptually:

```sh
ZE_ENABLE_PCI_ID_DEVICE_ORDER=1
ZE_AFFINITY_MASK=0,1             # example: reserved physical L0 ordinals
ONEAPI_DEVICE_SELECTOR=level_zero:*
CUDA_VISIBLE_DEVICES=
```

These are worker-generated process variables, not global exports or user
overrides. Translate durable inventory identities to verified current selectors;
do not assume Mycelium device IDs always equal Level Zero ordinals. The current
single-device `ONEAPI_DEVICE_SELECTOR=level_zero:0` must not survive unchanged:
after masking, it would expose only the first visible device. In a larger
machine, the Level Zero mask still excludes all unallocated cards. Verify the
intersection with the actual runtime and prevent environment overrides. A
selector is a visibility mechanism; native isolation/device-node access must
also cover exactly the assigned render devices. Container mapping must be
qualified separately, especially for an OpenVINO OpenCL route that cannot be
assumed to honor Level Zero masks.

All members/sidecars inherit the reserved set. DDP selects visible `xpu:0` or
`xpu:1` using `LOCAL_RANK`, initializes an XPU-capable process group and runs one
rank per GPU. vLLM uses `--tensor-parallel-size 2` and a qualified local
multiprocessing executor; its launcher owns rank binding. Keep rendezvous ports
inside existing worker port reservations and cleanup custody. Intel's
[reference commands](https://github.com/intel/llm-scaler/blob/ef201d20b42087fa05cddfb4f3503675bdb1efd6/vllm/README.md#32-reference-commands-for-running-the-supported-qwen3536-models)
use `VLLM_WORKER_MULTIPROC_METHOD=spawn`. Environment details are part of the
pinned variant, not a universal oneAPI recipe.

### Memory and preemption semantics

For this first homogeneous implementation, define existing `memory_gb` as a
**per-device floor**, unchanged for count=1. Count=2 with `memory_gb: 20` requires
20 GiB on **each** GPU; it does not request a combined 20 GiB pool. The planner
fits a vector, never the sum of advertised capacities. An asymmetric runtime
uses the larger per-rank bound initially; an authored per-rank vector can wait
for a demonstrated need. This conservative simplification can waste VRAM on the
lighter rank, but avoids a new public schema before measurements justify it.

Account host CPU/RAM **once per attempt**, including all ranks, sidecars and
loading. Discrete VRAM reservations are separate: do not add 64 GiB of VRAM to a
host-RAM charge, nor subtract host RAM from VRAM capacity. Record per-card peaks
and aggregate process/cgroup/driver-host observations; qualify load, steady
execution and checkpoint peaks. Learn from the exact runtime, precision, count,
topology and input-size regime. A count=1 observation cannot establish count=2
fit. No hashing, model loading or topology probing occurs under `q.mu`.

Preemption operates on **whole attempts**. If a higher-tier job needs either
card held by a two-card attempt, request its supported checkpoint/stop and
release both only after all ranks, sidecars and device handles are gone. Pending
release is planning evidence, not admission authority. A resumable trainer must
publish one complete optimizer/model/RNG checkpoint; experiment-tier restart is
allowed only under its declared contract. Do not call a process kill migration.
Preserve existing cancellation precedence, maintenance and optional host-wide
production takeover. Which machine hosts production, its reserve, hold times
and operating windows live in **local queue policy and DB**, never source code,
committed fleet configuration or public examples.

### Effort, qualification and production risk

Estimate: **5–8 engineer-days** for the planner/ledger, binding, whole-attempt
release/recovery/preemption and focused laptop regressions; **2–4 further days**
for scheduled native qualification and fixes if the existing driver stack works.
This is approximately 1–2.5 working weeks, not a deployment promise. A driver or
runtime collective defect is additional work, not something a scheduler fixes.

Laptop acceptance uses fake inventory/Clock: atomic reserve/fail, vector
persist/restart, shared masks, no partial release, stale-rank failure, and
higher-tier whole-attempt preemption. Native acceptance must show two distinct
GPU identities active, collectives and actual outputs, then cancellation,
failure/restart cleanup and pressure-free production re-entry. This maps directly
to #105's existing acceptance; it does not unpark #105 by itself.

On a host also running the queue and production speech workloads, count=2
occupies both accelerator components and competes for loading I/O, CPU and
driver resources. Host memory pressure can stall the control plane even though
weights fit in discrete VRAM. A driver-level wedge/reset can affect production
across cards and is not isolated by a Python environment or container. Use an
isolated user-space runtime rather than replacing production dependencies;
driver changes need their own qualification. Respect the existing locally
managed production priority and holds. A free card does not establish safe
coexistence, and a two-card model should not make production wait for its load.

## 2. VLM inference: what runs on XPU today?

### Runtime and quantization support

**vLLM has real XPU tensor parallelism today.** Its
[installation source](https://github.com/vllm-project/vllm/blob/8530a778f78dc7cd9a31e108ebc035c4b27ceb3c/docs/getting_started/installation/gpu.xpu.inc.md)
documents TP and beta pipeline parallelism. Its
[XPU model list](https://github.com/vllm-project/vllm/blob/8530a778f78dc7cd9a31e108ebc035c4b27ceb3c/docs/models/hardware_supported_models/xpu.md)
includes Arc Pro B-series, Qwen3-VL-32B, Qwen3.5-35B-A3B,
Gemma 4 31B/26B-A4B and Muse-Glimmer-30B. That is evidence of backend support,
not qualification of every quantized revision or video-processing path.

The upstream [quantization matrix](https://docs.vllm.ai/en/stable/features/quantization/)
lists Intel AWQ/GPTQ support, while some FP8 rows still say unsupported.
The model list explicitly recommends BF16/online FP8 for several XPU models.
These pages describe different implementations and are not a reliable universal
format promise. Pin the backend and inspect the actual quantizer/kernel path.
Do not infer that an arbitrary CUDA `compressed-tensors`, ModelOpt or Marlin
checkpoint works because its label says FP8 or INT4.

[Intel LLM Scaler's current guide](https://github.com/intel/llm-scaler/blob/ef201d20b42087fa05cddfb4f3503675bdb1efd6/vllm/README.md)
explicitly documents online `sym_int4`, online FP8, offline FP8, AWQ and GPTQ,
and TP=2 Qwen3.6-35B commands. Its Gemma section documents online FP8/INT4.
It supplies oneCCL P2P and host-staged USM modes; their performance and memory
behavior need qualification on the actual pair. Its specialized INT4 shared
library illustrates why a stock CUDA wheel is not interchangeable.
[Release records](https://github.com/intel/llm-scaler/blob/ef201d20b42087fa05cddfb4f3503675bdb1efd6/Releases.md)
name `0.26.0-b2` as latest stable in this snapshot. Pin an immutable image/package
digest for a trial and retain the matching PyTorch/oneAPI stack. Don't copy the
guide's privileged, broad-device container examples into Mycelium isolation.

**NVFP4 has no supported XPU execution route identified here.** NVIDIA's NVFP4
packing/scales and kernels are not Intel's INT4 or MXFP4. Use the original
BF16/FP16 or a separately validated XPU quant, rather than renaming the cached
NVFP4 files. FP8 is not inherently NVIDIA-only, but format, scaling granularity,
unquantized modules and architecture kernels still matter.

**IPEX-LLM** provides low-bit INT4/INT5/INT8 and FP formats through its
[API](https://ipex-llm.readthedocs.io/en/latest/doc/PythonAPI/LLM/optimize.html).
Its [multi-Arc example](https://github.com/intel/ipex-llm/blob/main/python/llm/example/GPU/Pipeline-Parallel-Inference/README.md)
partitions layers in **pipeline parallel**, with older Qwen/Llama/VL examples;
it is not evidence of TP for Qwen3.8, Gemma 4 or Nemotron. The
[repository](https://github.com/intel/ipex-llm) is archived as observed on the
research date. Prefer maintained vLLM/LLM Scaler or OpenVINO over adding a legacy
IPEX-LLM dependency for a new variant.

**OpenVINO** offers INT4-compressed IR models and is a strong single-card
alternative. Its [AUTO cumulative-throughput mode](https://docs.openvino.ai/2026/openvino-workflow/running-inference/inference-devices-and-modes/auto-device-selection.html)
loads replicas to several devices; it does **not** pool VRAM for one model.
There is also a real
[HETERO pipeline-parallel preview](https://docs.openvino.ai/2026/openvino-workflow/running-inference/inference-devices-and-modes/hetero-execution.html)
that partitions a model across devices using memory estimates. This is pipeline
parallelism, not TP. The general runtime API does not prove a working stateful
VLM/GenAI export on two B70s; qualify that path before recommending it over vLLM.

### Concrete model/artifact candidates

| Family | XPU-compatible evidence and candidate | Verdict for two B70s |
| --- | --- | --- |
| Qwen3.x VLMs, 27–35B | Intel documents Qwen3.5/3.6 online FP8/INT4 and Qwen3.8-27B/offline FP8. Published [Qwen3.8-27B INT4 OpenVINO](https://huggingface.co/OpenVINO/Qwen3.8-27B-int4-ov) and [Qwen3.6-35B-A3B INT4 OpenVINO](https://huggingface.co/OpenVINO/Qwen3.6-35B-A3B-int4-ov) are image-text models. | Credible TP=2 variant via Intel's vLLM stack; exact multimodal artifact/input path remains unqualified. Try a prequantized single-card INT4 variant first. |
| Gemma 4 31B / 26B-A4B | Intel vLLM guide supports online FP8/INT4. Published [Gemma 4 31B INT4 OpenVINO](https://huggingface.co/OpenVINO/gemma-4-31B-it-int4-ov) exists, but its card explicitly labels it experimental and not fully validated. | Credible variant, not automatic support for a particular FP8/QAT export. Measure vision and cache overhead. |
| Muse-Glimmer-30B and other listed VLMs | Upstream XPU model list and Intel FP8/INT4 support exist. Intel's reference Muse command explicitly disables image/video inputs. | A text-serving recipe is insufficient evidence for video-editor use; qualify visual input separately. |
| Nemotron | [Nano Omni 30B NVFP4 card](https://huggingface.co/nvidia/Nemotron-3-Nano-Omni-30B-A3B-Reasoning-NVFP4) documents a custom multimodal model and NVIDIA deployment. BF16/FP8 alternatives are a starting point, not XPU proof. No exact qualified XPU replacement for the consumer's Nemotron candidates was established. | Treat as a port/kernel investigation, particularly Mamba/selective-scan, causal convolution, MoE and audio/vision components. Keep the working CUDA variant eligible. |
| INT4 AWQ/GPTQ generally | Backend support exists. [Qwen3-30B-A3B-GPTQ-Int4](https://huggingface.co/Qwen/Qwen3-30B-A3B-GPTQ-Int4) is a concrete official export listed by vLLM XPU, **but text-only**. | A useful loader/collective canary, not a replacement for a VLM. Require the exact VLM architecture plus compatible packing/scales before claiming AWQ/GPTQ readiness. |

There is a concrete Nemotron portability concern beyond quantization: its
[published backbone source](https://huggingface.co/nvidia/Nemotron-3-Nano-Omni-30B-A3B-Reasoning-NVFP4/blob/main/modeling_nemotron_h.py)
imports Mamba Triton/selective-state and causal-convolution kernels and contains
an explicit `torch.cuda.stream` section. Some paths have fallbacks; their
existence does not prove every path is XPU-safe. Audit the chosen serving
implementation too, since vLLM need not execute this Transformers backbone.

For reproducibility, the three OpenVINO model metadata revisions read here were
`d15f4d91eccd11dd44f615ffdb539c61bf1f1e6e` (Qwen3.8),
`0403b0e96d49e6a32494dd766c601493e9e0ef87` (Qwen3.6), and
`e7b82c4f581d68ee3990eed530f2826c12840ceb` (Gemma). Existence/card inspection
does not imply weights were downloaded or executed. The Qwen cards specify
OpenVINO/GenAI 2026.4 or later. Pin the entire processor/tokenizer/runtime chain.

### VRAM fit and loading-time host memory

The following are **calculated packed-weight lower bounds**, using decimal
parameter counts divided by 2^30, not measured complete model footprints.

| Total parameters | BF16/FP16, GiB | 8-bit, GiB | 4-bit, GiB |
| --- | ---: | ---: | ---: |
| 27B | 50.3 | 25.1 | 12.6 |
| 31B | 57.7 | 28.9 | 14.4 |
| 35B | 65.2 | 32.6 | 16.3 |

Two 32 GiB cards can plausibly hold these 27–35B models in 8-bit or 4-bit
representations, with substantially more runtime headroom at 4-bit. Some may
fit on one card at 4-bit, removing the need for #105. A 27B BF16 model is
plausible at TP=2; 31B BF16 is tight; 35B BF16 exceeds 64 GiB before overhead.
Mixed-precision vision towers, embeddings, scales/zeros, replicated layers,
activations, workspaces, KV/recurrent caches and uneven sharding all consume
additional per-card space. An A3B MoE still stores all resident experts, not only
the active 3B. Long video/context may dominate even when packed weights fit.

**The 31 GiB host RAM is not a model-size cap.**
[vLLM's loader documentation](https://docs.vllm.ai/en/v0.31.0/api/vllm/config/load/)
distinguishes lazy safetensors mmap from eager whole-file CPU reads and supports
pre-sharded states. [Transformers big-model loading](https://huggingface.co/docs/transformers/main/big_models)
uses meta initialization and incremental loading to avoid duplicate full CPU
models. Intel's
[release history](https://github.com/intel/llm-scaler/blob/ef201d20b42087fa05cddfb4f3503675bdb1efd6/README.md)
records streaming model loading to reduce peaks. These are evidence that a
GPU-resident model larger than host RAM can load; they are not a measured peak
for a chosen VLM.

Prefer prequantized, pinned local safetensors/IR and lazy or supported streaming
loading. Inspect online-quantization intermediates, dtype conversion, shard size
and simultaneous rank loading; serialize/preshard loading where the pinned
runtime supports it. Mmap virtual size and disk checkpoint size are not resident
host RAM. Conversely, mmap does not prevent a loader from materializing a large
CPU tensor or touching every page. Don't assume a CUDA-only direct-I/O streamer
works on XPU, and do not enable CPU offload as an unnoticed fallback. Measure
transient aggregate cgroup/driver-host peaks through load, then separately size
ordinary host runtime use. Do conversion on another suitable machine if its
transient working set is larger. No RAM purchase or model rejection follows
from parameter count alone.

## 3. Robot practice training on PyTorch XPU

### Local source findings

The inspected practice code is ordinary LeRobot/PyTorch training, not an
Isaac Sim dependency. The existing device adapter already implements XPU
availability/device selection, RNG save/restore, synchronization and allocator
telemetry, and prefers native PyTorch XPU over legacy IPEX. Training uses
`torch.optim.AdamW`, gradient accumulation and checkpointed learned tensors.
The pi0.5 loader uses PEFT/LoRA with a frozen base and gradient checkpointing.
This is **not** full Adam training of every base-model parameter.

There are nevertheless concrete integration gaps: the active practice entry
selects CUDA; the frozen-checkpoint evaluator accepts CPU/MPS/CUDA only; native
qualification identities are backend-specific; and resume rejects a different
backend/device and restores the corresponding accelerator RNG. Existing XPU
helper code or a small unrelated smoke fit does not establish parity of this
full image-conditioned practice workload.

The pi0.5 loader constructs a CPU policy, strictly loads safetensors, then moves
it to the selected accelerator. It is a place to check and, if necessary,
replace transient CPU materialization with meta/streamed loading; host RAM does
not intrinsically limit the resulting GPU model. Saving/reloading a second
policy for numerical checkpoint proof also needs peak measurement. The CUDA
recipe casts to FP32 and disables TF32, while existing XPU/MPS branches retain
their original dtypes. Merely selecting XPU would therefore not preserve the
reference precision contract.

Public corroboration: LeRobot
[pi0.5 configuration](https://github.com/huggingface/lerobot/blob/v0.6.1/src/lerobot/policies/pi05/configuration_pi05.py)
offers FP32/BF16; its
[pi0.5 implementation](https://github.com/huggingface/lerobot/blob/v0.6.1/src/lerobot/policies/pi05/modeling_pi05.py)
and [SmolVLA attention](https://github.com/huggingface/lerobot/blob/v0.6.1/src/lerobot/policies/smolvla/smolvlm_with_expert.py)
contain eager attention paths. A mandatory external FlashAttention dependency
was not found in the inspected practice path. Preserve the frozen upstream
revision and any consumer patches when qualifying, rather than assuming a newer
upstream file is identical.

### One card first; DDP and FSDP are different projects

Native XPU is upstream PyTorch functionality. The
[XPU guide](https://docs.pytorch.org/docs/2.9/notes/get_start_xpu.html) explains
device execution, training and AMP. The
[PyTorch 2.8 release](https://pytorch.org/blog/pytorch-2-8/)
introduced XCCL for DDP/FSDP/TP and SYCL custom-extension support;
[2.9](https://pytorch.org/blog/pytorch-2-9/) expanded XPU packaging and
FlexAttention support. These establish capability, not pi0.5 qualification on
this exact card/driver. Use a compatible, isolated XPU wheel set and check
`torch.distributed.is_xccl_available()` before promising distributed execution.

| Variant | Required work | Expected value and limits |
| --- | --- | --- |
| One B70, SmolVLA | Parameterize train/eval backend, pin XPU runtime, preserve FP32 reference math, add XPU evaluation telemetry/qualification, measure load/train/save and prove parity. | Smallest useful offload; no multi-GPU scheduler change. One 32 GiB device is a credible fit candidate, subject to actual image/activation peaks. |
| One B70, pi0.5 PEFT | Same, plus base-loading/reload peaks, checkpointed backward coverage and attention/dtype validation. | Plausible one-card adapter fit; cannot claim measured fit or equivalent training quality from frozen-weight size alone. |
| Two B70s, DDP | #105 reservation first; `torchrun --standalone --nproc-per-node=2`, XCCL, one device/rank, deterministic nonduplicated sample split, gradient synchronization and rank-aware checkpoint/RNG custody. | Throughput if compute dominates communication. Each GPU holds a full model replica: DDP does **not** make an oversized replica fit. Two independent fits may use the cards better. |
| Two B70s, FSDP | #105 plus FSDP wrapping/initialization, PEFT frozen/trainable handling, shard-aware optimizer/save/load and per-rank RNG; avoid full CPU state reconstruction during initialization or save. | Shards model/grad/optimizer state and can enlarge fit. All-gather peaks and communication remain; frozen-base PEFT saves less optimizer memory than full fine-tuning, so measure whether this complexity pays. |

Preserve the present effective batch/schedule when testing DDP. Splitting the
existing global batch across ranks is different from doubling it. Accumulate
locally with correctly scoped `no_sync`, reduce/clamp gradients consistently,
save only complete collective checkpoints, and prove resume at the same global
update. FSDP may alter parameter names/state-dict layout and invalidate the
consumer's learned-tensor/frozen-base checks; use an explicit format change and
conversion/parity proof, not weakened checks. Cross-CUDA/XPU RNG states are not
portable byte sequences. Keep a run on its qualified backend initially; later
cross-variant resume needs a defined portable RNG/sampling contract and proof.

### Blockers to check, without inventing dependencies

* CUDA `.cuda()` calls, CUDA autocast/events/streams, NCCL-only setup and fused
  CUDA extensions require backend-neutral equivalents or a qualified XPU path.
  PyTorch SYCL extension support does not translate CUDA kernels automatically.
* Upstream [FlashAttention](https://github.com/Dao-AILab/flash-attention)
  advertises CUDA/ROCm paths, not a drop-in B70 package. Use the already present
  eager path first, or qualify PyTorch XPU SDPA separately. Matching an attention
  function's name is insufficient for mask, dtype and backward equivalence.
* Do **not** say bitsandbytes is categorically CUDA-only: its
  [0.50.2 support table](https://github.com/bitsandbytes-foundation/bitsandbytes/blob/0.50.2/README.md)
  lists Arc B-series XPU for INT8, 4-bit QLoRA and 8-bit optimizers. The inspected
  recipe uses ordinary AdamW and does not need it. Adding quantized training
  changes the recipe and requires separate parity; old CUDA wheels still cannot
  be reused.
* Audit pinned Transformers/PEFT/LeRobot imports and torchvision/video decoding,
  any remote code and custom Triton/CUDA kernels. Begin eager with compilation
  off; qualifying XPU compilation is optional performance work. Do not move
  CUDA-only simulation/rendering merely because policy fitting uses PyTorch.

### MY-19 parity required before production placement

Freeze identical weights, preprocessing/tokenizer, camera order, train/held-out
split, normalization, effective batch/schedule, optimizer settings and precision.
For a short deterministic test, supply identical CPU-created noise/timestep
tensors to both devices; equal seeds alone do not give equal CUDA/XPU random
streams. Compare preprocessing tensors, forward losses/action chunks, gradients,
clipping and learned-parameter updates against the CUDA FP32 reference. Record
max/relative error and finite-value checks using the consumer owner's accuracy
criteria, not a newly invented universal epsilon.

Then verify uninterrupted versus checkpoint/resume on XPU, unchanged frozen base,
complete optimizer/RNG restoration and the existing numerical reload proof.
Finally run matched practice fits and the frozen held-out task/closed-loop
evaluation: action error, success/failure outcomes and existing task constraints,
with seed/repeat coverage appropriate to training variability. A finite loss or
successful backward alone is not parity. Keep practice models labelled
development-only; platform qualification does not confer hardware-deployment
approval. MY-26 movement of an active training run additionally requires the
cross-backend checkpoint contract above, not just a valid fresh XPU start.

For VLM variants, apply the same MY-19 principle to fixed representative
clips/prompts and modality preprocessing: check actual image/video/audio
ingestion, structured output, task accuracy/ranking and truncation behavior
against the accepted reference. Quantization changes need their own accuracy
comparison; exact generated text need not be byte-identical if the owner's
criteria allow semantic equivalence.

## 4. Recommended order and next owner

Estimates below are engineering judgment for one engineer, excluding waits for
an authorized native window. They are not vendor throughput predictions.

| Priority | Work packet | Effort | Payoff / stopping condition |
| --- | --- | --- | --- |
| 1 | Qualify the existing trainer as a **single-XPU SmolVLA variant**, then pi0.5 PEFT. Consumer owner owns parity; Mycelium consumes qualification/resource evidence. | 1–3 days SmolVLA; another 2–5 for pi0.5/loading/parity if kernels work. | Frees scarce CUDA capacity without #105. Continue only if task parity and measured end-to-end throughput/fit hold. |
| 2 | Qualify one **single-XPU prequantized INT4 VLM**, starting with a documented Qwen/OpenVINO artifact and the actual multimodal contract. | 2–4 days; video/processor mismatches may extend it. | May move a 27–35B inference workload onto one 32 GiB card. Prefer a proven maintained artifact over converting NVFP4 or porting Nemotron first. |
| 3 | Implement **#105's generic same-host vector reservation**, then qualify vLLM TP=2 for a model/context that demonstrably needs it. | 5–8 days code/laptop proof + 2–4 native days; workload qualification additional. | Unlocks larger contexts/models and a reusable count=2 capability. Benchmark against one-card INT4 and two independent jobs; no assumed 2× speedup. |
| 4 | Add DDP only if one-card training is compute-bound; FSDP only for a measured memory problem. Investigate custom Nemotron kernels separately. | DDP 2–4 days after #105/parity; FSDP 5–10+; custom VLM port 1–3+ weeks with feasibility unresolved. | Expands throughput/capacity when measurements justify it. Retain CUDA where essential kernels remain unsupported. |

Express this as **MY-27 variants of one logical workload**: shared inputs,
outputs, checkpoints and accuracy contract, with implementation-specific runtime,
artifact digest, precision, device count, resource measurements and qualification.
Agents describe software once. Mycelium selects any qualified variant that fits
using MY-18 opportunity cost and MY-26 safe yield. Host names and production
windows are deployment policy in the local queue/DB. Variant declaration,
automatic portability advice and multi-card execution are separate implementation
work; this report does not claim all MY-27 behavior already exists.

**Current state:** research complete; #105 remains deferred and native
qualification is unproved. **Next step:** consumer owners can prepare one-card
XPU runtime/parity packets off-hardware; the planner owner schedules #105 when a
qualified workload needs two cards. Native operators consult current queue/DB
policy and #497 before any launch. Revisit the multi-card priority after the
single-card measurements show a capacity or throughput need.
