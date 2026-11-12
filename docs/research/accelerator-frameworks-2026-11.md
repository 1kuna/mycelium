# What runs on Intel Arc, Apple Metal, ARM CUDA and CPU (2026-11)

Research date: 2026-11-12. Answers [#1488](https://github.com/1kuna/mycelium/issues/1488)
for the map [#1485](https://github.com/1kuna/mycelium/issues/1485); it unblocks
[#1491](https://github.com/1kuna/mycelium/issues/1491). Builds on
[the dual-GPU and XPU variants research](dual-b70-and-xpu-variants.md) and the
native vLLM work in [#1456](https://github.com/1kuna/mycelium/issues/1456).

The fleet families covered here:

- **Arc:** Intel Arc Pro B70 (32 GiB each, two cards in one host), Linux amd64.
- **Metal:** Apple M4 Macs, macOS arm64, unified memory.
- **CUDA arm64:** NVIDIA GB10 (compute capability 12.1), Linux arm64, unified memory.
- **CPU:** the CPU of each of those three OS/architecture pairs.

Labels: **[doc]** a vendor or project document or wheel index says so;
**[live]** a Mycelium experiment job on fleet hardware showed it; **[inference]**
my reading of the two. Versions are what each source showed on the research date.

## What was not verified

- Only PyTorch, Transformers, PEFT and MLX ran live. vLLM on CUDA arm64 is live
  from #1456. Every other framework row is **[doc]** only: llama.cpp, Ollama,
  vLLM-XPU, vllm-metal, SGLang, OpenVINO, bitsandbytes, Unsloth, JAX, Isaac,
  MuJoCo Warp and ONNX Runtime were not installed or run here.
- The CUDA arm64 probe did not run: the GB10 node was disabled for operator
  maintenance while it waited. CUDA arm64 evidence is #1456 plus documentation;
  float64, collectives, the operator list, the tiny-LLM fine-tune and CPU-only
  PyTorch on Linux arm64 were not observed here.
- No real model or dataset was used. The probes use random weights and tiny
  synthetic batches; they show that operators and code paths work, not speed,
  memory fit for a real model, or MY-19 output parity.
- No multi-device or multi-process run. A single-rank process group passing is
  not evidence that two-card or multi-process training works.
- The Arc probe used torch 2.10+xpu (an existing project lock reused to avoid a
  fresh multi-GB install); Metal used torch 2.13, as did #1456 on GB10. Current PyTorch is
  2.15 ([release blog](https://pytorch.org/blog/pytorch-2-15-release-blog/)).
- `torch.compile` on the Arc host failed for a host library reason; see
  [Live probes](#live-probes) for the evidence and what is still unknown.

## Answer: one line per kind of work

| Kind of work | Verdict |
| --- | --- |
| **Training, including robot learning** (ordinary PyTorch: CNN/LSTM/Transformer, AdamW, bf16 autocast, LeRobot policies) | **Runs everywhere** (CUDA arm64, Arc, Metal, CPU): [live] on Arc, Metal and their CPUs; [doc] plus #1456 on CUDA arm64. Limits: Metal has no float64 (hard limit of the Metal Shading Language), a few missing operators, and no collectives, so one process per Mac. Custom CUDA kernels (FlashAttention, xformers, fused CUDA extensions) and NVIDIA simulators (Isaac Sim/Lab, MuJoCo Warp, Warp GPU) are **CUDA only**: hard limit, they are CUDA programs. MuJoCo itself runs on CPU everywhere. |
| **LLM fine-tuning** (Transformers + PEFT LoRA, bf16) | **Runs everywhere** PyTorch does: [live] on Arc, Metal and their CPUs, [inference] on CUDA arm64. 4-bit QLoRA via bitsandbytes: CUDA arm64 and Arc yes, Metal 4-bit yes but no 8-bit optimizers yet [doc]. Unsloth: CUDA (DGX Spark listed); Arc through a source install; not Metal in its core library [doc]. MLX-LM LoRA is the native Metal path [doc]. Model size is bounded by memory: 32 GiB per Arc card, the Mac's unified memory, the GB10's unified memory. |
| **Running and testing a local LLM** | **Runs everywhere, with a different engine per family.** llama.cpp/GGUF covers all four (Metal, CUDA, SYCL or Vulkan for Arc, CPU) but needs a build per platform [doc]. vLLM: CUDA arm64 from PyPI wheels [live, #1456]; Arc from nightly XPU wheels or Intel's images [doc]; Metal only through the community `vllm-metal` plugin on MLX [doc]; CPU from wheels on Linux amd64 and arm64, source build on macOS [doc]. Ollama: Metal, CUDA (GB10 listed), Arc via Vulkan, CPU [doc]. **Hard limit: weight formats.** NVFP4 runs only on NVIDIA Blackwell kernels; MLX weights only on MLX; OpenVINO IR only on OpenVINO. A portable job names a portable format (GGUF or BF16 safetensors) or one artifact per family. |
| **Batch inference** (offline, many inputs) | **Runs everywhere** with PyTorch/Transformers eager ([live] on Arc and Metal, [live] vLLM on CUDA arm64); vLLM offline `LLM` where vLLM runs (above). ONNX Runtime: CPU on all three, CUDA on Linux arm64, OpenVINO execution provider on Linux amd64 only [doc]. OpenVINO runtime: Arc GPU plus CPU on all three [doc]. |

## The smallest change for run-time device choice

A few lines in the job, plus a locked environment for each platform (last bullet).
Nothing else names a backend.

```python
import os, torch

acc = torch.accelerator.current_accelerator() if torch.accelerator.is_available() else None
device = torch.device(os.environ.get("MYCE_DEVICE") or acc or "cpu")
```

- `torch.accelerator` covers CUDA, XPU and MPS with one API: device count,
  synchronize, streams, RNG and memory statistics
  ([docs](https://docs.pytorch.org/docs/2.15/accelerator.html)) [doc]. The probes
  ran this selection unchanged on Arc and Metal [live].
- Mycelium already sets `MYCE_DEVICE` to the assigned device: the probes saw
  `xpu:0` on the Arc host and `mps` on a Mac, with the other vendors' visibility
  masks closed (`CUDA_VISIBLE_DEVICES=` empty, `ONEAPI_DEVICE_SELECTOR=!*:*` on the
  Mac) [live]. Honouring it keeps the job on the card Mycelium reserved.
- Then replace CUDA-named calls with device-neutral ones: `.cuda()` becomes
  `.to(device)`; `torch.cuda.amp.autocast()` becomes
  `torch.autocast(device_type=device.type, dtype=torch.bfloat16)`;
  `torch.cuda.synchronize()` becomes `torch.accelerator.synchronize()`; memory
  stats come from `torch.accelerator.max_memory_allocated()` [live on Arc and Metal].
- Distributed: choose the backend from the device (`nccl` for CUDA, `xccl` for
  XPU, `gloo` for CPU); `xccl` and `gloo` passed a single-rank all-reduce [live].
  Metal has none [live]; a Mac runs one process.
- Keep float64 work on CPU, or cast to float32, so the same code runs on Metal.
  `PYTORCH_ENABLE_MPS_FALLBACK=1` sends missing Metal operators to the CPU; it
  cannot help float64 ([MPS notes](https://docs.pytorch.org/docs/2.15/notes/mps.html)) [doc].
- **The environment is the part that cannot be one file today.** PyTorch's
  accelerator is chosen when the wheel is built: Linux PyPI wheels are CUDA 13
  builds on both amd64 and arm64; XPU wheels come only from
  `download.pytorch.org/whl/xpu`; the macOS wheel carries MPS
  ([2.15 release](https://pytorch.org/blog/pytorch-2-15-release-blog/),
  [XPU guide](https://docs.pytorch.org/docs/2.15/notes/get_start_xpu.html)) [doc].
  An accelerator wheel also runs on its host's CPU: the probes ran the CPU
  checks from the XPU wheel on Linux amd64 and from the macOS wheel on a Mac
  [live]; the CUDA wheel's CPU path on Linux arm64 was not observed. On this fleet each OS/architecture
  pair has exactly one accelerator family, so one `uv.lock` that takes torch from
  the XPU index on Linux amd64 and from PyPI elsewhere (split by
  `sys_platform`/`platform_machine` markers) could cover all four targets
  [inference]. It would not cover a future Linux amd64 CUDA host, since markers
  cannot tell CUDA from XPU on one OS/architecture, and CPU-only Linux amd64
  hosts would carry the Intel runtime wheels. Whether one such lock is enough, or
  the job needs a lock per platform, is what #1491 prototypes.

Libraries that already pick the device this way: LeRobot's
`auto_select_torch_device()` tries CUDA, then MPS, then XPU, then CPU, and refuses
float64 on MPS and on XPU devices without FP64
([source](https://github.com/huggingface/lerobot/blob/main/src/lerobot/utils/device_utils.py)) [doc].

## Why something does not run

| Reason | Cases | Fixable by Mycelium or the job? |
| --- | --- | --- |
| **Hard limit: the program is built for one vendor** | CUDA kernels (FlashAttention, xformers, TensorRT-LLM, NVFP4 kernels, cuRobo); Isaac Sim/Lab and MuJoCo Warp (NVIDIA GPU); MLX's Metal backend and vllm-metal (Apple); OpenVINO GPU and SYCL builds (Intel) | No. State it; keep the job on its family. |
| **Hard limit: hardware or shading language** | float64/complex128 on Metal | Move that computation to CPU or float32. |
| **Missing operators** | Metal: `linalg_eigh`, `ctc_loss` in the probe | CPU fallback env var, or a different op. Upstream adds ops each release (2.15 added several MPS and XPU ops). |
| **Missing packages for the platform** | vLLM XPU: nightly wheels only; vLLM macOS: source build only; flash-attn, DeepSpeed and llama-cpp-python: source distributions only on PyPI; onnxruntime-openvino: Linux amd64 only; JAX on Metal: not supported (`jax-metal` last released 2024) | Yes, with a build recipe or a different engine, at the job owner's cost. |
| **Memory** | A model larger than 32 GiB per Arc card needs two-card sharding (#105); Macs hold what their unified memory holds | Mycelium's placement, not the code. |
| **Host setup** | `torch.compile` on the Arc host (below) | Likely, but the cause is unknown ([#1518](https://github.com/1kuna/mycelium/issues/1518)). |

## Live probes

All jobs were Mycelium experiment-tier jobs with declared needs and no host
pins (`needs.platform` only). Source, scripts and results were kept in a scratch
project and removed afterwards; this table is the record.

| Probe | Arc (Linux amd64) | Metal (macOS arm64) | CUDA arm64 (GB10) |
| --- | --- | --- | --- |
| Job | `cbe5565f` | `689f8969` | #1456 jobs `4b267051`, `7cee22eb`, `389f0852` |
| Stack | Python 3.12.3, torch 2.10.0+xpu, triton-xpu 3.6.0, LeRobot 0.6.1, Transformers 5.4.0, PEFT 0.18.0 | Python 3.12.11, torch 2.13.0, MLX 0.32.4, MLX-LM 0.32.2, Transformers 5.22.2 | Python 3.12.11, torch 2.13.0+cu130, vLLM 0.31.0, FlashInfer 0.7.0.post1, Triton 3.7.1 |
| Device | Arc Pro B70, 31.9 GiB, `has_fp64` true, one visible card | `mps`, one device | GB10, capability (12, 1); torch arch list includes `sm_120` |
| Accelerator checks passed | 30 of 31 | 27 of 31 | not run (see below) |
| fp32/bf16/fp16 matmul, AdamW training, bf16 autocast, SDPA backward | pass | pass | not run; vLLM inference ran |
| float64 | pass | **fail**: "MPS framework doesn't support float64" | not run |
| 20 operators often missing on new backends | all pass | 18 pass; `linalg_eigh`, `ctc_loss` **not implemented** | not run |
| fp32 forward vs CPU, max abs diff | 6.0e-8 | 1.2e-7 | not run |
| `torch.compile` (Inductor) | **fail** (host library clash, below) | pass | pass inside vLLM (torch.compile with CUDA graphs) |
| Single-rank collective | `xccl` pass | **fail**: `allreduce` not implemented for MPS | not run |
| MLX training on GPU | n/a | pass (`Device(gpu, 0)`, loss 1.95 to 0.66) | n/a |
| Tiny LLM: full fine-tune, PEFT LoRA, generate (bf16) | pass (`78bbbd1d`): full fine-tune loss 5.55 to 3.90, LoRA 7,168 trainable parameters, 24-token generate | pass (`2e9afbe2`): same losses to two decimals, 24-token generate | vLLM NVFP4 generation [#1456]; tiny-LLM probe not run |
| CPU (same job, `device=cpu`) | all pass except compile | all pass (distributed check failed only because the failed MPS group was not torn down: a probe defect) | not run |

**`torch.compile` on the Arc host.** Inductor failed on both the XPU and the CPU
device with `OSError: <host oneAPI 2026.0>/lib/libsycl.so.9: undefined symbol:
urDeviceWaitExp`. The environment ships oneAPI 2025.3.1 runtime
wheels (`intel-sycl-rt`), and the torch process loads the environment's own
`libsycl.so.8`, but Inductor's compile step reaches a host-wide oneAPI 2026.0
`libsycl.so.9`. The job inherits that host install through `LD_LIBRARY_PATH`,
`ONEAPI_ROOT` and `PATH`. Removing each of those, and using fresh Inductor and
Triton caches, did not change the error; removing all of them together made the
process abort. **Cause: unknown.** What opens the host library is the next
evidence to collect; it is tracked in [#1518](https://github.com/1kuna/mycelium/issues/1518)
(jobs `25e106d2`, `48a0ed2d`, `dd23b527`, `24e7159a`, `da338690`). Eager mode is
unaffected, so this does not change any verdict above; it does mean a job that
calls `torch.compile` should fall back to eager on the Arc host until #1518 lands.

**The CUDA arm64 operator probe did not run.** Its jobs waited on a free
build core, and the GB10 node was then disabled for operator maintenance; they
were cancelled before starting. CUDA arm64 rows rest on #1456's live runs and
on vendor documentation. CUDA is PyTorch's reference backend, so the operator
gaps tested here are not expected there [inference], but float64, collectives
and the tiny-LLM fine-tune were not observed on GB10.

**Earlier live evidence.** #1456 ran vLLM 0.31.0 from upstream PyPI wheels (torch
2.13.0+cu130, FlashInfer 0.7.0.post1) on the GB10 node and generated from NVFP4
checkpoints in both compressed-tensors and ModelOpt formats; the ModelOpt FP8 KV
cache needed the host's CUDA toolkit for FlashInfer's JIT, or a bf16 KV cache.

## Framework matrix

✅ runs; 🟡 runs with a caveat in the note; ❌ does not run (reason given). All
[doc] unless marked.

### Training and fine-tuning

| Framework | Arc (Linux amd64) | Metal (macOS arm64) | CUDA arm64 | CPU (all three) |
| --- | --- | --- | --- | --- |
| PyTorch eager | ✅ [live] Arc B-series listed ([XPU guide](https://docs.pytorch.org/docs/2.15/notes/get_start_xpu.html), "Prototype") | ✅ [live] macOS 15+; no float64 ([MPS notes](https://docs.pytorch.org/docs/2.15/notes/mps.html)) | ✅ [live, #1456] PyPI cu132/cu134 aarch64 wheels ([2.15](https://pytorch.org/blog/pytorch-2-15-release-blog/)) | ✅ [live] Linux amd64 and macOS; Linux arm64 [doc] |
| `torch.compile` | 🟡 supported upstream; failed on this host ([#1518](https://github.com/1kuna/mycelium/issues/1518)) | ✅ [live] | ✅ [live, #1456, inside vLLM] | 🟡 [live] pass on Mac; host clash on the Arc host |
| Multi-process (DDP/FSDP) | 🟡 `xccl` single rank [live]; two-card unproven | ❌ no MPS collectives [live] | ✅ `nccl` | ✅ `gloo` |
| Transformers + PEFT LoRA | ✅ [live] | ✅ [live] | ✅ pure PyTorch modules on a supported torch [inference] | ✅ [live] |
| bitsandbytes (int8, 4-bit QLoRA, 8-bit optim.) | ✅ ✅ ✅ Arc B-series listed ([README](https://github.com/bitsandbytes-foundation/bitsandbytes)) | 🟡 int8 unoptimised, 4-bit ✅, 8-bit optim. planned | ✅ ✅ ✅ (SM75+) | — |
| Unsloth | 🟡 source install with an Intel extra ([guide](https://docs.unsloth.ai/get-started/install-and-update/pip-install)) | ❌ core: "Apple/Silicon/MLX is in the works"; Studio trains through MLX ([requirements](https://unsloth.ai/docs/get-started/beginner-start-here/unsloth-requirements)) | ✅ DGX Spark listed | ❌ training |
| MLX / MLX-LM (LoRA) | ❌ Apple and CUDA backends only | ✅ [live] native | 🟡 `mlx-cuda-13` publishes a Linux aarch64 wheel ([PyPI](https://pypi.org/project/mlx-cuda-13/)); untested | ✅ `mlx[cpu]` on Linux; CPU device on Mac ([install](https://ml-explore.github.io/mlx/build/html/install.html)) |
| JAX | 🟡 experimental Intel plugin `jax[oneapi]` | ❌ "not supported on Mac GPU" | ✅ aarch64 CUDA wheels | ✅ ([install](https://docs.jax.dev/en/latest/installation.html)) |
| LeRobot | ✅ [live import of 0.6.1]; main's device helper selects XPU | ✅ MPS needs macOS 15 | ✅ default Linux wheels are CUDA 13 | ✅ ([install](https://huggingface.co/docs/lerobot/installation)) |
| Isaac Sim / Isaac Lab | ❌ NVIDIA GPU required | ❌ not offered | ✅ Linux aarch64 incl. DGX Spark, with listed gaps (SkillGen, XR, livestream) ([Isaac Lab](https://isaac-sim.github.io/IsaacLab/main/source/isaaclab/installation.html)) | ❌ needs a CUDA-capable NVIDIA GPU |
| MuJoCo Warp / Warp | ❌ NVIDIA hardware | 🟡 Warp CPU only, no Metal ([Warp](https://github.com/NVIDIA/warp)) | ✅ (DGX Spark named) | 🟡 CPU for development ([MJWarp](https://github.com/google-deepmind/mujoco_warp)) |
| FlashAttention, xformers, DeepSpeed | ❌ CUDA/ROCm kernels; PyPI has flash-attn and DeepSpeed source only, xformers amd64/Windows wheels | ❌ | 🟡 source builds | ❌ |
| IPEX (Intel Extension for PyTorch) | ❌ archived 2026-03-30; Intel says use PyTorch directly ([repo](https://github.com/intel/intel-extension-for-pytorch)) | — | — | — |

### Serving and batch inference

| Engine | Arc (Linux amd64) | Metal (macOS arm64) | CUDA arm64 | CPU |
| --- | --- | --- | --- | --- |
| vLLM | 🟡 nightly XPU wheels plus `vllm-xpu-kernels` (Python 3.12 only), TP supported; "basic model inference and serving" ([install](https://docs.vllm.ai/en/latest/getting_started/installation/gpu.html)); Intel LLM-Scaler ships Docker images only ([releases](https://github.com/intel/llm-scaler/blob/main/Releases.md)) | 🟡 `vllm-metal` community plugin on MLX, Homebrew or script install ([repo](https://github.com/vllm-project/vllm-metal)) | ✅ [live, #1456] PyPI aarch64 wheels, NVFP4 | ✅ Linux amd64 (AVX512/AVX2) and arm64 wheels; macOS source build only ([CPU](https://docs.vllm.ai/en/latest/getting_started/installation/cpu.html)) |
| SGLang | 🟡 source install only; optimised for Arc B-series ([XPU](https://docs.sglang.io/docs/hardware-platforms/xpu)) | ❌ | ✅ aarch64 wheels; DGX Spark playbook ([playbooks](https://build.nvidia.com/spark)) | — |
| llama.cpp | ✅ SYCL (Arc B580 verified) or Vulkan ([SYCL](https://github.com/ggml-org/llama.cpp/blob/master/docs/backend/SYCL.md)) | ✅ Metal on by default ([build](https://github.com/ggml-org/llama.cpp/blob/master/docs/build.md)) | ✅ CUDA; DGX Spark playbook | ✅ |
| Ollama | 🟡 Intel only through Vulkan; Arc not named ([GPU](https://docs.ollama.com/gpu)) | ✅ | ✅ GB10 listed | ✅ |
| MLX-LM server | ❌ | ✅ | 🟡 untested CUDA backend | 🟡 MLX CPU device [inference] |
| OpenVINO / OpenVINO GenAI | ✅ Arc listed, Ubuntu 22.04–26.04 | 🟡 CPU only | 🟡 CPU only | ✅ x86, ARM64, Apple silicon ([system requirements](https://docs.openvino.ai/2026/about-openvino/release-notes-openvino/system-requirements.html)) |
| TensorRT-LLM | ❌ | ❌ | 🟡 NVIDIA only; PyPI has a source distribution and no DGX Spark playbook is listed; not checked further | ❌ |
| ONNX Runtime | 🟡 OpenVINO EP wheel Linux amd64 | 🟡 CPU (CoreML EP not checked) | ✅ `onnxruntime-gpu` aarch64 wheels | ✅ |

Wheel availability was read from the PyPI JSON API on the research date:
`vllm` 0.33.0 (linux amd64 and aarch64), `vllm-xpu-kernels` 0.1.16.4 (linux
amd64), `mlx` 0.32.4, `mlx-metal` (macOS arm64), `mlx-cuda-13` (linux amd64 and
aarch64), `bitsandbytes` 0.50.2 (all three), `openvino` 2026.4.1 (all three),
`sglang` 0.5.23 (linux amd64 and aarch64), `jaxlib` and `jax-cuda13-plugin`
0.12.0.1 (linux aarch64 included), `jax-metal` 0.1.1 (2024), `onnxruntime-gpu`
1.31.1 (linux amd64 and aarch64), `onnxruntime-openvino` 1.24.1 (linux amd64),
`warp-lang` 1.19.0 (all three), `intel-extension-for-pytorch` 2.8.0 (2025);
`flash-attn`, `deepspeed`, `llama-cpp-python` and `tensorrt-llm` publish source
distributions only.

## Open questions

- Whether vLLM's XPU nightly wheels and `vllm-metal` install from a frozen lock
  inside a Mycelium environment, and serve a small model. Next check: one probe
  each, small model, after #1491 settles the lock layout.
- Two-card XPU collectives and FSDP (#105).
- Whether `mlx-cuda-13` runs on GB10 (sm_121).
- Speed and memory of each path on real workloads; this file says only what runs.
