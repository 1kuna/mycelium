# The least an agent writes to run one entry on every accelerator (2026-11)

Prototype date: 2026-11-12. Answers [#1491](https://github.com/1kuna/mycelium/issues/1491)
for the map [#1485](https://github.com/1kuna/mycelium/issues/1485). Builds on
[the accelerator survey](accelerator-frameworks-2026-11.md) (#1488). The files are in
[`docs/examples/run-anywhere/`](../examples/run-anywhere/).

## Answer

**Four short files and four changed lines in the job.** The agent writes a
seven-line entry with no targets, one `pyproject.toml` with one extra index for
Intel's torch build, and the `uv.lock` that `uv lock` produces from it. In the job,
the device line is replaced by `MYCE_DEVICE` plus `torch.accelerator`, and the
CUDA-named calls are replaced by device-neutral ones. One lock covered all three
OS/architecture pairs on the fleet; no per-backend entry, lock or check was needed.

The same entry and lock trained the same model to the same result on Arc, Metal
and CPU (test accuracy 0.9822 on each, final loss 0.000647 to 0.000648). The
CUDA run did not run: the CUDA node was disabled for maintenance the whole time
(job `e5405dd0` waited an hour with that reason and was cancelled unrun).

The friction was not in the files. Without a memory figure, the first run on a
busy accelerator host waited for room it did not need: Arc held the job about
17 minutes for 24 GiB of free host RAM, for a job that peaked at 24 MiB on the card.
That is the first-run charge [#1484](https://github.com/1kuna/mycelium/issues/1484) owns.

## What was not verified

- **CUDA did not run** (see [the runs](#the-runs)). The lock resolves the PyPI
  `torch 2.15.0` aarch64 wheel with CUDA 13 libraries for Linux arm64. That this
  wheel sees the GB10 rests on [#1456](https://github.com/1kuna/mycelium/issues/1456)
  (torch 2.13+cu130 on the same node) and the survey, not on this prototype.
- **Linux CPU did not run.** The CPU run landed on a Mac. Whether the XPU and
  CUDA wheels train on their hosts' CPUs was not observed here (the survey ran CPU
  checks from the XPU wheel).
- **Eager mode only.** `torch.compile` fails on the Arc host for a reason still
  unknown ([#1518](https://github.com/1kuna/mycelium/issues/1518)), so the job
  does not call it.
- **One small job.** Equal accuracy on three devices is not MY-19 parity for
  any real workload, and nothing here measures speed. Single process and single
  card only.
- The CPU run's log also holds a Linux amd64 build of the same environment that
  ended "environment build failed: signal: killed" before the job ran on a Mac.
  The job was unaffected. Why that build stopped was not investigated.

## What the agent writes

Four files, all ordinary Python packaging plus one short entry. Nothing names
a backend except the one index line that tells uv where Intel's torch build lives.

**The entry** ([`.mycelium/adapter.yaml`](../examples/run-anywhere/.mycelium/adapter.yaml)):

```yaml
project: run-anywhere-example
entries:
  train:
    run: {command: [python, train.py]}
    environment: {lock_file: uv.lock, python: "3.12"}
    needs: {platform: any}
    outputs: {metrics.json: results}
```

No `targets`, no host, no memory figure, no per-backend entry. `platform: any`
means any accelerator the lock has a recipe for; it never selects CPU
([environments](../environments.md#cpu-tools-with-pixi)).

**The lock** ([`pyproject.toml`](../examples/run-anywhere/pyproject.toml), then
`uv lock`): one `uv.lock` for all three OS/architecture pairs on the fleet.

```toml
[project]
name = "digits-train"
version = "0.1.0"
requires-python = "==3.12.*"
dependencies = [
  "torch==2.15.0",
  "scikit-learn>=1.6",
  # torch+xpu needs Triton for XPU, which only the XPU index carries.
  "triton-xpu; sys_platform == 'linux' and platform_machine == 'x86_64'",
]

[tool.uv]
environments = [
  "sys_platform == 'darwin' and platform_machine == 'arm64'",
  "sys_platform == 'linux' and platform_machine == 'x86_64'",
  "sys_platform == 'linux' and platform_machine == 'aarch64'",
]

[[tool.uv.index]]
name = "pytorch-xpu"
url = "https://download.pytorch.org/whl/xpu"
explicit = true

[tool.uv.sources]
torch = [{ index = "pytorch-xpu", marker = "sys_platform == 'linux' and platform_machine == 'x86_64'" }]
triton-xpu = [{ index = "pytorch-xpu" }]
```

`uv lock` resolved 62 packages in under a second, reading only wheel metadata
(the PyTorch index serves `.metadata` files), so locking needs no multi-GB
download. The result pins `torch 2.15.0+xpu` with Intel's runtime wheels on
Linux amd64, `torch 2.15.0` with CUDA 13 wheels on Linux arm64, and `torch 2.15.0`
(MPS) on macOS arm64. Each worker installs only its own platform's part.

Two lines are there only because uv applies `sources` to direct dependencies:
`triton-xpu` must be listed directly, or uv looks for it on PyPI, which has only
old pre-releases. Without that line the lock fails with "only triton-xpu<=3.3.0b1
is available".

**The job's change** ([`train.py`](../examples/run-anywhere/train.py)), against
the usual CUDA-first script:

```diff
-device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
+acc = torch.accelerator.current_accelerator() if torch.accelerator.is_available() else None
+device = torch.device(os.environ.get("MYCE_DEVICE") or acc or "cpu")
+torch.empty(1, device=device)  # fail now, loudly, if this build cannot use the device

-x = torch.tensor(digits.images).unsqueeze(1) / 16.0          # float64
+x = torch.tensor(digits.images, dtype=torch.float32).unsqueeze(1) / 16.0

-torch.cuda.synchronize()
+torch.accelerator.synchronize()
-torch.cuda.max_memory_allocated()
+torch.accelerator.max_memory_allocated()
```

The job is a small CNN trained for 30 epochs on scikit-learn's bundled
handwritten digits (1,797 real 8x8 images; no download, so no network need). It
writes `metrics.json` with the device, torch build, loss, test accuracy and time.

## The runs

All jobs were experiment tier, submitted with `myce run` from a scratch Git
project, with no host pins, no typed memory and no target list. The scratch
project was removed afterwards; this table is the record.

| Run | How it was placed | Landed on | Device / torch | Test accuracy | Final loss | Train time | Peak on device | Queue to finish |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `3d30cbb5` | the entry as written (`platform: any`) | macOS arm64, Metal | `mps` / 2.15.0 | 0.9822 | 0.000648 | 1.8 s | 20.6 MiB | about 9 min |
| `120aa84d` | forced Metal | macOS arm64, Metal | `mps` / 2.15.0 | 0.9822 | 0.000648 | 3.1 s | 20.6 MiB | about 9 min |
| `8bc8672a` | forced Arc | Linux amd64, Arc Pro B70 | `xpu:0` / 2.15.0+xpu | 0.9822 | 0.000648 | 3.7 s | 24.3 MiB | about 19 min |
| `582ef7a5` | forced CPU | macOS arm64, CPU | `cpu` / 2.15.0 | 0.9822 | 0.000647 | 3.2 s | n/a | about 6 min |
| `e5405dd0` | forced CUDA | did not run | n/a | | | | | cancelled unrun after about an hour |

To land the same job on each platform once, the forced runs used a throwaway
copy of the entry with `needs.platform` set to that one platform. That copy is a
test harness, not part of what an agent writes: in ordinary use the one entry
above is submitted and Mycelium picks, as it did for `3d30cbb5` (Metal). The
harness was needed because `myce run --entry` refuses `--gpu` and `--cpu`.

What the time went on:

- **Environment preparation.** Each platform built its own environment from
  the one lock on first use. On Linux amd64 that downloads about 2.4 GiB of
  Intel runtime, `torch+xpu` (885 MiB) and `triton-xpu` (520 MiB) once per host.
- **The first-run memory charge.** With no memory figure, Mycelium treats the
  first run as exploratory and asks for headroom it cannot know the job does
  not need. On the Arc host status read "exploratory first run needs physical
  headroom above launch slack (-15.8 GiB available; 24.0 GiB allowance)" and the
  run reserved 6.29 GiB when it started; it used 24 MiB on the card. The Macs
  showed the same hold with 6 to 9 GiB allowances until other work finished. A
  copy of the Arc run with a typed figure (`memory_gb: 1, host_memory_gb: 2`)
  asked for 2 GiB instead (status: "2.0 GiB needed, 1.9 GiB available") while the
  unhinted run held the card; it was cancelled unrun once the unhinted run had
  finished. Typing a figure would have shortened the wait; it was not measured.
- **The CUDA node** was disabled for operator maintenance for the whole window
  (status: "the host is disabled for maintenance"). The CUDA run waited with that
  reason and no builder ever started.

## Where the two unconfirmed preferences shaped it

Two preferences from 2026-10-14 are carried on #1491. They are not recorded
rulings and the maintainer has not confirmed them; the prototype follows them.

- **GPU work never falls back to CPU.** The entry says `platform: any`, which
  Mycelium never places on CPU. In the job, `MYCE_DEVICE` wins over anything
  torch detects, and `torch.empty(1, device=device)` makes a wrong wheel fail at
  once instead of training on CPU. The CPU fallback after `or` applies only
  outside Mycelium, where `MYCE_DEVICE` is unset. `PYTORCH_ENABLE_MPS_FALLBACK=1`
  (which sends missing Metal operators to CPU) is not set. The CPU run is
  therefore a deliberate, separate request, not something the entry does by
  itself (see [gaps](#gaps-and-owners)).
- **No lossy conversion by default.** Every platform trains in float32; there
  is no per-device switch to bf16 or fp16. The one dtype change is float64 to
  float32 for Metal, and it is exact here: the pixel values are integers 0 to 16.

Under these two preferences the cheap path did not need a fallback, so there is
no question for the maintainer from this prototype.

## Compared with the variants path (#499)

[Variants](https://github.com/1kuna/mycelium/issues/499) describe each target
completely: one `target` (`os`, `arch`, `platform`), its own `environments`,
`needs`, `execution`, a qualification check and a native baseline, then a
qualification run per host and device (`myce extension check --gpu xpu --device 0`).
Variants cannot inherit from each other ([adapter](../adapter.md#write-an-extension)).

| | Variants | This entry |
| --- | --- | --- |
| Targets named | one block per OS/arch/platform (four here) | none |
| Locks | one per variant | one `uv.lock` |
| Device choice | per variant, or `MYCE_DEVICE` | `MYCE_DEVICE` |
| Before useful work | a qualification check and baseline per host/device | none; the first run on a new type is an ordinary attempt, and a failure there is a coverage finding that skips that type ([entries without targets](../adapter.md#entries-without-targets)) |
| Production | MY-19 parity per kind of machine | the same: production still needs explicit targets and parity |

For ordinary PyTorch work the cheap path makes these unnecessary:

- per-backend entries or variant blocks, and their duplicated command, needs
  and outputs;
- a lock per accelerator family;
- target lists and host pins for non-production runs;
- per-host qualification checks and baseline files before experiment work;
- typed memory figures (the entry types none; see the runs for what that cost).

Variants are still the right tool when the program itself differs per platform
(MLX on Mac and vLLM on CUDA), when a service sidecar is part of the job, or when
production needs a qualified baseline. They are not needed for one PyTorch
program that picks its device.

## Gaps and owners

- **The first run waits for room it does not need.** An untyped first run on a
  busy accelerator host is charged a large exploratory allowance, so the
  cheapest-to-write entry is also the slowest to start. Owner:
  [#1484](https://github.com/1kuna/mycelium/issues/1484) (charge first runs from
  their floor), with [#596](https://github.com/1kuna/mycelium/issues/596) for GPU
  first-run host memory and [#806](https://github.com/1kuna/mycelium/issues/806)
  for learned envelopes. This prototype is fresh evidence for them; no new issue.
- **CPU is a second request, not a fourth backend.** Following the no-CPU-fallback
  preference, `platform: any` covers the three accelerators and never CPU. To run
  the same entry on CPU on purpose, an agent today writes a second entry that
  differs by one line, because `myce run --entry` refuses `--gpu` and `--cpu`. The
  alternative, `needs.platform: {required: [cuda, xpu, metal, cpu]}`, passes
  `myce spec check` but was not run. It would let Mycelium put GPU work on a CPU
  by its least-valuable-machine rule (MY-18), which is the fallback that
  preference rules out. Whether one entry should ever span CPU and accelerators
  waits on that preference becoming a ruling. Nothing to build until then.
- **One lock per OS/architecture, not per accelerator.** uv markers split by OS
  and architecture, which on today's fleet means one accelerator family each.
  A Linux amd64 CUDA host would receive the XPU wheel. `MYCE_DEVICE=cuda:0` would
  then fail at once, which is correct, and entry coverage would skip that machine
  type. An inline project lock is one recipe for every platform, so splitting by
  accelerator on one OS/architecture needs a catalog recipe or variants
  ([#499](https://github.com/1kuna/mycelium/issues/499)). New kinds of machine are
  out of the map's scope, so this is a known limit, not a gap to build now.
- **Linux environment preparation** worked on the Arc host this time; the
  stuck-preparation problem [#727](https://github.com/1kuna/mycelium/issues/727)
  tracks was not seen. [#853](https://github.com/1kuna/mycelium/issues/853)
  (CPU targets from environment coverage) is untouched by this entry: uv locks do
  not limit OS coverage, so no CPU target list was needed either.
