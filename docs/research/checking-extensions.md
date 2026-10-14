# Checking an extension on a new hypha

Research for [issue #35](https://github.com/1kuna/mycelium/issues/35), 2026-10-14. Primary sources only; all sources below were read on that date. This is a recommendation for the next design, not a change to the approved product spec or an implemented check.

## Question

How can Mycelium mark an extension **qualified** on a particular **hypha**, comparing its actual work with a baseline recorded on hardware that runs that work natively, while allowing normal variation? How can a user without that hardware participate? The scope is one **kind of work** or **program**, within one **mycelium**; resource speed belongs in the **capability profile** and is distinct from output **similarity**. These terms and boundaries follow the glossary and the resolved job and extension decisions. [P1–P4]

## Short answer

Bundle a small set of fixed inputs, repeated native outputs, exact program/model identity, and measured comparison limits into the extension's existing `check` command. Run those inputs through the same execution path used for jobs on each new hypha. Require structural checks and per-kind similarity together; use existing array-comparison tools rather than a new validation service. This follows the saved-input/output pattern in NVIDIA Polygraphy and ONNX backend tests. [S3, S6, S7; recommendation]

The baseline need not be recorded by every user: the first extension's author can supply it, and later extensions reuse that reference for the same kind of work and model. Without a suitable native baseline, report **not qualified: native reference unavailable**. Known-answer scores and vendor diagnostics are useful supporting checks, but cannot establish that this extension reproduces the native computation. [P3, S1, S6, S9; recommendation]

Start with reviewed, frozen limits and clear failure reasons. Later, add reviewed measurements from more hardware generations and software combinations. Do not invent a universal percentage or let a failing hypha widen its own limits. No source establishes a general M4-to-M5 allowance. [S2–S5, S8, S10; recommendation]

## Findings

### MLPerf: quality floors and concrete reference implementations

MLPerf Inference measures system performance under specified quality constraints. The inspected rules give ResNet50 an ImageNet quality requirement of 99% of a reference FP32 score of 76.46%; BERT and 3D U-Net have 99% and 99.9% reference-quality variants. These are fractions of a benchmark quality score, not requirements that 99% of output elements match or universal relative-error limits. The reference repository provides concrete model, dataset, and framework implementations. [S1]

**Use for Mycelium:** reuse the idea of a fixed computation and explicit quality floor, and reuse an appropriate reference implementation when available. A label-based score alone can miss changes in numerical outputs: the NeurIPS study below found differences that were often invisible in test-set accuracy. Pair any known-answer quality check with direct comparison against recorded native outputs. Copying the whole MLPerf submission system would add machinery unnecessary for this check. [S1, S4; inference/recommendation]

### PyTorch and JAX: tolerances depend on precision and operation

PyTorch explicitly does not guarantee bitwise-identical floating-point outputs across platforms, releases, or CPU/GPU execution, even with identical inputs and controlled randomness. Batch layout, accumulation precision, and ill-conditioned computations can change results. Its `assert_close` checks values using `abs(actual-reference) <= atol + rtol * abs(reference)`, with configurable structural checks. Inspected FP32 defaults are `rtol=1.3e-6`, `atol=1e-5`; FP16 uses `1e-3`, `1e-5`. These are testing defaults, not measured limits for a Mycelium extension. [S2, S3]

JAX's inspected test helpers use dtype-specific tolerances, including FP32 `1e-6` and FP16 `1e-3`; individual tests override them. For example, its polynomial test increases selected tolerances on TPU. The FAQ also shows compilation changing numerical results through algebraic simplification. A seed and a hardware name alone therefore do not specify the computation adequately. [S5]

**Use for Mycelium:** explicit per-output limits, fixed precision/batching/settings, and a recorded execution configuration. The absolute term handles values near zero; the relative term handles scale. Existing defaults are useful during author investigation, but a published default must be justified by this kind of work's measurements. [P2, S2, S3, S5; recommendation]

### ONNX: fixtures plus backend execution, not model grading alone

ONNX has operator-level and model-level backend tests. Model tests contain a model definition and input/output pairs; large models can be downloaded separately. The inspected runner checks output count, shape, and dtype, compares strings exactly, and compares numerical outputs with tolerances, including precision-specific handling for bfloat16. These tests check a backend's computation against expected outputs rather than merely counting correct labels. They cover tested ONNX behavior, not every arbitrary program. [S6]

**Use for Mycelium:** follow this fixture format and borrow relevant conformance tests when the extension actually uses ONNX. Still run the extension's complete preprocessing, model execution, and output handling: passing an operator test cannot establish that those surrounding steps are correct. [S6; inference/recommendation]

### Numerical reproducibility and cross-hardware output drift

The NeurIPS 2023 paper *Causes and Effects of Unanticipated Numerical Deviations in Neural Network Inference Frameworks* studied fixed trained CNNs and inputs on 75 platforms. It associated CPU differences with SIMD behavior and GPU differences with runtime convolution-algorithm selection; some GPU executions also varied on the same platform. It reported up to 26 different final softmax vectors, while label changes were rare enough that ordinary accuracy metrics concealed much of the drift. Its TensorFlow/CNN results demonstrate the mechanism, not a bound for all models or Apple hardware. [S4]

The 2024 study *Impacts of floating-point non-associativity on reproducibility for HPC and deep learning applications* examines GPU reductions and PyTorch behavior. Its observed distributions depend on inputs and GPU family, and not all investigated variability is normally distributed. Assuming one Gaussian noise allowance for all programs is consequently unjustified. [S8]

The 2025 paper *Understanding and Mitigating Numerical Sources of Nondeterminism in LLM Inference* studies fixed models under different GPU types, counts, batch sizes, and precisions. Small logit changes can alter greedy tokens and cascade into very different generated responses. Higher computation precision improves stability but does not establish universal cross-hardware identity. [S10]

**Use for Mycelium:** measure native repeatability separately from cross-hardware drift. For generation, compare fixed-prefix logits where available alongside final-output similarity; identical sampled text is unsuitable as a universal rule. Neither a small numeric difference nor a different sentence by itself diagnoses defective hardware. A failed check establishes that the extension/configuration has not met the reference contract. [S2, S4, S8, S10; inference/recommendation]

### Vendor tools: reuse comparison machinery; keep health checks separate

NVIDIA Polygraphy's *Comparing Across Runs* example saves inputs and outputs and loads them for comparison on another platform or TensorRT version. Its comparison API supports per-output tolerances, structural checks, and several error statistics. Its elementwise absolute/relative rule is not identical to PyTorch's additive rule; record which comparator is used rather than treating identically named parameters as interchangeable. [S7]

NVIDIA DCGM diagnostics cover deployment, memory, compute, connectivity, stress, power, and thermal checks. NVIDIA explicitly says they are not comprehensive hardware diagnostics or replacements for field diagnostics. Intel's Processor Diagnostic Tool similarly checks processor functionality, frequency, features, and stress on its supported Windows platforms. A successful health suite does not compare the extension's own outputs with its reference. [S9, S12; inference for the final sentence]

Apple's Core ML typed-execution documentation says that, for the neural-network format, graph partitioning and execution precision vary with hardware and software versions. Typed ML programs specify minimum intermediate precision but can still use different execution placements. This provides a concrete reason to record precision, model format, and compute-unit settings; it supplies no measured M4/M5 tolerance. M4 versus M5 is a hardware-model change; changing neural-network weights or architecture is a separate computation change. [S11; distinction/recommendation]

## Options and trade-offs

- **Bitwise equality:** simple and appropriate for genuinely exact outputs, but rejects legitimate floating-point differences. Use for identities, discrete invariants, and declared exact programs, not all ML output. [S2, S5; recommendation]
- **Known-answer scoring alone:** understandable and useful for model quality, but can miss numerical drift and does not isolate the execution path. Keep as a secondary floor. [S1, S4; recommendation]
- **Native fixtures with measured limits:** directly compares the intended computation, fits one small extension folder, and can be reused without each user owning native hardware. Coverage is limited to recorded inputs, configurations, and sizes; it cannot prove every later job. **Recommended first version.** [P3, S6, S7; recommendation]
- **Vendor diagnostics or broad conformance suites:** useful for troubleshooting and specific backend coverage, but platform-specific and insufficient for end-to-end extension qualification. Optional, rather than a prerequisite for every laptop. [P1, S6, S9, S12; recommendation]
- **Automatic population-wide statistical learning:** could expand coverage later, but needs independently trusted measurements, sufficient samples, and protection against turning regressions into accepted variation. Defer; use a reviewed file of measured limits first. [S4, S8; recommendation]

## Recommended method

Everything in this section is a proposed workflow, not a validated statistical guarantee. It adapts ordinary differential tests to the extension contract; it does not introduce a service, separate registry, or scheduler. [P1, P3, S6, S7]

### 1. Record the baseline once, on an identified native path

The author of the first extension for a kind of work declares the intended native implementation and resource, then records its actual execution. Do not assume that any available CPU implementation is the native path. Use that extension's normal run path with public or synthetic fixtures covering ordinary work and a few demanding cases: different shapes/lengths, batching, near-zero values, and sensitive decisions. Include preprocessing and postprocessing, not just a model tensor. Check an independent known answer or functional invariant where available so a broken reference is not accepted merely because it is repeatable. [P3, S2, S4, S6; proposal]

Record repeated runs, including fresh process starts and warmed execution. A practical starting protocol is ten runs per fixture, five for choosing limits and five reserved for checking them; this count is an authoring budget, **not** a published confidence level or demonstrated false-alarm rate. If outputs cannot be constrained to a usable repeatability range, narrow the execution configuration or leave qualification unavailable for that mode. For stochastic programs, store exact generated input tensors rather than expecting a seed to reproduce inputs across backends. [S4, S7, S8; proposal]

Store a small baseline manifest and fixtures with the extension. The manifest names the kind of work, reference version, hashes of program/model/input/output artifacts, tokenizer and preprocessing versions, precision/quantization, seeds or saved random inputs, batching, native backend/runtime/compiler/driver/OS versions, hardware generation/revision, and relevant execution settings. Keep measured speed and resource use in the capability profile separately. Record power/thermal state when available as context, without assuming it changes the numerical allowance. Never include machine names, local account names, private paths, or secrets. [P1–P4, S2, S5, S7, S11; proposal]

Large models and baseline artifacts stay outside the small folder and are fetched by setup; the frozen manifest contains their hashes and retrieval references. Small native outputs can stay inside it. Later extensions reuse the same reference and per-kind final-output similarity rule; precision-specific numerical limits require separate measured justification. Different weights, preprocessing, or intended semantics require a separately identified reference; a deliberately lossy conversion remains compared against the original native reference and requires informed opt-in. [P2, P3, S6; proposal]

### 2. Choose and freeze tolerances from evidence

For each numerical output, use an existing comparator with explicitly declared semantics. For the additive array rule, let `b` be the stored native output and `x` a rerun:

```text
abs(x[i] - b[i]) <= atol + rtol * abs(b[i])
```

Choose the smallest limits on an author-declared candidate grid that accept all clean calibration runs, then require the reserved runs and fixtures to pass unchanged. Record observed maximum errors, selected limits, sample counts, and configurations. Any extra margin must have a written measured justification; zero observed variation does not establish zero variation everywhere. A framework's default is not a substitute for this evidence. [P2, S3, S5, S8; proposal]

Native repeats estimate variation on that native setup; they cannot estimate normal drift to an unmeasured chip generation or driver. Add such combinations only after independently checking that they run the right artifacts/path and produce acceptable output. Their reviewed measurements can justify a new frozen baseline/limit revision and extension version. A new candidate may pass existing limits immediately, but its own failure cannot justify relaxing those limits. An unexplained borderline result stays not qualified pending review. [P2, P3, S4, S8, S11; proposal]

Use the kind of work's final-output measure as well: for example, normalized edit similarity against the native transcript, or spatial similarity against a native segmentation. Document normalization so it cannot erase meaningful differences. Keep a minimum acceptable result quality separate from the numerical variation envelope: variation beyond acceptable semantics is not normal merely because it occurs repeatedly. For generated text, fixed-prefix logits can help distinguish numerical drift from changed decoding; the final-output rule still applies. These are measures to select and validate for each kind of work, not universal measured defaults. [P2, S1, S4, S10; proposal]

Record and apply the owner's per-job or global similarity-threshold overrides. A lower threshold for a lossy version needs informed opt-in; changing it does not waive artifact identity, structural checks, or the need for a native reference. [P2, P3; proposal]

### 3. Run the check on each hypha and fail clearly

Setup verifies artifact identity. The `check` command reruns the frozen fixtures through the same backend, precision, batching, and output path used for jobs; it reports enough backend placement evidence to catch silent CPU fallback. An unsupported or unobservable execution path remains not qualified. Different supported job modes must be covered explicitly rather than qualified by a test using another mode. [P3, S6, S7, S11; proposal]

Require all fixtures to pass: exit success, present outputs, expected count/shape/type, finite values unless particular nonfinite outputs are explicitly part of the contract, exact discrete invariants, numerical limits, and final-output similarity. Check localized errors as well as aggregate similarity; a good average can conceal a bad region. During authoring, demonstrate that the checker rejects missing output, altered model identity, wrong preprocessing, and deliberately corrupted output artifacts. This is a recommended future checker test, not an experiment performed for this report. [S2, S3, S6, S7; proposal/inference]

A numerical miss can prompt a bounded repeat for diagnosis under the declared normal settings, retaining every result. Do not rerun until one passes or discard intermittent failures. The initial version should require every prescribed check run to pass; stochastic acceptance rules need a separately measured design. Report the fixture, failed condition, observed difference, allowed limit, and execution configuration. Keep numeric mismatch separate from setup failure, timeout, unsupported backend, or missing reference. [P3, S4, S8; proposal]

Mycelium stores the result against the extension version, baseline/comparator identity, hypha, and relevant execution configuration. Version, hardware, driver, OS, runtime, or precision changes invalidate the affected result. Failure leaves the extension not qualified there; choose another qualified extension/hypha or explain why the job waits, with no silent CPU fallback. Thermal throttling or slow execution changes placement evidence; it does not justify weakening output limits. [P2, P3, S2, S11; proposal]

### 4. Handle users without native hardware honestly

A user can download the exact native baseline supplied with the extension and run its check locally; they need no native hardware to replay those fixtures. The author records the reference once, and the user's hypha produces the candidate output. For the first version, rely on reviewed extension authors within the existing trust boundary; artifact hashes detect changed bytes but do not prove a reference was honestly recorded. Shared-publication trust is a later concern in the map. [P1, P3, S6, S7; proposal]

If no suitable native recording exists, another backend's outputs, a CPU run, known-answer score, or agreement between two conversions can supply supporting evidence, but cannot establish the requested native comparison. Keep the extension not qualified and explain the missing reference. The next step is for an author or someone with the native hardware to record it; do not invent a fallback qualification grade. [P3, S1, S4, S9; proposal]

### 5. Keep extension qualification separate from every-job similarity

**Open boundary for the orchestrator:** #18 says every non-native run is measured against a native reference. A fixture check supplies references for its fixtures, not for arbitrary new job inputs. Qualification is necessary but does not satisfy that stronger per-job requirement. [P2, P3, S6; inference]

If that requirement remains literal, retain native outputs keyed by the job's exact inputs, program/model, settings, and reference identity; compare a non-native job with the matching recording. On previously unseen inputs, obtain a native run or wait with an explanation. This can duplicate expensive work and leaves users without native hardware dependent on supplied matching references. Accepting new jobs based only on extension qualification would require a new product decision; this research does not silently make it. **Next step:** resolve the native-reference source for new job inputs in #18/the map before implementing per-job acceptance. **Revisit trigger:** design of the first non-native job path. [P1–P3; proposal]

## First version and later additions

**First usable version:** one reviewed native fixture bundle, one ordinary `check` command using existing comparison code, frozen measured limits, an attempt/configuration-bound qualification result, and plain wait/failure reasons. Support deterministic inference and other bounded-repeatability modes first. Keep native recording and comparison authoring testable with ordinary files on a laptop; acquiring the true native baseline is the separate hardware prerequisite. [P1–P3, S6, S7; recommendation]

**Later:** larger and held-out fixture sets; reviewed measurements across chip generations and software revisions; uncertainty estimates based on adequate data; checks for stochastic/training programs; intermediate-output diagnostics; optional vendor health checks; and baseline publisher trust when shared extensions cross the current trust boundary. Version changed limits instead of learning them invisibly during qualification. [P1, P3, S4, S8–S12; recommendation]

## Confidence and limits

**High confidence** in saved-input/output differential checking, precision-aware limits, and the inadequacy of bitwise equality or model grading alone. **Moderate confidence** in this small workflow as the first usable design: it fits the existing command contract, but has not been implemented or measured in Mycelium. [P3, S1–S8, S10]

I could not verify any universal similarity threshold, safe repeat count, false-alarm rate, empirical M4/M5 drift envelope, or temperature-specific numerical allowance. The cited papers test particular models and hardware, not every kind of work or arbitrary program. A fixture pass neither proves all future inputs nor diagnoses the physical source of a mismatch. No experiments, source changes, hardware access, or fleet operations were performed. [S2, S4, S8, S10, S11; scope/limits]

**Current state:** research complete; recommendation recorded for #35. **Next step:** the orchestrator resolves the per-job reference boundary and turns the accepted method into the next design; a future implementation must demonstrate repeatability and rejection of bad outputs. This report does not close #35 or amend the approved spec. [P1–P3]

## Dated primary sources

Project decisions (2026-10-14):

- **P1:** [Map #17](https://github.com/1kuna/mycelium/issues/17) and [research question #35](https://github.com/1kuna/mycelium/issues/35): core first, laptop use, public-safe writing, and research direction.
- **P2:** [Resolved job contract #18, 2026-10-14](https://github.com/1kuna/mycelium/issues/18#issuecomment-6178165736): per-kind similarity, measured defaults, per-job overrides, and informed opt-in for loss.
- **P3:** [Resolved extension contract #20, 2026-10-14](https://github.com/1kuna/mycelium/issues/20#issuecomment-6178584766): small setup/run/check folder, shared native baseline, frozen versions, per-hypha qualification, and safe failure.
- **P4:** [Glossary on `docs/mycelium-vision`](https://github.com/1kuna/mycelium/blob/docs/mycelium-vision/GLOSSARY.md), branch snapshot read 2026-10-14.

External sources (all accessed 2026-10-14; an access date is not a publication date):

- **S1:** MLCommons, [Inference rules](https://github.com/mlcommons/inference_policies/blob/d3eba2f21026d868ad65cdcad2bb81e4a17ce3d3/inference_rules.adoc#L298-L308), repository snapshot 2026-08-20; [reference implementations README](https://github.com/mlcommons/inference/blob/3fbc329939999c13d0a7b5e67fb2092287e06047/README.md), snapshot 2026-09-02. Quality tables and reference app/framework/dataset links.
- **S2:** PyTorch 2.14, [Numerical accuracy](https://docs.pytorch.org/docs/2.14/notes/numerical_accuracy.html), page updated 2026-06-01. Sections on platform differences, batches, extremal values, and linear algebra.
- **S3:** PyTorch 2.14, [`torch.testing.assert_close`](https://docs.pytorch.org/docs/2.14/testing.html#torch.testing.assert_close), undated versioned documentation. Additive comparison, structural checks, and dtype defaults.
- **S4:** [*Causes and Effects of Unanticipated Numerical Deviations in Neural Network Inference Frameworks*](https://papers.nips.cc/paper_files/paper/2023/file/af076c3bdbf935b81d808e37c5ede463-Paper-Conference.pdf), NeurIPS 2023, sections 1–3. Fixed-model inference across 75 platforms.
- **S5:** JAX, [test tolerance definitions](https://github.com/jax-ml/jax/blob/b5f212098d3b5a0aa875dd718848a4e75fb71ec2/jax/_src/public_test_util.py#L46-L76) and [polynomial test overrides](https://github.com/jax-ml/jax/blob/b5f212098d3b5a0aa875dd718848a4e75fb71ec2/tests/lax_numpy_test.py#L1421-L1428), snapshot 2026-10-14; [FAQ: compilation changes numerics](https://docs.jax.dev/en/latest/faq.html#jit-changes-the-exact-numerics-of-outputs), undated documentation.
- **S6:** ONNX, [Backend Test description](https://github.com/onnx/onnx/blob/d6cfcf584e0d20d67bc2fcae6419f2ad499460f6/docs/OnnxBackendTest.md) and [runner comparison](https://github.com/onnx/onnx/blob/d6cfcf584e0d20d67bc2fcae6419f2ad499460f6/onnx/backend/test/runner/__init__.py#L186-L240), snapshot 2026-10-14. Operator/model fixtures and actual structural/numerical checks.
- **S7:** NVIDIA, [Polygraphy: Comparing Across Runs](https://github.com/NVIDIA/TensorRT/blob/release/10.13/tools/Polygraphy/examples/cli/run/02_comparing_across_runs/README.md), release/10.13, undated; [comparison API](https://docs.nvidia.com/deeplearning/tensorrt/11.4.0/_static/polygraphy/comparator/compare_func.html), Polygraphy 0.53.6 documentation, undated. Saved input/output reuse and configurable comparisons.
- **S8:** [*Impacts of floating-point non-associativity on reproducibility for HPC and deep learning applications*](https://arxiv.org/html/2408.05148v3), v3, 2024-10-30. GPU reduction distributions and reproducibility investigations.
- **S9:** NVIDIA, [DCGM diagnostics](https://docs.nvidia.com/datacenter/dcgm/3.0/user-guide/dcgm-diagnostics.html), v3.0 documentation, undated. Diagnostic scope and explicit limitations.
- **S10:** [*Understanding and Mitigating Numerical Sources of Nondeterminism in LLM Inference*](https://arxiv.org/html/2506.09501v2), v2, 2025-10-24. Fixed-model comparisons across GPU, batching, and precision configurations.
- **S11:** Apple, [Core ML Tools: Typed Execution](https://apple.github.io/coremltools/docs-guides/source/typed-execution.html), undated documentation, copyright 2024. Hardware/software-dependent graph partitioning and precision.
- **S12:** Intel, [Processor Diagnostic Tool](https://www.intel.com/content/www/us/en/download/15951/intel-processor-diagnostic-tool.html), release 4.5, undated page. Processor functionality, frequency, features, and stress checks on supported Windows platforms.
