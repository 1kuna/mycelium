# Placement without hardware requirements (v3)

Submit a kind of work or a program as described in
[Write an extension](write-an-extension.md). No hardware fields are needed.
Optional `size_hint.memory_gb` and `size_hint.host_memory_gb` are lower bounds,
never requirements to submit a job. Existing v2 command jobs keep their contract.

The queue requires a qualified extension for the work and accelerator type.
The capability profile bounds each hypha's capacity. Separate memory must fit
accelerator-reachable memory and additional system memory independently. Unified
memory must fit their sum in one shared pool. For example, 64 GiB RAM plus
24 GiB accelerator memory fits RAM-heavy work but not 40 GiB accelerator work;
128 GiB unified memory fits that work if enough of its shared pool is free.
Live free-memory checks, operator budgets and reservations still apply. Unknown
native profile sizes wait; explicitly fake resources use their configured
allowance. Older workers without profiles retain the v2 admission contract.

Among free eligible hyphae, measured reference throughput now supplies a
provisional predicted finish time, including file delivery and known preparation.
See [the onboarding benchmark](guide-benchmark-v3.md) for units, unknown-data
fallbacks and limits. Without comparable measurements a first program retains
the capacity order (accelerator-reachable memory, system memory, CPUs, hypha ID).
Priority, preemption, fairness, production holds and start authorization still
use the existing scheduler and cannot be bypassed by speed.

Successful current attempts save observations in the same SQLite queue. The
next matching submission and already queued matching programs reuse the largest
observed needs with 25% headroom, never below the extension's reservations or
optional hints. Learning is per project, program description, arguments,
configuration, input references, frozen extension digest and accelerator platform.
Changing any of these starts a new learning record. Input references must be
immutable: the current NAS transport cannot detect replacement bytes at submit
time. Use a new reference when changing an input; content-addressed transport
is separate work (#24). Measurements are observations, not a guarantee that
future data-dependent executions need exactly the same memory.

The command runner records OS peak process RAM. Extensions may write a bounded
JSON report to worker-owned `MYCE_USAGE_FILE`, containing `memory_gb`, optional
`host_memory_gb`, and `out_of_memory`. Values are finite, nonnegative GiB.
The path belongs to the exact attempt and is outside its published outputs.
Malformed, oversized and symlink reports are ignored; missing accelerator
observations retain the extension floor. OS peak RAM includes the interpreter
and may include shared accelerator allocations, so unified-memory learning can
conservatively overreserve. Descendant-process RAM is not a complete process-tree
measurement. No configured budget is labelled as an observation.

The PyTorch starter (since v0.2.3) runs the script in its Python process, preserving its
arguments, imports and signal handlers. CUDA/XPU report allocator high-water
marks; MPS samples current allocations every 50 ms, so brief peaks can be missed.
Its bundled-reference qualification remains required. Unsupported counters
remain unknown. This starter does not silently select the CPU.

An explicit extension OOM or kernel memory-ceiling OOM queues **one** retry on
an eligible hypha with no smaller memory pool and at least one strictly larger
pool. Reservations double after the failure. If no such hypha is available the
job explains the wait; it does not loop on the same or an equal-size hypha.
A second OOM fails visibly. OOM does not count as infrastructure loss, successful
learning or permission to loosen extension qualification. Cancel, production
start deadlines and cleanup/release gates still apply.
