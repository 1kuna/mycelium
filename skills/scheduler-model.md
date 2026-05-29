# Scheduler Model

The scheduler works on units and presets.

Pipeline:

1. Resolve preset.
2. Estimate claim.
3. Filter units by hard constraints.
4. Select candidates using speed preference.
5. Score candidates.
6. Admit, queue, or preempt.

Locked rules:

- Soft preemption is default.
- Hard preemption is opt-in.
- Fit-forced reallocation uses the same preemption test.
- `max_util` is never exceeded.
- Catastrophic units keep extra margin and do not stack concurrent loads.

