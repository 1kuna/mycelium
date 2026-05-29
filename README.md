# Mycelium

Mycelium is a hardware-aware inference control plane for a heterogeneous local fleet.
It is a single Go binary with one long-running peer role. A peer can run with
`compute: true` to own local accelerators/backends, or compute-off to submit and
coordinate work only:

- `mycelium run`

The control CLI surface is `myce`.

The implementation source of truth is:

1. `01-project-spec.md`
2. `02-testing-architecture.md`
3. `03-development-guide.md`

Those documents are read-only project contracts. Implementation decisions and blockers live in
`DECISIONS.md` and `BLOCKERS.md`.
