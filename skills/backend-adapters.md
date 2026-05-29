# Backend Adapters

Backend adapters translate a preset into a supervised engine process. They launch,
wait for readiness, and stop an engine through the `ports.BackendAdapter` contract.

Rules:

- Backends are subprocesses or containers, never in-process bindings.
- Readiness is mandatory before traffic.
- `Stop` is idempotent.
- Scheduler-computed tuning must reach the launch command.
- Fast tests use mocks; real engines appear only behind the `smoke` build tag.

