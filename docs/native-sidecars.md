# Native job sidecars

A native job may launch trusted helper processes with `run.sidecars`, or use
`execution.main` and `execution.sidecars`. Extensions use the same execution
schema. No Docker daemon is needed. For example, with these scripts already
available in the job workdir:

```yaml
name: native-example
project: example
priority: experiment
resumable: false
needs:
  memory_gb: 1
  host_memory_gb: 2
  platform: any
run:
  command: [python, job.py]
  env:
    HELPER_URL: '{sidecars.helper.url}'
  sidecars:
    - name: helper
      command: [python, -m, http.server, '{port}', --bind, 127.0.0.1]
      port: 0
      readiness: {path: /, timeout: 10s}
      memory_gb: 1
```

Names must be unique after uppercasing and replacing dots/hyphens with
underscores; `main` is reserved. Commands are argv arrays. Helpers inherit
main's workdir and environment, but use their own authored `env`. A named helper
`environment` references an existing catalog recipe (ordinary jobs) or variant
environment (extensions). All environments are acquired before any helper
starts and protected from eviction until group teardown. Extension/work and
member environment substitutions use the same existing cache/recipe mechanism.

A missing `port` declares a plain helper. `port: 0` allocates a free loopback
port; positive ports must be available. Reservations remain held until that
member's dispatch. HTTP readiness requires a port, absolute local path and
positive timeout. The worker verifies listener ownership before accepting 2xx;
a competing listener during handoff retries that member up to three times
before readiness, allocating a fresh port for `port: 0`. Exhaustion or an
unavailable fixed port uses the bounded infrastructure requeue. Unknown
listener ownership fails preparation.
Linux uses procfs; macOS requires `lsof`. Neither HTTP proxies nor redirects are
used. Loopback does not isolate the helper from other local users/processes.

For a helper named `my-helper`, main and all helpers receive:

- `MYCE_SIDECAR_MY_HELPER_PORT`: allocated port as decimal text.
- `MYCE_SIDECAR_MY_HELPER_URL`: `http://127.0.0.1:PORT`.

That helper additionally receives `MYCE_SIDECAR_PORT` and `MYCE_SIDECAR_URL`.
Literal `{port}` in its argv/env expands to its port. All members may use
`{sidecars.NAME.url}` and `{sidecars.NAME.port}` in argv/env. There is no shell
expansion. Identity, `MYCE_*` controls and accelerator selectors cannot be
replaced by authored env. All members receive the same worker-selected device
and exact attempt identity; the fleet token is stripped from every child.

Helpers start in order and become ready before main starts. Every launch uses
the shared authorization gate; readiness does not extend the start window.
A helper exiting before main, even successfully, fails the attempt. Readiness
timeouts also fail the attempt. Infrastructure stops use the existing requeue
classifier. During checkpoint grace, main receives TERM while helpers stay up;
after main exit or the shared deadline, cleanup kills and verifies the whole
process group. Preparation cancellation prevents subsequent members starting.
Before any payload gate opens, the existing durable attempt record holds its
PGID, member PID birth identities, and the aggregate cgroup when configured.
Restart authenticates surviving members, kills the whole group and cgroup, and
verifies release before reporting completion. Unconfirmed identity or cleanup
retains custody.

`memory_gb` is a positive member declaration bounded by the whole-job
`needs.host_memory_gb`. It is accounted within that reservation, not charged
again. With delegated Linux cgroup v2, the attempt parent retains that aggregate
cap and contains `control`, `main` and `sidecar-NAME` leaves. Each member is
attached before exec, with its declared cap as `memory.max`; omitted main RAM
uses the parent cap. Set main RAM with `execution.main.memory_gb` (or
`run.memory_gb`). Member OOM fails the attempt and identifies the member.
Cleanup kills the subtree, verifies population, and removes empty leaves before
the parent; unconfirmed release retains custody. macOS and foreground workers
without delegation remain admission-only; `--require-memory-ceilings` refuses
hosts without enforcement.
Native Windows is a separate slice. All member output uses the existing attempt
log policy, including `logs: none`.

All runtimes use the same outcome precedence:

| Observed evidence | Outcome |
| --- | --- |
| Member OOM, independent nonzero exit, early helper exit, readiness timeout | Stage failure, even with concurrent stop intent |
| Successful main completion, including completion observed during recovery | Success, after confirmed cleanup and output finalization |
| Operator or planned stop without independent failure/completion | Interruption with its own stop provenance |
| SIGKILL without stop evidence | Stage failure with an ambiguity diagnostic |
| Queue cancellation | Cancelled; takes precedence over a late runtime result |

An operator marker annotates the result and never changes a saved failure into
an interruption. Group retry/release uses the existing attempt-wide queue
budget: production infrastructure stops permit at most two requeues; planned
maintenance/preemption follows the existing free-stop policy. Members have no
retry budget or separate reservation. A repeated stop cannot extend the group's
persisted TERM/KILL deadline. Native restart authenticates and force-cleans the
saved whole subtree without granting fresh checkpoint grace. Missing saved
Docker output finalization remains a recovery interruption; a known payload
failure is retained.
