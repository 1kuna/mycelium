# Project instruction file

Entries default to native sandboxing. An entry can declare top-level
`isolation: unsandboxed` and a required nonempty `reason`; this covers its main,
result checks, queued qualification payloads and sidecars. Use
`isolation: sandboxed` for explicit opt-in when publishing a new version of a
legacy registered extension. The reason is public in status/job GET. See
[native isolation](native-isolation.md) for the ordinary host permissions of
unsandboxed stages, retained launch/device/cleanup checks and durable legacy
registration policy. This is an entry choice, not a global or per-sidecar flag.

`.mycelium/adapter.yaml` describes named entries in strict YAML. `myce run`
selects its `default`; `myce run --entry NAME` selects another entry.
`--adapter FILE` explicitly selects a file. Arguments after `--` append literally
when `--entry` or `--adapter` selects the entry. Plain `myce run -- CMD` retains
its installed-command behavior, including in a project containing an adapter.

```yaml
schema: 1
project: example
default: analyze
entries:
  analyze:
    source: {git: HEAD, paths: [scripts/]}
    run: {command: [python3, scripts/analyze.py]}
    needs: {platform: cpu, cpu_cores: 1, memory: auto}
    targets: [{os: darwin, arch: arm64, platform: cpu}]
    environment: {}
    outputs: {result.json: results}
    checks:
      result: [python3, scripts/analyze.py, --check]
```

Use the exact targets your software supports. Source defaults to committed
Git `HEAD`, class to `experiment`, and resumability to false. The authored
adapter is frozen separately, so its uncommitted edits are allowed; other selected
Git source must be clean. An unrelated entry edit does not change selected intent
or source identity. Directory/snapshot sources retain the existing safety rules.
Relative source and input paths resolve from the invocation root; invoke the
conventional file from its project root.

`run` and composed `execution` are alternatives. Existing resource, allowlist,
I/O, diagnostic and fan-out contracts still apply. `checks.result` runs after the
main command on each attempt or declared wave, before accepted output publication.
Omitting it requires command success and required-output verification. An argv is
never evaluated as shell text unless the author explicitly chooses a shell.

A production entry may declare `class: production` and `start_window: 600s`.
This is the same [relative job start window](job-reference.md#production-start-deadline):
positive Go duration, at most `1h`, default `150s`, inherited by executing fan-out
children from their assignment (dependent jobs start their budget at readiness).
An explicit absolute `start_by` takes precedence. Keep the policy on the entry,
alongside class and fan-out, rather than in hardware variants or extension recipes.
Experiment entries omit it and retain no start deadline.

Installed tools (`environment: {}`) and named catalog environments keep their
existing behavior. Project environment submission (#251) and worker build
eligibility (#252) remain separate work. Existing native composed environment
maps are supported; single `environment` and `environments` cannot coexist.

Native entries use complete `variants`, each with one exact `target`, `needs`,
`execution`, optional existing `environments`, and `checks.qualification` plus
native baselines. They cannot mix flat recipe fields with variants. Keep their
small description/source closure within the existing 1 MiB bundle contract;
large immutable assets use file references. Preparation triggers exact-host/device
checks from worker-owned scratch. Useful work waits for an accepted baseline
comparison; a warm repeat reuses matching evidence. Failure remains visible and
unqualified until an explicit qualification retry or a changed identity. Result
checks never replace native qualification.

This slice retains the existing profile receipt/observer recovery contract:

```sh
myce run --entry analyze
myce wait JOB_ID
myce result JOB_ID --out accepted-results
```

Submission returns a receipt; `myce result` verifies accepted artifact SHA-256 and
size. Foreground completion/retrieval and new operation UX remain #156's separate
slice. Queue outages never execute locally. Keep private receipts while recovery
matters, and use a new explicit `--key` for an intentional repeat.

See [conversion](adapter-convert.md) for explicit, non-destructive migration.
Existing authoring readers remain until the separately verified consumer cutover;
original frozen legacy/composed decoders, bundle bytes and accepted identities
remain available for retained jobs and registry snapshots.
