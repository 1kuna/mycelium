# Convert profiles and extensions

`myce adapter convert` reads only explicitly selected sources and prints
deterministic adapter YAML to stdout. Its field and caller-change report goes to
stderr. It does not submit work, prepare environments, or change saved jobs,
keys, receipts, checkpoints, registry snapshots or qualification reports.

Review a conversion from the project root:

```sh
myce adapter convert \
  --profile .mycelium/profiles/gate-full.yaml \
  --profile .mycelium/profiles/go-test.yaml \
  --out .mycelium/adapter.new.yaml
```

`--profile PATH` and `--extension DIR` are repeatable and may be combined.
Profiles must agree on their project. Extension-only conversion requires
`--project NAME`; an explicit project must match every profile. Entry names
retain the original `name`. Conflicting names are refused, even for identical
sources. `--default ENTRY` chooses the default; otherwise the first sorted entry
name is used. Source-flag order does not change the output.

`--out` alone reports a plan and writes nothing. After reviewing stdout and the
report, repeat the same invocation with `--apply`. Apply requires `--out` and an
existing parent directory, and creates the destination exclusively. Existing
files and symlinks are refused, including the input files. Originals stay in
place; conversion does not switch callers or retire old readers.

Profile fields remain unchanged except `project` moves to the header and
`check` becomes `checks.result`. Targets, machine allowlists, run argv/env,
memory class, input/output filenames, diagnostics and source selection survive.
Standard `.mycelium/profiles/` roots are respected; convert Git profiles from
their original project root. Other explicit profile paths already resolve from
the invocation root. Runtime `--input` arguments remain caller-owned.

Native checks become `checks.qualification`; their baselines, references and
sweeps keep their existing acceptance criteria. Setup, assets, resumability,
member caps/readiness and existing inline composed environments are preserved.
Native source points to the original small extension directory relative to the
invocation root, so sources need not move and existing commands keep working.
The existing 1 MiB native bundle limit still applies. Output lists use the
existing `filename: results` convention; omitted outputs remain omitted.
Legacy `benchmark` has no equivalent and is refused instead of becoming an
invented sweep.

Legacy platform-only extensions lack exact OS/architecture. Supply explicit
alternatives for every platform, for example:

```sh
myce adapter convert --extension path/to/native --project example \
  --target linux/amd64/cuda --target darwin/arm64/metal
```

Only matching legacy platforms use these flags. Composed exact targets stay
unchanged. Conversion reports all selected-source errors and missing explicit
native references; no partial adapter is emitted. It never invents targets,
references, output declarations or locks. Catalog environments stay catalog
environments; project environment and build-eligibility work (#251/#252) is
outside this converter.

Native names and version labels remain authored values, including baseline
keys. Freezing adds a content-derived internal version suffix, changing bundle
identity and requiring fresh qualification. Old qualification passes remain
historical and are never relabelled.

For the checked-in starter, invoke from this repository root:

```sh
myce adapter convert --extension examples/extensions/http-stage \
  --project adapter-example
myce run --adapter examples/adapters/http-stage.yaml --entry http-stage
```

`.mycelium/adapter.yaml` stages this project's gate-full and go-test entries
alongside the originals. Select `--entry mycelium-go-test` explicitly for a
small filtered test; the default `mycelium-gate-full` runs the full gate.

Current state (#253): converter and source fixtures are staged for parent-owned
behavior comparisons, recovery verification, test gates and independent review.
Consumer cutover and retirement of source readers belong to #254.
