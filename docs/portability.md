# Live portability advice

## Declare requirements, let Mycelium place

Agents state requirements rather than hosts: work kind or command, compatible OS/architecture/backend targets,
accelerator count/class, memory and environment. Mycelium selects an eligible
host using qualification, owner policy, available capacity and least-valuable-fit
ranking (MY-18). For ordinary CPU work, for example:

```yaml
needs: {platform: cpu, cpu_cores: 2, host_memory_gb: 2}
targets:
  - {os: linux}
  - {os: darwin}
environment: portable-tools
portable: true             # verified complete environment/payload coverage
```

Job targets require `os`; omit `arch` when the payload is portable across that
OS's architectures, and omit `platform` when `needs` already expresses the backend.
For example, `{os: linux}` suits `go test` compiled on the worker, scripts with
compatible installed tools, and environments built from source on the worker.
Matching hosts still pass all resource, environment and qualification checks;
MY-18 chooses the least valuable fit among them.

OS-only targets expand automatically only for plain commands without opaque
inputs and environments built from source on the worker without a shipped
binary. Uploaded files (including generated source archives), shared/inherited
payloads, container images, locked environments without a source build, and
single-arch extension variants need an explicit `arch` or the author's
`portable: true` declaration. File manifests do not prove which inputs are
executables or archives, so every uploaded payload requires that assertion.
A build hook beside a prebuilt environment binary does not establish portability.
Mycelium checks declarations, not binary signatures or runtime downloads.

For a verified source-only profile, for example:

```yaml
portable: true
targets: [{os: linux}]
```

`portable: true` asserts that the complete payload, including downloads and
nested archives, can run on every matching architecture. It does not create
missing extension variants, bypass qualification or ignore environment coverage.
Extension variants retain complete, exact OS/arch/platform targets.

A clear exec-format/wrong-architecture failure is reported as `arch_mismatch`.
Mycelium excludes that attempt's OS/arch and requeues once using normal placement
after exact cleanup. A second architecture mismatch ends recovery; it does not
spend infrastructure retries or loop through hosts. Exclusions survive restart
and later submissions of the same frozen executable contract inherit them.
Changed source/artifacts, commands, environment recipes or extension digests
establish a new contract. Exit codes alone are insufficient evidence. The worker checks stderr even with
`logs: none`; stdout text does not establish an architecture mismatch.

Use targets whose tools and outputs you have verified. `portable-tools` stands
for a registered catalog recipe covering those targets; native entries may use
their existing bundled per-variant environments. A single-platform lock requires
a platform restriction until another recipe is proven, not a particular host.
Use a multi-platform lock where possible and separate recipes where packages
differ. Cached packages alone prove neither command binding nor qualification;
custom-command reuse remains [#380](https://github.com/1kuna/mycelium/issues/380).

Host/device pins are exceptions for specific hardware, approved production
parity, or hardware qualification. Record an adapter exception alongside the pin:

```yaml
requirement_reasons:
  host: Approved production output parity currently covers this host only.
  device: This diagnostic qualifies the selected physical accelerator.
```

Declare only the reasons that apply. These are authored explanations, not proof
of parity or additional placement authority. A backend reason such as
`backend:cuda` explains a backend limit; it does not explain a particular host or
device. Keep the supporting evidence in the project's own records. Free-form
reason text is omitted from advice output.

`myce run` for adapters/profiles and `myce submit` print a non-blocking advisory
for pins without a declared matching reason. Plain profiles and job YAML have no
`requirement_reasons` field, so their pins receive the advisory; keep intentional
exception evidence with the profile. `myce portability --adapter …` uses the same
advice in its notes. This identifies an unexplained restriction, not a proven
unnecessary pin; it changes no intent, target, qualification or production rule.
Full live-coverage warnings at registration remain
[#326](https://github.com/1kuna/mycelium/issues/326).

Use the job's placement rationale when reporting “Mycelium placed it on X because
Y”. `machine` and `allocated_devices` are assignment results; `production_host`
can also be set by the scheduler and is not evidence of an agent-authored pin.
Status already reports scarcity/admission rationale. Retained intent summaries
and clearer origin/explanation are tracked in
[#418](https://github.com/1kuna/mycelium/issues/418): current job/status APIs redact
exact targets and environment recipes, so an audit cannot infer their absence.

## Inspect coverage

Ask before authoring an adapter, and again when inventory or requirements change:

```sh
myce portability
myce portability --adapter .mycelium/adapter.yaml --entry train
myce portability --extension example@1 --project example --json
```

The first command needs only installed queue authority and returns the cluster's
observed OS/architecture/backend combinations, including CPU hardware. No fleet
list is configured. `--queue`, `--config` and `--joining-file` use the same pinned
authority as `myce run`; keep credentials in installed configuration.

Each target separates observed inventory, declared coverage, Mycelium runtime
support, variant metadata digest, resources, lock inputs/coverage, worker build
state, preparation, qualification identity and current admission. Declared exact
targets absent from inventory remain visible. Heartbeat and hardware observation
times are separate; stale or incomplete inventory cannot claim current admission.
Busy, leaving or maintained workers keep their declared coverage. An unsupported
Mycelium runtime requires a runtime port independently of application changes.

Adapter mode reads the selected entry's current local metadata and small,
explicit lock/manifest/build inputs relative to the source directory (or the
current project root). It does not archive Git sources or read a source snapshot.
The metadata budget is 1 MiB, matching the extension description limit; larger or
unavailable inputs remain missing/unknown. It never executes payloads, solves
packages, registers an extension, submits a job or changes placement. Native
adapter metadata has no frozen registration digest, so it cannot establish that
an existing worker is prepared or qualified. Use the registered extension form
for that evidence. Variant digests describe the recipe metadata supplied to the
report, not a new registration. Plain entries retain their own resource needs,
target requirements, catalog environment and existing result checks.

Registered extension mode reads the exact immutable queue registration and its
verified bundled inputs. Per-work check states remain separate; admission names the direct extension
entry (or the first declared legacy kind/program). Qualification uses the same target, fingerprint,
environment and check identity predicate as dispatch. Supply `--project` to
include that project's admission policy; without it admission cannot claim a
project-specific pass. Admission uses current planner resources, policy,
maintenance and release gates. It is a snapshot, never a reservation or a claim
that unverified installed tools will work.

A missing variant is a portability gap, not a hard software requirement. Optional
`requirement_reasons` in an extension or adapter entry records authored reasons:

```yaml
requirement_reasons:
  backend:cuda: This dependency requires the CUDA runtime.
```

This is explanatory metadata, not a new placement constraint. It does not widen
or narrow targets or bypass qualification. The report identifies the constraint
and whether its reason is authored or unknown. Free-form reason text stays in
the source metadata because it may contain private paths or credentials. Worker
error/log text, command arguments, file contents and raw host IDs are also
excluded; host ordinals and hashed qualification identities permit comparison
within the snapshot. Do not put secrets in identifier fields.

Advice names the actual package target (for example `linux-aarch64` when an
arm64 Linux worker is observed). Include `uv.lock` with `pyproject.toml`, or
`pixi.lock` with `pixi.toml`; reuse multi-target locks where valid and split recipes
where packages differ. Pixi's selected environment target declaration is reported
separately from a successful native frozen install. uv/platform marker coverage
remains unknown until native frozen installation verifies it; a present lock does
not prove wheel, ABI or toolchain support. Missing explicit bundled build
arguments point back to their argument position without disclosing source paths.

Use `MYCE_DEVICE` and assigned visibility. Queue backend `metal` maps to the
application's `mps` device. Add CPU implementations explicitly, with their own
resources and checks. GPU qualification must exercise its selected backend and
never silently fall back to CPU. Replace OS-specific shell/filesystem assumptions
with portable APIs, prepare through the existing operator catalog or bundled
variant mechanism, and qualify natively against the recorded baseline. MY-19
still requires owner-accepted output parity before splitting work across targets.

## API

Authenticated `GET /api/portability` returns inventory-only advice.
`GET /api/portability?extension=example%401&project=example` reads a registration.
Authenticated `POST /api/portability` accepts one of:

```json
{"extension":"example@1","project":"example"}
```

```json
{"recipe":{"name":"example","version":"1","variants":[]},"project":"example","requirement_reasons":{"backend:cuda":"Authored reason"}}
```

```json
{"spec":{"project":"example","run":{"command":["python3","run.py"]},"needs":{"cpu_cores":2,"memory_gb":1,"platform":"cpu"},"targets":[{"os":"linux","arch":"arm64","platform":"cpu"}]}}
```

`recipe` uses extension metadata including optional bundled `files` (JSON byte
arrays use base64). It may be incomplete so missing inputs can be explained.
The API permits the same 2 MiB JSON transport as extension registration to
accommodate base64 encoding of the bounded description bundle.
Advice never resolves caller-supplied filesystem paths, fetches assets or executes
commands. The JSON result is the shared projection used by the text command.
Unavailable authentication/API is an error, not an all-platforms pass; an empty
successful inventory explicitly reports `unknown`.

## Laptop proof

`myce dev --gpu cpu` runs the real queue and command runner without a GPU.
The focused `TestPortabilityDevMixedInventory` check starts that dev runtime and
posts recovering, synthetic Linux amd64/XPU, Linux arm64/CUDA and Darwin
arm64/Metal worker check-ins through `/api/workers/checkin`. These metadata-only
workers accept no payloads. Advice includes their exact targets and CPU
alternatives; adding another synthetic target changes the report. This proves
inventory-driven guidance, not native accelerator execution or qualification.

Host/device-pin advisories are described above. Automatic live-coverage warnings
at registration and scarce-host yield remain later phases of
[#324](https://github.com/1kuna/mycelium/issues/324); advice does not implement yield.
