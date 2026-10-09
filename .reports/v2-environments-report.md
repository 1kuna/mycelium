# Mycelium v2 environments and machine setup

Branch: `v2-environments`, created from local `main`.

## Implemented

- NAS YAML environment catalogs with per-platform uv Python or plain-binary
  recipes; project requirements or uv/pyproject snapshots with Python version
  and optional platform restrictions. Content identity ignores mount paths.
- Local content-addressed cache with OS build locks, shared execution leases,
  LRU eviction, active-size monitoring, recoverable Trash, durable file sync,
  atomic ready publication, and interrupted-builder process-group cleanup.
- Idle build plans, real-work/network cancellation, ready-hash check-ins,
  measured build-time placement estimates, unified readiness ordering and
  strict environment/platform eligibility. Higher-priority cold setup cannot
  starve behind ready lower-priority commands.
- Real command PATH/virtualenv activation; uv launcher relocation validated
  against a real offline uv build in temporary storage. Jobs/builders remove
  the fleet token. Binary content is checked before local copying.
- Full build logs upload in bounded deduplicated chunks to SQLite and are
  available through normal job logs. Local report and upload cursors persist.
- Install worker/queue plans, Linux systemd user units and macOS launchd agents,
  private service credentials, preserved executable PATH, dry runs, recoverable
  uninstall and doctor checks. Detection covers dedicated CUDA, GB10 unified
  memory, integrated/discrete Intel XPU, and Apple Silicon Metal.
- Updated job reference, integration and operations docs plus environments guide
  and the Mac mini / 4090 quick-add instructions.

## Validation

- `go build ./...` and `go vet ./...` passed.
- `go test ./... -race` passed, including full-system queue/network/NAS/model
  faults and the new actual-builder interrupted/restarted-worker scenario.
- Full Python suite passed: 9 tests, zero skips, Python 3.12 with OpenAI and
  PyYAML installed only in a temporary virtual environment.
- Linux amd64 and arm64 binary cross-builds passed; outputs kept under `/tmp`.
- Focused tests cover content hashing, mount-independent reuse, concurrent build
  serialization, partial cleanup, in-use protection/LRU, size refusal,
  both/either/cold affinity, cold production starvation, consistent build versus
  execution placement, measured build selection, platform filtering/wait reasons,
  complete/deduplicated failure logs, real binary execution and real uv Python.
- Install CLI tests exercise fake detection with `--dry-run`; temp-only install
  tests use injected service runners. Doctor tests cover healthy and failed
  prerequisites. No test installs an actual service.

The first Python invocation used macOS's Python 3.9 (below the client's declared
3.10 minimum); a socket timeout surfaced there. Verification used supported
Python 3.12 with all optional dependencies and passed the complete suite.
One final validation command initially named the wrong temporary interpreter
path; it was corrected and the complete suite rerun.

## Boundaries and operational limits

No SSH, fleet changes, real service installation/removal, LibreOffice or push.
All package installations and real uv build artifacts were in temporary folders.
The approved design and AGENTS guidance were read but not rewritten.

A Linux user unit needs administrator-enabled lingering for boot without login;
a macOS agent starts at user login. Setup prints this and does not elevate.
Unknown discrete XPU memory needs an explicit operator budget. Doctor proves
prerequisites, not a GPU workload. Native Windows services are unsupported.

The environment cache limit bounds active software and monitors partial builds;
same-filesystem Trash remains recoverable and does not reclaim physical space.
Trusted plain-command builders are not sandboxed. NAS mounts, uv and custom build
tools must already exist; setup does not install system dependencies. uv locks
snapshot dependency metadata, not complete project workspaces. Local source
projects remain in the job workdir. Build scripts need reproducible inputs.

## DIGEST
Built software environments and machine setup on v2-environments.
Added content hashes, build locks, safe cache eviction, restart cleanup and logs.
Placement uses software/model readiness, supported platforms and build times.
Added install, dry run, recoverable uninstall and doctor.
Added laptop tests and an interrupted environment-build restart fault.
Go build, vet, race tests, all Python tests and both Linux builds passed.
Updated environment, job, integration and operations guides.
No fleet machines, actual services, LibreOffice or pushes were touched.
User boot services still need login or Linux lingering; hardware is untested.
