# Agent offload

Submit known heavy work; Mycelium chooses placement. A status read is diagnostic,
not permission to launch locally. This MVP changes no harness configuration and
adds no T3 dependency to Mycelium.

## Project profiles

`myce run --profile gate-full` reads `.mycelium/profiles/gate-full.yaml`, submits
and prints a JSON receipt. Use `--profile path.yaml` for an explicit file.
Arguments after `--` append literally to `run.command`. The new flag belongs on
`run`: `submit job.yaml` keeps its existing job-ID output and wire contract.

A profile is ordinary batch YAML plus `source` and `check` (an argv array). Reuse
`run`, `needs`, `environment`, `inputs`, `outputs`, `project`, and `class` from
[job reference](job-reference.md). Declare exact `targets` with `os`, `arch`, and
`platform` (the existing extension target grammar). Plain command profiles use
an isolated working folder; extension/Docker/sidecar profiles are outside this
MVP. `environment` selects an existing immutable catalog recipe; `{}` explicitly
uses installed tools. The gate profile checks the installed toolchain before
running. It declares two CPU cores, matches Go parallelism to that declaration,
and uses measured automatic memory so it also fits an 8 GiB host allowance.
Profiles opt into measured memory with:

```yaml
needs:
  cpu_cores: 2
  memory: auto
  platform: cpu
```

Without a prior peak or submitter hint, the first run explores using available
host headroom. A successful solo run with an unconstrained host-memory peak
teaches later comparable jobs. The same host uses the accepted peak with 25%
headroom; estimates from another host use the existing larger margins. An OOM
or constrained auto attempt clears comparable memory peaks. `memory.events`
`max > 0` or `high > 0` marks the peak constrained even without OOM. To get a
measured-need run after hitting the ceiling, submit the same comparable work with
`memory: auto` and no submitter hint: the next run explores available headroom,
and a successful unconstrained exploration re-learns the peak. Ceiling pressure
alone does not retry or change the job's outcome. Ceiling-bound explicit-memory
peaks cannot seed auto estimates; explicit declarations otherwise keep their
existing behavior. Optional member caps still apply, so remove a binding member
cap if the job should measure its need within the shared job envelope.

Memory comparability uses the project, profile YAML `name`, and a digest of the
normalized command/configuration, including appended arguments, the check,
environment and non-source inputs. The frozen source archive digest is excluded,
so a new commit of the same suite can reuse its peak. Changing the command or
configuration starts a separate memory class. These observations learn memory
only, without throughput or timing curves. Plain commands outside the profile
submission contract still require hints or explore on each run.

Choose exactly one source:

- `source: {git: HEAD}`: archive the committed tree. A tree hash or commit hash
  also works. The dirty-worktree check applies only to literal `HEAD`, excluding
  Mycelium's own receipts. Other Git references archive their resolved committed
  tree without checking worktree changes. Explicit `allow_dirty: true` acknowledges
  using only committed bytes; dirty/untracked changes and the Git directory are excluded.
- `source: {snapshot: source.tar}`: an explicit uncompressed tar archive.
- `source: {directory: prepared-source}`: explicitly upload regular files in that
  directory. `.git` and `.mycelium/receipts` are excluded. Prepare this directory
  deliberately: other ignored files are included, and no secret scan is implied.

Source paths and `inputs` values resolve from the project root (the parent of
`.mycelium` for conventional profiles, otherwise the current directory). Sources
reject symlinks, hardlinks, special files and escaping names. File modes survive
the source archive. Inputs map clean destination filenames to explicit local
files; they are frozen and applied after source extraction. Outputs map required
relative filenames to ordinary result prefixes. Command exit, then `check` exit,
then existing attempt-specific output verification must all succeed. Retrieve
accepted outputs through `Client.DownloadResult` / Python `download_result`;
these verify their SHA-256 and size.

Profiles upload their frozen source archive and explicit inputs to the queue's
local file store in either checkpoint mode. A NAS fleet keeps its existing
`--checkpoints` / worker `--checkpoint-root` configuration; input downloads and
verified result uploads use the queue API without changing NAS checkpoint paths.

Profile source/input and result blobs expire **7 days after the job becomes
terminal**, configurable on the queue with `--file-retention` (default `168h`).
Download results within that window. Job/key answers and hashes remain, with
`files_expired: true`; a keyed resubmit adopts the same job and never reruns it.
`Client.DownloadResult` / Python `download_result` fail with typed HTTP 410
`result_expired` after expiry. Replaying `myce run --profile` refreshes the job and
reports `result_expired` with exit 65, including for a receipt whose notification
was already delivered. Open downloads finish, but a disconnected caller gets no
extra grace beyond the window. Checkpoints and catalog storage remain protected.
Abandoned profile job-input uploads are reclaimed only when positively marked in
queue SQLite state, unreferenced by every job/keyed answer, checkpoint, catalog/model,
extension, environment and pending reclamation list, and older than the configured
window by both publish time and file mtime, outside the six-hour publication grace.
Job admission clears that marker; normal terminal retention then applies. Unmarked
imports (`myce file put`, models, Go `UploadFile`, older clients) are never orphan-swept,
including when the model catalog is empty or edited. Reusing an unmarked import as
a job input does not mark it; a generic import of marked bytes clears its marker.
Re-uploaded or HEAD-verified bytes receive a six-hour publication grace, covering
later multi-GB uploads, slow NAS, or a queue outage/deploy between the first file
and submit/complete establishing its reference. This does not restore an
expired job's result-recovery window. Reusable worker caches can evict expired
copies, so expiry does not block future profile input downloads.

Before any upload or submission, `.mycelium/receipts/` contains a synced, private
receipt with frozen source/input hashes, argv and submission key. Gitignore this
folder in each consuming project. The default key derives from profile, args and
source/input identities; identical work adopts its previous job. Git keys use
the resolved tree, while directory and extra inputs are hashed in place. Use
`--key KEY` to name an operation explicitly. Reusing it adopts the frozen intent
even if local source changes; changed profile/args fail with a conflict. Use a new key for an
intentional repeat. Keep receipts while recovery matters. Upload copies are
retained only until a job ID is durable, then removed because the verified queue
file store owns the bytes. Retries hash in place and copy only a new intent. Stale unpublished
`.prepare-*` folders are swept under the submission lock.
Do not put credentials in a profile, command, or `run.env`.

Queue submission atomically checks a known qualified target's total admitted
capacity, target, environment and placement policy. Occupancy, owner activity,
cooldown, maintenance, leaving/recovery and allowance acknowledgement do not
turn temporary holds into qualification failures; they queue work. Dispatch
and launch continue to enforce live admission. No host is chosen by the caller,
and there is no local fallback. Replay of an accepted key works after targets
disappear. Exit codes are `64` invalid profile/source, `65` intent conflict or
queue rejection, `69` queue/upload/authority unavailable, `74` observer/notifier
arming failed, `75` another local submission is preparing, and `78` no qualified
target. Failures after intent persistence print the receipt as well as a typed
stderr reason. An uncertain submit retains the key for safe recovery.

`no_qualified_target` names the closest eligible machine and its first failed
fit check, including exact resource needs and allowances (host RAM includes
admission slack). The HTTP error and CLI stderr carry that detail; the printed
and saved profile receipt retain it as `refusal_code` and `refusal_reason`.
Eligible targets rank ahead of placement mismatches, followed by the fewest
failed checks, smallest proportional resource shortfall, then machine/device
name. Retrying an accepted intent clears the saved refusal.

## Installed client commands

On an enrolled machine these commands read the installed worker config, or the
installed queue config when no worker config exists. They retain the joining
certificate pin; consumers do not read config files or handle credentials.
Explicit `--queue` / `--token` and `MYCE_QUEUE` / `MYCE_TOKEN` keep working.
`--config` and `--joining-file` remain available for explicit authority selection.
As with `run`, `MYCE_QUEUE` overrides the invitation address for `status`, `logs`
and `file get`, including when `--joining-file` is explicit.

```sh
myce status
myce wait JOB_ID
myce logs JOB_ID
myce logs JOB_ID --follow
myce result JOB_ID --out ./accepted-results
# For an explicitly supplied file identity (not accepted-result resolution):
myce file get --size-bytes BYTE_COUNT SHA256 ./new-destination
```

`myce result` resolves the succeeded job's accepted output manifest and calls
`DownloadResult` for every artifact. It verifies SHA-256 and byte count before
publishing each local file. Failed/running/cancelled jobs are refused with
`result_not_accepted`. Server-labelled errors, such as HTTP 401 authentication
failures or an unknown job, retain the server's code; HTTP 410 reports
`result_expired`. Only transport or verification failures, including hash/size
mismatches, report `result_download_failed`. All use exit code 1, with the typed
code first on stderr and no timestamp.
Existing destination files are replaced only after that artifact verifies.
Result directories may contain earlier verified artifacts if a later one fails.

```python
from mycelium import Client

client = Client.from_installed()
job = client.wait("JOB_ID")
client.download_result(job["id"], "result.json", "./accepted-results/result.json")
```

The Python library reads installed authority itself and retains the certificate
pin. `Client.from_installed()` requires **OpenSSL (3 recommended)** for
the queue's Ed25519 joining certificate. macOS `/usr/bin/python3` from Xcode
Command Line Tools uses LibreSSL and cannot complete that handshake; use
Homebrew or python.org Python. The client raises `UnsupportedTLSRuntimeError`
on LibreSSL before reading installed authority or attempting a connection;
macOS system LibreSSL is unsupported.
`Client(url=..., token=...)` keeps its existing explicit-credential behavior.
Neither flow adds credentials to job environments; jobs never inherit `MYCE_TOKEN`.

## CPU profile environments

A CPU profile can select locked tools with `environment: cpu-tools` rather than
depending on the worker's installed Python, uv or ffmpeg. For example, author
`cpu-tools/pixi.toml` with:

```toml
[workspace]
name = "cpu-tools"
channels = ["conda-forge"]
platforms = ["linux-64", "linux-aarch64", "osx-arm64", "osx-64"]

[dependencies]
python = ">=3.12,<3.13"
uv = "*"
ffmpeg = "*"
```

Run `pixi lock --manifest-path cpu-tools/pixi.toml`, then give the manifest and
generated lock to the queue operator. To register it, place both files in
`cpu-tools/` beside the existing NAS environment catalog, add this entry, and
restart the queue with its existing `--environment-config` path. Before enabling
the first catalog, read the
[preinstalled-environment compatibility warning](environments.md#preinstalled-environment-compatibility):
worker `--environments` names must also be registered or new submissions using
them will be rejected.

```yaml
environments:
  cpu-tools:
    cpu: {backend: pixi, lock_file: cpu-tools/pixi.lock}
```

The catalog is loaded at queue startup; there is no consumer registration API
or live reload command. Preserve existing entries and submit a new job after
the reload. See [CPU tools with pixi](environments.md#cpu-tools-with-pixi) for
the complete registration steps and workspace limits. Workers prepare the
immutable recipe through the usual cache. They reuse suitable pixi on their
configured PATH or download the pinned, checksum-verified release on first use;
pixi need not be preinstalled. Workers need outbound network access for that
download and the locked dependencies. The downloaded backend is not itself
added to the job's PATH; declare tools such as uv in the locked environment.

In the consuming profile, select the registered name and CPU resources:

```yaml
environment: cpu-tools
needs: {platform: cpu, cpu_cores: 2, memory_gb: 2}
run:
  command: [python, process.py]
```

Retain the profile's usual source, targets, outputs and check. Linux and Darwin
CPU targets, including GPU hosts serving CPU work, use the locked environment's
`bin` first on PATH. Python, uv and ffmpeg then come from that environment.
Declare each intended OS/architecture in the pixi lock and profile targets.
`platform: any` permits only platforms with recipes; CPU and GPU recipes never
implicitly substitute for each other.
CPU jobs must use `needs.platform: cpu`, or set CPU cores with no platform
selector. Neither `platform: any` nor `platform: {preferred: [cpu]}` places work
on CPU. A job whose permitted platforms do not overlap its environment recipes
is rejected at submission with HTTP 400 `job_environment_platform_mismatch`.

## Completion notification

Set `MYCE_NOTIFY` or pass `--notify /path/to/executable`. This is one local
executable, not shell text. It must:

1. Exit nonzero from `--check` when its receiver/configuration is missing.
2. On ordinary invocation, accept JSON on stdin and exit zero only after delivery.
3. Own its delivery timeout/retry policy; keep credentials in private configuration.

Notifiers posting to relays behind bot checks, such as T3's, should send an explicit
`User-Agent` header.
The T3 relay is behind Cloudflare, which rejects Python urllib's default
User-Agent with HTTP 403 `error code: 1010`. For example:
`urllib.request.Request(url, data, headers={"User-Agent": "<project>-notifier/1"})`.

One detached `myce wait --receipt ...` observes each job. A startup handshake and
an inherited OS lock establish observation; rerunning adopts the lock holder.
No PID guessing is used. The receipt reports `observation: armed|not_armed` and
`delivery: armed|not_armed|not_configured`. Both become non-armed when observation
exits; a configured delivery then reads `not_armed`. Missing/invalid hooks never
report delivery armed. `delivery: armed` means configured, not confirmed
delivered; the separate `delivery_status` becomes `delivered` or `failed`. A receipt
file is a past snapshot, not proof of a live watcher; rerun with `--key` to check
the OS lock and adopt or recover observation. Completion records
terminal `status` and immutable `result_reference`; observer errors instead record
`observation_error: timeout|transport_error|interrupted|tls_identity`, leaving job
state alone and attempting a notifier event with `status: observation_failed` and
`observation_error`, even on interruption.
Transient queue outages retry inside the observer. Permanent TLS identity or
certificate verification failures end observation and notify `tls_identity`.
Observation normally has no time limit. Rerun after observer loss or delivery
failure to recover the same job
and retry the same event delivery identity. IDs include a per-receipt nonce, so
recreating a receipt cannot suppress a new event at a deduplicating receiver.
Observation-failure IDs also include a nonce saved on each arm, so a later arm
failing the same way produces a new wake. Terminal IDs remain stable across arms.

A rerun adopts an already-running observer and prints its receipt directory;
it never replaces or stops that owner. For an observer started by a build before
this fix that is stuck queued/armed because its installed certificate pin was
lost, the owning session finds its
own `myce wait --receipt RECEIPT_DIR` process in `ps` by that exact receipt path
and stops it. SIGTERM or Ctrl-C first sends an `observation_failed` wake with
`observation_error: interrupted`; the terminal wake follows after rearm. Both
wakes are expected. Then rerun the original profile with the same `--key`. Once
the old owner releases the OS lock, the rerun re-arms observation using the fixed
installed authority, retains the job ID and retries notification. An exited
observer needs only the same-key rerun. No new submission key is needed.

Completed successful terminal delivery is not sent again. Receipt snapshots
cannot promise survival of a later observer-machine failure; this MVP has no
queue outbox or automatic boot restart.

Notifications contain only `delivery_id`, `job_id`, `key`, terminal `status`
(or `observation_failed` plus `observation_error`), and `result_reference` (output
names, hashes and sizes). Choose non-sensitive output names. The notifier never
receives the fleet token. It and its private receiver configuration stay on the
submitting machine, outside job argv and worker
payloads. Changing a hook while a watcher is alive does not replace that watcher;
finish or recover observation before changing the binding.

## Harness installation (owned by the harness operator)

For a Claude `PreToolUse` hook matching the shell tool, parse the structured tool
input and match known heavy launches to the project's profile. Return the
harness's deny decision with the exact `myce run --profile NAME` replacement, or
route through a supported tool adapter that runs it once and returns the receipt.
Permit small read-only commands and the adapter itself to avoid recursion. Do
not run both the original command and its replacement. Persist/reuse the key in
the harness operation record; only end the agent turn when observation and
delivery are both armed, completion is already verified, or an explicit typed
hold records its next owner.

For Codex, install the equivalent only through a supported pre-tool interception
interface in the host harness. Do not assume every Codex CLI or T3 provider
exposes Claude's `PreToolUse` event or JSON response schema. If it does not, expose
a wrapper/tool invoking this same CLI and record shell bypass as unsupported
coverage. No global configuration is installed by this change.

For T3, create/reuse an owning-thread webhook schedule and store its receiver
binding privately. Configure replay deduplication with `{{body.delivery_id}}`.
A local notifier's `--check` validates that private binding; its normal invocation
posts stdin JSON to it. The receiver wakes the owning task, deduplicates delivery
IDs, re-reads authoritative job state, downloads/verifies outputs and applies the
project's acceptance check. An `observation_failed` wake calls for observer
recovery, not a failed-job verdict. A wake is not a successful result or confirmed
resource release. Retry after ambiguous delivery can send the same ID again;
receivers must deduplicate. The existing positional `job-notify` protocol needs
a wrapper: an exit-zero missing-config path cannot satisfy this contract.

Command matching is cooperative routing, not an OS security boundary. Nested
shells, subprocesses, renamed binaries, SSH, containers and unhooked tools can
bypass it. Report coverage explicitly rather than claiming universal interception.

## Dogfood

The checked-in `gate-full` profile runs the real full gate from a committed
source snapshot with installed Go 1.25.6 or newer, Python 3.11, Git, Bash and uv. The
gate's HOME/XDG isolation hides uv-managed interpreters (#169). Before isolation,
`scripts/gate.sh` resolves installed Python 3.11 with `uv python find --offline
--no-python-downloads 3.11` and exposes only a `python3.11` symlink in a private
scratch bin directory on PATH. This covers both profile and local gate runs
without letting the interpreter's installation directory shadow other tools.
Missing Python fails during gate setup; install it separately. Dependency
preparation happens before the gate goes offline. On Linux, Darwin CGO release
builds explicitly skip and require separate native macOS verification; Linux
builds, vet and the full race/dev suite still run. This is laptop proof, not
native fleet qualification.

For local acceptance, build `myce`, start `myce dev --gpu cpu --workers 1
--file-store DEV_FILES --db DEV_DB` on loopback, then submit the profile with the
dev queue/token in environment and a local test notifier. Commit the intended
source first. Wait once using the armed observer, inspect its terminal receipt,
and verify `gate-result.txt` through the accepted result API. Do not launch an
additional full suite while two are already active.
Add `--checkpoints DEV_CHECKPOINTS` to exercise NAS checkpoint semantics with a
local test folder while keeping the same stored source/input/result transport.
