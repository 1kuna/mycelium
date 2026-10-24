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
and uses a provisional 8 GiB host-memory declaration. Automatic memory remains
#146 work; this value is not a measured whole-attempt peak.

Choose exactly one source:

- `source: {git: HEAD}`: archive the committed tree. A tree hash or commit hash
  also works. A dirty HEAD is refused. Explicit `allow_dirty: true` acknowledges
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

## Completion notification

Set `MYCE_NOTIFY` or pass `--notify /path/to/executable`. This is one local
executable, not shell text. It must:

1. Exit nonzero from `--check` when its receiver/configuration is missing.
2. On ordinary invocation, accept JSON on stdin and exit zero only after delivery.
3. Own its delivery timeout/retry policy; keep credentials in private configuration.

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
`observation_error: timeout|transport_error|interrupted`, leaving job state alone
and attempting a notifier event with `status: observation_failed` and
`observation_error`, even on interruption.
Transient queue outages retry inside the observer. Observation normally has no
time limit. Rerun after observer loss or delivery failure to recover the same job
and retry the same event delivery identity. IDs include a per-receipt nonce, so
recreating a receipt cannot suppress a new event at a deduplicating receiver.
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
source snapshot with installed Go 1.25.6 or newer, Python 3, Git, Bash and uv. Dependency
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
