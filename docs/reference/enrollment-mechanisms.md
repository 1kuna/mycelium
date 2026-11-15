# Enrollment mechanisms

Part of the [approved design](../design-proposal.md), moved verbatim apart
from relative links. Product contracts and maintainer rulings are unchanged.

### One-command enrollment
<a id="one-command-enrollment-b0b1b6-108-2026-10-21"></a>

A hypha reports all its components. The existing `myce invite` and `myce
install worker --enroll` paths provide bootstrap and enrollment. By default
invite prints exactly one paste-ready line to stdout; `--out FILE` also saves
it privately. Foreground `join` remains the worker runtime. NAS mode is
required. The [onboarding preflight](../design-proposal.md#onboarding-preflight) owns hardware
preflight, NAS/tools and saved service/policy fields; enrollment passes its
ordinary installer options through unchanged. Legacy conversion is retired
(MY-42); qualification remains separate.

Releases contain linux/amd64, linux/arm64 and native darwin/arm64+cgo binaries.
The publisher signs an exact JSON manifest with Ed25519. The queue verifies it
against an independently provisioned public key, never a key from the manifest,
then supplies approved public release metadata to pinned authenticated
inviters. Explicit local manifest/key/origin overrides perform the same
verification. Release versions are ordered semver values (for example
`v3.1.0`). Queue approval is persisted in SQLite and must strictly increase
unless `--allow-downgrade` explicitly re-approves or downgrades it. Repeating
an identical verified approval is idempotent. `myce release approve` advances
the running queue's default after a signed deployment without changing catalogs
or service settings. The queue persists the publisher key provisioned only on
its host; API approval requires an exact key match and verifies against that
stored key. Startup verifies then skips/logs older or equal flag manifests
unless explicitly forced, preserving the newer default across restart. Missing
stored trust refuses API approval until local provisioning; key rotation
remains local. Semver build metadata has equal precedence. The verified
manifest binds version, protocols, OS/arch, size, SHA-256 and native Mac
sensing. A small POSIX command bootstraps clean hosts, which have no myce, Go
or Git. Its embedded verified hash matrix selects the artifact and HTTPS
origin; curl resumes a private partial download, refuses redirects, and a
platform SHA-256 tool verifies bytes before execution. Publisher HTTPS plus the
signed manifest are separate from the LAN queue's exact certificate-DER pin.
Never confuse that pin with curl's SPKI pinnedpubkey.

Authenticated inviters mint 15-minute tickets (maximum one hour) over pinned TLS.
The existing SQLite stores only ticket digests, expiry, queue pin and consumed
state. Used and expired rows are pruned. One atomic conditional update redeems
exactly once. Tickets authorize only redemption, never the jobs/status API; expiry uses the injected queue Clock. The
fleet credential travels only in the pinned TLS response and mode-0600 installed
joining file, never argv, URLs, history or diagnostics. If a redemption response
is lost before durable saving, mint a new ticket; do not make replay reusable.
Existing private file invitations remain supported with explicit
`--private-file --out FILE` for compatibility. Installed
reruns use their saved joining authority and do not redeem the spent ticket.
The limited ticket enters the installer on stdin, never in its argv or environment.
Bootstrap authority is retained at mode 0600 only until the installed joining file
is durable, then deleted, including on interrupted-install recovery.

Enrollment completion requires the installed spool's exact session, the current
host-launch and keyed-attempt protocols, every expected device identity, CPU/RAM
facts and live memory, policy acknowledgement, fresh queue observations, a small
recorded runner smoke with no fleet token, and stable service-manager health.
A machine's receipt binds its service PID, exact release/session, expected
inventory, CPU count and total RAM; queue-owned CPU/RAM allowances must be
acknowledged.
Saved per-device settings or explicit selector lists independently establish expected
devices. A failed probe retains `device_probe_unavailable`; a successful probe
with a configured but absent device reports `device_selector_unmatched` and
cannot be ready. launchd health reads the service block's direct keys, ignoring
nested resource states. Healthy service identity must remain stable for 30 seconds.
Unchanged receipts are not rewritten; receipt-write failures are logged without
terminating the worker, and retried after the next successful check-in. Registration
alone is incomplete. A healthy daily-use hypha may finish as `polite_waiting`;
an operator-maintained host finishes as `disabled` or `drained`, never `ready`.
Unknown devices, queue outage/426, recovery or a start-loop cannot complete ready.
The summary includes the hypha name, OS/arch, devices, CPU/RAM allowances, storage,
participation and enforcement. This smoke does not qualify project extensions.

Each printed line freezes the approved release and checksums for
reproducibility. Reuse that line for interrupted installation and same-version
health checks; use a line for a newer approved release to upgrade. Installed
upgrades use saved authority without redeeming or requiring another ticket.
Same-version reruns verify health without changing files or restarting
services, and explicitly explain that installer flags are not reapplied. Older
releases are refused unless the operator passes `--allow-downgrade` explicitly;
comparison uses the installed binary's version, not an invitation's age or a
stale worker receipt. Equal versions (including `+build` metadata) require
identical bytes, otherwise `--allow-downgrade` is required. Replacing an
installed non-semver version such as `development` also requires that explicit
flag; the published target remains semver. A changed release refuses busy local
hosts before draining through existing queue maintenance. A race with new
custody is bounded: drain-wait failure conditionally restores its own exact
transition. Drain-wait and install/readiness have separate budgets. The
existing installer transaction and spool remain the install path. Existing
disabled/drained policy is preserved; an installer never resumes an
operator-disabled host. A private resume receipt fences a self-made drain by
host, name and policy timestamp. Its durable intent precedes the drain POST; an
intent plus an observed drain on rerun is promoted to the exact timestamp
receipt. Install/readiness failure restores the previous installation through
the existing transaction and verifies its enrolled readiness before resuming
only the self-made drain, with a separate cleanup budget. Errors state whether
the previous release resumed or why recovery failed. A crash loses the
in-memory installer snapshot; if already-installed bytes never become ready on
rerun, report rollback unavailable and preserve maintenance. Operator-created
maintenance remains respected. No fleet operation, reboot, driver install or
privileged service is implied.

### Onboarding preflight
<a id="one-command-onboarding-preflight-b2b4-108109"></a>

Onboarding reuses the machine inventory, installer overlay and service
transaction. It enumerates all components, preserves explicit selector lists
and per-device fixed budgets, and refuses typed driver, inventory or memory
failures instead of saving CPU fallback. Enumerate PCI display and
processing-accelerator classes, classify each vendor and integrated/discrete
topology from sysfs facts (local device memory or PCIe link width), then
correlate every runtime device with its PCI address. A CUDA device can expose a
nonzero PCI transport width while sharing host RAM. When its memory total is
unavailable, the selected PCI device's CUDA driver integrated attribute can
establish unified topology; an unavailable total alone cannot. Product names
never determine memory mode. Missing facts stay unknown; ambiguous multi-device
correlation refuses instead of guessing. NVIDIA devices and discrete Intel
endpoints require their runtime independently of any other working component. A
failed `nvidia-smi` is informational when PCI inventory confirms no NVIDIA
display device. Working integrated Intel graphics enrolls as `xpu` with unified
memory; a missing integrated runtime permits CPU, while a broken runtime still
refuses. Other display controllers are informational and do not block CPU
enrollment. Explicit CPU selection remains supported; re-detection is an
explicit install operation so upgrades do not silently broaden an owner's
component allowance. Inventory errors retain the `device_probe_unavailable`
runtime rejection while also carrying the more specific typed onboarding
diagnosis.

Enrollment completes hardware, storage, policy, state-root and service-session
preflights before acquiring a drain. The candidate binary and a private staged
config run the existing service-environment hardware probe before the live
installation changes. Post-publication validation and exact-session readiness
still verify the installed worker. Environment backend acquisition stays lazy.

NAS mode is the only supported onboarding storage mode. Checkpoint, model and
environment roots must use an existing reachable network filesystem. Use the
saved NAS root, an explicit `--nas PATH`, or the documented default (`/mnt/nas`
on Linux, `/Volumes/NAS` on macOS). Never adopt another discovered share;
missing selection refuses `nas_not_configured` with candidate mount paths and a
`--nas` hint before printing the plan. Preserve saved roots. Never mount a
share, modify fstab or substitute local directories. The legacy deviceless test
flag cannot bypass this requirement for CPU/GPU workers. The file store remains
the job input/output transport, not a second storage mode. Resolve uv/pixi on
the service PATH first and preserve that order on upgrades. Only a native
environment build that needs a backend may acquire its pinned vendor binary in
the ordinary user's installation; invoke it by absolute path. Include uvx when
shipped with uv. An offline machine with no recipes needs no backend download.
Do not execute remote installer scripts or print credentials.

NAS validation uses the existing bounded checkpoint I/O pattern with one 250 ms
network-filesystem budget per metadata/create/write/sync/close/rename/ cleanup
operation (following the worker CIFS allowance), with a 5 s cap on the whole
check, including mount inventory. Fresh/default NAS selection for both real and
dry-run plans uses that same bounded, cancellable mount helper. Slow healthy
sequences may exceed 250 ms in total. Timeout returns `nas_unresponsive`;
caller cancellation remains `context.Canceled`. Cancelled/timed-out results are
discarded before retrying the same key. An uninterruptible syscall can finish
afterward, with no second sequence launched while it is pending. Known NFS/SMB
mount types, autofs-managed mounts and explicitly network FUSE subtypes
(`fuse.sshfs`, `fuse.rclone`, `fuse.smbnetfs`) are eligible. Generic or local
FUSE is not. Autofs is triggered only inside bounded validation.

Save daily-use, laptop and desktop policy through the same flags/overlay/Args
as budgets. False and zero are values, including on rerun. Linux onboarding
enables and verifies user lingering without sudo, or reports the administrator
prerequisite. macOS installs a LaunchAgent in the logged-in gui domain and
validates its environment there. The injected Clock owns the service-probe
deadline; a real ticker paces local output polling without advancing scheduling
time. Bounded service stderr retains only allowlisted typed reasons. It starts at GUI login, not before login, and
does not promise survival after logout. CPU/RAM admission on macOS is not an OS
ceiling; required unsupported memory ceilings refuse explicitly. Hardware,
service-session and native participation qualification remain separate gates.

The enrollment command calls `PreflightHardware` and `ApplyHardwarePreflight`
while planning, then `OverlayHardwareSettings` after explicit memory overrides.
Call `PrepareOnboardingStorage` before publishing a service (discovery only for
a fresh default mount), `BootstrapEnvironmentTools`, and
`EnsureOnboardingServiceSession`. The existing `Install` transaction invokes
`ValidateWorkerServiceEnvironment`; `detect --config` returns the whole
selected hardware plan without reading credentials. These functions accept fake
probes; service-probe deadlines use the injected Clock. Ordinary install
upgrades keep saved component choices; `install worker --redetect` explicitly
refreshes them.

Tool versions are uv **0.8.22** and pixi **0.81.0**, with vendor GitHub release
archive SHA-256 pins for Linux/macOS amd64/arm64. Both use versioned user-local
directories; no existing system/user executable is replaced. The saved PATH
prepends those directories and remains stable on rerun. Unsupported releases,
downloads, checksums, archive shapes and executable versions refuse with typed
reasons. These pins are onboarding prerequisites, not extension qualification.

