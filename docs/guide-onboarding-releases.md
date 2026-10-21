# Publish and approve an onboarding release

The queue administrator provisions a trusted publisher HTTPS directory and an
Ed25519 public key independently of the release manifest. The signing key stays
with the publisher in a mode-0600 PKCS8 PEM file. Never distribute it with a release.
No release tooling existed before this flow; the script uses Go and standard
Ed25519/SHA-256 rather than adding a distribution service.

On an Apple Silicon macOS build host with Go and Xcode command-line tools:

```sh
scripts/release.sh v3.1.0 OUTPUT_DIR PRIVATE_SIGNING_KEY
```

Versions must be semver values, monotonically increasing for normal upgrades.
Build metadata (`+build`) does not change version precedence.
The shared `ReleaseTargets` data drives validation, manifest creation and
`myce release build-targets`, which the build script consumes.
This builds `myce-linux-amd64`, `myce-linux-arm64` (CGO disabled), and
`myce-darwin-arm64` (CGO enabled with the Apple SDK). The native Mac binary must
report real owner-sensing support. `myce release manifest` checks executable
headers, the Mac release version/protocol/sensing, and writes a signed
`release.json` plus `SHA256SUMS`. Publish the artifacts unchanged at an HTTPS
directory that serves them directly without redirects. Mac distribution signing,
Gatekeeper/notarization and actual launchd login/network behavior need their own
native release acceptance; never disable those controls to make onboarding pass.

Add these options to the existing queue command with its usual private credential,
database, checkpoint and joining-listener configuration:

```sh
myce queue --release-manifest RELEASE_JSON --release-key PUBLIC_PEM \
  --release-url HTTPS_PUBLISHER_DIRECTORY
```

The queue refuses invalid signatures, incomplete target matrices, incompatible
protocols or a Mac artifact lacking native sensing. Approval is persisted in the
SQLite queue and requires a version strictly newer than its current approval.
To intentionally re-approve the same release (including after restarting with the
same manifest), or approve an older one, append `--allow-downgrade` to the queue
command. Signature and artifact validation still apply. A pinned authenticated
`myce invite` fetches the approved public metadata; a queue without release
configuration returns `release_unavailable` and cannot mint a bootstrap command.
The publisher certificate trust is ordinary HTTPS. The LAN queue's full
certificate-DER pin is checked separately by the existing joining client;
it is not a curl SPKI `--pinnedpubkey` value.

An inviter can explicitly override the configured distribution with
`--release-manifest FILE --release-key PUBLIC_PEM --release-url HTTPS_DIRECTORY`.
That verifies the signature locally with the independently provisioned key.
The output embeds approved hashes instead of fetching and running a remote script.
The bootstrap resumes private hash-addressed `.partial` files, rejects redirect
responses and checks exact size/SHA-256 before running even `myce version`.
The version command then checks version, OS/arch, both protocols and Mac sensing
before install or ticket redemption. A corrupt full-size partial, a redirect body
or a checksum mismatch is discarded with a typed reason so a rerun can recover.
The ticket is supplied to the installer through stdin and is absent from its
argv/environment. The temporary private invitation is deleted once the installed
joining file is durable.

Each line binds one approved release. Repeat it after an interrupted download or
installation; print a line for a newer release to upgrade. An already-installed
hypha reuses its saved authority and does not spend a new ticket. A new host
cannot reuse a spent ticket. If redemption succeeds but the response is lost
before private authority is saved, issue a fresh ticket; replay stays refused.

See [joining](guide-joining-v3.md) for the user flow and successful enrollment states.

An older invitation line cannot silently downgrade an installed binary. The target
compares the signed release version with the installed executable's version and
refuses an older version unless `--allow-downgrade` is explicitly appended.
An equal version with different bytes also requires that flag; `+build` metadata
counts as equal. Identical-byte same-version reruns remain idempotent.

Existing fleet installations may report `development` or another non-semver
version. To replace one, use a currently approved semver release's printed line
with `--allow-downgrade` appended explicitly. This overrides the unordered installed
version, while retaining checksum, protocol, saved-authority, custody and readiness
checks. It does not convert an incompatible legacy installation; use the explicit
conversion path where required.
Same-version reruns explain that installer flags are not reapplied; use the normal
`myce install worker --config CONFIG` path to change those settings.

Upgrade preflight refuses busy hosts before changing policy. If work races with
the drain, a wait timeout restores the exact self-made drain through CAS, with a
separate cleanup budget. A mode-0600 intent receipt is written before the drain
POST. A rerun promotes an intent plus an observed drain into an exact timestamp
receipt, covering a crash before recording the POST response. Once recorded, the
receipt cannot authorize resuming an operator's later disable or drain.

Installer failure restores the existing install transaction and resumes only the
self-made drain. If newly installed bytes never become ready, that same transaction
restores the previous binary/config/service and verifies its enrolled readiness
before resuming. The error states the
recovery outcome. Failed rollback or conditional resume reports why the host could
not be resumed and asks for a rerun. A process crash loses the in-memory install
snapshot: if already-installed bytes fail readiness on rerun, the command explicitly
reports that rollback is unavailable and preserves maintenance for recovery.
Drain waiting, installation and readiness have separate timeout budgets.
