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

For native LaunchAgent distribution, set `MYCE_DEVELOPER_ID` to the provisioned
Developer ID Application identity. The build embeds Mycelium's stable identifier,
Local Network usage description and Bonjour service declaration in the single
Darwin executable. The script applies and verifies a timestamped hardened-runtime
signature before manifest generation, so Ed25519 authenticates the codesigned
bytes. Linux artifacts use the same CGO-disabled builds and are not codesigned.
Tests replace the signing tool and never access a real keychain.

When the Ed25519 key lives on a different publisher host, use
`sh scripts/build-release.sh VERSION OUTPUT_DIR` on the Mac, then generate and
sign the manifest on the key's host without modifying the artifacts. Never copy
the private key to the build host just to run the combined script.

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
protocols or a Mac artifact lacking native sensing. It persists both approval and
the locally provisioned publisher public key in SQLite. Only queue-host startup
can provision or rotate that key; fleet-token API callers cannot establish trust.
On restart, a verified startup manifest older than or equal to the stored approval
is skipped and logged, preserving a default advanced through `release approve`.
The local publisher key is still provisioned when the flag manifest is skipped,
so existing queues can acquire the stored trust without reverting their default.
Invalid startup manifests still fail verification, even when old enough to skip.
To deliberately apply an older or changed equal-version approval, append
`--allow-downgrade` to the queue command. Remove that override after the intended
change: leaving it with stale service flags explicitly reapplies their old release
on future restarts. Signature and manifest validation still apply. A pinned authenticated
`myce invite` fetches the approved public metadata; a queue without release
configuration returns `release_unavailable` and cannot mint a bootstrap command.
The publisher certificate trust is ordinary HTTPS. The LAN queue's full
certificate-DER pin is checked separately by the existing joining client;
it is not a curl SPKI `--pinnedpubkey` value.

For a private LAN HTTPS directory, the administrator may additionally pass
`--release-ca PUBLIC_CERTIFICATE_PEM` to the queue command. This explicitly
provisions the publisher's public CA certificate (or self-signed server
certificate); never pass its private key. The queue persists this certificate
with the approved release and supplies it to authenticated inviters. The inviter
must run a version supporting `--release-ca`. Its pinned queue connection
authenticates this trust material, which the bootstrap supplies to curl through
a private temporary `--cacert` file. TLS certificate validity and the publisher
URL's hostname/IP SAN are still verified; redirects remain refused. The temporary
certificate is removed before the executable runs and on download failure.
No system trust-store changes or TLS-verification bypass is used. Without this
option, ordinary HTTPS trust is unchanged. Existing joining certificates have no
hostname SAN and cannot serve as ordinary HTTPS publisher certificates; provision
a separate publisher certificate with the correct SAN. A static HTTPS directory
can serve the signed release without changing the queue's joining identity.

## Advance the default after a signed update

Installing signed runtime bytes on the queue or workers does **not** change the
queue's persisted default onboarding release. After publishing the artifacts and
installing/verifying the new queue runtime, advance that default as a deployment
step, before issuing fresh invites or using one-command onboarding:

```sh
myce release approve --config QUEUE_CONFIG \
  --release-manifest RELEASE_JSON --release-key PUBLIC_PEM \
  --release-url HTTPS_PUBLISHER_DIRECTORY
```

Run as the ordinary queue administrator. `--config` reads the existing saved
queue authority; no credential extraction or service reconfiguration is needed.
Installed hypha authority is also supported when `--config` is omitted. The queue
must already run a version supporting this command's approval endpoint.
Supply the independently provisioned publisher public key, never a key obtained
from the manifest. It must exactly match the key already persisted by the queue
host. The command verifies the signed manifest locally; the authenticated queue
verifies it again against its own stored key using the existing signature,
protocol, target-matrix and native-sensing checks before committing to SQLite.
The API cannot provision or rotate publisher keys, even with `--allow-downgrade`.
If the queue has no stored key (including an existing approval created by an older
version), the command refuses with `release_publisher_key_unavailable`. On the
queue host, restart the existing queue command with `--release-manifest`,
`--release-key` and `--release-url` as above to provision it. Retain the usual
catalog, database and service flags. Key rotation uses that same local procedure
with a valid manifest signed by the replacement key.
This approves the manifest; it does not download or install artifacts.

For a private publisher, add `--release-ca PUBLIC_CERTIFICATE_PEM` on each approval;
omitting it selects ordinary HTTPS trust. The command updates only the default
release, publisher URL and public CA. It preserves the loaded environment/model
catalogs, saved service settings, queue database path and joining identity, and
does not restart the queue or reinstall workers. Success prints the approved
version and publisher URL. Identical reruns are safe after a lost response;
older versions or changed metadata at equal version precedence require explicit
`--allow-downgrade`, which never bypasses signature or manifest validation.
Invalid approval input returns a 4xx code; queue storage/commit failures return
HTTP 500 `release_approval_failed` and leave the previous default and key intact.

After success, `GET /api/invites/release` over the authenticated queue connection
returns the new default. Run ordinary `myce invite` without release overrides for
fresh machines; one-command onboarding uses the same default. Previously printed
invitation lines remain bound to their original release.

## Invite and upgrade machines

An inviter can explicitly override the configured distribution with
`--release-manifest FILE --release-key PUBLIC_PEM --release-url HTTPS_DIRECTORY`.
For that local override, `--release-ca PUBLIC_CERTIFICATE_PEM` is also supported.
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

Hardware, NAS, policy, state-root, manager-session and candidate service-environment
preflights finish before onboarding drains or stops the installed worker.
Upgrade preflight also refuses busy hosts before changing policy. If work races with
the drain, a wait timeout restores the exact self-made drain through CAS, with a
separate cleanup budget. A mode-0600 intent receipt is written before the drain
POST, carrying a unique owner ID that the queue stores atomically with the drain.
A rerun requires that queue-side owner to match before recording a resume receipt.
Resume checks both owner and timestamp in the queue transaction. Independent
maintenance edits revoke ownership, even at the same clock timestamp. Old receipts
or queues without ownership proof cannot authorize resume: maintenance stays in
place and the error names the recorded drain owner and prints the exact
`myce host resume HOST` command for the operator. A drained same-release rerun
prints that recovery information even if its local intent receipt was lost.

Installer failure restores the existing install transaction and resumes only the
self-made drain. If newly installed bytes never become ready, that same transaction
restores the previous binary/config/service and verifies its enrolled readiness
before resuming. The error states the
recovery outcome. Failed rollback or conditional resume reports why the host could
not be resumed and asks for a rerun. A process crash loses the in-memory install
snapshot: if already-installed bytes fail readiness on rerun, the command explicitly
reports that rollback is unavailable and preserves maintenance for recovery.
Drain waiting, installation and readiness have separate timeout budgets.
