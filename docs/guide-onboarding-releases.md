# Publish and approve an onboarding release

The queue administrator provisions a trusted publisher HTTPS directory and an
Ed25519 public key independently of the release manifest. The signing key stays
with the publisher in a mode-0600 PKCS8 PEM file. Never distribute it with a release.
No release tooling existed before this flow; the script uses Go and standard
Ed25519/SHA-256 rather than adding a distribution service.

On an Apple Silicon macOS build host with Go and Xcode command-line tools:

```sh
scripts/release.sh VERSION OUTPUT_DIR PRIVATE_SIGNING_KEY
```

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
protocols or a Mac artifact lacking native sensing. A pinned authenticated
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
before install or ticket redemption.

Each line binds one approved release. Repeat it after an interrupted download or
installation; print a line for a newer release to upgrade. An already-installed
hypha reuses its saved authority and does not spend a new ticket. A new host
cannot reuse a spent ticket. If redemption succeeds but the response is lost
before private authority is saved, issue a fresh ticket; replay stays refused.

See [joining](guide-joining-v3.md) for the user flow and successful enrollment states.
