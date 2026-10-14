# Moving files and models without a NAS

Research for [#24][ticket], within [the wayfinder map #17][map]. **2026-10-14.**
This is a research recommendation for the next specification, not an approved
product change. The approved specification remains `docs/design-proposal.md`.
All sources below are primary sources, read on 2026-10-14; undated living
documentation is identified by this access date rather than an invented release
date. Current Mycelium documentation is pinned to main commit `b989acc`.
[M1][M2]

## Question

How should one mycelium move job inputs, outputs, checkpoints and model files
between hyphae without requiring a NAS, while remaining simple and resumable
on a home network? Compare content-addressed caches, direct transfers, one
designated storage hypha, rsync, Hugging Face caches, IPFS and BitTorrent.
[ticket]

## Short answer

**Recommendation:** start with one designated always-on storage hypha, defaulting
to the hypha running the queue, and immutable files identified by SHA-256 plus
size. Keep bounded caches on execution hyphae. Use standard HTTP range downloads
and an embedded tus implementation for resumable uploads, rather than a new
transfer protocol. A NAS can supply the same durable files when present. This
combines existing Mycelium cache/publication concepts with standard transfer
tools; it does not require peer discovery, swarms or replicated metadata.
[M2][H1][H2][T1][T2]

**Availability limit:** one store is one authoritative copy. If it is asleep or
unreachable, work needing missing files or checkpoint publication must wait; if
its disk is lost, a cache is not a promised backup. This proposal does not fulfill
the eventual aspiration of surviving loss of any hypha. Explicit second-copy
protection and standby coordination need a later, separately specified step.
The vision already stages standby and decentralized coordination for later.
[vision][map][G]

## Findings

### 1. Today's implementation has useful pieces, but not general file movement

The approved design uses one binary and one local SQLite queue, without peer
coordination or replicated state. Model masters live on NAS; execution hyphae
maintain bounded local caches, preserve interrupted copies, rehash prefixes,
verify full size and SHA-256, and publish by sync/rename. Pre-copy requests,
cache-aware placement and copy-speed reporting already exist. These are a
better starting point than restoring v1's complexity. [M2]

Submitting a batch sends its description, not its source or inputs. Today's
checkpoints require a shared NAS root, separate attempt directories and complete
monotonic generations; temporary saves never count. Generic model HTTP uploads
are request-scoped queue spools: 8 GiB per body, 16 GiB shared by default, removed
on completion/error and cleaned on startup. They are not a persistent artifact
store or a general resumable upload API. [M1][M3][M4]

The vision's storage decision is explicitly “no NAS required.” It also requires
cached, output-checked automatic conversions, no default lossy conversion, and
polite participation by daily-use hyphae. The glossary defines the spore as the
first hypha, not necessarily the one that remains in charge. Storage therefore
should be an assigned resource role, not a permanent privilege of the spore.
[vision][G]

### 2. Content addressing is an identity rule, not a topology or backup

Hugging Face's cache separates mutable revision references, stored file blobs
and revision snapshots. Snapshots reuse blobs across revisions. Its blob names
are not uniformly SHA-256: Git-backed files and Git-LFS files use different
identities. Mycelium already requires explicit SHA-256 and size for real models.
**Implication:** use an independent whole-file SHA-256 identity and a manifest
for a multi-file model or checkpoint; keep the upstream repository/revision as
provenance, not as the only integrity check. [F1][F4][M2]

**Proposal:** cache complete identical files once per hypha, regardless of which
job requests them. Treat a converted model as a distinct artifact, recording its
source identity, conversion recipe and output-check evidence. A matching hash
proves byte identity to the expected digest; it does not make an extension
qualified. In glossary terms, qualified means checked correct output on a
particular hypha. The extension library can distribute extension recipes and
qualification evidence; it should not become the home for private job data.
[G][vision][M2]

Whole-file deduplication cannot reuse only the unchanged portions of a changed
file. IPFS and BitTorrent provide block/piece-based alternatives, while rsync
uses a delta-transfer algorithm. **Recommendation:** defer a custom chunk index
until measured transfer waste demonstrates a need; whole-file identities plus
resumable prefixes meet the initial requirement with less metadata. [I1][B1][R1]

### 3. Use existing resume mechanisms, with explicit publication rules

HTTP defines byte ranges and partial responses; a server may ignore a Range
request. Resume clients must validate the response range and object identity,
and restart safely if the server returns a complete response. Go's
`net/http.ServeContent` handles ranges and conditional requests, including
`If-Range`, with an application-supplied ETag. This covers download serving
without a new wire protocol. [H1][H2]

For uploads, tus 1.0 defines obtaining the current offset with HEAD and resuming
with PATCH; mismatched offsets are rejected. Upload creation, checksums and
expiration are protocol extensions. The official tusd implementation can be
embedded in a Go program, using local file storage and file locking. These are
standard components; Mycelium would still need authorization, disk admission,
trusted expected digests and durable publication around them. Neither a tus
completion event nor an HTTP success alone specifies Mycelium's checkpoint
durability contract. [T1][T2][M3]

Rsync supports preserved partials, a partial directory, delta transfers and
bandwidth limits. Its `--checksum` flag changes the decision about whether an
existing file needs transfer; rsync separately verifies reconstructed transfers.
`--append` assumes the existing prefix matches, while `--append-verify` includes
existing content in verification. **Implication:** an operational copy can use
rsync plus a separate expected SHA-256 check, but neither a successful command
nor `--checksum` establishes application generation ordering or durable manifest
publication. Avoid modifying a live checkpoint in place. [R1][M3]

### 4. Distributed transfer has real benefits and additional obligations

IPFS identifies content with CIDs and retrieves blocks from providers. Pinning
protects local content from garbage collection; availability still requires
someone to retain and serve it. Public IPFS exposes provider metadata and does
not provide content encryption by default. **Implication:** installing IPFS is
not automatic private storage or backup. A home-only design would need explicitly
private routing/access configuration and retention policy. [I1][I2][I3]

BitTorrent's original protocol defines piece hashes, peer exchanges and tracker
coordination; its private-torrent convention disables DHT and peer exchange in
favor of the designated tracker. “Private torrent” is not authenticated,
encrypted private artifact storage. A usable seeding copy is still required.
**Implication:** swarming could reduce one store's outgoing bottleneck for large,
popular models, but adds a second availability/discovery system before Mycelium
has evidence that the bottleneck matters. [B1][B2][map]

Hugging Face caches are useful for approved model acquisition and immutable
revision reuse, not arbitrary local outputs or checkpoints. Xet's documentation
describes chunk-based deduplication, but current Hub environment documentation
says `HF_XET_CHUNK_CACHE_SIZE_BYTES` has no effect and local chunk caching is no
longer used for downloads. Do not base the design on older local-Xet-cache
advice, or require each hypha to contact an internet service when loading a model.
[F1][F2][F3][M2]

## Options and trade-offs for Mycelium

The judgments below are recommendations inferred from the cited mechanisms,
not benchmark results.

| Option | Benefit | Cost or limitation | Recommended place |
|---|---|---|---|
| One designated storage hypha + content-addressed caches | One authoritative publication location; reuses local-cache behavior. [M2][H2][T2] | Its storage and outgoing bandwidth bound cold transfers; sole-copy loss remains exposed. | First no-NAS design; normally colocate with the queue, not necessarily the spore. |
| Storage on a separate chosen hypha | Allows a larger storage resource while retaining one authority. [G][map] | Queue and storage must both be reachable; another role to configure and explain. | Optional choice, not automatic election. |
| Queue-directed direct hypha-to-hypha copy | Reuses a nearby cached copy and avoids relaying all bytes through the store. [H1][H2] | Mycelium must track available sources, authenticate access and fall back when a source sleeps. Cached copies do not replace durable publication. | Later optimization if measured cold-transfer demand warrants it. |
| rsync + expected checksums | Existing partial/delta transfer and rate limits. [R1] | Requires rsync at both ends and configured access; does not supply job identities or checkpoint commit rules. | Operator migration/copy path, not a prerequisite users configure to join. |
| Hugging Face cache/download tooling | Reuses upstream revision snapshots and model acquisition. [F1][F2] | Not a local general-purpose store; external acquisition must be authorized separately. [M2] | Import approved models once; then serve locally. |
| IPFS-like block store | Content addressing and reusable blocks/providers. [I1][I2] | Pinning, discovery, privacy and retention must be operated. [I2][I3] | Defer. |
| BitTorrent-like swarms | Multiple seeders can share outgoing transfer work; pieces are checked. [B1] | Tracker/peer lifecycle, seeding and access controls remain. [B1][B2] | Defer until repeated large-model fan-out justifies it. |

## Recommendation and proposed data lifecycle

This section proposes requirements for the orchestrator to reconcile into the
next approved specification. It does not claim these features exist. The
starting constraints are the simplicity rule, staged coordination and existing
attempt/generation contract. [M2][M3][map][vision]

1. **Inputs and source:** freeze an explicit input manifest before admitting a
   job as ready. Upload missing immutable files to the designated store; retain
   the submitting copy until durable acknowledgement. Fetch only absent files
   into a bounded cache. Materialize a private job directory; do not allow a
   command to modify shared cached blobs. Validate relative manifest paths and
   symlinks before materialization. No transparent copying of an entire home
   directory or arbitrary undeclared dependencies. [M1][M2][H1][T1]
2. **Models:** reuse existing digest/size verification, partial preservation,
   pre-copy and cache protection. Import approved upstream revisions once;
   include configuration/tokenizer files in a complete model manifest. Keep
   conversion artifacts distinct and use the relevant qualified extension.
   A NAS can remain the master source through the same manifest contract.
   [M2][F1][G][vision]
3. **Checkpoints:** let the command continue using `MYCE_CHECKPOINT_DIR` locally.
   Transfer immutable payloads first, then publish the generation manifest only
   after the storage hypha has verified and durably saved all referenced bytes.
   Distinguish “saved here” from “available to resume elsewhere.” On sudden loss,
   another hypha can resume only the newest complete transferred generation;
   newer local-only progress waits for the original hypha to return. Preserve
   attempt/execution identities, generation comparison and reconciliation; do
   not rank progress by timestamps or let a stale result replace the accepted
   execution's output. [M3][M2]
4. **Outputs:** stage by execution attempt; retain the local result until durable
   store acknowledgement. Publish a job's accepted output manifest only after
   the queue accepts that execution and all referenced payloads are complete.
   Make retryable external side effects the job's responsibility, as today.
   Interrupted uploads resume; they are never published as completed outputs.
   [M3][T1]
5. **Storage and connection loss:** reserve space for partials and complete files;
   bound transfer concurrency and rate so everyday use stays responsive. Protect
   active inputs/models, retained checkpoints and sole authoritative copies from
   cache eviction. Define retention separately from cache eviction, with no silent
   deletion of user outputs. Surface missing storage and insufficient capacity in
   plain status messages. Keep queue SQLite local and the existing request spool
   separate from long-lived artifacts. [M2][M4][vision]
6. **Transport:** use authenticated, encrypted home-only endpoints; a digest is
   not an access credential. Scope reads/writes to authorized jobs and digest
   objects, not unrestricted paths. Prefer embedded tusd plus Go HTTP serving;
   do not invent chunk discovery, distributed locks or an automatic storage-role
   election. Before implementation, check tusd's locking, fsync, restart and
   completion-hook behavior against the checkpoint contract; standard components
   do not remove that integration obligation. [T1][T2][H2][M3][I3]

The transfer resource also belongs in placement: prefer complete local files,
then estimate missing bytes using measured achieved speed, retaining existing
model pre-copy's separation of transfer and engine startup. The eventual spec
must define how transfer/upload time fits the stop grace; a home connection must
not turn a nominal ten-second stop into a promise that any checkpoint size can
be moved within ten seconds. [M2][M3][vision]

**Next step / revisit trigger:** resolve the authoritative-copy and retention
policy through #24 and the map before builders implement this. Revisit direct
copies, block deduplication or swarms only when measured repeated transfer cost
or store bandwidth makes the one-store design inadequate. Surviving permanent
store loss is a separate requirement: if needed in the first release, specify
explicit second-copy acknowledgement now rather than calling caches backups.
[ticket][map][G]

## Confidence and what was not verified

**High confidence** in the documented building blocks and current NAS dependence;
**medium confidence** that one designated store is the best initial product
trade-off. That judgment follows the staged vision and simplicity constraint,
not measurements of a particular home installation. [M1][M2][M3][M4][map][vision]

No experiments, benchmarks, source changes, remote machine access or credentials
were used. Throughput on Wi-Fi, checkpoint transfer deadlines, power-loss
durability, cross-platform tus client/library behavior and embedded tusd's exact
fsync/completion ordering remain unverified. The research does not qualify an
extension or establish performance/durability acceptance. Permanent-loss
recovery, backup ownership/retention, job-file API shape and the storage-role
setup experience still need specification. Current documentation is a source
contract, not evidence of live deployment. [G][M3][map]

## Primary sources

- **Mycelium, 2026-10-14 issue reads:** [#24 question][ticket]; [#17 map][map];
  [#16 vision decisions and corrections][vision].
- **Mycelium current main, commit `b989acc`:** [M1: Start here][M1];
  [M2: approved design, dated 2026-10-08 with later decision sections][M2];
  [M3: resumable jobs][M3]; [M4: general model servers and spool][M4].
- **G:** [glossary, vision commit `2587b22`][G].
- **H1:** [IETF RFC 9110, June 2022, §§13.1.5, 14, 15.3.7][H1].
- **H2:** [Go standard-library `net/http.ServeContent`, living API docs][H2].
- **T1:** [tus resumable upload protocol, version 1.0.0][T1].
- **T2:** [tusd official embedding guide, living documentation][T2].
- **R1:** [rsync official manual, living documentation][R1].
- **F1:** [Hugging Face Hub cache guide, living documentation][F1].
- **F2:** [Hugging Face Hub environment-variable reference, living documentation][F2].
- **F3:** [Hugging Face Xet storage documentation, living documentation][F3].
- **F4:** [Hugging Face Hub source, file-download cache identity documentation][F4].
- **I1–I3:** IPFS official [content addressing][I1], [persistence][I2],
  and [privacy/encryption][I3] documentation, living pages.
- **B1:** [BitTorrent BEP 3, original protocol, created 2008-01-10][B1].
- **B2:** [BitTorrent BEP 27, private torrents, created 2008-08-03][B2].

[ticket]: https://github.com/1kuna/mycelium/issues/24
[map]: https://github.com/1kuna/mycelium/issues/17
[vision]: https://github.com/1kuna/mycelium/issues/16
[M1]: https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/START-HERE.md
[M2]: https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/design-proposal.md
[M3]: https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/guide-resumable-jobs.md
[M4]: https://github.com/1kuna/mycelium/blob/b989acc138fc160c60430855e415ea3846048b44/docs/guide-servers.md
[G]: https://github.com/1kuna/mycelium/blob/2587b223b8d711080fb4a6ace3182a6e56fa0b62/GLOSSARY.md
[H1]: https://www.rfc-editor.org/rfc/rfc9110.html
[H2]: https://pkg.go.dev/net/http#ServeContent
[T1]: https://tus.io/protocols/resumable-upload
[T2]: https://tus.github.io/tusd/advanced-topics/usage-package/
[R1]: https://download.samba.org/pub/rsync/rsync.1
[F1]: https://huggingface.co/docs/huggingface_hub/guides/manage-cache
[F2]: https://huggingface.co/docs/huggingface_hub/package_reference/environment_variables
[F3]: https://huggingface.co/docs/hub/storage-backends
[F4]: https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/file_download.py
[I1]: https://docs.ipfs.tech/concepts/content-addressing/
[I2]: https://docs.ipfs.tech/concepts/persistence/
[I3]: https://docs.ipfs.tech/concepts/privacy-and-encryption/
[B1]: https://www.bittorrent.org/beps/bep_0003.html
[B2]: https://www.bittorrent.org/beps/bep_0027.html
