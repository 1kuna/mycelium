# Native server entries (#68)

Approved by maintainer on 2026-10-16. These additions reuse the existing scheduler,
server supervisor, environment recipes, health checks, pins and memory admission.
They do not qualify a CUDA arm64 runtime or change capacity declarations.

## Shared entries and placement

Optional `project` identifies the entry owner, accounts for its idle model
reservation, and supplies missing `/v1` project labels. Models remain shared:
any project may call or pin any entry. Explicit caller labels retain their own
call accounting and scheduling policy.

Optional `allowed_machines` intersects the owner's project machine policy, the
caller's policy, platform/device eligibility and pin restrictions. Empty lists
are unrestricted at that layer. Calls wait if no eligible worker is available;
status shows the placement fences. A tightened fence drains excluded copies and
retains memory custody until the worker confirms unloading. Load plans carry
current fences; workers recheck them. Pre-copy and pins obey the same fences.

## Environment and local named secrets

`env` is a map of non-secret literals, applied to start and stop commands.
`secret_env` maps environment-variable names to local secret names. Provision
one regular **mode-0600 file** per name in a **mode-0700 directory**, then pass
`myce join --secret-root SECRET_DIRECTORY`. Directory traversal, symlink files,
missing files and permissive modes refuse loading. Files may contain up to
64 KiB; one trailing LF or CRLF is stripped. No remote secret service is used.

Only names enter catalogs and queue messages. Values are read immediately before
launch and kept in process memory, including the stop helper's environment.
`secret_env` requires `logs: none`: even a server that prints its environment
must not leave secret values in Mycelium logs. Errors never include secret
contents or local secret paths. Plain commands can read their supplied secrets;
operators must also control application telemetry and external log sinks.

The maps cannot override `MYCE_*`, accelerator selectors, offline controls,
`LLAMA_ARG_*`, or worker-controlled executable/environment paths. Use `device`
and `device_env` for device selection. `MYCE_TOKEN` is stripped; provisioning
its value under another local secret name is rejected. Keep `env` literals
non-secret; arbitrary credentials cannot be identified from their spelling.

## Approved in-place model folders

On each host, explicitly approve the exact folder and a root bounding both the
folder and any symlinked HF snapshot blobs:

```sh
myce model approve-local APPROVAL_NAME \
  --path MODEL_FOLDER --approved-root SNAPSHOT_AND_BLOB_ROOT \
  --model-approval-root LOCAL_APPROVAL_DIRECTORY
```

This is a local command and does not contact the queue. It hashes regular files
once, records the canonical folder, host, complete file/directory metadata and
symlink targets, and prints portable `local_source`, `files`, `size_bytes` and
`sha256` fields. Put those fields in the entry. Omit `path`, `file_store` and
`direct_nas`. Start the worker with the same `--model-approval-root` directory.
Keep the approval directory mode 0700 and records private and local; they contain source paths and host
identity. File names in the portable manifest should also be reviewed before
sharing a catalog.

Loads use the original folder as `{model_path}`, with no copy, NAS fallback,
rehash or external-file eviction. Workers advertise only approved identities.
Every check uses the file set, size, mtime, mode, device/inode and symlink-target
metadata. A changed/missing file, new file, changed blob or different host
invalidates readiness. Re-run the explicit approval command after changes and
update the catalog manifest if its content identity changed. All symlink targets
must stay within the approved root; directory symlinks and special files are
rejected. Hard links are regular files: another name for the same inode can
exist outside that root, so the root does not bound hard-link aliases. This is
acceptable because approval trusts operator-controlled immutable storage, not a
filesystem sandbox. Protect every alias from writes; inode/size/mtime checks
detect ordinary changes through any alias. The ordinary copied-model no-symlink
rules are unchanged.

Each heartbeat checks each distinct local approval once, sharing the result with
cache inventory. The metadata pass has a 250 ms I/O budget. If storage stalls,
approval is not currently verifiable: new loads and routing are withheld, a
sanitized warning appears at most once per minute, and checks retry without
accumulating blocked filesystem calls. Existing allocations remain reserved until
the worker confirms unloading; a timeout alone never proves release. An expired
check cannot restore readiness, even if its filesystem call finishes later.

Protect approved snapshots/blobs against writes and HF cleanup while used.
Metadata checks trust operator-controlled immutable storage; they do not detect
a malicious writer preserving all observed metadata. Retain a recovery source.

## Header-less OpenAI clients

Fleet bearer authentication is always required. On `/v1` routes, an owned entry
supplies its `project` when `X-Myce-Project` is absent and uses `experiment` when
`X-Myce-Priority` is absent. A project policy can explicitly set
`default_call_priority: production`; that opt-in applies to missing priority
headers for the effective caller project. An explicit caller project therefore
uses its own production opt-in even on another owner's entry; the owner's
default does not override it. Explicit priorities remain supported, including
production calls under the existing contract. Project headers are trimmed.
Empty project/priority and duplicate routing headers fail. A deadline header is
ignored for non-deadline calls; deadline priority requires an RFC3339 deadline.

Entries without `project` retain the existing required-header behavior. Generic
`/models/NAME/PATH` calls validate required headers before reading or spooling
the body. Valid requests retain upload spooling; these
changes do not resolve the separate body-storage privacy hold.

[The dry Atlas example](examples/atlas-server-entries.yaml) uses placeholders.
Replace every placeholder using approved runtime argv, an environment recipe,
qualified memory declarations and the local approval output. Pinning is separate
and requires agreed capacity/hours. Native runtime, memory peaks and application
telemetry need separate authorized hardware verification.
