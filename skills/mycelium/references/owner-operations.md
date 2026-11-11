# Installed owner authority

Read this only for a queue-owner write or release maintenance within existing
task authority. Ordinary consumers use the installed CLI/Client.from_installed()
and retain typed setup holds. Never open, print, copy, pass or set fleet
credentials; never source authority env files or extract joining tokens.

## Supported owner-write route

Requires the CLI owner-authority change from #1079/#1080 (known merged source
7ffbdcd64e4de19b1bb79eae4640ba7b9b3f48be). Check the installed image before use;
no first signed release containing it is established by this reference.
Run on the queue host as the account that can read its installed queue config:

```sh
myce policy get --yaml --config <installed-queue-config> > policy.yaml
# Edit policy.yaml; N is the exported revision.
myce policy set policy.yaml --if-revision N --config <installed-queue-config>
```

Use that same installed --config for other authorized owner writes, such as
`myce model sync MODEL --to all --config <installed-queue-config>`.
The CLI resolves credentials internally and preserves installed certificate trust.
owner_authority_unavailable requires the installation owner to restore access;
owner_authority_unpinned requires provisioned HTTPS trust. Retain either refusal;
credential extraction and TLS overrides are not recovery.

## Release work

Read [operations](../../../docs/operations.md), [current rulings](../../../docs/DECISIONS.md#current-rulings)
and [development gates](../../../docs/development.md) before acting. MY-33/MY-35
still govern healthy production runs, signed/approved rolling updates, exact
custody, stopped unhealthy advancement and operator rollback. An optional
release feature or shorter consumer guide grants no deployment or hardware
authority. Private installation policy and destination bindings stay private.

## Mycelium repository merges

Read [the repository merge procedure](../../../docs/development.md#local-merge-proof)
and [current rulings](../../../docs/DECISIONS.md#my-41). Under MY-41, merge on quick
plus targeted tests, run the full suite once per release batch, and fix breakage
forward; no independent review or per-PR full is required. Development gates,
admission and stale custody may be repaired or reset when they block iteration;
protect running production jobs and secrets. Keep actual failed, interrupted and
skipped results. This reference grants no deployment authority.
