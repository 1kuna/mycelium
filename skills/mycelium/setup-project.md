# Set up a project

Adopting Mycelium is the project's work, and Mycelium keeps it small: one
`.mycelium/adapter.yaml`, one command per run. A dependency lock does not
establish application compatibility or production output parity.

## New project

From the project root, `myce init` writes one entry and picks up `pixi.lock` or
`uv.lock` (`--lock` chooses when both exist, `--python 3.X` sets uv's Python).
Add `outputs`, commit the source the entry runs, ignore `.mycelium/receipts/`
and your `--out` folder, then `myce run --entry NAME --wait --out DIR`. The
[guide](SKILL.md) walks through it; [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md)
has every field.

## Existing project

1. Convert compatibility profiles: `myce adapter convert --profile PATH ...
   --out .mycelium/adapter.new.yaml` prints a plan; repeat with `--apply` to
   write it. Non-production targets are dropped (`--keep-targets` keeps them),
   fields equal to a default are omitted, and production keeps its targets. It
   never switches callers or retires old readers
   ([conversion](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#convert-profiles-and-extensions)).
2. Prove one real run: `myce run --entry NAME --wait --out DIR`, read its logs
   and apply the project's own result check.
3. Switch callers to that command, or Python `client.run_entry`, and delete
   what the guide's "What you can now delete" list names.
4. Pick each entry's class on purpose: user-facing work your project calls
   production is `production`; the rest stays `experiment`
   ([classes](references/waiting.md#classes-and-preemption)).

Keep accepted keys and receipts while switching: a new key for the same work
can duplicate it. Keep CI unchanged unless its runner has installed queue
authority. Record the entry and its proof in the project's own records. A
library or server uses the service contract, and a native recipe needs its
recorded baseline and exact-target qualification; neither follows from a
command's success.

## Production

Production entries keep explicit targets until exact target and recipe output
parity is accepted under MY-19: native variants with recorded baselines and
parity acceptance, or the project's own `production_parity_hosts` list, widened
with `myce parity add` after a passing output comparison. Plain-command
admission, preparation and exit-zero smoke checks do not establish parity.

## Install the agent skill

Every `myce` binary carries this skill for its own release. Install it from the
CLI, not from a source checkout, so agents read the guide for the release they
actually run:

```sh
myce skill install
```

This writes `skills/mycelium/` under each agent home that exists: `~/.claude`
(Claude Code), `~/.agents` and `$CODEX_HOME` (default `~/.codex`). Pass skill
folders to choose others, e.g. `myce skill install ~/.claude/skills/mycelium`.
`myce install` does the same for agent homes on that machine. After an upgrade,
the next `myce` command refreshes every copy it wrote to the new release; a copy
you edited is left alone. `myce skill status` shows each copy's state.

A symlink, an edited copy or a copy `myce` didn't write (from an older manual
`cp`) is kept and reported. `myce skill install --replace` replaces it: a symlink
is removed without touching its target, and a directory is moved to the user
cache's `mycelium/skill-backups/`. Reload skills or start a new session
afterwards. Other harnesses can read the guide with `myce skill show`. This
installs agent instructions; routing shell commands depends on the harness
([harness binding](harness-binding.md)).
