# Set up a project

Adopting Mycelium is the project's work, and Mycelium keeps it small: one
`.mycelium/adapter.yaml`, one command per run. A dependency lock does not
establish application compatibility or production output parity.

## Find the installed CLI

A non-interactive shell can omit user bin directories even when Mycelium is
installed. Check `command -v myce`. For the default installation, add its
stable binary directories to this shell's PATH, then check again:

```sh
export PATH="$HOME/.local/bin:$HOME/.local/share/mycelium/worker:$HOME/.local/share/mycelium/queue:$PATH"
command -v myce
```

These paths keep pointing at the installed binary after an upgrade. This changes
only the current shell; a task runner can use the same PATH or the absolute
binary path. For a custom install root or Linux worker instance, the installation
owner supplies that binary's path. If none exists, keep the missing-CLI hold for
the installation owner. Do not inspect credential files or start a local queue
as a substitute for authorized fleet work.

## New project

From the project root, `myce init` writes one entry and picks up `pixi.lock` or
`uv.lock` (`--lock` chooses when both exist, `--python 3.X` sets uv's Python).
Without a lock, pass the command after `--`. Run `git init` first for a new folder.
Pass repeatable `--output FILE_OR_FOLDER` flags, commit the source the entry runs, ignore `.mycelium/receipts/`
and your `--out` folder, then `myce run --entry NAME --wait --out DIR`. The
[guide](SKILL.md) walks through it; [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md)
has every field.

## Try a fresh project on a laptop

Use a built or installed `myce` on PATH (build instructions are in
[Start here](https://github.com/1kuna/mycelium/blob/main/docs/START-HERE.md#five-minute-start)).
Create a throwaway folder with `input.txt` containing `fresh project` and this
`report.sh`:

```sh
set -eu
mkdir -p output/nested
tr a-z A-Z < input.txt > output/result.txt
printf 'done\n' > output/nested/receipt.txt
```

From that folder, start `myce dev -- sh`. It waits for two fake workers and opens
a shell connected only to its temporary local queue; it sets no fleet defaults.
Run these six steps, with no submit/wait/fetch script:

```sh
git init -q
myce init --entry report --output output/ -- sh report.sh
printf 'accepted/\n.mycelium/receipts/\n' >> .gitignore
git add . && git commit -qm "Run report on Mycelium"
myce run --entry report --out accepted
cat accepted/output/result.txt accepted/output/nested/receipt.txt
```

Expect `FRESH PROJECT` and `done`. `--out` implies `--wait`: exit 0 means the
accepted outputs were downloaded and size/SHA-256 verified. Repeat the same
`myce run` command: its saved receipt adopts the same job and verifies the files
again. `git status --porcelain` stays empty because receipts and downloads are
ignored. Exit the shell to stop dev and remove its queue and worker scratch;
the project and accepted files remain. This proves the real API and command
runner with fake inventory, not live-fleet deployment or native qualification.

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
parity acceptance, or the project's own approval of a kind of machine (for
example `cuda`, `mac` or `linux/amd64/cuda`, never a host; MY-51), added with
`myce parity add` after a passing output comparison on a machine of that kind. Plain-command
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
`myce install` does the same for agent homes on that machine. Automatic refresh
fills missing copies, repairs malformed markers and upgrades unedited managed
copies when the binary's release is newer. Equal and newer copies stay in place.
Development builds skip automatic refresh. `myce skill status` shows each
copy's state.

A symlink, an edited copy or a copy `myce` didn't write (from an older manual
`cp`) is kept and reported. `myce skill install --replace` replaces it: a symlink
is removed without touching its target, and a directory is moved to the user
cache's `mycelium/skill-backups/`. Use `--replace` for a deliberate rollback.
Reload skills or start a new session
afterwards. Other harnesses can read the guide with `myce skill show`. This
installs agent instructions; routing shell commands depends on the harness
([harness binding](harness-binding.md)).
