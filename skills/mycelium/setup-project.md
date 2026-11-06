# Set up project offload

For new reusable work, follow [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md):
one `.mycelium/adapter.yaml`, explicit targets, deliberately selected source, result checks,
and submit/wait/verified retrieval. Keep this package's [SKILL.md](SKILL.md) and
supporting files together when installing it.

Before choosing targets, run `myce portability`, inspect `myce env get NAME`,
then run `myce spec check --resolve --entry NAME .mycelium/adapter.yaml` on the
authored entry. Repeat after inventory or recipe changes. From the next release,
resolve and portability compare authored eligibility with dependency candidates
from the selected immutable lock environment, including all command members.
The report names missing lock coverage, unknown build support, preparation and
current admission separately. uv/installed-tool/binary coverage stays unknown
without matching native evidence. A Pixi lock's platform keys are dependency
metadata, not proof that payload binaries, downloads or shell commands work.

Keep explicit targets for now. `portable: true` permits architecture omission
within a declared OS; it does not expand existing lists or enable omitted
targets in reusable entries. Plain-command admission needs no native baseline,
but widening production work still needs owner-accepted MY-19 output parity.
Preparation and exit-zero smoke checks do not supply it. Automatic omitted-target
discovery and its isolated failures remain pending #853; the project-carried
recipe decision hold in #251/#252 is unchanged.

## Existing profile compatibility

Keep existing `.mycelium/profiles/NAME.yaml` callers working. To initialize a
compatibility profile, use `myce profile init --name NAME --environment ENV -- CMD`,
review source/targets/resources/outputs, and commit the intended source. The
[profile reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#project-profiles)
explains source safety, automatic memory, per-item inputs, typed receipts and
recovery; [conversion](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md#convert-profiles-and-extensions)
is explicit and never switches callers. Use a new key for an intentional repeat.

Before switching callers, prove a real queued run, inspect logs and retrieve its
verified accepted output; apply the project's own result check. Keep CI unchanged
unless its runner has authorized installed queue authority. Record the entry or
profile and its proof in the project's own integration records, following that
project's rules. A library/server uses the service contract; a native recipe needs
its recorded baseline and exact-target qualification. Neither follows from a
profile's successful command alone.

## Install the agent skill

From this repository, copy the skill directory, including its supporting files,
into your coding agent's skill directory. For Codex:

```sh
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills/mycelium"
cp -R skills/mycelium/. "${CODEX_HOME:-$HOME/.codex}/skills/mycelium/"
```

For Claude Code, use `~/.claude/skills/mycelium/` as the destination.
Reload skills or start a new session after installation. Other harnesses can
load `skills/mycelium/SKILL.md` directly. This installs agent instructions;
shell interception depends on the harness integration linked below.


For harness interception, see [harness-binding.md](harness-binding.md).
