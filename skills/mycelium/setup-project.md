# Set up project offload

Before using an optional contract below, check its installed components against
[capability evidence](references/release-behavior.md#capability-evidence).
“Next release” describes source intent, not installed availability; a dependency
lock does not establish application compatibility or production output parity.


For new reusable work, follow [the adapter reference](https://github.com/1kuna/mycelium/blob/main/docs/adapter.md):
one `.mycelium/adapter.yaml`, verified targets or managed CPU discovery, deliberately selected source, result checks,
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

From the next release, non-production native CPU entries/profiles may declare
`portable: true` and omit targets when their main environment is a managed Pixi
recipe with machine-readable selected-lock coverage. All sidecar environments
must cover the same target; sidecars without an environment inherit main's.
Mycelium intersects those immutable locks with supported runtimes, fresh observed
inventory and ordinary resource, tool, maintenance and custody gates. Missing or
unknown coverage is a hold, never universal support. No platform list is derived
from the submitter's OS or the installation's machine names.

```yaml
portable: true # Whole payload, including binaries/downloads/shell, is portable.
needs: {platform: cpu, cpu_cores: 1, memory_gb: 0.25}
environment: cpu-tools # Existing managed recipe; inspect `myce env get cpu-tools`.
# Omit targets for discovery; keep the entry's source/run/checks/outputs.
```

An explicit nonempty target list always binds, including with `portable: true`;
existing Linux-only jobs and accepted keyed intent never widen. A failed native
preparation excludes its exact target/recipe candidate without failing useful
alternatives or retrying idle deterministic failures. Warm exact identities are
reused. A changed immutable recipe and new submission key can reopen discovery;
replaying the old key keeps the old recipe and targets. If all candidates failed,
inspect the retained preparation reason, fix the recipe, then submit new intent.

Production discovery remains held until exact target/recipe output parity is
accepted under MY-19. Use existing native variants with recorded baselines and
production parity acceptance, or declare an explicit owner-accepted target list.
Plain-command admission is not native qualification, and preparation or exit-zero
smoke checks do not establish parity. This is the orchestrator default under
existing MY-19 recorded in #853; the project-carried recipe hold in #251/#252 is
unchanged.

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
