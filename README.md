# Mycelium

**Turn the computers you already own into one big computer.**

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/how-it-works-dark.svg">
  <img alt="Your machines (a gaming PC, a Mac mini, a laptop and an AI box) join one Mycelium. Each job you send goes to a free machine with the right hardware." src="docs/assets/how-it-works.svg">
</picture>

Maybe you have a gaming PC, a Mac mini, a laptop and an AI box. Mycelium links
them together. You hand it a job, and it runs that job on whichever machine is
free and has the right hardware. Files pass between machines through your
shared network drive, so you never have to pick a machine or copy anything by hand.

Like the fungus it's named after, it's one network with many branches, and each
of your computers is a branch.

## What it's for

- **Transcribing recordings.** In a real run, one 25-minute recording was split
  across three machines. Speech-to-text ran on an Nvidia PC, telling the
  speakers apart ran on an Intel GPU box, and the final document was written on a
  Mac mini. Nobody chose where any step ran.
- **Training and testing AI or robot models** on whichever GPU is idle.
- **Running lots of AI prompts** against models on your own hardware.
- **Anything else you'd run in a terminal:** rendering, converting video,
  building code, crunching data. It isn't just for AI.
- **Using spare power politely.** Mark a computer as in daily use, and jobs wait
  until you step away from it.
- **Growing in one step.** Adding a machine takes one pasted command.

It works with Nvidia, Intel and Apple GPUs, or just a CPU, on Linux and Apple
Silicon Macs. Coming next: Windows, and splitting one huge job across several GPUs.

## Set it up with your AI agent

Open Claude Code, Codex or another coding agent on the computer that should be
the hub (one that stays on), and paste this:

```text
Set up Mycelium on my computers. Clone https://github.com/1kuna/mycelium,
then read docs/agent-setup.md and follow it step by step. Ask me which
computer is the hub, which others to add, and where my shared network drive
is mounted on each. Show me every install plan before running it, and never
print or commit the secret token.
```

The agent installs the hub, adds your other machines, checks that each one shows
up, and runs a test job. Before you start, you need:

- Linux or Apple Silicon Mac computers on the same home network
- A shared network drive (NAS) mounted on each one
- Working GPU drivers, if a machine has a GPU

## Try it without any setup

On one laptop with [Go](https://go.dev/dl/) installed, from a clone of this repo,
start a pretend cluster with fake GPUs. The jobs really run.

```sh
go build -o myce ./cmd/myce && ./myce dev
# in a second terminal:
MYCE_QUEUE=http://127.0.0.1:7331 MYCE_TOKEN=dev-token ./myce run -- echo hello
```

## More

[Guides](docs/START-HERE.md) · [Agent setup](docs/agent-setup.md) ·
[Laptop walkthrough](docs/try-it-locally.md) · [Running jobs](docs/job-reference.md) ·
[Operations](docs/operations.md) · [Design](docs/design-proposal.md)
