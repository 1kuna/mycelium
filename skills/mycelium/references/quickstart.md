# Runnable examples

Use the installed `myce` CLI with its existing queue configuration. To try the
examples without fleet credentials, start a shell with `myce dev -- sh`.

## Run one command

```sh
myce run -- sh -c 'echo "hello from $(uname -sm)"'
```

The command runs with the worker's installed tools in an empty directory.
The CLI streams its logs and returns its exit code.

## Fetch a verified file

```sh
myce run --out accepted --output result.txt -- sh -c "printf 'mycelium-ease-audit-ok\n' > result.txt"
cat accepted/result.txt
```

`--out` waits, downloads each declared output and verifies its size and SHA-256.

## Create and run a reusable entry

Use a disposable Git project with no environment lock. Run `git init` in a new
directory if needed and configure a Git commit identity.

```sh
myce init --entry hello --output result.txt -- sh -c "printf 'mycelium-ease-audit-ok\n' > result.txt"
```

This writes `.mycelium/adapter.yaml` with the following entry. The `project`
field takes its name from your directory.

```yaml
project: example
entries:
  hello:
    run: {command: [sh, -c, "printf 'mycelium-ease-audit-ok\n' > result.txt"]}
    outputs: {result.txt: results}
```

The entry sends your committed `HEAD`. Ignore the fetched files and local
receipts before committing and running it.

```sh
printf 'accepted/\n.mycelium/receipts/\n' >> .gitignore
git add .gitignore .mycelium/adapter.yaml && git commit -qm "Run hello on Mycelium"
myce run --entry hello --wait --out accepted
```

## Run the same entry from Python

With the [Python client](https://github.com/1kuna/mycelium/blob/main/clients/python/README.md)
installed, run this from the same project. It adopts the same job and fetches
the verified result again.

```python
from pathlib import Path
from mycelium import Client

client = Client.from_installed()
client.run_entry("hello", out="accepted")
assert Path("accepted/result.txt").read_bytes() == b"mycelium-ease-audit-ok\n"
```
