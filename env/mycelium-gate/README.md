# Gate CPU environment

`gate-full` uses this catalog environment for Go 1.25.6+, Python 3.11, uv and Git
on linux-64, linux-aarch64 and osx-arm64. The lock pins each platform's packages;
workers install with `pixi install --locked`. Linux selects `go-cgo`, including
its C compiler/sysroot, because the gate runs Go's race detector. Bash remains a
host prerequisite; macOS also needs its native Xcode/Command Line Tools SDK for
the gate's CGO build.

## Registration at the next queue deploy

Copy `pixi.toml` and `pixi.lock` into `mycelium-gate/` beside the existing queue
catalog. Verify their SHA-256 hashes against the delivery receipt, then add this
entry under the existing `environments` mapping, preserving all three existing
entries:

```yaml
environments:
  mycelium-gate:
    cpu:
      backend: pixi
      lock_file: mycelium-gate/pixi.lock
      environment: default
```

Retain the queue's existing `--environment-config` path. The catalog loads at
queue startup, so registration takes effect at the next deploy/restart; submit
a new profile job afterward. Existing submissions keep their recipe snapshots.
No per-host tool installation or target restriction is needed. See
[CPU environments](../../docs/environments.md#cpu-tools-with-pixi).

To update the lock deliberately:

```sh
pixi lock --manifest-path env/mycelium-gate/pixi.toml
```

On macOS, run `./scripts/check-gate-env.sh` after environment changes. It enters
the locked environment and builds `./cmd/myce` with CGO enabled, checking that
the activated C compiler works with the native SDK. Requires `pixi` on PATH.
