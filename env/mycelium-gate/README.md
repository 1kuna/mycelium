# Gate CPU environment

The full and quick gate entries use this committed recipe for Go 1.25.6+,
Python 3.11, uv, Git and PostgreSQL on linux-64, linux-aarch64 and osx-arm64.
The shared-queue tests start a disposable PostgreSQL cluster using `initdb`
and `pg_ctl` from the locked recipe. The lock pins each platform's packages;
workers install with `pixi install --locked`. Linux selects `go-cgo`, including
its C compiler/sysroot, because the gate runs Go's race detector. Bash remains a
host prerequisite; macOS also needs its native Xcode/Command Line Tools SDK for
the gate's CGO build.

## Catalog registration

The managed gate entries freeze this recipe directly from the checkout. The
release build and ad-hoc Go test profiles use the named catalog recipe. To
register that recipe, copy `pixi.toml` and `pixi.lock` into `mycelium-gate/` beside the queue catalog. Verify the lock and manifest against the approved
source. Add this entry to the existing `environments` mapping.

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
