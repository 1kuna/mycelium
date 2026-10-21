# Onboard-b re-review repair (#108, #113)

Base: `ba815cedc3ec89659d8171dd51e6fd426174ba03`. Scope is local B2–B4
repair; no fleet access or native hardware/service/NAS qualification.

## Regression evidence

The first new regression run used the base implementation with only test
isolation and the new tests added. It exited **1**, observing each of these:

- A healthy three-operation NAS sequence (120 ms per operation, over 350 ms
  total) was refused at 252 ms. It now passes with 250 ms per operation and
  a 5 s whole-check cap including mount inventory. Existing hung metadata/create/probe tests
  still refuse and prevent duplicate outstanding calls. A fake-time deep-root
  test verifies the 5 s cap independently of the per-operation budget.
- A cancelled NAS check was returned as `nas_unresponsive`. Cancellation now
  preserves `context.Canceled`, including cancellation during mount inventory;
  timeout still reports `nas_unresponsive`. The added mount-inventory tests
  first exited **1** on the intermediate repair and pass after applying the
  shared total deadline and cancellation classification there too.
- The old checkpoint guard served late success after a timed-out check under
  the same key. Context-bound checks now discard abandoned results; explicit
  timeout and cancellation regressions require the fresh callback's error.
  Durable worker operations retain their separate consume-late-completion
  contract so completed checkpoint preparation/publication is not repeated.
- ASPEED-only PCI inventory with `nvidia-smi` exit 9 was refused. Install
  preflight now selects CPU with an information note, and join detection
  reports no GPU. Actual NVIDIA hardware or unknown PCI inventory still
  retains typed driver failures.
- Working integrated Intel graphics was excluded from preflight; a broken
  integrated runtime was also silently ignored. Working Level Zero/SYCL now
  yields `xpu` and unified memory in both install and join detection. Missing
  integrated runtime yields CPU; broken runtime refuses.
- The release matrix still selected pixi 0.55.0. It now requires 0.81.0 in
  every version and URL; fakePixi reports the same version.

The existing non-Intel unsupported-render-node expectations remain. Integrated
Intel coverage moved to explicit working/missing/broken-runtime cases to follow
the binding hardware decision. The existing CUDA CLI refusal fixture now models
an NVIDIA PCI display device; its refusal and no-mutation assertions remain.

Lazy acquisition now receives its home lookup, downloader, version command
runner and release metadata through `EnvironmentCache.Tools`. The new end-to-end
test runs normal environment acquisition, fake archive checksum/extraction,
install and JSON activation, and reuses the verified tool on a second recipe.
It checks one fake download and installation only under the injected home.

To reproduce the old lazy bug without removing the new type seam, a scratch
copy used the exact `ba815ce` lazy resolver body with the new regression. It
exited **1** at the vendor-download guard, before the network transport.
Restoring the fixed resolver passes. Separately removing fakePixi's `--version`
line in a scratch copy made the existing single-flight fixture exit **1** at
the same guard. This is expected mutation failure, not a download.

Both package TestMain functions install a shared guard before helpers run:
HOME and XDG locations point to an owned temporary home, and vendor uv/pixi
HTTP acquisition panics before transport. A subprocess regression verifies the
guard itself. Verification also used a workspace-local Go cache/module copy
and blocked external HTTP proxies; no test acquired a real tool.

## Pixi v0.81.0 vendor evidence

All four archives were fetched explicitly outside tests into ignored project
scratch. Their computed SHA-256 values match their respective vendor-published
`.tar.gz.sha256` files under:
<https://github.com/prefix-dev/pixi/releases/tag/v0.81.0>.

```text
aarch64-apple-darwin
f4e32ea91970d4e11739488817979a5f2c6ebbb9cedb0d6dea74b2b790b272dc
x86_64-apple-darwin
9859588ba57f390b5c77d56b2654fab37bc00e10952da25e3efd6e3434557e5a
aarch64-unknown-linux-musl
9f8d2113fe9dc01788a65f5c2acec34fa56b1193461a5c3e9a775d6d2d621bcb
x86_64-unknown-linux-musl
7aa3ec39aecceff9062fa2ed4d42cbaa0bdc25ddea727d048e061cf188d434f6
```

With the verified native archive extracted outside the test, explicit
`MYCE_PIXI_BIN` qualification ran `TestPixiRealEnvironment` and
`TestPixiRealExtensionActivationJob` in the isolated test HOME. Both logged
`pixi 0.81.0` and passed, exit **0**. This covers empty lock generation,
`install --locked --no-config`, JSON `shell-hook --locked --no-config`,
activation scripts and the activated extension command. Existing PATH precedence,
offline-idle and idempotent-bootstrap tests also pass.

## Verification and delivery

- New base regression run: **1**, expected failures above.
- Missing-version mutation and legacy-lazy-resolver reproduction: **1** each,
  both stopped at the guard before network transport.
- Focused onboarding/review/pixi/install tests: **0** for both Go packages.
- Real pixi qualification: **0**.
- `go build -o myce ./cmd/myce`: **0**.
- `go vet ./...`: **0**.
- `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o <ignored-output> ./cmd/myce`: **0**.
- `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o <ignored-output> ./cmd/myce`: **0**.
- Built-binary `myce dev` smoke on fake and CPU backends: **0**; authenticated
  real API, command effects, queue maintenance and graceful child shutdown.
- Preliminary unfiltered serial race package pair: **0** (1,005.96 s); launch
  gate **1** other exact `go test -race` process. It began before the mount-inventory
  cancellation/deadline coverage was added. The required final-source package
  pair and `./...` results follow below.
- `go test -race -p 1 -count=1 -timeout 30m ./internal/myce ./cmd/myce`:
  **0** (1,110.48 s), launch gate **0** other exact race processes.
  Package times: internal **1,022.521 s**, CLI **75.277 s**.
- `go test -race -p 1 -count=1 -timeout 30m ./...`: **0** (1,018.45 s),
  launch gate **1** other exact race process. Internal package **938.569 s**,
  CLI **75.959 s**, host bridge **1.126 s**.
- Post-suite real-home Mycelium tool-tree absence check: **0**, absent.
- Known #112 flake retry: **not used**.

Preliminary focused runs exposed and corrected a too-broad late-completion
change, a missing PCI fixture, and fake-time channel cleanup in the new cap test.
Those runs exited 1; no existing assertion or timeout was relaxed.

Ignored project scratch retains the detailed logs, gate/exit JSON receipts and
vendor checksum receipts. Owned reproducible archives, compiler/module caches
and temporary test/snapshot trees were removed after test/build processes joined.

Current state: all local re-review repairs and required acceptance checks
verified. Next: orchestrator review/integration under #108 and separately
authorized native qualification. #113 has compatible native pixi evidence;
no native GPU, NAS, service-manager or fleet acceptance is claimed.
