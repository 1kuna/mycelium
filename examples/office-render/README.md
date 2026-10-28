# Office PDF render helper

This Linux example converts one document per process through LibreOfficeKit's
stable C API. A native job or render stage can invoke it with `subprocess.run`
(check the exit status) instead of launching `soffice --convert-to`.
LibreOfficeKit disables the desktop single-instance RequestHandler IPC thread.
The helper makes no socket-policy exceptions and requires an installed
LibreOffice build with LibreOfficeKit and the `svp` headless backend.

Build during environment preparation with the matching LibreOfficeKit SDK
headers (`libreofficekit-dev` on distributions that package them separately):

```sh
cc -std=c11 -O2 -Wall -Wextra examples/office-render/render.c -ldl -o office-render
# If the SDK is elsewhere, add -I/path/to/sdk/include.
```

Inside the job, with input staged into work and the output directory created:

```sh
office-render /usr/lib/libreoffice/program input.docx output.pdf
```

`MYCE_SCRATCH_DIR` must be the existing job scratch directory. Every invocation
creates a unique child for its HOME, XDG runtime/config/data, profile and font
cache. A private fontconfig configuration uses fonts in `/usr/share/fonts` and
`/usr/local/share/fonts`; provision the fonts required by the document in the
environment. The job runner removes attempt scratch after cleanup. The helper
does not scan or remove shared caches or existing profiles.

For distributions whose installed registry symlinks outside the runtime tree,
stage that registry as ordinary job inputs and set `MYCE_OFFICE_REGISTRY` to its
staged directory. Alternatively the operator can grant the narrow installed
configuration directory read-only. The runtime, SDK and fonts are environment
dependencies; the helper never installs or downloads them. Use ordinary
operator runtime declarations for a helper installed outside the default
runtime trees. No network requirement or unsandboxed stage is needed.

The helper saves to an owned temporary file beside the destination, destroys
the document and then Office, verifies PDF header/EOF, calls `fsync` and checks
close before atomically replacing the destination. A failed render returns
nonzero and removes its temporary output; an existing destination remains
unchanged. The caller must treat nonzero exit as failure, regardless of any
older destination. Content checks (for example `pdftotext`) belong to the caller.

LibreOffice 24.2 can crash in exit-time global destructors even after canonical
Office destruction, including outside confinement. This standalone helper uses
`_exit` after the checked save/destroy/verify/flush/publish sequence to avoid that
known lifecycle failure. It never catches a crash or turns a failed save, flush
or cleanup into success. Keep it as a subprocess rather than embedding this
exit strategy in a long-lived application.

The existing native qualification test can exercise this helper without
changing any denial or PDF assertions:

```sh
MYCE_TEST_NATIVE_OFFICE=/usr/bin/soffice \
MYCE_TEST_NATIVE_OFFICE_RENDERER=/absolute/path/office-render \
MYCE_TEST_NATIVE_OFFICE_REGISTRY=/etc/libreoffice/registry \
go test ./internal/myce -run '^TestNativeIsolationLibreOffice$' -v
```

The registry variable is optional; the test stages the selected registry before
confinement. These instructions target Linux. Packaged macOS Office builds that
fall back to Aqua/AppKit remain unqualified under Seatbelt; this helper does not
provide a macOS bypass or establish document-layout parity with another runtime.
