# S11 native worker identity probe (not yet run)

`S11_NATIVE_WORKER_CREDENTIAL_AND_MANAGER` is required on Linux before deployed
root host control is accepted. Laptop protocol tests exercise the real launcher,
child entry and environment as the same user; they do not qualify credential
changes or the real user manager. Run only with current authorization against a
disposable installed worker fixture.

## Separate root deployment

Root must run `/usr/local/libexec/myce/myce`, **never** the worker-owned copy under
its home. Invoke the root copy by **absolute path**, never via `$PATH`, in
sudoers, units and controllers. The root copy is installed by the operator or a
root installer;
`myce install`/upgrade run as the worker and never create or update this copy.
Obtain the release artifact and expected SHA-256 through a trusted release
channel. Stage the artifact in a root-owned directory with root-owned ancestors,
none group/other-writable and none carrying a Linux access ACL. Do not accept a
worker-writable source or a checksum supplied by that worker as deployment authority.

In a root shell on Linux, after independently verifying that `/usr`, `/usr/local`
and existing `/usr/local/libexec` ancestors are root-owned directories with
`mode & 022 == 0` and no symlinks, use this exact copy-and-verify sequence. The
source below must already be a trusted regular file, mode 0755, in the protected
staging directory; substitute the trusted release digest:

```sh
expected_sha256='<trusted release SHA-256>'
printf '%s  %s\n' "$expected_sha256" /root/myce-release/myce | sha256sum --check --status || exit 1
install -d -o root -g root -m 0755 /usr/local/libexec /usr/local/libexec/myce
install -o root -g root -m 0755 /root/myce-release/myce /usr/local/libexec/myce/myce
printf '%s  %s\n' "$expected_sha256" /usr/local/libexec/myce/myce | sha256sum --check --status || exit 1
stat -c '%u %a %F' /usr/local/libexec/myce/myce
namei -l /usr/local/libexec/myce/myce
getfacl -cp / /usr /usr/local /usr/local/libexec /usr/local/libexec/myce /usr/local/libexec/myce/myce
```

Before copying, any existing destination must already be a trusted root-owned
regular file (or absent); refuse symlinked/untrusted destination paths. The final
file must report UID 0 and mode 755; every ancestor through `/` must be root-owned
and not group/other-writable. Inspect `getfacl` for every listed path: reject any
access ACL (named user/group or mask entries; `system.posix_acl_access` must be
absent). An ACL inspection failure also blocks deployment. Recheck after copying,
since a default directory ACL may create an access ACL on the copy. Independently
verify these properties rather than repairing unknown trees. Keep the worker and root copies on the same reviewed
release. The startup guard checks the executing inode with `fstat` of
`/proc/self/exe`, its resolved path, and every ancestor, returning typed
`ErrHostTarget` code `root_binary_untrusted` on failure. It cannot make starting
an already untrusted executable safe: establish trust before invocation.

## Native checks

1. Invoke **the separate root copy** with `host close --local --worker-user
   <fixture-user> --config <fixture-config>`. Use a nondefault worker installation
   root and state directory. Verify the child is re-executed via `/proc/self/exe`,
   its executable inode matches the trusted copy, and its real/effective UID/GID
   and supplementary groups match the OS account database. HOME and
   `XDG_RUNTIME_DIR=/run/user/<uid>` must select the fixture user's manager.
   Root's read-only `systemctl --user --machine=<fixture-user>@.host show` must
   reach that same manager. Never print environments or credentials.
2. Trace file access: root performs passwd lookup and string-level path checks,
   but never opens/stats/walks worker config, state, spool or registry trees.
   Resolve, config reads, state reads and tree validation must run in the dropped
   child. Inspect every new/changed intent, lock, spool and registry file: all
   must belong to the worker. Trace native/Docker mutations to child PIDs. Inject
   a manager/mutation failure and verify durable closed intent precedes discovery,
   a typed failure is returned, and no acceptance/PASS is emitted. Repeat after
   logout and reboot with linger.
3. Snapshot bytes, modes, owners and modification times before and after status;
   verify no writes or lock creations, including in its read-only child. Confirm
   the S07 collector runs as root; as the worker, real GPU-holder evidence must
   remain UNKNOWN. Dev's explicit fake inventory must still work.
4. Check wrong account/host mapping, foreign-owned and group/other-writable
   targets/locks, access ACLs on targets/ancestors (even with a zero mask),
   root-owned writable ancestors without the sticky bit, worker-owned symlink
   ancestors, symlinked `host-state.json` and
   config (including links to `/dev/zero`), FIFOs, corrupt state, revision mismatch
   and a missing manager. Each must fail promptly with the matching typed error,
   without raw paths, bytes or read errors. Verify a root-owned symlinked ancestor
   such as `/home -> /var/home` works when its resolved worker targets are safe.
   Do not repair an unknown tree with chown.
5. On a disposable fixture, verify the guard rejects wrong executable ownership,
   writable mode/ancestors, access ACLs on the binary/ancestors (use a zero-mask
   ACL on the trusted disposable fixture), unexpected resolved path and replaced
   executing
   inode. Use a trusted test fixture to exercise rejection; **never execute
   worker-writable code as root to test the guard**. Verify every other root
   command (`install`, `service`, `host convert`, `resolve-docker`, etc.) refuses.
   Open a harmless inherited fd ≥3 before invocation and verify it is absent
   from child processes (both close_range and the older-kernel fd fallback).
6. Inject a slow close and cancellation at the deadline: the child gets the
   remaining timeout minus two seconds, receives SIGTERM on parent cancellation
   or at the overall deadline minus its one-second `WaitDelay`,
   and returns the durable intent revision with a typed partial/unconfirmed
   result. Verify the forced-exit wait fits inside `--timeout`, including an
   uncooperative child. A handler completing during cancellation must never report
   confirmed
   cleanup. A killed/failed child must prevent acceptance/PASS. Verify no fleet
   token reaches child argv/environment, output/errors, or any launched job.

`--state-dir` overrides only the shared host-control directory. It does not adopt
legacy state, select a different account, or change the installed worker spool,
queue or services. The override must already exist, be owned by the selected
worker, and contain current-format intent with the installed host identity.
Use `--config` for a nondefault worker installation root/instance. No caller HOME,
MYCE_HOST_STATE_DIR or caller session-bus inference selects privileged identity.
S05 supplies mandatory reconciliation/previous-close settlement to
`HostMutationExecutor`; S08 owns the final status/verdict envelope. The S06
adapter's cleanup confirmation is never the complete local-exclusion PASS.
S07 must record `DOCKER_HOST` at install and read the endpoint from config;
endpoint persistence is not implemented by S06.
