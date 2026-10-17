# S11 native worker identity probe (not yet run)

`S11_NATIVE_WORKER_CREDENTIAL_AND_MANAGER` is required on Linux before deployed
root host control is accepted. Laptop protocol tests do not qualify it. Run only
with current authorization against a disposable installed worker fixture.

1. Invoke the installed binary as root with `host close --local --worker-user
   <fixture-user> --config <fixture-config>`. Use a nondefault installed root and
   state directory. Verify the child executable is the same executable, its real
   and effective UID/GID and supplementary groups match the OS account database,
   and HOME and `/run/user/<uid>` select the fixture user's manager. Verify root
   read-only `systemctl --user --machine=<fixture-user>@.host show` reaches that
   same manager. Never print the environment or credentials.
2. Inspect ownership of every new/changed intent, lock, spool and registry file:
   all must belong to the worker. Trace native/Docker mutations to child PIDs;
   root must perform only observations. Inject a manager/mutation failure and
   verify durable closed intent precedes discovery, a typed failure is returned,
   and no acceptance/PASS is emitted. Repeat after logout and reboot with linger.
3. Snapshot bytes, modes, owners and modification times before and after status;
   verify there are no writes or lock creations. Confirm the S07 collector runs
   as root; as the worker, real GPU-holder evidence must remain UNKNOWN. Dev's
   explicit fake inventory must still work.
4. Check wrong account/host mapping, foreign-owned state/locks, a symlink in the
   selected path and a missing manager. Each must fail with the matching typed
   error. Do not repair an unknown tree with chown. Verify no fleet token reaches
   child argv/environment, output/errors, or a subsequently launched job.

`--state-dir` overrides only the shared host-control directory. It does not adopt
legacy state, select a different account, or change the installed worker spool,
queue or services. The override must already exist, be owned by the selected
worker, and contain current-format intent with the installed host identity.
Use `--config` for a nondefault installation root/instance. No caller HOME,
MYCE_HOST_STATE_DIR or caller session-bus inference selects privileged identity.
S05 supplies mandatory reconciliation/previous-close settlement to
`HostMutationExecutor`; S08 owns the final status/verdict envelope. The S06
adapter's cleanup confirmation is never the complete local-exclusion PASS.
