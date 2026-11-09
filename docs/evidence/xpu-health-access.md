# Native XPU health access evidence (#1185)

Retained from the bounded read-only diagnosis on 2026-11-09 with xpu-smi 2.2.0.
These excerpts were recovered from the original diagnostic command outputs;
they are not a new device probe or a reconstructed syscall example.

Sanitization removes the SSH transport/account identity, replaces the PCI
address and MEI node ordinal with placeholders, and omits repeated/unrelated
opens and JSON output. `<N>` denotes the same observed node in both excerpts.
Open flags, syscall result, driver path and command exit result are unchanged.

Worker-identity trace, captured at 17:11:45 UTC:

```text
strace -f -e trace=openat -e status=failed xpu-smi health -d <PCI-BDF> -j
openat(AT_FDCWD, "/dev/mei<N>", O_RDWR|O_CLOEXEC) = -1 EACCES (Permission denied)
+++ exited with 0 +++
worker_traced_probe_exit=0
```

Matching driver observation from the separate read-only sysfs enumeration:

```text
/sys/class/mei/mei<N> /sys/bus/auxiliary/drivers/mei_gsc
```

The unmodified health command attempted `O_RDWR|O_CLOEXEC` on a GPU
`mei_gsc` node. Linux checks both read and write permission for that open;
group-read permission alone cannot satisfy this observed invocation. This is
evidence for the access mode used by the command, not proof that MEI alone
caused the unknown temperatures: the same trace also recorded denied PMT
`O_RDONLY` opens, and memory health remained Unknown in the root comparison.

The `0660` grant is **device access, not a health-only API**. Any process with
the worker's primary group can attempt channel operations beyond health
queries, subject to the driver's own controls. The installed root helper
manages permissions; it does not mediate or restrict those channel operations.
A narrower helper that exposes only qualified health queries remains
unimplemented and unqualified future work. Native rollout must still verify
the worker-identity temperature evidence and real-fault automatic clearance.
