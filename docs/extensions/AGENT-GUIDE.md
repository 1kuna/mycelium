# Extension authoring reference

For new reusable integrations, read [the adapter and extension reference](../adapter.md).
It contains the recommended one-file path, complete native variants, baseline
recording/replay, registry callers and the unchanged-app example. This path is
retained because current `myce extension init` output points here.

Use the [qualification error contract](../adapter.md#report-a-qualification-error)
to surface a bounded diagnostic on the useful job when an automatic check fails.
