# Retired migrations

Split-worker conversion and rollback are below the [supported release floor](../DECISIONS.md#my-42). Their code and procedures remain in source history. Current installs use one worker per machine.

## Migrate storage guards to automatic admission

The unfinished migration is parked on [`park/storage-protection`](https://github.com/1kuna/mycelium/tree/park/storage-protection).
`myce file admission` and its activation/rollback endpoints are removed from main.
Retained activation metadata cannot disable configured caps/reserve/floors.
See [the current storage behavior](file-store.md#parked-native-storage-integration-564) and
[#504](https://github.com/1kuna/mycelium/issues/504) for the backend qualification
and preservation requirements before any future replacement.
