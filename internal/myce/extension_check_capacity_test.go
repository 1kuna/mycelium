package myce

import ()

// Replay #1686's calibrated transfer. The check never ran on the destination:
// a later accepted source check changed a physical first run into a transfer
// larger than the destination's entire RAM. That prediction must not refuse a
// first measurement. Actual local peaks, authored needs and OOM floors still
// prove impossibility, and an unrelated larger target cannot hide that verdict.
