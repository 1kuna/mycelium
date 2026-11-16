package myce

import "time"

// Retained to read the adjacent release's in-progress operations. MY-55 cancels
// undelivered normal checkpoint-stop plans. Explicit critical stops are separate.
type FleetPlannedStop struct {
	Obligation HostObligation `json:"obligation"`
	StopAfter  time.Time      `json:"stop_after"`
	Reason     string         `json:"reason"`
}
