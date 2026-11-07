package myce

import (
	"encoding/hex"
	"time"
)

func validAttemptIdentity(identity string) bool {
	if len(identity) != 32 {
		return false
	}
	_, err := hex.DecodeString(identity)
	return err == nil
}

// A cleared actor is no longer active custody, but its result, signal, usage,
// launch and cancellation records remain unique attempt evidence. A log policy
// authorizes log expiry only. No age grants deletion of these records.
func pruneAttemptState(_ string, _ time.Time, _ time.Duration) error { return nil }
