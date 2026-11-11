package myce

import (
	"time"
)

// Expire on proven identity drift, never elapsed time. Negative evidence and
// reversible absence still have readers. maintainEvidence adds open-work pins.
func expiredWorkObservation(p WorkObservation, _ time.Time) bool {
	return !p.Forbidden && !p.MemoryPeak.constrained() && p.MemoryEscalation == nil &&
		p.StaleReason != "" && p.StaleReason != "component unavailable" && !memoryIdentityUsable(p)
}
