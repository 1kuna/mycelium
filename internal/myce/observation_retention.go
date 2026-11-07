package myce

import (
	"context"
	"time"
)

// Expire on proven identity drift, never elapsed time. Negative evidence and
// reversible absence still have readers. maintainEvidence adds open-work pins.
func expiredWorkObservation(p WorkObservation, _ time.Time) bool {
	return !p.Forbidden && !p.MemoryPeak.constrained() && p.MemoryEscalation == nil &&
		p.StaleReason != "" && p.StaleReason != "component unavailable" && !memoryIdentityUsable(p)
}

func expiredStorageObservation(p StorageObservation, _ time.Time) bool {
	// These immutable keys are read by storage sizing, including failure floors.
	// Only a proven replacement can retire them; absence of recent work cannot.
	return false
}

// Find candidates under the read lock only, then delete rechecked rows in a
// separate small update. The normal commit removes their SQLite rows.
func (q *Queue) pruneObservations(requestContexts ...context.Context) error {
	return nil // maintainEvidence performs fenced, dependency-aware retirement.
}
