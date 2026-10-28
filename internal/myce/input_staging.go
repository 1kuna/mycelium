package myce

import "time"

// Call only after the existing execution/session fences. Worker timestamps are
// diagnostic evidence, never scheduling inputs; missing legacy facts stay nil.
func retainInputsStagedAt(j *Job, at *time.Time) {
	if j.InputsStagedAt == nil && at != nil && !at.IsZero() {
		copy := *at
		j.InputsStagedAt = &copy
	}
}
