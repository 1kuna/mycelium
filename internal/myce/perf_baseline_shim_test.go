package myce

import "testing"

// Baseline-only shim: lets the candidate's release-perf benchmarks build on r6.10.
func startTestDev(t testing.TB, c DevConfig, configure ...func(*Queue)) (*Dev, error) {
	t.Helper()
	return startDev(c, func(q *Queue) {
		for _, fn := range configure {
			fn(q)
		}
	})
}
