package myce

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func readMutexFixture(t testing.TB) *Queue {
	t.Helper()
	q, _ := candidateCostFixture(t)
	for i := 160; len(q.s.Jobs) < 8000; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := cloneJob(source)
		j.ID = fmt.Sprintf("retained-%04d", i)
		j.Timing.Execution.ID = j.ID
		q.s.Jobs[j.ID] = &j
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	budgetHost(q, 8<<30, 4)
	return q
}

// Measure the actual queue mutex, including locks outside capture. Request
// spans also include response-cache serialization and are not a queue-lock probe.
func TestReadHandlersQueueMutexBound(t *testing.T) {
	q := readMutexFixture(t)
	handler := (API{Queue: q, Token: "test"}).Handler()
	for _, path := range []string{"/api/status", "/api/status?jobs=active", "/api/status?jobs=all&limit=8000", "/api/jobs/queued-001", "/api/jobs/running-1", "/api/jobs/retained-001"} {
		t.Run(path, func(t *testing.T) {
			var total, longest time.Duration
			var holds int
			q.mu.observeHold = func(d time.Duration) { total += d; longest = max(longest, d); holds++ }
			defer func() { q.mu.observeHold = nil }()
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			start := time.Now()
			handler.ServeHTTP(w, r)
			elapsed := time.Since(start)
			if w.Code != 200 {
				t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
			}
			t.Logf("jobs=8000 holds=%d total=%s longest=%s handler=%s bytes=%d", holds, total, longest, elapsed, w.Body.Len())
			// A read must leave nearly all of the one-second heartbeat interval
			// available to control traffic, even for an explicit history response.
			if longest > 25*time.Millisecond {
				t.Errorf("read held queue mutex for %s (bound 25ms)", longest)
			}
		})
	}
}
