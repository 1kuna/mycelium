package myce

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func readMutexFixture(t testing.TB, retained int) *Queue {
	t.Helper()
	q, _ := candidateCostFixture(t)
	q.mu.Lock()
	for i := 160; len(q.s.Jobs) < retained; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := cloneJob(source)
		j.ID = fmt.Sprintf("retained-%04d", i)
		j.Timing.Execution.ID = j.ID
		q.s.Jobs[j.ID] = &j
	}
	q.mu.Unlock()
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	// Complete the active async capture before installing lock observers.
	q.admissionWaitBuild.mu.Lock()
	done := q.admissionWaitBuild.done
	q.admissionWaitBuild.mu.Unlock()
	if done != nil {
		<-done
	}
	budgetHost(q, 8<<30, 4)
	return q
}

var readMutexPaths = []string{"/api/status", "/api/status?jobs=active", "/api/status?jobs=all&limit=8000", "/api/jobs/queued-001", "/api/jobs/running-1", "/api/jobs/retained-001"}

// Retained history must not add queue-lock turns, and response I/O must never
// own the queue mutex. Unlike elapsed time, these checks are independent of
// descheduling and race-instrumentation overhead on a busy shared host.
func TestReadHandlersQueueMutexBound(t *testing.T) {
	// Lock turns and response ownership do not depend on the incident scale.
	// The benchmark retains the full 8k history.
	q := readMutexFixture(t, 480)
	handler := (API{Queue: q, Token: "test"}).Handler()
	for _, path := range readMutexPaths {
		t.Run(path, func(t *testing.T) {
			holds := 0
			q.mu.observeHold = func(time.Duration) { holds++ }
			defer func() { q.mu.observeHold = nil }()
			w := &readMutexResponse{ResponseRecorder: httptest.NewRecorder(), check: func() {
				// Probe the actual RWMutex without adding an observed queue-lock turn.
				if !q.mu.mu.TryLock() {
					t.Error("response I/O holds the queue mutex")
					return
				}
				q.mu.mu.Unlock()
			}}
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer test")
			handler.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
			}
			if holds > 1 {
				t.Errorf("read acquired queue mutex %d times; budget 1", holds)
			}
		})
	}
}

type readMutexResponse struct {
	*httptest.ResponseRecorder
	check func()
}

func (w *readMutexResponse) WriteHeader(code int) { w.check(); w.ResponseRecorder.WriteHeader(code) }
func (w *readMutexResponse) Write(data []byte) (int, error) {
	w.check()
	return w.ResponseRecorder.Write(data)
}

// Run explicitly under controlled host load. Preserve #1212's actual mutex
// timing bound, including locks outside capture, without making a normal full
// suite fail when the OS deschedules its lock holder. Bulk-decode/control
// exclusion remains covered by TestReadSnapshotBulkWorkDoesNotHoldQueue.
func BenchmarkReadHandlersQueueMutexBound(b *testing.B) {
	q := readMutexFixture(b, 8000)
	handler := (API{Queue: q, Token: "test"}).Handler()
	for _, path := range readMutexPaths {
		b.Run(path, func(b *testing.B) {
			var total, longest time.Duration
			q.mu.observeHold = func(d time.Duration) { total += d; longest = max(longest, d) }
			defer func() { q.mu.observeHold = nil }()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r := httptest.NewRequest("GET", path, nil)
				r.Header.Set("Authorization", "Bearer test")
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Fatalf("HTTP %d: %s", w.Code, w.Body.String())
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "hold-ns/op")
			b.ReportMetric(float64(longest.Nanoseconds()), "max-hold-ns")
			// Keep nearly all of the one-second heartbeat interval available.
			if longest > 25*time.Millisecond {
				b.Errorf("read held queue mutex for %s (bound 25ms)", longest)
			}
		})
	}
}
