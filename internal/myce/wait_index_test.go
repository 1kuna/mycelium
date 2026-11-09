package myce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// One wave represents 200 concurrent waiters over 6,500 retained jobs. Idle
// long-polls consume one such wave per 20 seconds, independently of history.
// Real process CPU includes HTTP dispatch/encoding and waiter goroutines.
func BenchmarkWaitIndex200(b *testing.B) {
	b.Run("active", func(b *testing.B) { benchmarkWaitIndex200(b, false) })
	b.Run("idle", func(b *testing.B) { benchmarkWaitIndex200(b, true) })
}

func benchmarkWaitIndex200(b *testing.B, idle bool) {
	clock := &fakeClock{now: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	for i := range 6500 {
		id := fmt.Sprintf("history-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded"}
	}
	for i := range 200 {
		id := fmt.Sprintf("waiting-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "queued"}
	}
	handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
	var before, after syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	b.ResetTimer()
	for range b.N {
		clock.Advance(20 * time.Second)
		var wg sync.WaitGroup
		q.mu.Lock() // Requests cannot depend on or hold this mutex.
		for i := range 200 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("waiting-%08d", i)
				request := WaitRequest{IDs: []string{id}}
				if idle {
					request.Seconds = 1
					request.After = map[string]uint64{id: q.waitStates.jobs[id].Revision}
				}
				body, _ := json.Marshal(request)
				r := httptest.NewRequest("POST", "/api/wait", bytes.NewReader(body))
				r.RemoteAddr = fmt.Sprintf("192.0.2.%d:1234", i)
				r.Header.Set("Authorization", "Bearer fixture")
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Errorf("status %d", w.Code)
				}
			}(i)
		}
		wg.Wait()
		q.mu.Unlock()
	}
	b.StopTimer()
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec)*1000 + float64(r.Utime.Usec+r.Stime.Usec)/1000
	}
	b.ReportMetric((cpu(after)-cpu(before))/float64(b.N), "cpu-ms/200")
	b.ReportMetric(0, "queue-lock-ns/200")
}

// Batch-first consumers must retain deadline/event metadata without projecting
// execution inputs or sharing the queue's mutable timestamp pointers.
