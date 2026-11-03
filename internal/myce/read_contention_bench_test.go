//go:build !race

package myce

import (
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// Run with -benchtime=1x. The injected queue clock stays frozen; wall timers
// drive offered HTTP load only. A timing observer deliberately extends the
// actual Checkin update's q.mu hold, modeling the incident's slow writer.
// This benchmark also compiles unchanged on the parent commit for comparison.
func BenchmarkJobGETWriterContention(b *testing.B) {
	for _, delay := range []time.Duration{0, 100 * time.Millisecond, time.Second} {
		for _, rate := range []int{0, 124, 248} {
			b.Run(fmt.Sprintf("hold=%s/GETs=%d", delay, rate), func(b *testing.B) {
				clock := &fakeClock{now: time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)}
				q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock, WithCheckpointAvailability(func() bool { return true }))
				if err != nil {
					b.Fatal(err)
				}
				defer q.Close()
				m := machine("writer", "fake")
				if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
					b.Fatal(err)
				}
				var ids []string
				for range 31 {
					j, err := q.Submit(spec("experiment"))
					if err != nil {
						b.Fatal(err)
					}
					ids = append(ids, j.ID)
				}
				a, err := q.Checkin(Checkin{Machine: m})
				if err != nil || a.Job == nil {
					b.Fatal("assignment", err)
				}
				if err := q.Started(result(a.Job, true)); err != nil {
					b.Fatal(err)
				}
				contact := Checkin{Machine: m, Running: &Execution{ID: a.Job.ID, Attempt: a.Job.Attempt}}
				handler := API{Queue: q, Token: "test"}.Handler()
				var waits, holds, gets []time.Duration
				var mu sync.Mutex
				q.observeUpdate = func(wait, hold time.Duration) {
					start := time.Now()
					if delay > 0 {
						time.Sleep(delay)
					}
					waits, holds = append(waits, wait), append(holds, hold+time.Since(start))
				}
				var readers sync.WaitGroup
				read := func(i int) {
					defer readers.Done()
					r := httptest.NewRequest("GET", "/api/jobs/"+ids[i%len(ids)], nil)
					r.Header.Set("Authorization", "Bearer test")
					w := httptest.NewRecorder()
					start := time.Now()
					handler.ServeHTTP(w, r)
					elapsed := time.Since(start)
					mu.Lock()
					gets = append(gets, elapsed)
					mu.Unlock()
					if w.Code != 200 {
						b.Errorf("GET: %d", w.Code)
					}
				}
				b.ResetTimer()
				for range b.N {
					stop := make(chan struct{})
					loadDone := make(chan struct{})
					go func() {
						defer close(loadDone)
						if rate == 0 {
							return
						}
						ticker := time.NewTicker(time.Second / time.Duration(rate))
						defer ticker.Stop()
						for i := 0; ; i++ {
							select {
							case <-stop:
								return
							case <-ticker.C:
								readers.Add(1)
								go read(i)
							}
						}
					}()
					until := time.Now().Add(1100 * time.Millisecond)
					for time.Now().Before(until) {
						if _, err := q.Checkin(contact); err != nil {
							b.Fatal(err)
						}
						time.Sleep(20 * time.Millisecond)
					}
					close(stop)
					<-loadDone
					readers.Wait()
				}
				b.StopTimer()
				report := func(name string, samples []time.Duration) {
					if len(samples) == 0 {
						return
					}
					sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
					var total time.Duration
					for _, d := range samples {
						total += d
					}
					b.ReportMetric(float64(total.Microseconds())/float64(len(samples)), name+"-mean-us")
					b.ReportMetric(float64(samples[(95*len(samples)-1)/100].Microseconds()), name+"-p95-us")
				}
				report("GET", gets)
				report("checkin-hold", holds)
				report("checkin-wait", waits)
				b.ReportMetric(float64(len(gets))/b.Elapsed().Seconds(), "GET/s")
			})
		}
	}
}
