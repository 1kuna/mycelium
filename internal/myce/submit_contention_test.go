package myce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Replay the incident's 31.4 s writer backlog with five occupied control turns.
// The delay is injected service occupancy, not a claim about individual real
// check-in body cost. synctest advances fake time; this takes no wall sleeps.
// Without separate writer turns the HTTP submit waits behind the whole backlog.
func TestSubmitContentionIncidentBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _ := fixture(t)
		// This test proves production receipts stay independent of evidence builds.
		asynchronousAdmissionWait(t, q)
		budgetHost(q, 64<<20, 1)
		handler := API{Queue: q, Token: "test"}.Handler()
		q.mu.Lock()
		var workers sync.WaitGroup
		failures := make(chan error, 5)
		for range 5 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				failures <- q.update(func() error { time.Sleep(31400 * time.Millisecond / 5); return nil })
			}()
			synctest.Wait()
		}
		s := spec("production")
		s.SubmissionKey = "incident-backlog"
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(data))
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		done := make(chan struct{})
		started := time.Now()
		go func() { handler.ServeHTTP(w, r); close(done) }()
		synctest.Wait()
		q.mu.Unlock()
		<-done
		elapsed := time.Since(started)
		workers.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
		if w.Code != 200 || elapsed >= 20*time.Second {
			t.Fatalf("production submit: status=%d elapsed=%s body=%s", w.Code, elapsed, w.Body.String())
		}
		t.Logf("31.4s control backlog: production receipt in %s, 503s=0", elapsed)
	})
}

// Real HTTP handlers, SQLite FULL commits and the incident's retained-history
// shape. Five independent workers check in continuously without pacing; submits
// arrive serially, as in a consumer batch. No fleet or private data is used.
func BenchmarkSubmitContention(b *testing.B) {
	q, workers := candidateCostFixture(b)
	asynchronousAdmissionWait(b, q)
	admissionWaitBenchmarkSettle(q)
	q.mu.Lock()
	for i := 160; i < 6500; i++ {
		source := q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)]
		j := *source
		j.ID = fmt.Sprintf("retained-%04d", i)
		timing := *source.Timing
		timing.Execution.ID = j.ID
		j.Timing = &timing
		q.s.Jobs[j.ID] = &j
	}
	q.mu.Unlock()
	if err := q.update(func() error { return nil }); err != nil {
		b.Fatal(err)
	}
	// Fix measured memory headroom across before/after; the actual OS probe's
	// error/refusal behavior has separate deterministic coverage.
	budgetHost(q, 8<<30, 4)
	var mu sync.Mutex
	samples := map[string][]time.Duration{}
	q.observeRequest = func(route string, total, wait, hold, after time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		if route == "POST /api/jobs" {
			for name, value := range map[string]time.Duration{"submit": total, "mutex-wait": wait, "mutex-hold": hold, "after-unlock": after} {
				samples[name] = append(samples[name], value)
			}
		} else if route == "POST /api/workers/checkin" {
			samples["checkin"] = append(samples["checkin"], total)
		}
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	request := func(path string, value any) (int, string) {
		data, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		r := httptest.NewRequest("POST", path, bytes.NewReader(data))
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	stop := make(chan struct{})
	var running, ready sync.WaitGroup
	errors := make(chan string, len(workers))
	for i, m := range workers {
		running.Add(1)
		ready.Add(1)
		go func() {
			defer running.Done()
			first := true
			for {
				select {
				case <-stop:
					return
				default:
				}
				code, body := request("/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: m.Session, Started: true}}})
				if first {
					ready.Done()
					first = false
				}
				if code != 200 {
					errors <- fmt.Sprintf("checkin: %d %s", code, body)
					return
				}
			}
		}()
	}
	ready.Wait()
	var failures, unavailable int
	b.ResetTimer()
	for i := range b.N {
		s := cpuSpec(2, 2)
		s.Project, s.Name, s.Priority = "submit-benchmark", "production", "production"
		s.SubmissionKey = fmt.Sprintf("submit-%d", i)
		code, body := request("/api/jobs", s)
		if code == 503 {
			unavailable++
		}
		if code != 200 {
			failures++
			b.Logf("submit: %d %s", code, body)
		}
	}
	b.StopTimer()
	close(stop)
	running.Wait()
	admissionWaitBenchmarkSettle(q)
	close(errors)
	for err := range errors {
		b.Error(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for name, values := range samples {
		slices.Sort(values)
		for _, p := range []int{50, 99} {
			at := max(0, int(math.Ceil(float64(len(values))*float64(p)/100))-1)
			b.ReportMetric(float64(values[at])/float64(time.Millisecond), fmt.Sprintf("%s-p%d-ms", name, p))
			if name == "submit" && p == 99 && values[at] >= 20*time.Second {
				b.Errorf("submit p99 %s exceeded consumer budget", values[at])
			}
		}
	}
	b.ReportMetric(float64(unavailable), "503s")
	b.ReportMetric(float64(failures), "failures")
	b.ReportMetric(float64(len(samples["checkin"])), "checkins")
	if failures != 0 {
		b.Errorf("%d submissions failed (%d HTTP 503)", failures, unavailable)
	}
}
