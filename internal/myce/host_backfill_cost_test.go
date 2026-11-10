package myce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"
)

// Synthetic incident scale, not a copy of a deployment's queue: 5,665 terminal
// jobs, 141 queued, 91 paused, five running and 1,908 observations. The injected
// queue clock stays fixed; wall time measures handler/SQLite contention only.
func hostBackfillLoadFixture(t testing.TB) (*Queue, []Machine) {
	t.Helper()
	q, workers := candidateCostFixture(t)
	for i := 160; i < 5665; i++ {
		j := cloneJob(q.s.Jobs[fmt.Sprintf("retained-%03d", i%160)])
		j.ID = fmt.Sprintf("retained-%04d", i)
		j.Timing.Execution.ID = j.ID
		q.s.Jobs[j.ID] = &j
	}
	for i := 110; i < 141; i++ {
		j := cloneJob(q.s.Jobs[fmt.Sprintf("queued-%03d", i%110)])
		j.ID = fmt.Sprintf("queued-%03d", i)
		q.s.Jobs[j.ID] = &j
	}
	for i := 0; i < 91; i++ {
		j := cloneJob(q.s.Jobs[fmt.Sprintf("queued-%03d", i%110)])
		j.ID, j.State = fmt.Sprintf("paused-%03d", i), "paused"
		q.s.Jobs[j.ID] = &j
	}
	for i := 1400; i < 1908; i++ {
		q.s.WorkObservations[fmt.Sprint(i)] = q.s.WorkObservations[fmt.Sprint(i%1400)]
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return q, workers
}

// Run with -benchtime=1x -count=1 (and optionally -cpuprofile). A finite wave
// uses five workers at one-second cadence and ten consumers, each making two
// reads. This approximates fan-out, not the unknown incident request rates:
// there are no submissions, uploads, blobs, large inventories or native jobs.
// Small per-route samples report maxima, not production tail estimates.
func BenchmarkHostBackfillControlLoad(b *testing.B) {
	q, workers := hostBackfillLoadFixture(b)
	handler := API{Queue: q, Token: "test"}.Handler()
	var mu sync.Mutex
	var holds []time.Duration
	latencies := map[string][]time.Duration{}
	codes := map[int]int{}
	q.observeUpdate = func(_, hold time.Duration) {
		mu.Lock()
		holds = append(holds, hold)
		mu.Unlock()
	}
	do := func(method, path string, body []byte) int {
		r := httptest.NewRequest(method, path, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		r.Header.Set("Accept-Encoding", "gzip")
		w := httptest.NewRecorder()
		started := time.Now()
		handler.ServeHTTP(w, r)
		elapsed := time.Since(started)
		mu.Lock()
		latencies[method+" "+path] = append(latencies[method+" "+path], elapsed)
		codes[w.Code]++
		mu.Unlock()
		return w.Code
	}
	b.ResetTimer()
	for range b.N {
		var wg sync.WaitGroup
		for i, worker := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tick := time.NewTicker(time.Second)
				defer tick.Stop()
				for range 8 {
					body, err := json.Marshal(Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: worker, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: fmt.Sprintf("running-%d", i), Attempt: 1}, Session: worker.Session, Started: true}}})
					if err != nil {
						b.Error(err)
						return
					}
					if code := do("POST", "/api/workers/checkin", body); code != 200 {
						b.Errorf("checkin status %d", code)
						return
					}
					<-tick.C
				}
			}()
		}
		for i := range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				path := fmt.Sprintf("/api/jobs/queued-%03d", i)
				switch i {
				case 5, 6:
					path += "?view=wait"
				case 7:
					path = "/api/status?detail=full"
				case 8:
					path = "/api/status"
				case 9:
					path = "/api/inspection"
				}
				for range 2 {
					code := do("GET", path, nil)
					if code == 429 {
						return // No retry; honor backoff by ending this finite consumer.
					}
					if code != 200 {
						b.Errorf("%s status %d", path, code)
						return
					}
				}
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
	report := func(name string, values []time.Duration) {
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		if len(values) != 0 {
			b.Logf("%s n=%d p50/p95/max_ms=%.3f/%.3f/%.3f", name, len(values), holdMS(values[(len(values)-1)/2]), holdMS(values[(95*len(values)+99)/100-1]), holdMS(values[len(values)-1]))
		}
	}
	report("checkin hold", holds)
	for path, values := range latencies {
		report(path, values)
	}
	b.Logf("response statuses: %v", codes)
}
