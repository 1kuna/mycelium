//go:build !race

package myce

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Opt-in incident replay: MYCE_JOB_GET_LOAD=1 go test -count=1 -v -run TestJobSnapshotPollingLoad ./internal/myce
// The uncached branch is the previous job GET projection, using the same HTTP
// encoding and real SQLite check-ins. Wall time paces traffic only; queue time
// advances through the injected synchronized clock.
func TestJobSnapshotPollingLoad(t *testing.T) {
	if os.Getenv("MYCE_JOB_GET_LOAD") != "1" {
		t.Skip("opt-in load benchmark")
	}
	for _, cached := range []bool{false, true} {
		for _, rate := range []int{30, 120, 240} {
			t.Run(fmt.Sprintf("cached=%v/rate=%d", cached, rate), func(t *testing.T) {
				q, c := fixture(t)
				clock := &workerFakeClock{now: c.Now()}
				q.clock = clock
				var workers []Machine
				for i := range 5 {
					m := machine(fmt.Sprintf("load-worker-%d", i), "fake")
					m.FreeMemoryGB = 0
					check(t, q, m, nil)
					workers = append(workers, m)
				}
				for i := range 6500 {
					id := fmt.Sprintf("history-%04d", i)
					q.s.Jobs[id] = &Job{ID: id, Spec: spec("experiment"), State: "succeeded", ReleaseConfirmed: true, FinishedAt: &c.now, Queued: c.Now()}
				}
				for i := range 30 {
					id := fmt.Sprintf("demand-%04d", i)
					s := spec("experiment")
					s.Run.Command = []string{"echo", strings.Repeat("payload", 128)}
					q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
				}
				q.s.WorkObservations = map[string]WorkObservation{}
				for i := range 1300 {
					q.s.WorkObservations[fmt.Sprint(i)] = WorkObservation{Key: ObservationKey{Work: fmt.Sprint(i), Hardware: strings.Repeat("hardware", 8), Configuration: "config"}, Samples: 10, LastSeconds: 20, RecordedAt: clock.Now()}
				}
				if err := q.update(func() error { return nil }); err != nil {
					t.Fatal(err)
				}
				var captures atomic.Int64
				var copyNS atomic.Int64
				q.observeRead = func(kind string, _, hold time.Duration) {
					if kind == "job" {
						captures.Add(1)
						copyNS.Add(int64(hold))
					}
				}
				var metricsMu sync.Mutex
				var holds, waits []time.Duration
				q.observeUpdate = func(wait, hold time.Duration) {
					metricsMu.Lock()
					waits = append(waits, wait)
					holds = append(holds, hold)
					metricsMu.Unlock()
				}
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var j *Job
					var err error
					if cached {
						j, err = q.jobSnapshot(r.PathValue("id"))
					} else {
						j, err = q.jobSnapshotGeneration(r.PathValue("id"))
					}
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					if err = json.NewEncoder(w).Encode(j); err != nil {
						t.Error(err)
					}
				})
				mux := http.NewServeMux()
				mux.Handle("GET /api/jobs/{id}", handler)
				var before, after syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &before)
				start := time.Now()
				var wg sync.WaitGroup
				// Four seconds includes two TTL windows, with 5 check-ins per second.
				wg.Add(1)
				go func() {
					defer wg.Done()
					tick := time.NewTicker(time.Second / 4)
					defer tick.Stop()
					for range 16 {
						<-tick.C
						clock.Advance(time.Second / 4)
					}
				}()
				wg.Add(1)
				go func() {
					defer wg.Done()
					tick := time.NewTicker(time.Second)
					defer tick.Stop()
					for range 4 {
						<-tick.C
						for _, m := range workers {
							if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
								t.Error(err)
							}
						}
					}
				}()
				tick := time.NewTicker(time.Second / time.Duration(rate))
				for i := range rate * 4 {
					<-tick.C
					id := fmt.Sprintf("demand-%04d", i%30)
					wg.Add(1)
					go func() {
						defer wg.Done()
						r := httptest.NewRequest("GET", "/api/jobs/"+id, nil)
						mux.ServeHTTP(httptest.NewRecorder(), r)
					}()
				}
				tick.Stop()
				wg.Wait()
				elapsed := time.Since(start).Seconds()
				syscall.Getrusage(syscall.RUSAGE_SELF, &after)
				cpu := func(r syscall.Rusage) float64 {
					return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
				}
				meanMax := func(ds []time.Duration) (float64, float64) {
					var total, max time.Duration
					for _, d := range ds {
						total += d
						if d > max {
							max = d
						}
					}
					return float64(total) / float64(len(ds)) / 1e6, float64(max) / 1e6
				}
				hm, hx := meanMax(holds)
				wm, wx := meanMax(waits)
				t.Logf("GET/s=%d captures=%d capture-hold-ms/s=%.3f process-CPU-ms/s=%.3f checkin-hold-mean/max-ms=%.3f/%.3f wait-mean/max-ms=%.3f/%.3f", rate, captures.Load(), float64(copyNS.Load())/1e6/elapsed, 1000*(cpu(after)-cpu(before))/elapsed, hm, hx, wm, wx)
			})
		}
	}
}
