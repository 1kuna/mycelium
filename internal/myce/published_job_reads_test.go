package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

// Incident #1052: every cache state, including a fan-out parent, must finish
// from committed inputs while a check-in owns the live transaction mutex.
func TestPublishedJobReadsDuringControlTransaction(t *testing.T) {
	q, clock := fixture(t)
	for _, id := range []string{"warm-receipt", "expired-receipt", "cold-receipt"} {
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment")}
	}
	q.s.Jobs["queued-receipt"] = &Job{ID: "queued-receipt", State: "queued", Queued: clock.Now(), Spec: spec("experiment")}
	parent := &Job{ID: "parent-receipt", State: "running", Queued: clock.Now(), Spec: spec("experiment"), FanOut: &FanOutProgress{}}
	q.s.Jobs[parent.ID] = parent
	for i := range 32 {
		id := fmt.Sprintf("child-%08d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: clock.Now(), Spec: spec("experiment"), Batch: &BatchWork{Parent: parent.ID}}
		parent.FanOut.Children = append(parent.FanOut.Children, id)
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	budgetHost(q, 512<<20, 3)
	handler := (API{Queue: q, Token: "test"}).Handler()
	read := func(id string) (int, Job) {
		r := httptest.NewRequest("GET", "/api/jobs/"+id, nil)
		r.Header.Set("Authorization", "Bearer test")
		ctx, cancel := context.WithTimeout(r.Context(), testWaitTimeout)
		defer cancel()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r.WithContext(ctx))
		var got Job
		if w.Code == 200 {
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Error(err)
			}
		}
		return w.Code, got
	}
	for _, id := range []string{"warm-receipt", "expired-receipt"} {
		if code, _ := read(id); code != 200 {
			t.Fatalf("warm %s: %d", id, code)
		}
	}
	q.jobSnapshotsMu.Lock()
	q.jobSnapshots["expired-receipt"].expires = clock.Now()
	q.jobSnapshotsMu.Unlock()
	q.mu.Lock()
	var unlock sync.Once
	release := func() { unlock.Do(q.mu.Unlock) }
	defer release()
	measured := os.Getenv("MYCE_COMMIT_READ_MEASURE") != ""
	// A finite identical holder is used only for parent/candidate measurement.
	// The regression itself keeps the writer held until every response returns.
	if measured {
		time.AfterFunc(50*time.Millisecond, release)
	}
	type sample struct {
		id      string
		code    int
		job     Job
		elapsed time.Duration
	}
	ids := []string{"warm-receipt", "expired-receipt", "cold-receipt", "queued-receipt", "parent-receipt"}
	results := make(chan sample, 16*len(ids))
	var wg sync.WaitGroup
	for range 16 {
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				started := time.Now()
				code, got := read(id)
				results <- sample{id, code, got, time.Since(started)}
			}(id)
		}
	}
	wg.Wait()
	close(results)
	latencies := map[string][]time.Duration{}
	for r := range results {
		if r.code != 200 || r.job.ID != r.id {
			t.Errorf("writer-held %s: code=%d id=%s", r.id, r.code, r.job.ID)
			continue
		}
		latencies[r.id] = append(latencies[r.id], r.elapsed)
	}
	for _, id := range ids {
		d := latencies[id]
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		if len(d) > 0 {
			t.Logf("job=%s successful=%d p50=%s p99=%s", id, len(d), d[len(d)/2], d[len(d)-1])
		}
	}

	cancelled := httptest.NewRequest("GET", "/api/jobs/cold-receipt", nil)
	cancelled.Header.Set("Authorization", "Bearer test")
	ctx, cancel := context.WithCancel(cancelled.Context())
	cancel()
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, cancelled.WithContext(ctx))
	if w.Code != 503 {
		t.Fatalf("cancelled read treated as invalid input: %d %s", w.Code, w.Body.String())
	}

}
