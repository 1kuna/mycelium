package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const defaultStatusJobs = 100 // synthetic page capacity, not a runtime default

func setStatusTestCapacity(q *Queue) {
	q.requestBudgets.lanes = 1
	q.requestBudgets.rowTime = 1
	q.requestBudgets.controlTime = defaultStatusJobs
	q.requestBudgets.memory = func(context.Context) (int64, error) {
		return 256 << 20, nil
	}
}

func readStatusOptions(t *testing.T, q *Queue, query string) Snapshot {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/status"+query, nil)
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("%s: %d %s", query, w.Code, w.Body.String())
	}
	var s Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Cursor and time/project filtering need only a few pages, independent of the
// over-limit summary fixture above.
func TestStatusOptionsPaging(t *testing.T) {
	q, c := fixture(t)
	for i := range 12 {
		id := fmt.Sprintf("history-%04d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true,
			Queued: c.Now().Add(-time.Duration(i) * time.Second), Spec: Spec{Project: "history"}}
	}
	q.s.Jobs["other"] = &Job{ID: "other", State: "succeeded", ReleaseConfirmed: true, Queued: c.Now(), Spec: Spec{Project: "other"}}
	// Walk a project-filtered page set, with a timestamp filter and no duplicates.
	seen := map[string]bool{}
	query := "?jobs=all&project=history&limit=3&since=" + url.QueryEscape(c.Now().Add(-6500*time.Millisecond).Format(time.RFC3339Nano))
	for before := ""; ; {
		page := readStatusOptions(t, q, query+before)
		for _, j := range page.Jobs {
			if seen[j.ID] {
				t.Fatal("duplicate page row", j.ID)
			}
			seen[j.ID] = true
		}
		if page.JobsPage.NextBefore == "" {
			break
		}
		before = "&before=" + url.QueryEscape(page.JobsPage.NextBefore)
	}
	if len(seen) != 7 {
		t.Fatalf("lost history: %d", len(seen))
	}
}

// Disk inventory can stall on external storage. The queue must keep accepting
// control transactions while a summary waits for that independent observation.
func TestStatusOptionsFileIOOutsideQueueLock(t *testing.T) {
	q, _ := fixture(t)
	retentionClient(t, q)
	entered, release := make(chan struct{}), make(chan struct{})
	q.files.diskProbe = func(string) (DiskReading, error) {
		close(entered)
		<-release
		return DiskReading{TotalBytes: 100 << 30, FreeBytes: 90 << 30}, nil
	}
	done := make(chan error, 1)
	go func() { _, err := q.cachedStatusJSON(); done <- err }()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	<-entered
	if !q.mu.TryLock() {
		t.Fatal("disk I/O holds the control-plane mutex")
	}
	q.mu.Unlock()
}

// Optional private snapshot replay: only sizes/timings are emitted. It uses an
// empty local usage DB and no production storage or execution payloads.
func BenchmarkStatusOptionsSnapshot(b *testing.B) {
	path := os.Getenv("MYCE_STATUS_BENCHMARK_SNAPSHOT")
	if path == "" {
		b.Skip("set MYCE_STATUS_BENCHMARK_SNAPSHOT to a saved authenticated snapshot")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	var snapshot Snapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		b.Fatal(err)
	}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), &fakeClock{now: snapshot.Now})
	if err != nil {
		b.Fatal(err)
	}
	defer q.Close()
	for _, j := range snapshot.Jobs {
		q.s.Jobs[j.ID] = j
	}
	for _, m := range snapshot.Machines {
		q.s.Machines[m.ID] = m
	}
	q.s.WorkObservations = map[string]WorkObservation{}
	for i, p := range snapshot.WorkCurves {
		q.s.WorkObservations[fmt.Sprint(i)] = p
	}
	q.s.Preemptions = snapshot.Usage.Preemptions
	q.s.ProductionStarts = snapshot.Usage.ProductionStarts
	for _, name := range []string{"full", "summary", "active"} {
		b.Run(name, func(b *testing.B) {
			var options *statusOptions
			if name != "full" {
				options = &statusOptions{jobs: "recent", limit: defaultStatusJobs}
				if name == "active" {
					options.jobs = "active"
				}
			}
			var hold time.Duration
			q.observeRead = func(_ string, _, d time.Duration) { hold += d }
			var bytes int
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s, err := q.publicStatusSnapshot(options)
				if err != nil {
					b.Fatal(err)
				}
				data, err := json.Marshal(s)
				if err != nil {
					b.Fatal(err)
				}
				bytes = len(data)
			}
			b.ReportMetric(float64(bytes), "reply-bytes/op")
			b.ReportMetric(float64(hold.Nanoseconds())/float64(b.N), "lock-ns/op")
		})
	}
}
