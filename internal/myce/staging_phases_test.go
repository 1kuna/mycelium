package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise real cache admission, expired victim checks, download/verification,
// private placement, queue fencing and persisted attempt history together.
func TestStagingPhasesRecordedInAttemptHistory(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)}
	database, rootPath := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(database, rootPath, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	m := machine("staging", "fake")
	check(t, q, m, nil) // register first
	j := submit(t, q, spec("experiment"))
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("not assigned")
	}
	recorder := &stagingRecorder{clock: clock, value: StagingTimings{StartedAt: clock.Now()}}
	ctx := context.WithValue(context.Background(), stagingRecorderKey{}, recorder)
	root := t.TempDir()
	if err := PrepareStateRoots(root); err != nil {
		t.Fatal(err)
	}
	ref := sharedFixtureFile(t, t.TempDir(), "source", "new-data")
	expired := sharedFixtureFile(t, root, "expired", "expired!")
	victim := sharedFixtureFile(t, root, "victim", "old-data")
	for i, item := range []struct {
		name string
		ref  FileRef
	}{{"expired", expired}, {"victim", victim}} {
		path := filepath.Join(root, item.ref.SHA256)
		if err := os.Rename(filepath.Join(root, item.name), path); err != nil {
			t.Fatal(err)
		}
		at := clock.Now().Add(time.Duration(i-10) * time.Hour)
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, expired.SHA256) {
			w.WriteHeader(410)
			return
		}
		hash := ref.SHA256
		if strings.HasSuffix(r.URL.Path, victim.SHA256) {
			hash = victim.SHA256
		}
		w.Header().Set("ETag", fmt.Sprintf("%q", hash))
		w.Header().Set("Content-Length", "8")
		if r.Method == "GET" {
			_, _ = io.WriteString(w, "new-data")
		}
	}))
	defer server.Close()
	client := Client{URL: server.URL, Clock: clock, HTTP: &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		// Known server-wait latency, separate from the response body read.
		clock.Advance(2 * time.Second)
		res, err := http.DefaultTransport.RoundTrip(r)
		if err == nil && r.Method == "GET" {
			res.Body = &stagingTimedBody{ReadCloser: res.Body, clock: clock}
		}
		return res, err
	})}}
	budget := newWorkerDiskBudget(clock, func(string) (DiskReading, error) {
		return DiskReading{Volume: "fixture", TotalBytes: 100 << 30, FreeBytes: 50 << 30}, nil
	})
	cache := FileCache{Root: root, Limit: 16, Clock: clock, Client: client, budget: budget}
	finish := stagingMeasure(ctx, stageFiles)
	dest := filepath.Join(t.TempDir(), "input")
	if err := cache.copyTo(ctx, ref, dest); err != nil {
		t.Fatal(err)
	}
	// Reuse the same input through the warm path; no additional blob request.
	if err := cache.copyTo(ctx, ref, filepath.Join(t.TempDir(), "input")); err != nil {
		t.Fatal(err)
	}
	finish(nil)
	recorder.complete(clock.Now())
	value := recorder.snapshot()
	if value.CacheHits != 1 || value.CacheMisses != 1 || value.Evicted != 1 {
		t.Fatalf("cache facts: %+v", value)
	}
	for _, phase := range []stagingPhase{stageDiskBudget, stageCacheLock, stageCollectionLock, stageCacheLookup, stageCacheEviction, stageEvictionCheck, stageBlobHead, stageBlobHeaders, stageDownload, stageVerify, stageCachePublish, stagePlacement} {
		if value.Phases[phase].Calls == 0 {
			t.Error("missing phase", stagingPhaseNames[phase])
		}
	}
	if got := value.Phases[stageBlobHeaders]; got.Seconds != 2 || got.Calls != 1 {
		t.Fatal("headers must include server wait only", got)
	}
	if got := value.Phases[stageDownload]; got.Seconds != 3 {
		t.Fatal("body timing", got)
	}
	if got := value.Phases[stageEvictionCheck]; got.Calls != 2 || got.Failures != 1 || got.Seconds != 4 || got.HTTP4xx != 1 || got.Expired != 1 {
		t.Fatal("victim checks", got)
	}
	if data, err := os.ReadFile(dest); err != nil || string(data) != "new-data" {
		t.Fatal("input changed", string(data), err)
	}
	// The same evidence must be rejected from the wrong execution.
	report := AttemptReport{Execution: Execution{ID: a.ID, Attempt: a.Attempt + 1}, Session: a.Session, StagingTimings: value}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.StagingTimings != nil {
		t.Fatal("stale timing accepted")
	}
	report.Attempt = a.Attempt
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	r := result(a, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.StagingTimings != nil || len(got.AttemptHistory) != 1 || got.AttemptHistory[0].StagingTimings == nil {
		t.Fatal("retry lost history or retained live facts", got)
	}
	// Read from SQLite again, then through the real status history HTTP projection.
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(database, rootPath, clock)
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	var history Snapshot
	if err := (Client{URL: api.URL, Token: "test"}).Call(context.Background(), "GET", "/api/status?detail=full", nil, &history); err != nil {
		t.Fatal(err)
	}
	if len(history.Jobs) != 1 || len(history.Jobs[0].AttemptHistory) != 1 || history.Jobs[0].AttemptHistory[0].StagingTimings == nil {
		t.Fatal("full history omitted phases")
	}
	got, err = (Client{URL: api.URL, Token: "test"}).GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(got)
	if err != nil || !strings.Contains(string(data), `"staging_timings"`) || !strings.Contains(string(data), `"name":"eviction_check","calls":2,"seconds":4,"failures":1`) {
		t.Fatal("history JSON lost phases", string(data), err)
	}
	if text := got.AttemptHistory[0].StagingTimings.String(); !strings.Contains(text, "eviction_check=4.000s calls=2 failures=1") {
		t.Fatal(text)
	}
}

type stagingTimedBody struct {
	io.ReadCloser
	clock    *workerFakeClock
	advanced bool
}

func (b *stagingTimedBody) Read(p []byte) (int, error) {
	if !b.advanced {
		b.clock.Advance(3 * time.Second)
		b.advanced = true
	}
	return b.ReadCloser.Read(p)
}
