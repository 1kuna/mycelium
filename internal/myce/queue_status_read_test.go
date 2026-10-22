package myce

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func retainedStatusQueue(t testing.TB) *Queue {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	// Approximately 37 MB JSON, matching retained jobs, keyed answers and registry
	// at the incident. Public status has no reason to copy frozen extension files.
	e := Extension{Name: "retained", Files: map[string][]byte{"fixture": make([]byte, 45<<10)}}
	q.s.Submissions = map[string]*submissionRecord{}
	q.s.Extensions = map[string]Extension{"retained": e}
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("retained-%03d", i)
		j := &Job{ID: id, State: "succeeded", Queued: clock.Now(), Spec: Spec{Name: "retained", Project: "fixture", Needs: ResourceNeeds{Platform: Platform{Any: true}}}, Extensions: []Extension{e}}
		q.s.Jobs[id] = j
		q.s.Submissions[id] = &submissionRecord{Kind: submissionBinding, JobID: id, Job: j}
	}
	return q
}

func TestQueueStatusReadsDoNotWriteRetainedHistory(t *testing.T) {
	q := retainedStatusQueue(t)
	before, _ := json.Marshal(q.s)
	// Any persistence attempt now fails; status still has to work.
	if _, err := q.db.Exec("PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	s, err := q.publicSnapshot()
	if err != nil || len(s.Jobs) != 300 {
		t.Fatalf("read-only status failed: %v", err)
	}
	after, _ := json.Marshal(q.s)
	if string(before) != string(after) {
		t.Fatal("status mutated durable state")
	}
	for _, j := range s.Jobs {
		if len(j.Extensions) > 0 {
			t.Fatal("private retained payload in status")
		}
	}
	// Exercise the stock Go client's transparent gzip support: the realistic
	// history must remain readable without transferring its repetitive JSON.
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/status", nil)
	req.Header.Set("Authorization", "Bearer test")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var received Snapshot
	if err := json.NewDecoder(response.Body).Decode(&received); err != nil || !response.Uncompressed || len(received.Jobs) != 300 {
		t.Fatalf("compressed read-only status: gzip=%v jobs=%d error=%v", response.Uncompressed, len(received.Jobs), err)
	}
	result := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			if _, err := q.publicSnapshot(); err != nil {
				b.Fatal(err)
			}
		}
	})
	t.Logf("retained JSON=%d; %s; %s", len(before), result.String(), result.MemString())
	if result.AllocedBytesPerOp() > 8<<20 {
		t.Fatalf("status copied retained payload: %s", result.MemString())
	}
}

func BenchmarkQueueStatusRetainedHistory(b *testing.B) {
	q := retainedStatusQueue(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := q.publicSnapshot(); err != nil {
			b.Fatal(err)
		}
	}
}
