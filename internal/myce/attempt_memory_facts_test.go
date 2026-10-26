package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func memoryFactsFile(t *testing.T, a *memoryAttempt, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}

func memoryFactsRestart(t *testing.T, q *Queue, clock Clock) *Queue {
	t.Helper()
	path, root := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func memoryFactsGET(t *testing.T, q *Queue, j Job, peak *AttemptMemoryPeak, python bool) {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	got, err := c.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []AttemptMemoryFacts{{Attempt: j.Attempt, Machine: j.Machine, Session: j.Session, MemoryPeak: peak, MemoryEnvelope: j.MemoryEnvelope}}
	if !reflect.DeepEqual(got.MemoryAttempts, want) {
		t.Fatalf("GET lost attempt memory facts: got %+v, want %+v", got.MemoryAttempts, want)
	}
	var raw map[string]any
	if err := c.Call(context.Background(), "GET", "/api/jobs/"+j.ID, nil, &raw); err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, compileWireSchema(t, "job-v1.schema.json"), raw)
	data := wireJSON(t, raw)
	if strings.Contains(string(data), "private-memory-command") {
		t.Fatal("memory facts bypassed GET payload redaction")
	}
	if python {
		// Exercise Python Client.get against the same real GET, rather than a
		// separate mock that could hide response projection/decoding drift.
		cmd := exec.Command("python3", "-c", `import json, sys
from mycelium import Client
print(json.dumps(Client(sys.argv[1], "test").get(sys.argv[2])["memory_attempts"]))`, server.URL, j.ID)
		cmd.Env = append(os.Environ(), "PYTHONPATH=../../clients/python/src", "PYTHONDONTWRITEBYTECODE=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Python Client.get: %v: %s", err, out)
		}
		var facts []AttemptMemoryFacts
		if err := json.Unmarshal(out, &facts); err != nil || !reflect.DeepEqual(facts, want) {
			t.Fatalf("Python lost attempt facts: %s (%v)", out, err)
		}
	}
}

// #210: a first constrained observation has no comparable learned point. It
// must still survive SQLite reload and job GET, without becoming a learned peak.
func TestAttemptMemoryFactsConstrainedFirstRunGET(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	s.Args = []string{"private-memory-command"}
	j := startObservation(t, q, m, s)
	if len(q.s.WorkObservations) != 0 {
		t.Fatal("regression requires a first run without comparable points")
	}
	a, err := fakeCeilings(t).create("constrained-first", 5<<29, 0)
	if err != nil {
		t.Fatal(err)
	}
	memoryFactsFile(t, a, "memory.stat", "anon 2684354560\nkernel 0\nshmem 0\n")
	memoryFactsFile(t, a, "memory.peak", "3221225472")
	memoryFactsFile(t, a, "memory.events", "high 7\nmax 6264\noom 2\noom_kill 1\noom_group_kill 1\n")
	memoryFactsFile(t, a, "memory.swap.peak", "536870912")
	peak := a.peak()
	if peak == nil || !peak.Constrained || peak.Events == nil || peak.Events.Max != 6264 || peak.SwapGB == nil || *peak.SwapGB != .5 {
		t.Fatalf("missing constrained observations: %+v", peak)
	}
	clock.Advance(time.Second)
	r := result(&j, true)
	r.MemoryPeak = sampledMemoryPeak(nil, peak)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal("completion replay", err)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryPeak != nil {
			t.Fatal("constrained first run seeded learning", point)
		}
	}
	q = memoryFactsRestart(t, q, clock)
	memoryFactsGET(t, q, j, peak, false)
}

func TestAttemptMemoryFactsAcceptedGET(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	j := startObservation(t, q, m, s)
	a, err := fakeCeilings(t).create("accepted", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	memoryFactsFile(t, a, "memory.stat", "anon 1073741824\nkernel 0\nshmem 0\n")
	memoryFactsFile(t, a, "memory.peak", "2147483648")
	// Unread counters stay absent through GET, unlike the read zeros below.
	if err := os.Remove(filepath.Join(a.path, "memory.events")); err != nil {
		t.Fatal(err)
	}
	unread := a.peak()
	if unread.Events != nil || unread.SwapGB != nil {
		t.Fatal("unavailable counters became zeros", unread)
	}
	memoryFactsFile(t, a, "memory.events", "high 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n")
	memoryFactsFile(t, a, "memory.swap.peak", "0")
	peak := sampledMemoryPeak(unread, a.peak())
	if peak.Constrained || peak.Events == nil || peak.SwapGB == nil || *peak.SwapGB != 0 {
		t.Fatal("read zero counters lost", peak)
	}
	clock.Advance(time.Second)
	r := result(&j, true)
	r.MemoryPeak = peak
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, s)
	if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 1.25 {
		t.Fatal("accepted learning changed", next.MemoryEnvelope)
	}
	// A second job with an unread platform reading remains distinct from zero.
	r = result(&next, true)
	r.MemoryPeak = unread
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	q = memoryFactsRestart(t, q, clock)
	memoryFactsGET(t, q, j, peak, true)
	memoryFactsGET(t, q, next, unread, false)
}
