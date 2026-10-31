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

func memoryFactsGET(t *testing.T, q *Queue, j Job, peak *AttemptMemoryPeak, python bool, expectedReason ...string) {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	got, err := c.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	reason := "eligible"
	if peak == nil {
		reason = "peak_unavailable"
	} else if peak.constrained() {
		reason = "constrained"
	} else if peak.PressureUnavailable {
		reason = "pressure_unavailable"
	}
	if len(expectedReason) > 0 {
		reason = expectedReason[0]
	}
	want := []AttemptMemoryFacts{{LearningReason: reason, Attempt: j.Attempt, Machine: j.Machine, Session: j.Session, MemoryPeak: peak, MemoryEnvelope: j.MemoryEnvelope}}
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
	if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatal("accepted total peak lost headroom", next.MemoryEnvelope)
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

func TestAttemptMemoryFactsGB10Accounting(t *testing.T) {
	for _, available := range []bool{true, false} {
		t.Run(map[bool]string{true: "process counters", false: "unavailable"}[available], func(t *testing.T) {
			q, clock, m, s := unifiedAutoFixture(t, 0)
			queued := submit(t, q, s)
			first := assignedJob(machineCheck(t, q, m))
			if first == nil || first.ID != queued.ID {
				t.Fatal("CUDA exploration not assigned")
			}
			if err := q.Started(result(first, false)); err != nil {
				t.Fatal(err)
			}
			// Synchronized read-only GB10 evidence: process device memory can
			// far exceed CURRENT cgroup use. Retain the lifetime cgroup peak
			// independently; neither that peak nor shared RAM proves overlap.
			host := float64(2387402752+53055488+54992896) / (1 << 30)
			total, device, swap := float64(21502136320)/(1<<30), 18556.0/1024, 0.0
			peak := &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, SwapGB: &swap, Events: &MemoryEventCounts{}}
			if available {
				peak.DeviceGB = &device
			} else {
				peak.DeviceUnavailable = deviceMemoryUnsupported
			}
			clock.Advance(time.Second)
			r := result(first, true)
			r.MemoryPeak = peak
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			q = memoryFactsRestart(t, q, clock)
			memoryFactsGET(t, q, *first, peak, !available)
			// Re-establish live component/qualification evidence after restart
			// before requesting reuse; startup deliberately marks it unavailable.
			machineCheck(t, q, m)
			queued = submit(t, q, s)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil || next.ID != queued.ID {
				t.Fatal("next CUDA attempt not assigned")
			}
			if available {
				want := (total+device)*1.25 + m.MemoryCeilingSlackGB
				if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.MemoryEnvelope.HostGB != want || next.Spec.Needs.HostMemoryGB != total*1.25 || next.Spec.Needs.MemoryGB != device*1.25 {
					t.Fatalf("GB10 memory lost a host/device floor: %+v; %+v", next.MemoryEnvelope, next.Spec.Needs)
				}
			} else if next.MemoryEnvelope.Source != "exploratory first run" {
				t.Fatalf("unknown device use enabled a smaller unified charge: %+v", next.MemoryEnvelope)
			}
		})
	}
}

// Native grace-expired cleanup reports its stored peak as release-only, even
// after cancellation. That acknowledgement must preserve the kill-time facts.
func TestAttemptMemoryFactsReleaseOnlyKillGET(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	if _, err := q.CancelJob(j.ID); err != nil {
		t.Fatal(err)
	}
	peak := &AttemptMemoryPeak{Events: &MemoryEventCounts{OOMKill: 1}, Constrained: true}
	r := result(&j, false)
	r.ReleaseOnly, r.Forced, r.MemoryPeak = true, true, peak
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "cancelled" || !got.ReleaseConfirmed {
		t.Fatal("release-only acknowledgement changed cancellation", got)
	}
	q = memoryFactsRestart(t, q, clock)
	memoryFactsGET(t, q, j, peak, false, "release_only")
}

func TestAttemptMemoryFactsRetentionGET(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	submit(t, q, spec("experiment"))
	refused := check(t, q, m, nil).Job
	r := result(refused, false)
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, refused.ID); len(got.MemoryAttempts) != 0 || got.MemoryAttemptsDropped != 0 {
		t.Fatal("peakless prelaunch refusal created a memory row", got)
	}
	const launched = 36
	for i := 1; i <= launched; i++ {
		j := check(t, q, m, nil).Job
		if j == nil || j.Attempt != i+1 {
			t.Fatal("missing assignment", i, j)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		r := result(j, false)
		r.Interrupted, r.PoliteStop = true, true
		if i > 1 {
			host := float64(i)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	q = memoryFactsRestart(t, q, clock)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	got, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), refused.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Starts counts dispatched offers, including the initial prelaunch refusal.
	if len(got.MemoryAttempts) != 32 || got.MemoryAttemptsDropped != 4 || got.Starts != launched+1 || got.Losses != 0 {
		t.Fatalf("incorrect retention count or changed retry charging: rows=%d dropped=%d starts=%d losses=%d", len(got.MemoryAttempts), got.MemoryAttemptsDropped, got.Starts, got.Losses)
	}
	for i, facts := range got.MemoryAttempts {
		wantAttempt := launched - 32 + i + 2
		if i == 0 {
			wantAttempt = 2 // First launched attempt, even without a reading.
		}
		if facts.Attempt != wantAttempt || facts.Machine != m.ID || facts.Session != m.Session || facts.MemoryEnvelope == nil {
			t.Fatalf("wrong retained row %d: %+v", i, facts)
		}
		if i == 0 && facts.MemoryPeak != nil || i > 0 && (facts.MemoryPeak == nil || facts.MemoryPeak.HostGB == nil || *facts.MemoryPeak.HostGB != float64(wantAttempt-1)) {
			t.Fatalf("wrong retained peak %d: %+v", i, facts)
		}
	}
	validateWireValue(t, compileWireSchema(t, "job-v1.schema.json"), got)
}
