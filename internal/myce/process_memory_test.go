package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// A worker without cgroups (macOS) samples the attempt's process group.
func processMemoryWorker(t *testing.T, resident uint64) (Worker, *[]map[int]bool) {
	t.Helper()
	w := recoveryWorker(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	var mu sync.Mutex
	var groups []map[int]bool
	w.processMemory = &processMemoryProbe{
		Pressure: func(context.Context) (*gpuHostState, error) {
			return &gpuHostState{available: 64, level: 1}, nil
		},
		Resident: func(ids map[int]bool) (uint64, int, error) {
			mu.Lock()
			defer mu.Unlock()
			groups = append(groups, ids)
			return resident, len(ids), nil
		},
	}
	return w, &groups
}

func runProcessMemoryAttempt(t *testing.T, w Worker) Result {
	t.Helper()
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sleep", "1"})
	j.Spec.Resumable = false
	j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	return d5Finish(t, p)
}

// Host-wide macOS swap or pressure cannot say this attempt was constrained, so
// a darwin attempt must never wipe peaks other hosts learned (#211 review).

func TestProcessGroupResident(t *testing.T) {
	out := []byte("  10   10  1024\n  11   10  2048\n  12   99  4096\n")
	if total, members, err := processGroupResident(out, map[int]bool{10: true}); err != nil || total != 3<<20 || members != 2 {
		t.Fatal("charged another group or lost a member", total, members, err)
	}
	if _, _, err := processGroupResident([]byte("10 10 x\n"), map[int]bool{10: true}); err == nil {
		t.Fatal("malformed inventory became zero")
	}
}

// #210: event counts persist beside the peak; unread counters stay absent.
func TestAttemptMemoryEventCountsPersist(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	a, err := fakeCeilings(t).create("attempt-events", 5<<29, 0)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(a.path, "memory.events")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	write("memory.stat", "anon 1073741824\nkernel 0\nshmem 0\n")
	if p := a.peak(); p == nil || p.Events != nil {
		t.Fatalf("missing memory.events became zero counts: %+v", p)
	}
	stored := func() string {
		t.Helper()
		var data string
		if err := q.db.QueryRow("SELECT data FROM queue_state WHERE key LIKE 'work_observations/%'").Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	complete := func(events string) Job {
		t.Helper()
		j := startObservation(t, q, m, s)
		write("memory.events", events)
		clock.Advance(time.Second)
		r := result(&j, true)
		r.MemoryPeak = a.peak()
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		return j
	}
	// Unpressured samples remain learnable, with read zero counts retained.
	accepted := complete("low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n")
	acceptedPeak := a.peak()
	if data := stored(); !strings.Contains(data, `"events":{"high":0,"max":0,"oom":0,"oom_kill":0,"oom_group_kill":0}`) || strings.Contains(data, `"constrained":true`) {
		t.Fatalf("accepted peak lost its event counts: %s", data)
	}
	// Status must show the memory-only curve's observed peak, rather than its
	// zero legacy host/device usage fields.
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.WorkCurves) != 1 {
		t.Fatalf("status lost memory-only curve: %+v", snap.WorkCurves)
	}
	point := snap.WorkCurves[0]
	if point.MemoryPeak == nil || point.MemoryPeak.Events == nil || point.MemoryPeak.Events.Max != 0 || !strings.Contains(point.Summary(), "peak host 1.00 GiB") {
		t.Fatalf("status lost memory-only facts: %+v; %s", point, point.Summary())
	}
	write("memory.stat", "anon 2684354560\nkernel 0\nshmem 0\n")
	constrained := complete("low 0\nhigh 0\nmax 6264\noom 0\noom_kill 0\noom_group_kill 0\n")
	// Native ceiling pressure invalidates memory even on success. Throughput
	// and exact-attempt diagnostic facts remain independent.
	assertInvalidated := func() {
		t.Helper()
		var learned WorkObservation
		if err := json.Unmarshal([]byte(stored()), &learned); err != nil {
			t.Fatal(err)
		}
		if !learned.MemoryPeak.constrained() || !learned.MemoryRecordedAt.IsZero() || learned.MemoryRange != nil || learned.GPUHostRange != nil || learned.Samples != point.Samples+1 {
			t.Fatalf("constrained success retained accepted memory or lost throughput: %+v", learned)
		}
	}
	assertInvalidated()
	// Both eligible zero counters and rejected nonzero counters persist as job
	// facts after restart, independently of the reusable accepted observation.
	q = memoryFactsRestart(t, q, clock)
	assertInvalidated()
	memoryFactsGET(t, q, accepted, acceptedPeak, false, "eligible")
	memoryFactsGET(t, q, constrained, a.peak(), false)
}
