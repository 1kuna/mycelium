package myce

import (
	"context"
	"os"
	"path/filepath"
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
	write("memory.stat", "anon 2684354560\nkernel 0\nshmem 0\n")
	constrained := complete("low 0\nhigh 0\nmax 6264\noom 0\noom_kill 0\noom_group_kill 0\n")
	// A constrained sample supplies a lower bound. It keeps the earlier
	// clean observation and preserves both executions' diagnostic counters.
	q = memoryFactsRestart(t, q, clock)
	key := pooledEvidenceKey(&accepted, &m)
	learned := q.memoryEvidence.exact[key]
	if len(learned.Peaks) != 1 || learned.Peaks[0] != 1 || learned.Lower != 2.5 {
		t.Fatalf("wrong retained evidence: %+v", learned)
	}
	memoryFactsGET(t, q, accepted, acceptedPeak, false, "eligible")
	memoryFactsGET(t, q, constrained, a.peak(), false)
}
