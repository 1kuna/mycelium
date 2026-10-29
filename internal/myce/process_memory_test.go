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

func TestProcessMemoryPeakTeachesComparableReruns(t *testing.T) {
	w, groups := processMemoryWorker(t, 2<<30)
	r := runProcessMemoryAttempt(t, w)
	peak := r.MemoryPeak
	if !r.Success || peak == nil || peak.HostGB == nil || *peak.HostGB != 2 || peak.Constrained || peak.Source != processGroupMemorySource || peak.Events != nil {
		data, _ := json.Marshal(r)
		t.Fatalf("darwin attempt did not record an unenforced process-group peak: %s", data)
	}
	if len(*groups) == 0 || len((*groups)[0]) != 1 {
		t.Fatalf("sampler did not target the attempt process group: %+v", *groups)
	}

	q, clock, m, s := observationFixture(t)
	m.MemoryEnforcement = "memory ceiling not enforced on this host (requires Linux cgroup v2)"
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	if first.MemoryEnvelope.Source != "exploratory first run" {
		t.Fatalf("first darwin auto run must explore: %+v", first.MemoryEnvelope)
	}
	clock.Advance(time.Second)
	done := result(&first, true)
	done.MemoryPeak = peak
	if err := q.Complete(done); err != nil {
		t.Fatal(err)
	}
	same := startObservation(t, q, m, s)
	if same.Spec.Needs.HostMemoryGB != 2.5 || same.MemoryEnvelope.Source != "accepted peak × 1.25" {
		t.Fatalf("same-host rerun not sized from the peak: %+v", same.MemoryEnvelope)
	}
	clock.Advance(time.Second)
	if err := q.Complete(result(&same, true)); err != nil {
		t.Fatal(err)
	}

	// A 16 GiB host with 4.6 GiB free after its reserve cannot explore
	// (half the allowance), but fits the same-family estimate.
	m.ID, m.HostID, m.Session = "m4-16", "m4-16-host", "session-m4-16"
	m.CapabilityProfile.SystemMemoryBytes = 16 << 30
	m.HostAvailableMemoryGB = 4.6
	e := first.Extensions[0]
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "m4-16-check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	if _, why := q.exploratoryMemory(&first, q.s.Machines[m.ID]); !strings.Contains(why, "half the host allowance") {
		t.Fatalf("16 GiB host must refuse exploration: %q", why)
	}
	s.AllowedMachines = []string{m.ID}
	queued := submit(t, q, s)
	next := check(t, q, m, nil).Job
	if next == nil {
		t.Fatalf("16 GiB host refused the learned estimate: %s", job(t, q, queued.ID).Reason)
	}
	if next.Spec.Needs.HostMemoryGB != 3.75 || next.MemoryEnvelope.Source != "cross-machine estimate" {
		t.Fatalf("same-family peak must use 1.25 × 1.5: %+v", next.MemoryEnvelope)
	}
	for id, point := range q.s.WorkObservations {
		point.Key.Target.Arch = "arm64"
		q.s.WorkObservations[id] = point
	}
	probe := *q.s.Jobs[next.ID]
	probe.State = "queued"
	if other := q.memoryCandidate(&probe, q.s.Machines[m.ID]); other.Spec.Needs.HostMemoryGB != 5 {
		t.Fatalf("other-family peak must use 1.25 × 2: %+v", other.Spec.Needs)
	}
}

// Host-wide macOS swap or pressure cannot say this attempt was constrained, so
// a darwin attempt must never wipe peaks other hosts learned (#211 review).
func TestProcessMemoryPeakNeverInvalidatesLearnedPeaks(t *testing.T) {
	q, clock, linux, s := observationFixture(t)
	s.Needs.Memory = "auto"
	learn := startObservation(t, q, linux, s)
	clock.Advance(time.Second)
	done := result(&learn, true)
	two := 2.0
	done.MemoryPeak = &AttemptMemoryPeak{HostGB: &two}
	if err := q.Complete(done); err != nil {
		t.Fatal(err)
	}

	darwin := linux
	darwin.ID, darwin.HostID, darwin.Session = "m4", "m4-host", "session-m4"
	darwin.MemoryEnforcement = "memory ceiling not enforced on this host (requires Linux cgroup v2)"
	report := newExtensionReport(learn.Extensions[0], &darwin, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "m4-check"
	darwin.ExtensionReports = []ExtensionReport{report}
	check(t, q, darwin, nil)
	onDarwin := s
	onDarwin.AllowedMachines = []string{darwin.ID}
	squeezed := startObservation(t, q, darwin, onDarwin)
	clock.Advance(time.Second)
	done = result(&squeezed, true)
	six := 6.0
	done.MemoryPeak = &AttemptMemoryPeak{HostGB: &six, Source: processGroupMemorySource, Constrained: true}
	if err := q.Complete(done); err != nil {
		t.Fatal(err)
	}

	onLinux := s
	onLinux.AllowedMachines = []string{linux.ID}
	next := startObservation(t, q, linux, onLinux)
	if next.Spec.Needs.HostMemoryGB != 2.5 || next.MemoryEnvelope.Source != "accepted peak × 1.25" {
		t.Fatalf("darwin attempt invalidated the Linux peak: %+v", next.MemoryEnvelope)
	}
}

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
	complete("low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n")
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
	if point.MemoryPeak == nil || point.MemoryPeak.Events == nil || point.MemoryPeak.Events.Max != 0 || point.HostMemoryGB != 0 || point.DeviceMemoryGB != 0 || !strings.Contains(point.Summary(), "peak host/device 1.00/0.00 GiB") {
		t.Fatalf("status lost memory-only facts: %+v; %s", point, point.Summary())
	}
	write("memory.stat", "anon 2684354560\nkernel 0\nshmem 0\n")
	constrained := complete("low 0\nhigh 0\nmax 6264\noom 0\noom_kill 0\noom_group_kill 0\n")
	if data := stored(); !strings.Contains(data, `"events":{"high":0,"max":6264,"oom":0,"oom_kill":0,"oom_group_kill":0}`) || !strings.Contains(data, `"constrained":true`) {
		t.Fatalf("constrained run did not persist its event counts: %s", data)
	}
	// Constrained provenance can be invalidated in learning, but the job's
	// attempt facts must remain available after restart independently of it.
	q = memoryFactsRestart(t, q, clock)
	memoryFactsGET(t, q, constrained, a.peak(), false)
}
