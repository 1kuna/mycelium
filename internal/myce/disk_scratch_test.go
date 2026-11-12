package myce

import (
	"strings"
	"testing"
	"time"
)

// A job that declares more work dir/TMPDIR scratch than a host has free waits
// for, or goes to, a host with room instead of starting and failing inside.
func TestDiskScratchHintKeepsJobOffSmallHost(t *testing.T) {
	q, c := fixture(t)
	small := reportDisk(t, machine("small", "fake", "small-host"), c, "small-volume", []string{"data"}, 100<<30, 50<<30)
	check(t, q, small, nil)
	s := spec("experiment")
	s.Disk = &DiskNeeds{ScratchBytes: 60 << 30}
	j := submit(t, q, s)
	if got := check(t, q, small, nil).Job; got != nil {
		t.Fatalf("scratch-heavy job placed without room: %+v", got)
	}
	held := job(t, q, j.ID)
	if held.State != "queued" || held.DiagnosticCode != "disk_headroom" || !strings.Contains(held.Reason, "disk: 60.0 GiB needed") || !strings.Contains(held.Reason, "free on small-host") {
		t.Fatalf("waiting reason does not name disk: code=%s reason=%s", held.DiagnosticCode, held.Reason)
	}
	big := reportDisk(t, machine("big", "fake", "big-host"), c, "big-volume", []string{"data"}, 400<<30, 200<<30)
	if got := check(t, q, big, nil).Job; got == nil || got.ID != j.ID {
		t.Fatalf("job did not go to the host with room: %+v", job(t, q, j.ID))
	}
}

// After one accepted run the learned scratch peak replaces a smaller hint.
func TestDiskScratchLearnedPeakDrivesAdmission(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	m = reportDisk(t, m, clock, "volume", []string{"data"}, 100<<30, 50<<30)
	check(t, q, m, nil)
	s.Disk = &DiskNeeds{ScratchBytes: 1 << 20}
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	if err := q.Complete(storageResult(&first, out, 60<<30)); err != nil {
		t.Fatal(err)
	}
	if p := onlyStorage(t, q); p.Scratch.Samples != 1 {
		t.Fatalf("accepted peak not learned: %+v", p)
	}
	next := submit(t, q, s)
	q.mu.Lock()
	reason := q.diskHeadroomReason(q.s.Jobs[next.ID], q.s.Machines[m.ID])
	roomy := *q.s.Machines[m.ID]
	q.mu.Unlock()
	if !strings.Contains(reason, "disk: 60.0 GiB needed") {
		t.Fatalf("learned peak did not drive admission: %q", reason)
	}
	if hint := diskHeadroomReason(&Job{Spec: s}, &roomy); hint != "" {
		t.Fatalf("declared hint alone should fit: %s", hint)
	}
	roomy = reportDisk(t, roomy, clock, "volume", []string{"data"}, 400<<30, 200<<30)
	check(t, q, roomy, nil)
	q.mu.Lock()
	reason = q.diskHeadroomReason(q.s.Jobs[next.ID], q.s.Machines[m.ID])
	q.mu.Unlock()
	if reason != "" {
		t.Fatalf("learned peak refused a host with room: %s", reason)
	}
}

// An environment build a host has not cached charges the environment volume
// with the size another host reported for the same key.
func TestDiskScratchUncachedEnvironmentBuild(t *testing.T) {
	q, c := fixture(t)
	key := strings.Repeat("e", 64)
	builder := machine("builder", "fake", "builder-host")
	builder.EnvironmentBytes = map[string]uint64{key: 40 << 30}
	check(t, q, builder, nil)
	cold := reportDisk(t, machine("cold", "fake", "cold-host"), c, "cold-volume", []string{"environment"}, 100<<30, 30<<30)
	check(t, q, cold, nil)
	j := &Job{Spec: spec("experiment"), EnvironmentRecipes: map[string]EnvironmentRecipe{"fake": {Key: key}}}
	q.mu.Lock()
	defer q.mu.Unlock()
	if reason := q.diskHeadroomReason(j, q.s.Machines[cold.ID]); !strings.Contains(reason, "environment") || !strings.Contains(reason, "disk: 40.0 GiB needed") {
		t.Fatalf("uncached build not charged: %q", reason)
	}
	q.s.Machines[cold.ID].Environments = []string{key}
	if reason := q.diskHeadroomReason(j, q.s.Machines[cold.ID]); reason != "" {
		t.Fatalf("cached environment charged again: %s", reason)
	}
}
