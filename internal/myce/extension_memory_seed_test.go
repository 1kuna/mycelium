package myce

import (
	"testing"
	"time"
)

func TestNewVersionSeedsFromPriorAcceptedMemory(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "reference only", true: "same model command"}[shared], func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = "auto"
			complete := func(spec Spec, peak, growth float64) {
				run := startObservation(t, q, m, spec)
				clock.Advance(time.Second)
				r := result(&run, true)
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, GrowthGB: &growth}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			complete(s, 4, .5)
			check := s
			check.AllowedMachines = []string{m.ID}
			check.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
			check.Workload = nil
			check.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
			complete(check, 2, .25)
			old := registeredExtension(t, q, s.Extension)
			next := old
			next.Version = "2"
			if shared {
				next.Variants[0].Check = next.Variants[0].Execution.Main.Command
			}
			next = freezeComposition(t, next)
			if err := q.AddExtension(next); err != nil {
				t.Fatal(err)
			}
			report := newExtensionReport(next, &m, s.workType())
			report.Setup = true
			m.ExtensionReports = append(m.ExtensionReports, report)
			check.Extension, check.ExtensionDigest = extensionKey(next), next.Digest
			queued := submit(t, q, check)
			candidate := q.memoryCandidate(queued, q.s.Machines[m.ID])
			want := 2.25
			if shared {
				want = 6
			} // 4 GiB peak + the measured 4-2 GiB range.
			if e := candidate.MemoryEnvelope; e.Source != priorVersionMemorySource || e.Basis != "exploratory" || candidate.Spec.Needs.HostMemoryGB != want {
				t.Fatalf("new version lost accepted predecessor evidence: %+v needs=%+v want=%g", e, candidate.Spec.Needs, want)
			}
			// Accepted evidence of the nearest version replaces the older,
			// larger prior, rather than accumulating every historical version.
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID {
				t.Fatal("seeded check not assigned", job(t, q, queued.ID).Reason)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			peak, growth := 1.0, .1
			r := result(run, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, CgroupGB: &peak, GrowthGB: &growth}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			next.Version = "3"
			next = freezeComposition(t, next)
			if err := q.AddExtension(next); err != nil {
				t.Fatal(err)
			}
			check.Extension, check.ExtensionDigest = extensionKey(next), next.Digest
			third := submit(t, q, check)
			if got := q.memoryCandidate(third, q.s.Machines[m.ID]).Spec.Needs.HostMemoryGB; got != 1.1 {
				t.Fatalf("nearest accepted predecessor not selected: %g", got)
			}
		})
	}
}
