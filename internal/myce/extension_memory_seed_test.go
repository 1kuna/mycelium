package myce

import (
	"testing"
	"time"
)

func TestNewVersionSeedsFromPriorAcceptedMemory(t *testing.T) {
	for _, mode := range []string{"reference only", "same model command", "changed model command"} {
		t.Run(mode, func(t *testing.T) {
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
			workPeak, workGrowth := 4.0, .5
			checkPeak, checkGrowth := 2.0, .25
			complete(s, workPeak, workGrowth)
			check := s
			check.AllowedMachines = []string{m.ID}
			check.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
			check.Workload = nil
			check.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
			complete(check, checkPeak, checkGrowth)
			old := registeredExtension(t, q, s.Extension)
			next := old
			next.Version = "2"
			if mode == "changed model command" {
				next.Variants[0].Execution.Main.Command = []string{"sh", "-c", "echo different model"}
			}
			if mode != "reference only" {
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
			candidate := seedMemoryCandidate(t, q, queued, q.s.Machines[m.ID])
			// The seed must cover every comparable accepted row, without
			// adding between-row spread a second time (#1580). Derive the
			// envelope from the authored peaks rather than pinning a stale sum.
			want := checkPeak + checkGrowth
			if mode == "same model command" {
				want = max(want, workPeak+workGrowth)
			}
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
			// A fresh check of this version replaces the older workload
			// prior before its first ordinary workload has completed.
			report.Qualified, report.CheckID = true, run.ID
			m.ExtensionReports[len(m.ExtensionReports)-1] = report
			machineCheck(t, q, m)
			work := s
			work.Extension = extensionKey(next)
			firstWork := submit(t, q, work)
			if got := seedMemoryCandidate(t, q, firstWork, q.s.Machines[m.ID]).Spec.Needs.HostMemoryGB; got != peak+growth {
				t.Fatalf("historical workload hid fresh current-version check: %g", got)
			}
			next.Version = "3"
			next = freezeComposition(t, next)
			if err := q.AddExtension(next); err != nil {
				t.Fatal(err)
			}
			check.Extension, check.ExtensionDigest = extensionKey(next), next.Digest
			third := submit(t, q, check)
			if got := seedMemoryCandidate(t, q, third, q.s.Machines[m.ID]).Spec.Needs.HostMemoryGB; got != peak+growth {
				t.Fatalf("nearest accepted predecessor not selected: %g", got)
			}
		})
	}
}

func TestNewVersionSeedRetainsCurrentVersionOOM(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory, s.Workload = "auto", nil
	s.AllowedMachines = []string{m.ID}
	s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	s.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
	run := startSeedCheck(t, q, m, s)
	clock.Advance(time.Second)
	peak, growth := 2.0, .25
	r := result(&run, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := registeredExtension(t, q, s.Extension)
	next.Version = "2"
	next = freezeComposition(t, next)
	if err := q.AddExtension(next); err != nil {
		t.Fatal(err)
	}
	report := newExtensionReport(next, &m, s.workType())
	report.Setup = true
	m.ExtensionReports = append(m.ExtensionReports, report)
	s.Extension, s.ExtensionDigest = extensionKey(next), next.Digest
	run = startSeedCheck(t, q, m, s)
	clock.Advance(time.Second)
	peak, growth = 8, 1
	r = result(&run, false)
	r.OutOfMemory = true
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth, Events: &MemoryEventCounts{OOM: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	separate := submit(t, q, s)
	if separate.MemoryOOMFloor != nil {
		t.Fatal("new submission unexpectedly has the original job's retry floor")
	}
	candidate := seedMemoryCandidate(t, q, separate, q.s.Machines[m.ID])
	if candidate.Spec.Needs.HostMemoryGB < 9 {
		t.Fatalf("predecessor seed hid retained current-version OOM demand: %+v", candidate.MemoryEnvelope)
	}
}

func TestNewVersionUnifiedSeedRequiresPairedDevicePeak(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	s.Workload = nil
	s.AllowedMachines = []string{m.ID}
	s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	s.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
	run := startSeedCheck(t, q, m, s)
	clock.Advance(time.Second)
	peak := 2.0
	r := result(&run, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for _, point := range q.s.WorkObservations {
		if q.acceptedMemoryPoint(&point) && point.Key.Check && point.MemoryLayout == "unified" && point.MemoryPeak.DeviceGB == nil {
			accepted++
		}
	}
	if accepted == 0 {
		t.Fatal("fixture lost the accepted unpaired host peak")
	}
	next := registeredExtension(t, q, s.Extension)
	next.Version = "2"
	next = freezeComposition(t, next)
	if err := q.AddExtension(next); err != nil {
		t.Fatal(err)
	}
	s.Extension, s.ExtensionDigest = extensionKey(next), next.Digest
	queued := submit(t, q, s)
	target := m.Devices[0].machine(*q.s.Machines[m.ID])
	candidate := seedMemoryCandidate(t, q, queued, &target)
	if candidate.MemoryEnvelope.Source == priorVersionMemorySource || !hostFits(&target, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) {
		t.Fatalf("unpaired predecessor peak stranded the shared-pool probe: %+v", candidate.MemoryEnvelope)
	}
}

func TestCheckSharedSidecarDoesNotProveSharedModel(t *testing.T) {
	e := compositionExtension(t, false)
	v := e.Variants[0]
	v.Execution.Sidecars = []NativeSidecar{{Name: "metrics", Command: []string{"sh", "-c", "echo metrics"}}}
	if checkSharesWorkload(e, v, e, v) {
		t.Fatal("incidental helper made a different check command comparable to the workload")
	}
}

func TestNewVersionSeedMixedVersionOrdering(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory, s.Workload = "auto", nil
	s.AllowedMachines = []string{m.ID}
	s.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
	base := registeredExtension(t, q, s.Extension)
	// The nearest semantic predecessor is neither the newest acceptance nor
	// the opaque version. All are real accepted checks on the same target.
	for i, version := range []string{"legacy", "2", "1"} {
		e := base
		e.Version = version
		e = freezeComposition(t, e)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		report := newExtensionReport(e, &m, s.workType())
		report.Setup = true
		m.ExtensionReports = append(m.ExtensionReports, report)
		s.Extension, s.ExtensionDigest = extensionKey(e), e.Digest
		run := startSeedCheck(t, q, m, s)
		clock.Advance(time.Second)
		peak, growth := float64(6-i), .1
		r := result(&run, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, GrowthGB: &growth}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	base.Version = "3"
	base = freezeComposition(t, base)
	if err := q.AddExtension(base); err != nil {
		t.Fatal(err)
	}
	s.Extension, s.ExtensionDigest = extensionKey(base), base.Digest
	queued := submit(t, q, s)
	for range 50 {
		candidate := seedMemoryCandidate(t, q, queued, q.s.Machines[m.ID])
		if candidate.Spec.Needs.HostMemoryGB != 5.1 {
			t.Fatalf("mixed versions changed the selected predecessor: %+v", candidate.MemoryEnvelope)
		}
	}
}

func startSeedCheck(t *testing.T, q *Queue, m Machine, s Spec) Job {
	t.Helper()
	queued := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != queued.ID {
		t.Fatal("check not assigned", job(t, q, queued.ID).Reason)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return *run
}

func seedMemoryCandidate(t *testing.T, q *Queue, j *Job, m *Machine) *Job {
	t.Helper()
	candidates := extensionJobs(j, m, true)
	if len(candidates) != 1 {
		t.Fatalf("expected one resolved extension variant, got %d", len(candidates))
	}
	return q.memoryCandidate(candidates[0], m)
}
