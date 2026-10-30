package myce

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestFanOutMemoryLearningAndPressure(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	s.Args = []string{"base"}
	s.FanOut = &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "piece", Args: []string{"piece-arg"}}}}
	start := func() *Job {
		t.Helper()
		parent := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.Batch == nil || j.Batch.Parent != parent.ID || j.Batch.Run || !slices.Equal(j.Spec.Args, []string{"base", "piece-arg"}) {
			t.Fatalf("serial child/arguments: %+v", j)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	first := start()
	clock.Advance(time.Second)
	host, total, device := 2.0, 19.918118, 22.318359
	r := result(first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, DeviceGB: &device, Events: &MemoryEventCounts{}}
	stale := r
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) || len(q.s.WorkObservations) != 0 {
		t.Fatalf("stale child learned: %v", err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("clean serial child did not learn: %+v", q.s.WorkObservations)
	}
	for _, p := range q.s.WorkObservations {
		if p.MemoryPeak == nil || !p.MemoryRecordedAt.Equal(clock.Now()) || p.Rate != 0 || p.Seconds != 0 {
			t.Fatalf("memory must learn independently of throughput: %+v", p)
		}
	}
	// Existing ordinary timing must not become a prediction for the child.
	for id, p := range q.s.WorkObservations {
		p.Rate, p.Units, p.Seconds = 10, 10, 1
		q.s.WorkObservations[id] = p
	}
	next := start()
	if timing := job(t, q, next.ID).Timing; timing.PredictedSeconds != nil || timing.ActualSeconds != nil || timing.Reason != "declared wave; serial occupancy is not comparable" {
		t.Fatalf("serial child invented comparable throughput: %+v", timing)
	}
	if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.MemoryEnvelope.HostGB != (total+device)*1.25+.125 {
		t.Fatalf("next piece lost peak/arguments: %+v", next.MemoryEnvelope)
	}
	clock.Advance(time.Second)
	r = result(next, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, DeviceGB: &device, Events: &MemoryEventCounts{Max: 1}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	for _, p := range q.s.WorkObservations {
		if !p.MemoryRecordedAt.IsZero() || !p.MemoryPeak.constrained() {
			t.Fatalf("pressure did not invalidate comparable child memory: %+v", p)
		}
	}
	if facts := job(t, q, next.ID).MemoryAttempts; len(facts) != 1 || !facts[0].MemoryPeak.constrained() {
		t.Fatalf("lost attempt pressure facts: %+v", facts)
	}
	if got := start().MemoryEnvelope.Source; got != "exploratory (previous peak constrained)" {
		t.Fatalf("pressured child remained accepted: %s", got)
	}
}

func TestFanOutAutoMemorySerialQualifiedDevice(t *testing.T) {
	for _, learned := range []bool{true, false} {
		name := "exploratory"
		if learned {
			name = "learned peak"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			e.Kinds = nil
			v := &e.Variants[0]
			v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "xpu"}
			v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
			delete(v.Baselines, "kind:sample")
			v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
			e = freezeComposition(t, e) // No sweep: memory evidence grants no wave.
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			m := m3Machine()
			m.HostMemoryBudgetGB = 24
			m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
			m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "xpu", MemoryBytes: 30 << 30}}
			for i := range m.Devices {
				m.Devices[i].Platform = "xpu"
				m.Devices[i].ExtensionFingerprint = "synthetic-xpu"
				m.Devices[i].TotalMemoryGB, m.Devices[i].BudgetGB, m.Devices[i].FreeMemoryGB = 30, 30, 30
			}
			target := m.Devices[1].machine(m)
			r := newExtensionReport(e, &target, "extension:sample")
			r.Setup, r.Qualified, r.CheckID = true, true, "device1-check"
			m.Devices[1].ExtensionReports = []ExtensionReport{r}
			machineCheck(t, q, m)
			s := Spec{Name: "auto-piece", Project: "synthetic", Priority: "experiment", Extension: extensionKey(e),
				Targets: []ExecutionTarget{v.Target}, Workload: &Workload{InputShape: "small-v1", Units: 10},
				FanOut: &FanOut{MaxConcurrency: 2, Pieces: []FanOutPiece{{Key: "piece"}}}}
			parent := submit(t, q, s)
			target = m.Devices[1].machine(*q.s.Machines[m.ID])
			candidates := extensionJobs(q.fanOutTemplate(q.s.Jobs[parent.ID]), &target, true)
			if len(candidates) != 1 {
				t.Fatal("device1 must qualify before sizing")
			}
			ordinary := candidates[0]
			if ordinary.Spec.Needs.HostMemoryGB != 0 || ordinary.Spec.Needs.MemoryGB != 0 {
				t.Fatal("fixture must have no numeric floors")
			}
			if learned {
				probe := *ordinary
				probe.Machine, probe.Session, probe.AllocatedDevices = m.ID, target.Session, []string{"1"}
				key, why := q.workObservationKey(&probe)
				if key == nil {
					t.Fatal(why)
				}
				host, device := 4.005908966064453, 4.512691497802734
				q.s.WorkObservations = map[string]WorkObservation{}
				q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, Source: "accepted work", Samples: 1,
					RecordedAt: clock.Now(), MemoryRecordedAt: clock.Now(), MemoryLayout: "separate",
					MemoryConfiguration: memoryConfiguration(probe.Spec.Needs, probe.Spec.Args, probe.Spec.Program),
					MemoryPeak:          &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, DeviceGB: &device, Events: &MemoryEventCounts{}},
					MemoryRange:         &MemoryPeakRange{Samples: 2, MinGB: host, MaxGB: host}}
			}
			want := q.memoryCandidate(ordinary, &target)
			if want.MemoryEnvelope.Refusal != "" || want.Spec.Needs.HostMemoryGB <= 0 || want.Spec.Needs.MemoryGB <= 0 {
				t.Fatalf("ordinary sizing must be admissible: %+v", want.MemoryEnvelope)
			}
			if learned && (want.MemoryEnvelope.Source != "accepted peak × 1.25" || want.Spec.Needs.HostMemoryGB != 5.007386207580566 || want.Spec.Needs.MemoryGB != 5.640864372253418) {
				t.Fatalf("peak not reused: %+v", want.MemoryEnvelope)
			}
			if !learned && want.MemoryEnvelope.Source != "exploratory first run" {
				t.Fatalf("missing exploration: %+v", want.MemoryEnvelope)
			}
			run := assignedJob(machineCheck(t, q, m))
			if run == nil {
				t.Fatalf("qualified device1 must receive the serial child: %s", job(t, q, parent.ID).Reason)
			}
			if run.Machine != m.ID || !slices.Equal(run.AllocatedDevices, []string{"1"}) || run.Batch == nil || run.Batch.Run || run.Batch.Concurrency != 1 {
				t.Fatalf("wrong serial placement: %+v", run)
			}
			if run.MemoryEnvelope == nil || *run.MemoryEnvelope != *want.MemoryEnvelope || run.Spec.Needs.HostMemoryGB != want.Spec.Needs.HostMemoryGB || run.Spec.Needs.MemoryGB != want.Spec.Needs.MemoryGB {
				t.Fatalf("child lost ordinary memory sizing: needs=%+v envelope=%+v want=%+v", run.Spec.Needs, run.MemoryEnvelope, want.MemoryEnvelope)
			}
			got := job(t, q, parent.ID)
			if got.FanOut.Next != 1 || !slices.Equal(got.FanOut.Children, []string{run.ID}) {
				t.Fatalf("parent must advance exactly one piece: %+v", got.FanOut)
			}
		})
	}
}
