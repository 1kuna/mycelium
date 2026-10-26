package myce

import (
	"slices"
	"testing"
)

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
					MemoryPeak:          &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}}
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
