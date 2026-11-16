package myce

import (
	"math"
	"strings"
	"testing"
	"time"
)

// Replay #1686's calibrated transfer. The check never ran on the destination:
// a later accepted source check changed a physical first run into a transfer
// larger than the destination's entire RAM. A larger unrelated target must not
// hide the dead end. No floor or learned bound is reduced to make it fit.
func TestExtensionCheckMemoryCapacityDeadEnd(t *testing.T) {
	for _, scenario := range []string{"transfer", "OOM floor", "larger eligible", "larger wrong target", "unknown capacity", "allowance only"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			e := registeredExtension(t, q, s.Extension)
			e.Version = "transfer-capacity"
			v := e.Variants[0]
			v.Target = ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cuda"}
			e.Variants = append(e.Variants, v)
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			s.Extension = extensionKey(e)
			sourceMachine := cloneMachine(&m)
			sourceMachine.ID, sourceMachine.HostID, sourceMachine.Session = "source", "source", "source-session"
			sourceMachine.CapabilityProfile.Arch = "arm64"
			sourceMachine.CapabilityProfile.MemoryLayout = "unified"
			sourceMachine.CapabilityProfile.SystemMemoryBytes = 128 << 30
			sourceMachine.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 31 << 30}}
			sourceMachine.HostMemoryBudgetGB, sourceMachine.HostAvailableMemoryGB = 128, 128
			for i := range sourceMachine.Devices {
				sourceMachine.Devices[i].Platform, sourceMachine.Devices[i].MemoryMode = "cuda", "unified"
				sourceMachine.Devices[i].UnifiedMemory = true
			}
			machineCheck(t, q, sourceMachine)
			sourceTarget := sourceMachine.Devices[0].machine(*q.s.Machines[sourceMachine.ID])
			sourceKey, why := memoryReferenceKey(e, &sourceTarget, "extension:sample")
			if sourceKey == nil {
				t.Fatal(why)
			}
			sourceKey.Check = true
			s.Workload = nil
			s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
			queued := submit(t, q, s)
			target := m.Devices[0].machine(*q.s.Machines[m.ID])
			candidate := extensionJobs(queued, &target, true)[0]
			candidate.Machine, candidate.Session = m.ID, m.Session
			candidate.AllocatedDevices = []string{"0"}
			key, why := q.workObservationKey(candidate)
			if key == nil {
				t.Fatal(why)
			}
			if err := q.update(func() error {
				configuration := memoryConfiguration(candidate.Spec.Needs, candidate.Spec.Args, candidate.Spec.Program)
				add := func(k ObservationKey, configuration, layout string, host, growth float64) {
					device := 1.0
					q.putWorkObservation(identityHash(k), WorkObservation{Key: k, Source: "accepted work", Samples: 1, MemorySamples: 1, RecordedAt: clock.Now(), MemoryRecordedAt: clock.Now(), MemoryLayout: layout, MemoryConfiguration: configuration, MemoryPeak: &AttemptMemoryPeak{HostGB: &host, GrowthGB: &growth, DeviceGB: &device}})
				}
				source := *sourceKey
				add(source, configuration, "unified", 17.72748565673828, .6516458962226749)
				// Two paired work identities calibrate max(ratio) + observed spread.
				for i, pair := range [][2]float64{{5.060905456542969, 10.790241241455078}, {21.220745086669922, 8.982704162597656}} {
					from, to := source, *key
					from.Configuration, to.Configuration = []string{"calibration-a", "calibration-b"}[i], []string{"calibration-a", "calibration-b"}[i]
					add(from, from.Configuration, "unified", pair[0], 0)
					add(to, to.Configuration, "separate", pair[1]-minimumHostMemoryGB, 0)
				}
				q.memoryObservationIndex = nil
				q.evidenceGeneration++
				if scenario == "OOM floor" {
					j := q.touchJob(queued.ID)
					j.OOMRetries = 1
					j.MemoryOOMFloor = &SizeHint{MemoryGB: 1, HostMemoryGB: 70.59160131162189}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			sized := q.memoryCandidate(candidate, &target)
			q.mu.Unlock()
			if math.Abs(sized.Spec.Needs.HostMemoryGB-70.59160131162189) > 1e-9 {
				t.Fatalf("transfer demand=%+v", sized.MemoryEnvelope)
			}
			if scenario == "larger eligible" || scenario == "larger wrong target" {
				larger := cloneMachine(&m)
				larger.ID, larger.HostID, larger.Session = "larger", "larger", "larger-session"
				larger.CapabilityProfile.SystemMemoryBytes = 128 << 30
				larger.HostMemoryBudgetGB, larger.HostAvailableMemoryGB = 128, 128
				if scenario == "larger wrong target" {
					larger.CapabilityProfile.Arch = "arm64"
				}
				machineCheck(t, q, larger)
			}
			if scenario == "unknown capacity" || scenario == "allowance only" {
				if err := q.update(func() error {
					owner := q.touchMachine(m.ID)
					profile := *owner.CapabilityProfile
					profile.SystemMemoryBytes = 0
					if scenario == "allowance only" {
						profile.SystemMemoryBytes = 128 << 30
					}
					owner.CapabilityProfile = &profile
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, queued.ID)
			wantDead := scenario == "transfer" || scenario == "OOM floor" || scenario == "larger wrong target"
			if wantDead {
				if got.State != "failed" || got.DiagnosticCode != deadEndWait || !strings.Contains(got.Reason, "cannot fit on any eligible host") || got.Starts != 0 {
					t.Fatalf("impossible check did not terminate: %+v", got)
				}
				if scenario == "OOM floor" && got.MemoryOOMFloor.HostMemoryGB != 70.59160131162189 {
					t.Fatal("OOM floor lost")
				}
				q = memoryFactsRestart(t, q, clock)
				if got = job(t, q, queued.ID); got.State != "failed" || got.DiagnosticCode != deadEndWait {
					t.Fatal("terminal verdict lost on restart", got)
				}
			} else if got.State != "queued" {
				t.Fatalf("possible/unknown check terminated: %+v", got)
			}
		})
	}
}

func TestExtensionCheckHoldInDependentSummary(t *testing.T) {
	q, _ := fixture(t)
	e, eligible, other := variantsFixture(t, false)
	// The other qualified variant is deliberately outside the job's target.
	variant := e.Variants[0]
	variant.Target.Arch = "arm64"
	e.Variants = append(e.Variants, variant)
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	eligible.ExtensionReports = nil
	other.GPU = "fake"
	other.TotalMemoryGB = 32
	other.CapabilityProfile.Arch = "arm64"
	report := newExtensionReport(e, &other, "extension:"+e.Name)
	report.Setup, report.Qualified, report.CheckID = true, true, "other-check"
	other.ExtensionReports = []ExtensionReport{report}
	check(t, q, eligible, nil)
	check(t, q, other, nil)
	s := variantSpec(e, "experiment")
	s.Targets = []ExecutionTarget{machineTarget(&eligible)}
	demand := submit(t, q, s)
	checkSpec := s
	checkSpec.Name, checkSpec.Project = "extension-check", "extension-check"
	checkSpec.AllowedMachines = []string{eligible.ID}
	checkSpec.ExtensionCheck = &ExtensionCheckRequest{Work: s.workType(), Directory: t.TempDir()}
	pending := submit(t, q, checkSpec)
	if err := q.update(func() error {
		j := q.touchJob(pending.ID)
		r := newExtensionReport(e, &eligible, s.workType())
		j.Spec.ExtensionCheck.Automatic = &r
		// The live stored check has no projected reason; the dependent must resolve it.
		j.Reason = ""
		floor := 70.59160131162189
		j.Spec.Needs.Memory = "auto"
		j.MemoryFallback = &SizeHint{MemoryGB: 1, HostMemoryGB: floor}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(demand.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hold == nil || got.Hold.Kind != "native_qualification" || !strings.Contains(got.Reason, ShortID(pending.ID)) || !strings.Contains(got.Reason, memoryEvidenceRecovering) || strings.Contains(got.Reason, other.ID+" needs") {
		t.Fatalf("dependent summary lost real hold: reason=%s hold=%+v", got.Reason, got.Hold)
	}
	if !strings.Contains(got.Hold.Detail, memoryEvidenceRecovering) {
		t.Fatal("hold detail retained unrelated variant", got.Hold)
	}
}
