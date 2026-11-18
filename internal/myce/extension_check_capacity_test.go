package myce

import (
	"math"
	"strings"
	"testing"
	"time"
)

// Replay #1686's calibrated transfer. The check never ran on the destination:
// a later accepted source check changed a physical first run into a transfer
// larger than the destination's entire RAM. That prediction must not refuse a
// first measurement. Actual local peaks, authored needs and OOM floors still
// prove impossibility, and an unrelated larger target cannot hide that verdict.
func TestExtensionCheckMemoryCapacityDeadEnd(t *testing.T) {
	for _, scenario := range []string{"transfer", "workload transfer", "local fitting", "local constrained fits", "local accepted", "local constrained", "declared", "OOM floor", "larger eligible", "larger wrong target", "unknown capacity", "allowance only"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			capacity := 31.206165313720703
			m.CapabilityProfile.SystemMemoryBytes = uint64(capacity * (1 << 30))
			m.HostAvailableMemoryGB = 11.36440658569336
			machineCheck(t, q, m)
			e := registeredExtension(t, q, s.Extension)
			e.Version = "transfer-capacity"
			if scenario == "declared" {
				e.Variants[0].Execution.Main.MemoryGB = 70.59160131162189
			}
			v := e.Variants[0]
			v.Target = ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cuda"}
			e.Variants = append(e.Variants, v)
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			// The native check must cross ordinary preparation before it can
			// run; the fixture's original report names the previous version.
			for i := range m.Devices {
				target := m.Devices[i].machine(m)
				report := newExtensionReport(e, &target, "extension:sample")
				report.Setup = true
				if scenario == "workload transfer" {
					report.Qualified, report.CheckID = true, "check"
				}
				m.Devices[i].ExtensionReports = []ExtensionReport{report}
			}
			machineCheck(t, q, m)
			s.Extension, s.ExtensionDigest = extensionKey(e), e.Digest
			s.AllowedMachines = []string{m.ID}
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
			if scenario != "workload transfer" {
				s.Workload = nil
				s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			}
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
					to.Check = true
					from.Configuration, to.Configuration = []string{"calibration-a", "calibration-b"}[i], []string{"calibration-a", "calibration-b"}[i]
					add(from, from.Configuration, "unified", pair[0], 0)
					add(to, to.Configuration, "separate", pair[1]-minimumHostMemoryGB, 0)
				}
				if strings.HasPrefix(scenario, "local ") {
					host := 70.59160131162189
					if scenario == "local fitting" {
						host = 9.5
					} else if scenario == "local constrained fits" {
						host = 15
					}
					add(*key, configuration, "separate", host, 0)
					if strings.Contains(scenario, "constrained") {
						point := q.s.WorkObservations[identityHash(*key)]
						point.MemoryPeak.Constrained = true
						q.putWorkObservation(identityHash(*key), point)
					}
				}
				q.memoryObservationIndex = nil
				q.evidenceGeneration++
				if scenario == "OOM floor" || scenario == "larger eligible" || scenario == "larger wrong target" || scenario == "unknown capacity" || scenario == "allowance only" {
					j := q.touchJob(queued.ID)
					j.OOMRetries = 1
					j.MemoryOOMFloor = &SizeHint{MemoryGB: 1, HostMemoryGB: 70.59160131162189}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			// Retain proof of the original prediction; revised sizing must not
			// install it as a local physical requirement.
			estimate := q.memoryEstimate(candidate, &target)
			sized := q.memoryCandidate(candidate, &target)
			q.mu.Unlock()
			if scenario == "transfer" || scenario == "workload transfer" {
				if math.Abs(estimate.measuredHostGB-70.59160131162189) > 1e-9 {
					t.Fatalf("fixture no longer reproduces transfer: %+v", estimate)
				}
				if e := sized.MemoryEnvelope; e.HostGB < 10.790241241455078 || e.HostGB > 12 || e.Basis != "exploratory" || e.Refusal != "" || !strings.Contains(e.Source, checkTransferProbe) || e.Enforcement != m.MemoryEnforcement {
					t.Fatalf("weak transfer still blocks guarded native measurement: %+v", e)
				}
			}
			if scenario == "local fitting" && (sized.MemoryEnvelope.Basis != "learned" || math.Abs(sized.Spec.Needs.HostMemoryGB-(9.5+minimumHostMemoryGB)) > 1e-9) {
				t.Fatalf("transferred estimate overrode direct destination measurement: %+v", sized.MemoryEnvelope)
			}
			if scenario == "local constrained fits" && sized.Spec.Needs.HostMemoryGB < 15 {
				t.Fatalf("probe lost destination lower bound: %+v", sized.MemoryEnvelope)
			}
			if scenario == "larger wrong target" {
				larger := cloneMachine(&m)
				larger.ID, larger.HostID, larger.Session = "larger", "larger", "larger-session"
				larger.CapabilityProfile.SystemMemoryBytes = 128 << 30
				larger.HostMemoryBudgetGB, larger.HostAvailableMemoryGB = 128, 128
				larger.CapabilityProfile.Arch = "arm64"
				machineCheck(t, q, larger)
			}
			if scenario == "unknown capacity" || scenario == "allowance only" || scenario == "larger eligible" {
				if err := q.update(func() error {
					owner := q.touchMachine(m.ID)
					profile := *owner.CapabilityProfile
					profile.SystemMemoryBytes = 0
					if scenario != "unknown capacity" {
						profile.SystemMemoryBytes = 128 << 30
					}
					if scenario == "larger eligible" {
						owner.HostMemoryBudgetGB = 128
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
			wantDead := scenario == "local accepted" || scenario == "local constrained" || scenario == "declared" || scenario == "OOM floor" || scenario == "larger wrong target"
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
			if scenario == "transfer" {
				run := assignedJob(machineCheck(t, q, m))
				if run == nil || run.ID != queued.ID || run.MemoryEnvelope.HostGB > 12 {
					t.Fatalf("guarded native check not runnable: %+v; %s", run, job(t, q, queued.ID).Reason)
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				// A real failure after the probe becomes negative evidence. Its
				// floor survives restart; the transfer fallback cannot erase it.
				clock.Advance(time.Second)
				r := result(run, false)
				peak := run.Spec.Needs.HostMemoryGB
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: true, Events: &MemoryEventCounts{OOM: 1}}
				r.OutOfMemory = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				q = memoryFactsRestart(t, q, clock)
				recovered := job(t, q, queued.ID)
				if recovered.MemoryOOMFloor == nil || recovered.MemoryOOMFloor.HostMemoryGB < peak || recovered.OOMRetries == 0 {
					t.Fatalf("real OOM lost after guarded probe: %+v", recovered.MemoryOOMFloor)
				}
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
	checkSpec.ExtensionDigest = e.Digest
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
