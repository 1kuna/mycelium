package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func incidentGuardJob() Job {
	return Job{Spec: Spec{Needs: ResourceNeeds{Memory: "auto", HostMemoryGB: 3.9251251220703125, Platform: Platform{Required: []string{"cpu"}}}}, MemoryEnvelope: &JobMemoryEnvelope{Basis: "learned", Enforcement: learnedMemoryGuardEnforcement}}
}

// Replay the recorded overage, without claiming the missing historical host
// pressure was known. The two host states establish the intended stop contract.
func TestLearnedMemoryGuardIncidentHeadroom(t *testing.T) {
	for _, mode := range []string{"room", "pressure", "growth headroom", "swap", "declared", "host allowance", "unknown", "shared RSS", "inventory gap"} {
		t.Run(mode, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 11, 6, 6, 32, 17, 0, time.UTC)}
			w := Worker{Machine: Machine{CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}}
			j := incidentGuardJob()
			if mode == "declared" {
				j.Spec.Needs.Memory, j.MemoryEnvelope.Basis = "", "declared"
			}
			if mode == "host allowance" {
				w.Machine.HostMemoryBudgetGB = 4
			}
			g := w.learnedGuard(j)
			var evidence *RSSLimitEvidence
			samples := 4
			if mode == "growth headroom" {
				samples = 5
			}
			for i := 0; i < samples; i++ {
				host := &gpuHostState{available: 8, level: 1, swapOut: 100}
				bytes := uint64(4816551936) // 4.4857635498046875 GiB.
				var sampleErr error
				switch mode {
				case "pressure":
					host.level = 2
				case "growth headroom":
					host.available = max(0, 1.25-float64(i)*.5)
					bytes += uint64(i) << 29 // Measured .5 GiB/s; no percentage floor.
				case "swap":
					host.swapOut += uint64(i)
				case "unknown":
					host = nil
				case "shared RSS":
					bytes = 20 << 30 // Sum can exceed physical RAM through sharing.
				case "inventory gap":
					host.level = 2
					if i == 1 {
						sampleErr = errors.New("inventory unavailable")
					}
				}
				evidence = g.observeMemory(clock.Now(), map[int]string{7: "attempt-birth"}, bytes, 2, sampleErr, host)
				if evidence != nil {
					break
				}
				clock.Advance(time.Second)
			}
			wantStop := mode == "pressure" || mode == "growth headroom" || mode == "swap" || mode == "declared" || mode == "host allowance"
			if (evidence != nil) != wantStop {
				t.Fatalf("stop=%+v want=%v", evidence, wantStop)
			}
			if evidence != nil && mode != "growth headroom" && mode != "host allowance" && evidence.LimitBytes != 4214571008 {
				t.Fatalf("lost incident reservation: %+v", evidence)
			}
		})
	}
}

func TestLearnedMemoryGuardLinuxCeiling(t *testing.T) {
	w := Worker{Machine: Machine{HostMemoryBudgetGB: 12, CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}}
	j := incidentGuardJob()
	bytes, err := w.jobCeilingBytes(j)
	if err != nil || bytes != 12<<30 {
		t.Fatalf("learned cap: %d %v", bytes, err)
	}
	c := fakeCeilings(t)
	a, err := c.create("learned-incident", bytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.cleanup(&fakeClock{now: time.Unix(100, 0)})
	data, err := a.fs.ReadFile(filepath.Join(a.path, "memory.max"))
	if err != nil || string(data) != strconv.FormatInt(12<<30, 10) {
		t.Fatalf("kernel cap: %s %v", data, err)
	}
	j.MemoryEnvelope.Basis = "exploratory"
	bytes, err = w.jobCeilingBytes(j)
	if err != nil || bytes != 12<<30 {
		t.Fatalf("bounded exploratory cap: %d %v", bytes, err)
	}
	j.MemoryEnvelope.ProbeFloorGB = 1 // Cold probes also use physical containment.
	bytes, err = w.jobCeilingBytes(j)
	if err != nil || bytes != 12<<30 {
		t.Fatalf("cold physical cap changed: %d %v", bytes, err)
	}
	j.Spec.Needs.Memory, j.MemoryEnvelope.Basis = "", "declared"
	bytes, err = w.jobCeilingBytes(j)
	if err != nil || bytes != 4214571008 {
		t.Fatalf("declared cap changed: %d %v", bytes, err)
	}
}

// Saved #850 follow-up: a changed profile identity used a bounded exploratory
// estimate, then RSS-stopped with normal pressure and 8.38 GiB still available.
// Replay the exact peak/estimate through negotiation, the guard and learning.
// GrowthGB is retained as observed; its timing is investigated separately.
func TestLearnedMemoryGuardExploratoryIncident(t *testing.T) {
	const limit, peak = 3.2467041015625, 3.3578033447265625
	const observedGrowth = 2.1867791904561273
	for _, pressure := range []bool{false, true} {
		t.Run(fmt.Sprintf("pressure=%v", pressure), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "worker-host", "cpu", 8, 16)
			m.ExtensionFingerprint = "cpu-v1"
			m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 12, .125
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "darwin", "arm64"
			m.CapabilityProfile.SystemMemoryBytes = 16 << 30
			m.MemoryEnforcement = darwinRSSGuardStatus
			m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
			check(t, q, m, nil)
			s := cpuSpec(1, 0)
			s.Run.Command = []string{"sh", "-c", "echo prior-profile"}
			runMemory(t, q, clock, m, s, limit-.125-minimumHostMemoryGB, func(r *Result) {
				r.MemoryPeak.CgroupGB = nil
				r.MemoryPeak.Source = processGroupMemorySource
			})
			s.Run.Command = []string{"sh", "-c", "echo changed-profile"}
			pending := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != pending.ID {
				t.Fatal("changed profile not assigned")
			}
			e := run.MemoryEnvelope
			if e.Basis != "exploratory" || e.ProbeFloorGB != 0 || math.Abs(e.HostGB-limit) > 1e-9 || e.Enforcement != learnedMemoryGuardEnforcement {
				t.Fatalf("estimated exploratory contract not negotiated: %+v", e)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			run = job(t, q, run.ID) // Start records the exact attempt's timing key.
			w := Worker{Machine: m, HostMemoryReserveGB: 3}
			guard := w.learnedGuard(*run)
			var stop *RSSLimitEvidence
			for i := 0; i < 4; i++ {
				host := &gpuHostState{available: 8.378646850585938, level: 1}
				if pressure {
					host.available, host.level = .5, 2
				}
				stop = guard.observeMemory(clock.Now(), map[int]string{7: "birth"}, uint64(peak*(1<<30)), 2, nil, host)
				if stop != nil {
					break
				}
				clock.Advance(processMemorySamplePeriod)
			}
			if (stop != nil) != pressure {
				t.Fatalf("exploratory stop=%+v pressure=%v", stop, pressure)
			}
			if !pressure {
				// Re-reserve through #852's common forecast, with no RSS resident
				// credit and no second calculation for exploratory estimates.
				footprint := peak
				report := AttemptReport{Execution: Execution{ID: run.ID, Attempt: run.Attempt}, Session: run.Session, Started: true,
					CurrentHostMemory: &CurrentHostMemory{FootprintGB: &footprint, GrowthObserved: true, GrowthGB: observedGrowth, GrowthRateGB: observedGrowth}}
				sample := currentHostMemorySamples([]AttemptReport{report}, clock.Now())[jobKey(*run)]
				charge, pending := jobHostMemoryCharge(job(t, q, run.ID), &m, limit, sample, clock.Now())
				if math.Abs(charge-5.669582535182689) > 1e-9 || pending != charge {
					t.Fatalf("exploratory growth not re-reserved without RSS credit: charge=%g pending=%g", charge, pending)
				}
			}
			r := classifyAttemptOutcome(result(run, stop == nil), attemptCause{memberOutcome: memberOutcome{RSSLimit: stop}})
			p, growth := peak, observedGrowth
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &p, GrowthGB: &growth, Source: processGroupMemorySource, Constrained: stop != nil}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			ended := job(t, q, run.ID)
			if len(ended.MemoryAttempts) != 1 {
				t.Fatal("missing exact attempt learning facts")
			}
			facts := ended.MemoryAttempts[0]
			if pressure {
				if facts.LearningReason != "oom" || !facts.MemoryPeak.Constrained || !r.OutOfMemory || safeRSSLimit(r.ExecutionCause.RSSLimit) == nil {
					t.Fatalf("real contention lost stop/learning evidence: %+v result=%+v", facts, r)
				}
			} else if facts.LearningReason != "eligible" || facts.MemoryPeak.Constrained || r.OutOfMemory || ended.MemoryOOMFloor != nil || ended.State != "succeeded" {
				t.Fatalf("estimate overage became OOM evidence: %+v job=%+v", facts, ended)
			}
			accepted := false
			for _, point := range q.s.WorkObservations {
				if point.Key == *run.Timing.Key && !point.MemoryRecordedAt.IsZero() && point.MemoryPeak != nil && !point.MemoryPeak.constrained() {
					accepted = true
					if *point.MemoryPeak.HostGB != peak || memoryGrowthGB(point.MemoryPeak) != observedGrowth {
						t.Fatalf("clean grown measurement changed: %+v", point.MemoryPeak)
					}
				}
			}
			if accepted == pressure {
				t.Fatalf("accepted learning=%v pressure=%v", accepted, pressure)
			}
			if !pressure {
				next := q.memoryCandidate(submit(t, q, s), &m)
				if next.MemoryEnvelope.Basis != "learned" || next.MemoryEnvelope.Evidence < 1 || next.MemoryEnvelope.HostGB < peak {
					t.Fatalf("healthy grown run did not teach estimate: %+v", next.MemoryEnvelope)
				}
			}
		})
	}
}

type learnedPressureMemoryFS struct {
	memoryFS
	pressure bool
	clock    *workerFakeClock
}

func (f learnedPressureMemoryFS) ReadFile(path string) ([]byte, error) {
	if filepath.Base(path) == "memory.current" {
		f.clock.Advance(time.Second)
	}
	if f.pressure && path == "/proc/meminfo" {
		return []byte("MemAvailable: 0 kB\nSwapTotal: 8388608 kB\nSwapFree: 8388608 kB\n"), nil
	}
	return f.memoryFS.ReadFile(path)
}

func TestLearnedMemoryGuardCgroupIncident(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		t.Run(strconv.FormatBool(pressure), func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = "cpu"
			w.Machine.HostMemoryBudgetGB = 12
			w.Machine.CapabilityProfile = &CapabilityProfile{SystemMemoryBytes: 16 << 30}
			clock := &workerFakeClock{now: time.Unix(100, 0)}
			w.Clock = clock
			w.HostMemorySource = func(context.Context) (float64, error) { return 8, nil }
			w.memoryCeilings = fakeCeilings(t)
			w.memoryCeilings.clock = clock
			w.memoryCeilings.fs = learnedPressureMemoryFS{memoryFS: w.memoryCeilings.fs, pressure: pressure, clock: clock}
			w.processMemory = nil
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			j := recoveryJob(t, []string{"sleep", "4"})
			j.Spec.Needs, j.MemoryEnvelope = incidentGuardJob().Spec.Needs, incidentGuardJob().MemoryEnvelope
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(clock.Now(), 0, true, true) })
			for name, value := range map[string]string{"memory.current": "4816551936", "memory.peak": "4816551936", "memory.stat": "anon 4816551936\nkernel 0\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n"} {
				if err := os.WriteFile(filepath.Join(p.memory.path, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			r := d5Finish(t, p)
			if r.Success == pressure || r.OutOfMemory != pressure || r.ReleasePending || r.MemoryPeak == nil || r.MemoryPeak.Constrained != pressure {
				t.Fatalf("cgroup incident: %+v", r)
			}
			if pressure && (r.ExecutionCause == nil || r.ExecutionCause.RSSLimit == nil || r.ExecutionCause.RSSLimit.Quality != "cgroup_current") {
				t.Fatalf("lost cgroup stop: %+v", r)
			}
		})
	}
}

// Keep the real command/receipt/cleanup path in the incident replay. Resource
// readings and time are injected; no native RAM stress or host change is needed.
func TestLearnedMemoryGuardWorkerIncident(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		t.Run(strconv.FormatBool(pressure), func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = "cpu"
			w.Machine.CapabilityProfile = &CapabilityProfile{SystemMemoryBytes: 16 << 30}
			clock := &workerFakeClock{now: time.Unix(100, 0)}
			w.Clock = clock
			w.HostMemorySource = func(context.Context) (float64, error) { return 8, nil }
			w.processMemory = &processMemoryProbe{
				Pressure: func(context.Context) (*gpuHostState, error) {
					level := 1
					if pressure {
						level = 2
					}
					return &gpuHostState{available: 8, level: level}, nil
				},
				OwnedResident: func(map[int]string) (uint64, int, error) { clock.Advance(time.Second); return 4816551936, 2, nil },
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			j := recoveryJob(t, []string{"sleep", "3"})
			j.Spec.Needs, j.MemoryEnvelope = incidentGuardJob().Spec.Needs, incidentGuardJob().MemoryEnvelope
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(clock.Now(), 0, true, true) })
			r := d5Finish(t, p)
			if r.Success == pressure || r.OutOfMemory != pressure || r.ReleasePending || r.MemoryPeak == nil || r.MemoryPeak.Constrained != pressure {
				t.Fatalf("incident outcome: %+v", r)
			}
			if !pressure && (r.ExecutionCause != nil || r.MemoryPeak.HostGB == nil || *r.MemoryPeak.HostGB != 4.4857635498046875) {
				t.Fatalf("healthy growth lost: %+v", r)
			}
			if pressure && (r.ExecutionCause == nil || r.ExecutionCause.Class != rssLimitExceeded) {
				t.Fatalf("pressure cause lost: %+v", r)
			}
		})
	}
}

func TestLearnedMemoryGuardChargeUsesProbeForecast(t *testing.T) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	m := Machine{HostMemoryBudgetGB: 12, CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}
	j := incidentGuardJob()
	j.ID, j.Attempt, j.Session, j.State = "incident", 1, "session", "running"
	for _, source := range []string{"cgroup", "RSS"} {
		t.Run(source, func(t *testing.T) {
			footprint := 4.4857635498046875
			resident := footprint
			if source == "RSS" {
				resident = 0
			} // Never credit shared/compressed RSS.
			s := &CurrentHostMemory{FootprintGB: &footprint, NonreclaimableGB: resident, GrowthObserved: true, GrowthGB: .1, GrowthRateGB: .05, AgeSeconds: .5}
			r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true, CurrentHostMemory: s}
			sample := currentHostMemorySamples([]AttemptReport{r}, clock.Now())[jobKey(j)]
			charge, pending := jobHostMemoryCharge(&j, &m, j.Spec.Needs.HostMemoryGB, sample, clock.Now())
			want := footprint + .1 + .025 + exploratoryDriftMarginGB(&m)
			if math.Abs(charge-want) > 1e-9 || math.Abs(pending-(charge-resident)) > 1e-9 {
				t.Fatalf("charge=%g pending=%g want=%g", charge, pending, want)
			}
			clock.Advance(hostMemorySampleFreshFor)
			charge, _ = jobHostMemoryCharge(&j, &m, j.Spec.Needs.HostMemoryGB, sample, clock.Now())
			if charge < want || charge >= 12 {
				t.Fatalf("stale forecast lost growth or charged physical cap: %g", charge)
			}
			// A replacement cannot inherit the old sample or its released credit.
			replacement := j
			replacement.Attempt++
			sample = currentHostMemorySamples([]AttemptReport{r}, clock.Now())[jobKey(replacement)]
			charge, _ = jobHostMemoryCharge(&replacement, &m, j.Spec.Needs.HostMemoryGB, sample, clock.Now())
			if charge != j.Spec.Needs.HostMemoryGB {
				t.Fatalf("replacement inherited old growth: %g", charge)
			}
		})
	}
}

func TestLearnedMemoryGuardNegotiatesBeforeWidening(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 16)
	m.HostMemoryBudgetGB = 12
	m.CapabilityProfile.SystemMemoryBytes = 16 << 30
	m.ExtensionFingerprint = "memory-v1"
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	runMemory(t, q, clock, m, s, 3, func(r *Result) { r.MemoryPeak.Events = &MemoryEventCounts{} })
	pending := submit(t, q, s)
	legacy := q.memoryCandidate(pending, &m)
	if legacy.MemoryEnvelope.Basis != "learned" {
		t.Fatalf("fixture has no accepted estimate: %+v", legacy.MemoryEnvelope)
	}
	if adaptiveMemoryCeilingGB(legacy, &m) > 0 {
		t.Fatal("old worker acquired wider ceiling")
	}
	m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
	current := q.memoryCandidate(pending, &m)
	if cap := adaptiveMemoryCeilingGB(current, &m); cap != 12 {
		t.Fatalf("new queue did not acknowledge accounting: %+v cap=%g", current.MemoryEnvelope, cap)
	}
	current.MemoryEnvelope.Enforcement = "memory ceilings enforced (cgroup v2)"
	if adaptiveMemoryCeilingGB(current, &m) > 0 {
		t.Fatal("old queue authorized wider worker cap")
	}
}

// #389 retained only a peak/pressure interval, not a per-second trace. Replay a
// synthetic accelerating fork shape toward its 2,004 children / 64.07 GiB sum
// on 16 GiB RAM; do not mislabel this as a reconstruction of historical timing.
func TestLearnedMemoryGuardForkBurstBeforeCritical(t *testing.T) {
	for _, declared := range []bool{true, false} {
		for _, room := range []bool{false, true} {
			t.Run(fmt.Sprintf("declared=%v/room=%v", declared, room), func(t *testing.T) {
				clock := &fakeClock{now: time.Unix(100, 0)}
				w := Worker{Machine: Machine{CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}}
				j := incidentGuardJob()
				j.Spec.Needs.HostMemoryGB = 2.125
				if declared {
					j.Spec.Needs.Memory, j.MemoryEnvelope.Basis = "", "declared"
				}
				g := w.learnedGuard(j)
				trace := []struct {
					rss, available float64
					members, level int
					swapOut        uint64
				}{
					{1, 10, 2, 1, 0},
					{8, 4, 252, 1, 0},
					{32, .25, 1002, 2, 1 << 20},
					{64.07, 0, 2004, 4, 45 << 30},
				}
				stopped := -1
				for i, s := range trace {
					if room {
						s.available, s.level, s.swapOut = 10, 1, 0
					}
					e := g.observeMemory(clock.Now(), map[int]string{7: "birth"}, uint64(s.rss*(1<<30)), s.members, nil,
						&gpuHostState{available: s.available, level: s.level, swapOut: s.swapOut})
					if e != nil {
						stopped = i
						if safeRSSLimit(e) == nil {
							t.Fatalf("invalid evidence: %+v", e)
						}
						break
					}
					clock.Advance(processMemorySamplePeriod)
				}
				if !room && stopped != 1 {
					t.Fatalf("fork burst stopped at sample %d; want 1, before warning/critical pressure or swap", stopped)
				}
				if room && !declared && stopped != -1 {
					t.Fatalf("shared RSS/count growth with stable headroom stopped at %d", stopped)
				}
				if room && declared && stopped != 3 {
					t.Fatalf("declared limit lost ordinary grace: stop=%d", stopped)
				}
			})
		}
	}
}

func TestLearnedMemoryGuardHardAllowanceHeadroomBaseline(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			clock := &fakeClock{now: time.Unix(100, 0)}
			w := Worker{Machine: Machine{HostMemoryBudgetGB: 12, CapabilityProfile: &CapabilityProfile{SystemMemoryBytes: 16 << 30}}}
			g := w.learnedGuard(incidentGuardJob())
			trace := []struct {
				rss, available float64
				members        int
			}{
				{10, 10, 2}, {13, 8, 3}, {14, 7, 4}, {14, 7, 4},
			}
			for i, s := range trace {
				host := &gpuHostState{available: s.available, level: 1}
				if missing && i == 1 {
					host = nil
				}
				e := g.observeMemory(clock.Now(), map[int]string{7: "birth"}, uint64(s.rss*(1<<30)), s.members, nil, host)
				if i < 3 && e != nil {
					t.Fatalf("sample %d inherited stale headroom: %+v", i, e)
				}
				if i == 3 && (e == nil || e.LimitBytes != 12<<30) {
					t.Fatalf("sustained explicit allowance not enforced: %+v", e)
				}
				clock.Advance(processMemorySamplePeriod)
			}
		})
	}
}
