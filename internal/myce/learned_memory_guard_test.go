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
	j.Spec.Needs.Memory, j.MemoryEnvelope.Basis = "", "declared"
	bytes, err = w.jobCeilingBytes(j)
	if err != nil || bytes != 4214571008 {
		t.Fatalf("declared cap changed: %d %v", bytes, err)
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
			if charge != 12 {
				t.Fatalf("stale grew attempt dropped to estimate: %g", charge)
			}
			// A replacement cannot inherit the old sample or its released credit.
			replacement := j
			replacement.Attempt++
			sample = currentHostMemorySamples([]AttemptReport{r}, clock.Now())[jobKey(replacement)]
			charge, _ = jobHostMemoryCharge(&replacement, &m, j.Spec.Needs.HostMemoryGB, sample, clock.Now())
			if charge != 12 {
				t.Fatalf("wrong execution released charge: %g", charge)
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
	if learnedMemoryCeilingGB(legacy, &m) > 0 {
		t.Fatal("old worker acquired wider ceiling")
	}
	m.Capabilities = append(m.Capabilities, learnedMemoryGuardCapability)
	current := q.memoryCandidate(pending, &m)
	if cap := learnedMemoryCeilingGB(current, &m); cap != 12 {
		t.Fatalf("new queue did not acknowledge accounting: %+v cap=%g", current.MemoryEnvelope, cap)
	}
	current.MemoryEnvelope.Enforcement = "memory ceilings enforced (cgroup v2)"
	if learnedMemoryCeilingGB(current, &m) > 0 {
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
