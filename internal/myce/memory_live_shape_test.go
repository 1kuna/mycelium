package myce

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// Synthetic capacities replay the CPU-only render/OCR contention without any
// installation identity. A CPU job on a unified host still uses host RAM only.
func TestMemoryLiveShapeLearnedFamilyCoSchedules(t *testing.T) {
	for _, tc := range []struct {
		ram      float64
		evidence int
	}{{2, 1}, {2, 3}} { // Host size only scales these values (#1086).
		ram := tc.ram
		t.Run(fmt.Sprintf("%g/%d", ram, tc.evidence), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 32, ram)
			m.HostMemoryBudgetGB, m.ExtensionFingerprint = ram*.75, "cpu-v1"
			m.CapabilityProfile.SystemMemoryBytes = uint64(ram * (1 << 30))
			m.UnifiedMemory = true
			m.Devices = []MachineDevice{}
			m.CPUExtensionFingerprint = m.ExtensionFingerprint
			check(t, q, m, nil)
			s := cpuSpec(2, 0)
			low, peak, spread := ram/16, ram/12, ram/12-ram/16
			for i := 0; i < tc.evidence; i++ {
				n := low
				if i%2 == 0 {
					n = peak
				}
				runMemory(t, q, clock, m, s, n, func(r *Result) { r.MemoryPeak.NonreclaimableGB = &n; r.MemoryPeak.Events = &MemoryEventCounts{} })
			}
			if tc.evidence == 1 {
				spread = minimumHostMemoryGB
			}
			// New commands in the same work family must reuse all accepted
			// attempts, with observed spread rather than an arbitrary multiplier.
			s.Run.Command = []string{"echo", "ocr-next-band"}
			for i := 0; i < 5; i++ {
				submit(t, q, s)
			}
			var active []*Job
			for i := 0; i < 5; i++ {
				j := assignedJob(machineCheck(t, q, m, active...))
				if j == nil {
					t.Fatalf("job %d serialized", i)
				}
				e := j.MemoryEnvelope
				if e.Basis != "exploratory" || e.Evidence != tc.evidence || math.Abs(e.HostGB-(peak+spread)) > 1e-8 {
					t.Fatalf("not measured peak plus spread: %+v", e)
				}
				active = append(active, j)
			}
		})
	}
}

func TestMemoryLiveShapeProtectedAttemptAllowsColdProbe(t *testing.T) {
	for _, ram := range []float64{2, 256} {
		t.Run(fmt.Sprint(ram), func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 16, ram)
			m.HostMemoryBudgetGB, m.ExtensionFingerprint = ram*.75, "cpu-v1"
			m.CapabilityProfile.SystemMemoryBytes = uint64(ram * (1 << 30))
			m.UnifiedMemory = true
			m.Devices = []MachineDevice{}
			m.CPUExtensionFingerprint = m.ExtensionFingerprint
			check(t, q, m, nil)
			submit(t, q, cpuSpec(2, ram/8))
			holder := assignedJob(machineCheck(t, q, m))
			clock.Advance(time.Second)
			s := cpuSpec(2, 0)
			s.Run.Command = []string{"echo", "unseen-ocr"}
			pending := submit(t, q, s)
			next := assignedJob(machineCheck(t, q, m, holder))
			if next == nil || next.ID != pending.ID {
				t.Fatalf("blind protected gate: %s", job(t, q, pending.ID).Reason)
			}
		})
	}
}

func TestMemoryLiveShapeLinuxCacheIsAvailable(t *testing.T) {
	// Reclaimable cache dwarfs MemFree, but Linux's MemAvailable includes it.
	info := []byte("MemTotal: 134217728 kB\nMemFree: 4194304 kB\nCached: 100663296 kB\nMemAvailable: 117440512 kB\n")
	read := func(string) ([]byte, error) { return info, nil }
	ctx := context.Background()
	host, err := newHostMemorySource(0, "linux", read, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := host(ctx); err != nil || n != 112 {
		t.Fatal("host discarded reclaimable cache", n, err)
	}
	device, err := newMemorySource("cuda", 0, 0, "unified", "linux", read, func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	if n, err := device(ctx); err != nil || n != 112 {
		t.Fatal("unified source discarded reclaimable cache", n, err)
	}
	q, _ := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 128)
	m.HostAvailableMemoryGB, _ = host(ctx)
	m.HostMemoryBudgetGB = 96
	m.CapabilityProfile.SystemMemoryBytes = 128 << 30
	m.Devices = []MachineDevice{}
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	check(t, q, m, nil)
	submit(t, q, cpuSpec(2, 80))
	holder := assignedJob(machineCheck(t, q, m))
	pending := submit(t, q, cpuSpec(2, 20))
	machineCheck(t, q, m, holder)
	why := holdDetail(q.memoryWait(job(t, q, pending.ID), q.s.Machines))
	if !strings.Contains(why, "live available 112.0 GiB") {
		t.Fatal("status mislabels availability as usage", why)
	}
}

func TestMemoryLiveShapeOnePointTransferProbeSharesAfterMeasurement(t *testing.T) {
	for _, ram := range []float64{2, 256} {
		t.Run(fmt.Sprint(ram), func(t *testing.T) {
			q, clock := fixture(t)
			source := cpuMachine("source", "source-host", "cpu", 32, ram)
			target := cpuMachine("target", "target-host", "cpu", 32, ram)
			for _, m := range []*Machine{&source, &target} {
				m.HostMemoryBudgetGB, m.ExtensionFingerprint = ram*.75, m.ID+"-v1"
				m.CPUExtensionFingerprint = m.ExtensionFingerprint
				m.Devices = []MachineDevice{}
				m.CapabilityProfile.SystemMemoryBytes = uint64(ram * (1 << 30))
			}
			target.CapabilityProfile.Arch, target.UnifiedMemory = "arm64", true
			check(t, q, source, nil)
			check(t, q, target, nil)
			ocr := cpuSpec(2, 0)
			ocr.Name, ocr.Run.Command = "ocr", []string{"echo", "ocr"}
			ocr.AllowedMachines = []string{target.ID}
			// Repeated accepted peaks establish a learned OCR envelope.
			// Two distinct acceptances suffice; 41 identical runs add no evidence shape.
			const ocrEvidence = 2
			for range ocrEvidence {
				runMemory(t, q, clock, target, ocr, ram/32, nil)
			}
			render := cpuSpec(2, 0)
			render.Name, render.Run.Command = "render", []string{"echo", "render"}
			render.AllowedMachines = []string{source.ID}
			runMemory(t, q, clock, source, render, ram/8, nil)
			render.AllowedMachines = []string{target.ID}
			first := submit(t, q, render)
			a := assignedJob(machineCheck(t, q, target))
			if a == nil || a.ID != first.ID || a.MemoryEnvelope.Basis != "exploratory" || a.MemoryEnvelope.Evidence != 1 {
				t.Fatalf("one-point transfer not probed: %+v", a)
			}
			ceiling := a.Spec.Needs.HostMemoryGB
			if ceiling < ram*.7 {
				t.Fatal("initial probe did not retain physical containment", ceiling)
			}
			second := submit(t, q, render)
			var sampleID int64
			report := func(j *Job) AttemptReport {
				// Distinct measurements may replace an earlier unknown-growth
				// sample; an anonymous replay must retain that sample's fence.
				sampleID++
				footprint := ram / 16
				return AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
					CurrentHostMemory: &CurrentHostMemory{SampleID: sampleID, NonreclaimableGB: ram / 32, FootprintGB: &footprint, GrowthGB: ram / 64, GrowthRateGB: ram / 64, GrowthObserved: true}}
			}
			pull := func(reports ...AttemptReport) *Job {
				t.Helper()
				r, err := q.Checkin(Checkin{Machine: target, AttemptProtocol: keyedAttemptProtocol, Attempts: reports})
				if err != nil {
					t.Fatal(err)
				}
				return assignedJob(r)
			}
			// No telemetry, one untimed sample, old workers and stale identity
			// must not authorize oversubscription of the initial ceiling.
			if pull(AttemptReport{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true}) != nil {
				t.Fatal("missing measurement earned credit")
			}
			r := report(a)
			r.CurrentHostMemory.GrowthObserved = false
			if pull(r) != nil {
				t.Fatal("unknown growth earned credit")
			}
			r = report(a)
			r.Attempt++
			unknown := report(a)
			unknown.CurrentHostMemory = nil
			if pull(unknown, r) != nil {
				t.Fatal("stale attempt earned credit")
			}
			target.HostAvailableMemoryGB = ram * .875
			r = report(a)
			b := pull(r)
			if b == nil || b.ID != second.ID {
				t.Fatalf("probe held entire host: %s", job(t, q, second.ID).Reason)
			}
			if job(t, q, a.ID).Spec.Needs.HostMemoryGB != ceiling {
				t.Fatal("resized immutable execution ceiling")
			}
			w := Worker{Machine: target, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return target.HostAvailableMemoryGB, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{r}, clock.Now())}
			if err := localAttemptAdmission(w, []Job{*a}, *b, nil); err != nil {
				t.Fatal("worker disagrees with queue", err)
			}
			// Both probes remain active while all five measured OCR jobs start.
			reports := []AttemptReport{report(a), report(b)}
			for i := 0; i < 5; i++ {
				submit(t, q, ocr)
			}
			for i := 0; i < 5; i++ {
				j := pull(reports...)
				if j == nil || j.MemoryEnvelope.Basis != "learned" || j.MemoryEnvelope.Evidence != ocrEvidence {
					t.Fatalf("OCR %d serialized behind probes: %+v", i, j)
				}
				reports = append(reports, report(j))
			}
			clock.Advance(hostMemorySampleFreshFor)
			if err := localAttemptAdmission(w, []Job{*a}, *b, nil); err == nil {
				t.Fatal("worker credited expired probe forecast")
			}
			q.refreshHostMemory()
			if q.s.Machines[target.ID].FreeHostMemoryGB != 0 {
				t.Fatal("queue credited expired probe forecast")
			}
		})
	}
}
