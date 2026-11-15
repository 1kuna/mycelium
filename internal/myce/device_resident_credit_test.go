package myce

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

// Replay #1044's retained 13:00:19Z comparison, without inventing a native
// physical/process trace. The device peak is a conditional live direct sample.
func TestDeviceResidentCreditRefusalReplay(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.HostMemoryBudgetGB, m.ReservedMemoryGB = 113.64812850952148, 4
	m.CapabilityProfile.SystemMemoryBytes = 129992077312
	m.CapabilityProfile.LogicalCPUs = 32
	m.HostAvailableMemoryGB = 78.12232208251953
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121.06455612182617, UsedGB: 38.94223403930665, ReserveGB: 4}
	check(t, q, m, nil)
	var jobs []Job
	var reports []AttemptReport
	for i, row := range [][3]float64{
		{43.29749156746103, 1.142669677734375, 22.443359375},
		{8.125, 1.7652435302734375, 0},
		{14.125, 8.193050384521484, 0},
	} {
		s := cpuSpec(1, row[0]-.125)
		if i == 0 {
			s = spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = row[2], row[0]-row[2]-.125
		}
		at := clock.Now()
		j := Job{ID: []string{"gpu", "cpu-a", "cpu-b"}[i], State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, StartedAt: &at, Spec: s}
		jobs = append(jobs, j)
		q.s.Jobs[j.ID] = &jobs[len(jobs)-1]
		reports = append(reports, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true,
			CurrentHostMemory: &CurrentHostMemory{SampleID: 1, NonreclaimableGB: row[1]}})
	}
	near := func(got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-9 {
			t.Fatalf("got %.12f, want %.12f", got, want)
		}
	}
	refresh := func() *HostMemoryLedger {
		q.hostMemorySamples[m.ID] = currentHostMemorySamples(reports, clock.Now())
		q.refreshHostMemory()
		return q.hostMemoryLedgers[m.ID]
	}
	before := refresh()
	near(before.ReservedGB, 65.54749156746104)
	near(before.GrowthGB, 54.44652797493173)
	near(before.FreeGB, 23.675794107587798)
	device := 22.318359375
	reports[0].CurrentHostMemory.DeviceGB = &device
	// DeviceGB alone is a potentially overlapping sum: old workers stay safe.
	near(refresh().FreeGB, before.FreeGB)
	reports[0].CurrentHostMemory.DeviceResidentGB = &device
	// Exercise the real remote representation, with no worker-local fields.
	b, err := json.Marshal(reports)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &reports); err != nil {
		t.Fatal(err)
	}
	after := refresh()
	near(after.ReservedGB, before.ReservedGB)
	near(after.GrowthGB, 43.22913219246103)
	near(after.FreeGB, 34.8931898900585)
	near(after.AllowanceGB-after.ReservedGB, 48.10063694206045)
	if after.FreeGB >= 43.29749156746103 {
		t.Fatal("overlap uncertainty incorrectly qualified a second naming")
	}
	// All three admission paths use the same physical comparison, including
	// a fitting boundary and a refusal just beyond it.
	rt := modelRuntime{clock: clock, memoryMachine: m, batchJobs: jobs, batchHostMemorySamples: q.hostMemorySamples[m.ID]}
	rt.publishHostLedgerLocked()
	near(rt.hostLedger.Load().pending, after.GrowthGB)
	for _, wantFit := range []bool{true, false} {
		charge := after.FreeGB
		if !wantFit {
			charge += .001
		}
		next := Job{ID: "next", State: "running", Attempt: 1, Spec: cpuSpec(1, charge-.125)}
		w := Worker{Machine: m, Clock: clock, hostMemorySamples: q.hostMemorySamples[m.ID], HostMemorySource: func(context.Context) (float64, error) { return m.HostAvailableMemoryGB, nil }, HostMemoryReserveGB: 4}
		if err := localAttemptAdmission(w, jobs, next, nil); (err == nil) != wantFit {
			t.Fatalf("local fit=%t: %v", wantFit, err)
		}
		rt.batchJobs = append(append([]Job{}, jobs...), next)
		if err := rt.checkHostAdmissionLocked(m.HostAvailableMemoryGB, true, q.hostMemorySamples[m.ID]); (err == nil) != wantFit {
			t.Fatalf("final launch fit=%t: %v", wantFit, err)
		}
	}
	// Loss of live device evidence restores the exact conservative refusal.
	reports[0].CurrentHostMemory.DeviceAgeSeconds = deviceMemorySampleFreshFor.Seconds()
	near(refresh().FreeGB, before.FreeGB)
}

func TestDeviceResidentUnionEvidence(t *testing.T) {
	_, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	at := clock.Now()
	j := Job{ID: "gpu-a", State: "running", Attempt: 1, StartedAt: &at, Spec: spec("experiment")}
	s := hostMemorySample{gb: 3, at: at, deviceGB: 22, residentGB: 22, deviceAt: at}
	for _, tc := range []struct {
		name string
		edit func(*Machine, *Job, *hostMemorySample)
		want float64
	}{
		{"host/device max", func(*Machine, *Job, *hostMemorySample) {}, 19},
		{"host covers device", func(_ *Machine, _ *Job, s *hostMemorySample) { s.gb = 24 }, 0},
		{"stale", func(_ *Machine, _ *Job, s *hostMemorySample) { s.deviceAt = at.Add(-deviceMemorySampleFreshFor) }, 0},
		{"future", func(_ *Machine, _ *Job, s *hostMemorySample) { s.deviceAt = at.Add(time.Second) }, 0},
		{"missing", func(_ *Machine, _ *Job, s *hostMemorySample) { s.deviceAt = time.Time{} }, 0},
		{"sum without lower bound", func(_ *Machine, _ *Job, s *hostMemorySample) { s.residentGB = 0 }, 0},
		{"invalid lower bound", func(_ *Machine, _ *Job, s *hostMemorySample) { s.residentGB = 23 }, 0},
		{"ambiguous", func(_ *Machine, _ *Job, s *hostMemorySample) { s.exclusive = true }, 0},
		{"fallback", func(_ *Machine, _ *Job, s *hostMemorySample) { s.source = deviceUnifiedResidentSource }, 0},
		{"discrete", func(m *Machine, _ *Job, _ *hostMemorySample) { m.UnifiedMemory = false }, 0},
		{"multi-card", func(_ *Machine, j *Job, _ *hostMemorySample) { j.AllocatedDevices = []string{"a", "b"} }, 0},
		{"unstarted", func(_ *Machine, j *Job, _ *hostMemorySample) { j.StartedAt = nil }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			machine, job, sample := m, j, s
			tc.edit(&machine, &job, &sample)
			var u deviceResidentUnion
			u.observe(&job, &machine, sample, at, false)
			if got := u.credit(jobKey(job)); got != tc.want {
				t.Fatalf("credit=%g, want %g", got, tc.want)
			}
		})
	}
	// Repeated process counters can describe the same allocation. Carry only
	// the largest process, and never add another attempt's overlapping counter.
	resident := 0.0
	total, _, err := cudaAttemptMemoryReading([]byte("GPU-one,42,22528\nGPU-one,43,22528\n"), "GPU-one", map[int]bool{42: true, 43: true}, &resident)
	if err != nil || total != 44 || resident != 22 {
		t.Fatal(total, resident, err)
	}
	s.deviceGB, s.residentGB = total, resident
	neighbor := j
	neighbor.ID = "gpu-b"
	var u deviceResidentUnion
	u.observe(&j, &m, s, at, false)
	u.observe(&neighbor, &m, s, at, false)
	if u.credit(jobKey(j))+u.credit(jobKey(neighbor)) != 16 {
		t.Fatal("overlapping process/attempt counters or host pages were added", u)
	}
	// A CPU co-tenant's anon may contain those same device pages too.
	cpu := Job{ID: "cpu", Spec: cpuSpec(1, 24)}
	u.observe(&cpu, &m, hostMemorySample{gb: 24, at: at}, at, false)
	if u.credit(jobKey(j))+u.credit(jobKey(neighbor)) != 0 {
		t.Fatal("co-tenant host pages received duplicate device credit", u)
	}
}
