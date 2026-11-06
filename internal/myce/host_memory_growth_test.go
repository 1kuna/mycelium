package myce

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHostMemoryRemainingGrowthAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		used, available, budget, age          float64
		missing, wrongAttempt, wrongJob, want bool
	}{
		{name: "fresh admits production", used: 2.75, available: 14, budget: 24, want: true},
		// Exactly 2 GiB cannot admit .36: 14 - (16.356244 - 2) < 0.
		{name: "exact two GiB still needs growth", used: 2, available: 14, budget: 24},
		{name: "near envelope under pressure", used: 16.2, available: .4, budget: 24},
		{name: "stale", used: 2.75, available: 14, budget: 24, age: 5},
		{name: "missing", available: 14, budget: 24, missing: true},
		{name: "wrong attempt", used: 2.75, available: 14, budget: 24, wrongAttempt: true},
		{name: "wrong job", used: 2.75, available: 14, budget: 24, wrongJob: true},
		{name: "full budget cap", used: 16, available: 14, budget: 16.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("growth-host", "growth-host", "cpu", 16, 24)
			m.Devices = []MachineDevice{}
			m.HostMemoryBudgetGB = tc.budget
			first := submit(t, q, cpuSpec(4, 16.35624408721924))
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != first.ID {
				t.Fatal("holder not assigned")
			}
			clock.Advance(time.Second)
			report := AttemptReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: m.Session, Started: true}
			if !tc.missing {
				report.CurrentHostMemory = &CurrentHostMemory{NonreclaimableGB: tc.used, AgeSeconds: tc.age}
			}
			if tc.wrongAttempt {
				report.Attempt++
			}
			m.HostAvailableMemoryGB = tc.available
			if tc.wrongJob {
				report.ID = "old-job"
			}
			if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
				t.Fatal(err)
			}
			// Production is deliberately submitted after a competing same-size job.
			experiment := submit(t, q, cpuSpec(2, .36))
			s := cpuSpec(2, .36)
			s.Priority = "production"
			prod := submit(t, q, s)
			reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
			if err != nil {
				t.Fatal(err)
			}
			got := assignedJob(reply)
			if tc.want {
				if got == nil || got.ID != prod.ID {
					t.Fatalf("production not admitted: %+v; reason=%s", got, job(t, q, prod.ID).Reason)
				}
				requireState(t, q, experiment.ID, "queued")
			} else if got != nil {
				t.Fatalf("unsafe admission: %+v", got)
			}

			// The worker must independently recheck growth and the full static budget.
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return tc.available, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now())}
			next := *job(t, q, prod.ID)
			if err := localAttemptAdmission(w, []Job{*assigned}, next, nil); (err == nil) != tc.want {
				t.Fatalf("worker admission=%v want %v", err, tc.want)
			}
		})
	}
}

func TestHostMemoryRemainingGrowthExpiresWithoutRenewal(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("growth", "growth", "cpu", 16, 24)
	m.Devices = []MachineDevice{}
	m.HostMemoryBudgetGB = 24
	submit(t, q, cpuSpec(4, 16.35624408721924))
	holder := check(t, q, m, nil).Job
	if holder == nil {
		t.Fatal("no holder")
	}
	m.HostAvailableMemoryGB = 14
	report := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: m.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2.75}}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	q.refreshHostMemory()
	room := q.s.Machines[m.ID].FreeHostMemoryGB
	q.mu.Unlock()
	if room < .36 {
		t.Fatal("fresh growth room missing", room)
	}
	w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 14, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{report}, clock.Now())}
	next := Job{Spec: Spec{Needs: ResourceNeeds{CPUCores: 2, HostMemoryGB: .36, Platform: Platform{Required: []string{"cpu"}}}}}
	if err := localAttemptAdmission(w, []Job{*holder}, next, nil); err != nil {
		t.Fatal("worker refused fresh growth room", err)
	}
	clock.Advance(hostMemorySampleFreshFor)
	if err := localAttemptAdmission(w, []Job{*holder}, next, nil); err == nil {
		t.Fatal("worker credited expired telemetry")
	}

	q.mu.Lock()
	q.refreshHostMemory()
	room = q.s.Machines[m.ID].FreeHostMemoryGB
	q.mu.Unlock()
	if room != 0 {
		t.Fatal("stale sample still credited", room)
	}
	// A fresh heartbeat carrying an aged actor sample cannot renew credit.
	report.CurrentHostMemory.AgeSeconds = hostMemorySampleFreshFor.Seconds()
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	room = q.s.Machines[m.ID].FreeHostMemoryGB
	q.mu.Unlock()
	if room != 0 {
		t.Fatal("cached sample renewed", room)
	}
}

func TestHostMemoryCurrentCreditsOnlyAnonymousResident(t *testing.T) {
	_, clock := fixture(t)
	dir := t.TempDir()
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	write("memory.stat", "anon 2952790016\nfile 10737418240\nkernel 1073741824\n")
	write("memory.current", "14763950080")
	sample := a.currentHostMemory(clock)
	if sample == nil || sample.NonreclaimableGB != 2.75 {
		t.Fatal("credited reclaimable cache or peak", sample)
	}
	write("memory.current", "1073741824")
	if sample = a.currentHostMemory(clock); sample == nil || sample.NonreclaimableGB != 1 {
		t.Fatal("credit exceeded current charge", sample)
	}
	write("memory.stat", "file 10737418240\n")
	if a.currentHostMemory(clock) != nil {
		t.Fatal("missing anon accepted")
	}
	// Darwin/admission-only workers have no cgroup and must keep full growth.
	if (*memoryAttempt)(nil).currentHostMemory(clock) != nil {
		t.Fatal("unsupported telemetry fabricated")
	}
	for _, sample := range []hostMemorySample{{gb: math.NaN(), at: clock.Now()}, {gb: math.Inf(1), at: clock.Now()}, {gb: 2.75, at: clock.Now().Add(time.Second)}} {
		if remainingHostGrowth(16.356, sample, clock.Now()) != 16.356 {
			t.Fatal("invalid telemetry credited")
		}
	}
	reports := []AttemptReport{{Execution: Execution{ID: "holder", Attempt: 1}, Session: "old", Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2.75}}}
	samples := currentHostMemorySamples(reports, clock.Now())
	if remainingHostGrowth(16.356, samples[attemptKey{"holder", 1, "new"}], clock.Now()) != 16.356 {
		t.Fatal("stale session credited")
	}
}

func TestHostMemoryProbeForecastRequiresCompleteTimedFootprint(t *testing.T) {
	_, clock := fixture(t)
	dir := t.TempDir()
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &memoryAttempt{fs: osMemoryFS{}, path: dir}
	write("memory.current", "8589934592")
	stats := "kernel 134217728\nshmem 134217728\nunevictable 0\nfile_dirty 67108864\nfile_writeback 0\nfile 6442450944\n"
	write("memory.stat", "anon 1073741824\n"+stats)
	first := a.currentHostMemory(clock)
	if first == nil || first.GrowthObserved || first.FootprintGB == nil || *first.FootprintGB != 1.3125 {
		t.Fatal("invented first horizon or counted reclaimable cache", first)
	}
	clock.Advance(time.Second)
	write("memory.stat", "anon 2147483648\n"+stats)
	second := a.currentHostMemory(clock)
	if second == nil || !second.GrowthObserved || second.GrowthGB != 1 || second.GrowthRateGB != 1 || second.FootprintGB == nil || *second.FootprintGB != 2.3125 || second.NonreclaimableGB != 2 {
		t.Fatal("missing measured footprint/growth", second)
	}
	clock.Advance(time.Second)
	write("memory.stat", "anon 2147483648\n")
	missing := a.currentHostMemory(clock)
	if missing == nil || missing.GrowthObserved || missing.FootprintGB != nil {
		t.Fatal("partial counters authorize forecast", missing)
	}
	clock.Advance(time.Second)
	write("memory.stat", "anon 2147483648\n"+stats)
	if a.currentHostMemory(clock).GrowthObserved {
		t.Fatal("bridged missing observation interval")
	}
}

func TestHostMemoryProbeForecastAgesBeforeExpiry(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 16)
	m.HostMemoryBudgetGB = 16
	j := Job{State: "running", Spec: cpuSpec(2, 0), MemoryEnvelope: &JobMemoryEnvelope{ProbeFloorGB: minimumHostMemoryGB}}
	j.Spec.Needs.Memory, j.Spec.Needs.HostMemoryGB = "auto", 16
	j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	sample := hostMemorySample{gb: 1, footprintGB: 1, growthGB: .1, rateGB: 1, growthObserved: true, at: clock.Now()}
	clock.Advance(4 * time.Second)
	charge, _ := jobHostMemoryCharge(&j, &m, 16, sample, clock.Now())
	if charge < 5.1 {
		t.Fatalf("aged forecast charges %g GiB; measured rate predicts at least 5.1", charge)
	}
	// The same aged measurement must prevent a 12 GiB neighbor on a 16 GiB
	// allowance, independently of raw physical availability.
	q.s.Machines[m.ID] = &m
	j.ID, j.Attempt, j.Machine, j.Session = "probe", 1, m.ID, m.Session
	q.s.Jobs[j.ID] = &j
	q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{m.ID: {jobKey(j): sample}}
	q.refreshHostMemory()
	if q.s.Machines[m.ID].FreeHostMemoryGB >= 12 {
		t.Fatal("queue admits predictable growth beyond allowance")
	}
	w := Worker{Machine: m, Clock: clock, hostMemorySamples: map[attemptKey]hostMemorySample{jobKey(j): sample}, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
	next := Job{Spec: cpuSpec(2, 12)}
	next.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	next.Spec.Needs.HostMemoryGB, next.Spec.Needs.MemoryGB = 12, 0
	if err := next.Spec.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := localAttemptAdmission(w, []Job{j}, next, nil); err == nil {
		t.Fatal("worker admits predictable growth beyond allowance")
	}
}
