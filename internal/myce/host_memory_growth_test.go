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
