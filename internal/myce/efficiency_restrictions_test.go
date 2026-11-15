package myce

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A job that excludes a host with room accrues stranded queue time, and its
// project sees the cost and the advice by platform, never by host name.
func TestEfficiencyRestrictionCost(t *testing.T) {
	q, c := fixture(t)
	busy := cpuMachine("busy", "busy", "cpu", 64, 121)
	busy.HostMemoryBudgetGB = 121
	busy.CapabilityProfile.SystemMemoryBytes = 121 << 30
	check(t, q, busy, nil)
	idle := cpuMachine("idle", "idle", "cpu", 16, 64)
	idle.CapabilityProfile.Arch = "arm64"
	check(t, q, idle, nil)
	var running, waiting *Job
	if err := q.update(func() error {
		m := q.touchMachine("busy")
		s := cpuSpec(4, 84)
		if err := normalizeResources(&s); err != nil {
			return err
		}
		running = &Job{ID: "running", Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, Queued: c.Now(), StartedAt: timePointer(c.Now()), MemoryEnvelope: &JobMemoryEnvelope{Basis: "exploratory", HostGB: 84}}
		q.putJob(running.ID, running)
		s = cpuSpec(4, 40)
		if err := normalizeResources(&s); err != nil {
			return err
		}
		s.ExcludedMachines = []string{"idle"}
		waiting = &Job{ID: "waiting", Spec: s, State: "queued", Queued: c.Now()}
		q.putJob(waiting.ID, waiting)
		q.refreshHostMemory()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !waiting.Efficiency.Stranded {
		t.Fatalf("not stranded: %+v stranded=%+v", waiting.Efficiency, q.s.StrandedDemand)
	}
	c.Advance(2 * time.Hour)
	check(t, q, busy, running)
	check(t, q, idle, nil)
	if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	var p *ProjectRestrictions
	for i := range q.efficiencyView.Restrictions {
		if q.efficiencyView.Restrictions[i].Project == waiting.Spec.Project {
			p = &q.efficiencyView.Restrictions[i]
		}
	}
	if p == nil || p.Jobs != 2 || p.HostRestricted != 1 || p.HostWaitSeconds != 2*3600 || p.HostStrandedJobs != 1 || p.Restricted != 1 || p.RestrictedWaitSeconds != 2*3600 || p.StrandedJobs != 1 || p.StrandedSeconds != 2*3600 || p.TypedMemory != 2 || p.TypedCores != 2 {
		t.Fatalf("restrictions=%+v", q.efficiencyView.Restrictions)
	}
	text := strings.Join(q.efficiencyView.ProjectLines(waiting.Spec.Project), "\n")
	for _, want := range []string{"named-host lists (pins): 1/2 (50%)", "plain queue wait 2.0 hours", "review allowed_machines and excluded_machines", "a linux/arm64 host has room", "stop typing host_memory_gb"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in\n%s", want, text)
		}
	}
	if strings.Contains(text, "idle") || strings.Contains(text, "busy") {
		t.Fatalf("project view names a host:\n%s", text)
	}
	// Once the excluded host has no room, the stranded clock stops.
	q.s.Machines["idle"].FreeCPUCores = 0
	if err := q.updateEfficiencyDiagnostics(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Hour)
	if got := q.efficiencyReport().Restrictions; got[0].StrandedSeconds != 2*3600 {
		t.Fatalf("stranded time kept counting: %+v", got)
	}
}

func TestEfficiencyRestrictionCountsAndPlainWait(t *testing.T) {
	q, c := fixture(t)
	now := c.Now()
	queued := now.Add(-2 * time.Hour)
	started := queued.Add(30 * time.Minute)
	for _, j := range []*Job{
		{ID: "lists", Spec: Spec{Project: "demo", AllowedMachines: []string{"one"}, ExcludedMachines: []string{"two"}}, State: "queued", Queued: queued},
		{ID: "targets", Spec: Spec{Project: "demo", Targets: []ExecutionTarget{{OS: "linux"}}}, State: "succeeded", Queued: queued, StartedAt: &started},
		{ID: "both", Spec: Spec{Project: "demo", AllowedMachines: []string{"one"}, Targets: []ExecutionTarget{{OS: "linux"}}}, State: "queued", Queued: queued,
			Efficiency: &JobEfficiency{At: now.Add(-time.Minute), Blocker: "capacity", WaitSeconds: map[string]float64{"capacity": 60, "holds": 60},
				Rate: efficiencyRate{Host: "one"}, Allocations: []EfficiencyAllocation{{Host: "one", ResourceIntegral: ResourceIntegral{Seconds: 10}}}}},
		{ID: "platform-only", Spec: Spec{Project: "other", Targets: []ExecutionTarget{{OS: "darwin"}}}, State: "succeeded", Queued: queued, StartedAt: &started},
	} {
		q.putJob(j.ID, j)
	}
	rows := q.projectRestrictions(now, []StrandedDemand{
		{JobID: "lists", Project: "demo", Platform: "linux", Fields: []string{"allowed_machines"}},
		{JobID: "lists", Project: "demo", Platform: "darwin", Fields: []string{"excluded_machines"}},
		{JobID: "targets", Project: "demo", Platform: "linux", Fields: []string{"targets"}},
		{JobID: "both", Project: "demo", Platform: "linux", Fields: []string{"allowed_machines", "targets"}},
		{JobID: "both", Project: "demo", Platform: "darwin", Fields: []string{"allowed_machines", "policy.projects.allowed_machines"}},
	})
	if len(rows) != 2 || rows[0].Jobs != 3 || rows[0].Restricted != 3 || rows[0].HostRestricted != 2 || rows[0].Targeted != 2 || rows[0].StrandedJobs != 3 || rows[0].HostStrandedJobs != 1 || rows[0].HostWaitSeconds != 7380 || rows[0].RestrictedWaitSeconds != 9180 || rows[1].Restricted != 1 || rows[1].HostRestricted != 0 || rows[1].HostWaitSeconds != 0 || rows[1].HostStrandedJobs != 0 {
		t.Fatalf("restrictions=%+v", rows)
	}
	if q.s.Jobs["both"].Efficiency.WaitSeconds["capacity"] != 60 {
		t.Fatal("report mutated recorded wait")
	}
	if q.s.Jobs["both"].Efficiency.Allocations[0].Seconds != 10 {
		t.Fatal("wait report mutated resource accounting")
	}
	other := strings.Join((&EfficiencyReport{Restrictions: rows}).ProjectLines("other"), "\n")
	if !strings.Contains(other, "named-host lists (pins): 0/1 (0%)") || !strings.Contains(other, "plain queue wait 0.0 hours") || strings.Contains(other, "drop targets") || strings.Contains(other, "targets cost") {
		t.Fatalf("platform compatibility reported as pin cost: %s", other)
	}
	text := strings.Join((&EfficiencyReport{Restrictions: rows}).ProjectLines("demo"), "\n")
	for _, want := range []string{"named-host lists (pins): 2/3 (67%)", "platform targets (software compatibility): 2/3 (67%)", "plain queue wait 2.0 hours", "1 stranded jobs now", "named-host counterfactual wait saving unavailable"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in\n%s", want, text)
		}
	}
}
