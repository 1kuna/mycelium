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
	if p == nil || p.Jobs != 2 || p.HostRestricted != 1 || p.StrandedSeconds != 2*3600 || p.TypedMemory != 2 || p.TypedCores != 2 {
		t.Fatalf("restrictions=%+v", q.efficiencyView.Restrictions)
	}
	text := strings.Join(q.efficiencyView.ProjectLines(waiting.Spec.Project), "\n")
	for _, want := range []string{"2.0 queue hours waited while an excluded host had room", "advice: your restrictions cost 2.0 queue hours", "drop allowed_machines and excluded_machines", "a linux/arm64 host has room", "stop typing host_memory_gb"} {
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
