package myce

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestStatusPageRendersSnapshot(t *testing.T) {
	job := &Job{ID: "job-1", State: "queued", Reason: "atlas is ahead of its fair share; verso goes first"}
	job.Spec.Name = "index backlog"
	job.Spec.Project = "atlas"
	job.Spec.Priority = "experiment"
	machine := &Machine{
		ID:                 "spark",
		Disabled:           "planned host maintenance",
		MemoryEnforcement:  "memory ceiling not enforced on this host (requires Linux cgroup v2)",
		HostMemoryBudgetGB: 5,
		GPU:                "cuda",
		Device:             "0",
		TotalMemoryGB:      60, // 21.5 GiB in use.
		FreeMemoryGB:       38.5,
		ReservedMemoryGB:   4,
		CachedModels:       []string{"qwen"},
		CacheError:         "cache read failed",
		Environments:       []string{"torch-cuda"},
		CopyReports:        []CopyReport{{Model: "embed", State: "copying", Bytes: 50, Total: 100}},
	}
	snapshot := Snapshot{
		Holds:    []Hold{scopedHold("host_disabled", "b70", "host b70 disabled by operator; operator action required", true)},
		Jobs:     []*Job{job},
		Machines: []*Machine{machine},
		LoadedModels: []ModelSlot{
			{Machine: "spark", Model: "qwen", State: "ready", InFlight: 2, Error: "load warning"},
		},
		Pins: []ModelPin{{Machine: "spark", Assigned: "spark", Model: "qwen", Project: "atlas"}},
		Usage: UsageReport{
			Projects: []UsageSummary{{Name: "atlas", GPUHours: 9.5}},
			Machines: []UsageSummary{{Name: "spark", BusyPercent: 72, IdleHours: 2}},
			Daily: []DailyUsage{
				{Date: "2026-10-08", Project: "atlas", GPUHours: 2.25},
				{Date: "2026-10-08", Machine: "spark", BusyPercent: 72},
			},
		},
	}

	var rendered bytes.Buffer
	if err := statusPage.Execute(&rendered, snapshot); err != nil {
		t.Fatal(err)
	}
	html := rendered.String()
	for _, want := range []string{
		"disabled by operator", "Mycelium", "spark", "cuda", "21.5", "38.5", "qwen", "torch-cuda",
		"memory ceiling not enforced on this host (requires Linux cgroup v2)", "Host ceiling budget", "5 GiB",
		"2 calls", "load warning", "cache read failed",
		"index backlog", "atlas is ahead of its fair share; verso goes first", "operator action required",
		`<svg class="chart"`, `data-project="atlas"`, `setInterval(refresh, 5000)`,
	} {
		if !strings.Contains(html, want) {
			t.Errorf("rendered page does not contain %q", want)
		}
	}
	if strings.Contains(strings.ToLower(html), "authorization") || strings.Contains(strings.ToLower(html), "myce_token") {
		t.Error("refresh script must not read or send the fleet token")
	}
}

func TestStatusDurationTracksCurrentAttempt(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("production"))
	m := machine("one", "fake")
	assigned := check(t, q, m, nil).Job
	result := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: assigned.Attempt}}
	if e := q.Started(result); e != nil {
		t.Fatal(e)
	}
	c.Advance(20 * time.Second)
	snap, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if got := JobAge(snap.Jobs[0], snap.Now); got != "running for 20s" {
		t.Fatal(got)
	}
	result.Success = true
	if e = q.Complete(result); e != nil {
		t.Fatal(e)
	}
	c.Advance(10 * time.Second)
	snap, e = q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if got := JobAge(snap.Jobs[0], snap.Now); got != "duration 20s" {
		t.Fatal(got)
	}
	// These fields are serialized by cloneJob, just like durable queue state.
	copy := cloneJob(snap.Jobs[0])
	if copy.FinishedAt == nil || JobAge(&copy, snap.Now) != "duration 20s" {
		t.Fatal(copy)
	}
}
