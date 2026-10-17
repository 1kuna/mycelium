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
		Disabled:           "host closed",
		MemoryEnforcement:  "memory ceiling not enforced on this host (requires Linux cgroup v2)",
		HostMemoryBudgetGB: 5,
		DockerCleanup:      "cleanup unconfirmed: start",
		GPU:                "cuda",
		Device:             "0",
		MemoryUsedGB:       21.5,
		FreeMemoryGB:       38.5,
		ReservedMemoryGB:   4,
		CachedModels:       []string{"qwen"},
		CacheError:         "cache read failed",
		Environments:       []string{"torch-cuda"},
		CopyReports:        []CopyReport{{Model: "embed", State: "copying", Bytes: 50, Total: 100}},
	}
	snapshot := Snapshot{
		HostHolds: []string{"b70: Docker cleanup unconfirmed; operator action required (myce resolve-docker)"},
		Jobs:      []*Job{job},
		Machines:  []*Machine{machine},
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
		"2 calls", "load warning", "cache read failed", "Docker cleanup unconfirmed: start",
		"index backlog", "atlas is ahead of its fair share; verso goes first", "operator action required (myce resolve-docker)",
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

func TestStatusPageEscapesSnapshotData(t *testing.T) {
	attack := `<script>alert("owned")</script>`
	job := &Job{ID: attack, State: "failed", Reason: attack}
	job.Spec.Name = attack
	job.Spec.Priority = "production"
	snapshot := Snapshot{
		Warning: attack,
		Jobs:    []*Job{job},
		Usage: UsageReport{
			Projects: []UsageSummary{{Name: attack, GPUHours: 1}},
			Daily:    []DailyUsage{{Date: "2026-10-08", Project: attack, GPUHours: 1}},
		},
	}

	var rendered bytes.Buffer
	if err := statusPage.Execute(&rendered, snapshot); err != nil {
		t.Fatal(err)
	}
	html := rendered.String()
	if strings.Contains(html, attack) {
		t.Fatal("snapshot data was rendered without HTML escaping")
	}
	if !strings.Contains(html, `&lt;script&gt;alert`) {
		t.Fatal("escaped snapshot data is missing")
	}
}

func TestStatusPageEmptyStates(t *testing.T) {
	var rendered bytes.Buffer
	if err := statusPage.Execute(&rendered, Snapshot{}); err != nil {
		t.Fatal(err)
	}
	html := rendered.String()
	for _, want := range []string{"No hyphae have joined", "No production work", "No usage samples yet"} {
		if !strings.Contains(html, want) {
			t.Errorf("empty page does not contain %q", want)
		}
	}
}

func TestStatusPageJobLabelsAndAge(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	start := now.Add(-65 * time.Second)
	running := &Job{ID: "12345678abcdef", State: "running", Attempt: 1, StartedAt: &start, Spec: Spec{Name: "production run", Priority: "production"}}
	waiting := &Job{ID: "abcdefgh123456", State: "queued", Queued: now.Add(-2 * time.Minute), Spec: Spec{Name: "waiting", Priority: "production"}, QueueOrder: 1}
	finished := &Job{ID: "done", State: "succeeded", StartedAt: &start, FinishedAt: &now}
	var b bytes.Buffer
	if e := statusPage.Execute(&b, Snapshot{Now: now, Jobs: []*Job{waiting, running, finished}}); e != nil {
		t.Fatal(e)
	}
	html := b.String()
	for _, want := range []string{`title="12345678abcdef">12345678</code>`, "running · attempt 1", "running for 1m5s", "waited 2m0s", "duration 1m5s"} {
		if !strings.Contains(html, want) {
			t.Error(want)
		}
	}
	for _, bad := range []string{"No additional details", "attempt 0", "running · 0"} {
		if strings.Contains(html, bad) {
			t.Error(bad)
		}
	}
	if strings.Index(html, "production run") > strings.Index(html, "waiting</div>") {
		t.Fatal("running must appear first")
	}
	if JobState(waiting) != "queued" {
		t.Fatal("unstarted jobs must not show attempt zero")
	}
}

func TestModelCallAgeUsesAssignmentClock(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	start := now.Add(-5 * time.Second)
	call := &ModelCall{ID: "call", Queued: now.Add(-time.Minute), StartedAt: &start}
	job := callJob(call)
	job.State = "running"
	if got := JobAge(job, now); got != "running for 5s" {
		t.Fatal(got)
	}
	job.State = "queued"
	if got := JobAge(job, now); got != "waited 1m0s" {
		t.Fatal(got)
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

func TestRequeuedWaitAgeExcludesPreviousRunningTime(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	check(t, q, m, nil)
	c.Advance(20 * time.Second)
	if e := q.update(func() error { q.retry(q.s.Jobs[j.ID], "preempted"); return nil }); e != nil {
		t.Fatal(e)
	}
	c.Advance(3 * time.Second)
	snap, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if got := JobAge(snap.Jobs[0], snap.Now); got != "waited 3s" {
		t.Fatal(got)
	}
	if !snap.Jobs[0].Queued.Equal(j.Queued) {
		t.Fatal("display age changed dispatch's submission time")
	}
}

func TestStatusPageStoppedWorkerBudgetIsLastReported(t *testing.T) {
	for _, effective := range []bool{false, true} {
		m := &Machine{ID: "stopped", Gone: true, HostMemoryBudgetGB: 22}
		if effective {
			m.EffectiveHostMemoryBudgetGB = 22
		}
		var b bytes.Buffer
		if err := statusPage.Execute(&b, Snapshot{Machines: []*Machine{m}}); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(b.String(), "Host ceiling budget (last reported): 22 GiB") {
			t.Fatal(b.String())
		}
	}
}
