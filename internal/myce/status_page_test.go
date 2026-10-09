package myce

import (
	"bytes"
	"strings"
	"testing"
)

func TestStatusPageRendersSnapshot(t *testing.T) {
	job := &Job{ID: "job-1", State: "queued", Reason: "atlas is ahead of its fair share; verso goes first"}
	job.Spec.Name = "index backlog"
	job.Spec.Project = "atlas"
	job.Spec.Priority = "experiment"
	machine := &Machine{
		ID:               "spark",
		GPU:              "cuda",
		Device:           "0",
		MemoryUsedGB:     21.5,
		FreeMemoryGB:     38.5,
		ReservedMemoryGB: 4,
		CachedModels:     []string{"qwen"},
		CacheError:       "cache read failed",
		Environments:     []string{"torch-cuda"},
		CopyReports:      []CopyReport{{Model: "embed", State: "copying", Bytes: 50, Total: 100}},
	}
	snapshot := Snapshot{
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
		"Mycelium", "spark", "cuda", "21.5", "38.5", "qwen", "torch-cuda",
		"2 calls", "load warning", "cache read failed",
		"index backlog", "atlas is ahead of its fair share; verso goes first",
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
	for _, want := range []string{"No machines have joined", "No production work", "No usage samples yet"} {
		if !strings.Contains(html, want) {
			t.Errorf("empty page does not contain %q", want)
		}
	}
}
