package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Shaped like the live fleet on 2026-11-11 (five hosts, 67 current jobs of which
// 62 paused, 49 alert conditions in 11 kinds). The previous default rendered
// that queue as 33,924 bytes; the summary must stay a few KB and keep what an
// agent acts on: running and waiting jobs with reasons, holds, hosts, alerts.
func TestCompactStatusIsSmallAndKeepsWhatMatters(t *testing.T) {
	now := time.Date(2026, 11, 11, 6, 0, 0, 0, time.UTC)
	release := myce.RunningRelease{Version: "v3.1.0-test.2"}
	s := myce.Snapshot{Now: now, QueueRelease: release,
		Summary: &myce.StatusSummary{RetainedJobs: 8600, Alerts: []myce.StatusAlertGroup{
			{Kind: "efficiency/memory_prediction_relative_error", Count: 18, Since: now.Add(-72 * time.Hour)},
			{Kind: "recipe", Count: 17, Since: now.Add(-time.Hour), Scopes: map[string]int{"gpu-a": 8, "gpu-b": 5, "mac-a": 3, "mac-b": 1}},
			{Kind: "efficiency/wait_holds_seconds", Count: 3, Since: now, Scopes: map[string]int{"project:a": 1, "project:b": 1, "project:c": 1}},
		}},
		HealthAlertDelivery: &myce.HealthAlertDelivery{Pending: 22415},
		FleetUpgrade:        &myce.FleetUpgradeStatus{Operation: "done-upgrade", State: "complete"},
		HostStates:          map[string]string{"gpu-a": "healthy", "gpu-b": "degraded"},
		ComponentExclusions: []myce.ComponentExclusion{{Selector: "host=mac-c", Active: true, SetBy: "operator", Reason: "storage faults"}},
	}
	for i, id := range []string{"gpu-a", "gpu-b", "mac-a", "mac-b", "mac-c"} {
		m := &myce.Machine{ID: id, GPU: "cuda", FreeMemoryGB: 30, Release: &release, HostMemoryMeasured: true, FreeHostMemoryGB: 8}
		if i == 4 {
			m.Disabled, m.Release = "operator", &myce.RunningRelease{Version: "v3.1.0-test.1"}
		}
		s.Machines = append(s.Machines, m)
	}
	started := now.Add(-29 * time.Minute)
	for i := range 3 {
		s.Jobs = append(s.Jobs, &myce.Job{ID: fmt.Sprintf("run%05d-full-id", i), State: "running", Attempt: 1, Machine: "gpu-a", StartedAt: &started, Spec: myce.Spec{Name: "train", Project: "a", Class: "experiment"}})
	}
	s.Jobs = append(s.Jobs, &myce.Job{ID: "wait0000-full-id", State: "queued", Queued: now.Add(-64 * time.Hour), Reason: "memory_capacity: free memory insufficient for declared needs", Spec: myce.Spec{Name: "viewer", Project: "b", Class: "experiment"}})
	s.Jobs = append(s.Jobs, &myce.Job{ID: "wait0001-full-id", State: "queued", Queued: now.Add(-77 * time.Hour), Spec: myce.Spec{Name: "naming", Project: "c", Class: "experiment"}})
	for i := range 62 {
		reason, project := "latest tail-first priority", "b"
		if i >= 55 {
			reason, project = "superseded diagnostic head "+strings.Repeat("x", 300), "a"
		}
		s.Jobs = append(s.Jobs, &myce.Job{ID: fmt.Sprintf("pause%03d-full-id", i), State: "paused", Pause: &myce.JobPause{Reason: reason}, Spec: myce.Spec{Name: "vod", Project: project, Class: "experiment"}})
	}
	var out bytes.Buffer
	printCompactStatus(&out, s, false)
	text := out.String()
	if out.Len() > 4096 {
		t.Fatalf("default status is %d bytes:\n%s", out.Len(), text)
	}
	for _, want := range []string{
		"jobs: 67 current / 8600 retained · 3 running · 2 waiting · 62 paused",
		"alerts: 38 active in 3 kinds",
		"efficiency/memory_prediction_relative_error ×18 since",
		"recipe ×17 (gpu-a 8, gpu-b 5, mac-a 3, mac-b) since",
		"22415 retained alerts",
		"host gpu-b: degraded",
		`exclusion host=mac-c: set by operator`,
		"hypha gpu-a: cuda, 30.0 GiB free, hardware capability profile unavailable; host RAM 8.0 GiB for admission; running 3",
		"hypha mac-c: cuda, 30.0 GiB free, disabled by operator; host RAM 8.0 GiB for admission; release v3.1.0-test.1",
		"run00000 train [a/experiment] running · attempt 1 on gpu-a running for 29m0s\n",
		"wait0000 viewer [b/experiment] queued waited 64h0m0s; memory_capacity: free memory insufficient for declared needs",
		"wait0001 naming [c/experiment] queued waited 77h0m0s; waiting for a hypha to take this job",
		"paused 55 b: latest tail-first priority\n",
		"paused 7 a: superseded diagnostic head xxx",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
	for _, unwanted := range []string{"unknown", "done-upgrade", "host gpu-a:", "pause000", "full-id", strings.Repeat("x", 200)} {
		if strings.Contains(text, unwanted) {
			t.Errorf("default printed %q:\n%s", unwanted, text)
		}
	}
	// Default --json is the same projection: full IDs for every job, no full
	// job or machine objects.
	data, err := json.Marshal(myce.NewCompactStatus(s))
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Counts map[string]int          `json:"counts"`
		Jobs   []myce.CompactJob       `json:"jobs"`
		Paused []myce.CompactPaused    `json:"paused"`
		Hyphas []myce.CompactHypha     `json:"hyphas"`
		Alerts []myce.StatusAlertGroup `json:"alert_groups"`
	}
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatal(err)
	}
	if len(data) > 10<<10 || len(view.Jobs) != 5 || len(view.Hyphas) != 5 || len(view.Alerts) != 3 || view.Counts["paused"] != 62 ||
		len(view.Paused) != 2 || len(view.Paused[0].IDs) != 55 || view.Paused[0].IDs[0] != "pause000-full-id" || view.Jobs[3].ID != "wait0000-full-id" || view.Jobs[3].Since == nil ||
		!strings.Contains(view.Paused[1].Reason, strings.Repeat("x", 300)) || strings.Contains(string(data), `"spec"`) || strings.Contains(string(data), "capability_profile") {
		t.Fatalf("compact JSON (%d bytes): %s", len(data), data)
	}
	out.Reset()
	printCompactStatus(&out, s, true)
	if !strings.Contains(out.String(), "run00000-full-id train") {
		t.Fatal("--full lost full job IDs")
	}
}
