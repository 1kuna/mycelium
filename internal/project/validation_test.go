package project

import (
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestValidateProject(t *testing.T) {
	valid := domain.Project{ID: "project-a", Priority: domain.PriorityNormal, SpeedPref: domain.SpeedAuto, Preemption: domain.PreemptSoft}
	if err := Validate(valid); err != nil {
		t.Fatalf("valid project: %v", err)
	}
	for _, tt := range []struct {
		name    string
		project domain.Project
		want    string
	}{
		{name: "missing id", project: domain.Project{}, want: "project id"},
		{name: "priority", project: domain.Project{ID: "project-a", Priority: "urgent"}, want: "priority"},
		{name: "speed", project: domain.Project{ID: "project-a", SpeedPref: "fast"}, want: "speed_pref"},
		{name: "preemption", project: domain.Project{ID: "project-a", Preemption: "always"}, want: "preemption"},
		{name: "context", project: domain.Project{ID: "project-a", ContextCap: -1}, want: "context_cap"},
		{name: "concurrency", project: domain.Project{ID: "project-a", ExpectedConcurrency: -1}, want: "expected_concurrency"},
		{name: "latency", project: domain.Project{ID: "project-a", LatencyTargetMS: -1}, want: "latency_target_ms"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(tt.project); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate err = %v want %q", err, tt.want)
			}
		})
	}
}

func TestValidateProjectSet(t *testing.T) {
	project := domain.Project{ID: "project-a"}
	if err := ValidateSet([]domain.Project{project}, "project-a"); err != nil {
		t.Fatalf("valid set: %v", err)
	}
	if err := ValidateSet([]domain.Project{project, project}, "project-a"); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate err = %v", err)
	}
	if err := ValidateSet([]domain.Project{project}, "missing"); err == nil || !strings.Contains(err.Error(), "default_project") {
		t.Fatalf("default err = %v", err)
	}
}
