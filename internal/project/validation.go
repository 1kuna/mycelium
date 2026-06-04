package project

import (
	"fmt"

	"mycelium/internal/domain"
)

func Validate(project domain.Project) error {
	if project.ID == "" {
		return fmt.Errorf("project id is required")
	}
	if !validPriority(project.Priority) {
		return fmt.Errorf("project %q priority %q is invalid", project.ID, project.Priority)
	}
	if !validSpeedPref(project.SpeedPref) {
		return fmt.Errorf("project %q speed_pref %q is invalid", project.ID, project.SpeedPref)
	}
	if !validPreemption(project.Preemption) {
		return fmt.Errorf("project %q preemption %q is invalid", project.ID, project.Preemption)
	}
	if project.ContextCap < 0 {
		return fmt.Errorf("project %q context_cap must be non-negative", project.ID)
	}
	if project.ExpectedConcurrency < 0 {
		return fmt.Errorf("project %q expected_concurrency must be non-negative", project.ID)
	}
	if project.LatencyTargetMS < 0 {
		return fmt.Errorf("project %q latency_target_ms must be non-negative", project.ID)
	}
	return nil
}

func ValidateSet(projects []domain.Project, defaultProject string) error {
	seen := map[string]struct{}{}
	for _, project := range projects {
		if err := Validate(project); err != nil {
			return err
		}
		if _, ok := seen[project.ID]; ok {
			return fmt.Errorf("duplicate project id %q", project.ID)
		}
		seen[project.ID] = struct{}{}
	}
	if defaultProject != "" && len(projects) > 0 {
		if _, ok := seen[defaultProject]; !ok {
			return fmt.Errorf("default_project %q is not defined in projects", defaultProject)
		}
	}
	return nil
}

func validPriority(priority domain.Priority) bool {
	switch priority {
	case "", domain.PriorityInteractive, domain.PriorityNormal, domain.PriorityBackground:
		return true
	default:
		return false
	}
}

func validSpeedPref(speed domain.SpeedPref) bool {
	switch speed {
	case "", domain.SpeedThroughput, domain.SpeedLatency, domain.SpeedAuto:
		return true
	default:
		return false
	}
}

func validPreemption(preemption domain.Preemption) bool {
	switch preemption {
	case "", domain.PreemptInherit, domain.PreemptSoft, domain.PreemptHardForInteractive, domain.PreemptHard:
		return true
	default:
		return false
	}
}
