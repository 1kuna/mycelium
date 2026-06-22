package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunBootstrapPlannerConformance(t *testing.T, name string, newPlanner func() ports.BootstrapPlanner, host domain.HostFacts, profile domain.EngineProfile) {
	t.Run(name+"/plan_contains_requested_profile", func(t *testing.T) {
		plan, err := newPlanner().PlanBootstrap(context.Background(), domain.BootstrapRequest{RequestedEngines: []domain.Backend{profile.Backend}}, host, []domain.EngineProfile{profile})
		assert.NoError(t, "PlanBootstrap", err)
		assert.True(t, plan.ID != "", "plan id should be set")
		assert.True(t, len(plan.ResultingProfiles) > 0, "plan should include resulting profiles: %+v", plan)
	})
}

func RunEngineInstallerConformance(t *testing.T, name string, newInstaller func() ports.EngineInstaller, plan domain.BootstrapPlan) {
	t.Run(name+"/apply_reports_plan_result", func(t *testing.T) {
		var events []domain.BootstrapEvent
		result, err := newInstaller().ApplyBootstrapPlan(context.Background(), plan, func(event domain.BootstrapEvent) {
			events = append(events, event)
		})
		assert.NoError(t, "ApplyBootstrapPlan", err)
		assert.Equal(t, plan.ID, result.PlanID, "result plan id")
		_ = events
	})
}

func RunEngineVerifierConformance(t *testing.T, name string, newVerifier func() ports.EngineVerifier, profile domain.EngineProfile) {
	t.Run(name+"/verify_reports_profile", func(t *testing.T) {
		verification, err := newVerifier().VerifyEngine(context.Background(), profile)
		assert.NoError(t, "VerifyEngine", err)
		assert.Equal(t, profile.ID, verification.ProfileID, "verification profile id")
	})
}

func RunEngineRegistryConformance(t *testing.T, name string, newRegistry func() ports.EngineRegistry, profile domain.EngineProfile) {
	t.Run(name+"/save_list_mark_unready", func(t *testing.T) {
		registry := newRegistry()
		assert.NoError(t, "SaveEngineProfile", registry.SaveEngineProfile(context.Background(), profile))
		profiles, err := registry.ListEngineProfiles(context.Background())
		assert.NoError(t, "ListEngineProfiles", err)
		assert.True(t, len(profiles) == 1 && profiles[0].ID == profile.ID, "profiles = %+v", profiles)
		assert.NoError(t, "MarkEngineProfileUnready", registry.MarkEngineProfileUnready(context.Background(), profile.ID, "failed"))
		profiles, err = registry.ListEngineProfiles(context.Background())
		assert.NoError(t, "ListEngineProfiles after unready", err)
		assert.True(t, len(profiles) == 1 && !profiles[0].Ready && profiles[0].UnreadyReason == "failed", "profiles after unready = %+v", profiles)
	})
}
