package myce

import "testing"

func TestTierFairnessModelDispatchIgnoresIncompatibleUsage(t *testing.T) {
	q, clock, m := modelFixture(t)
	policy := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "shared", Selectors: []string{"*/*"}}}, Projects: map[string]ProjectPolicy{"a": {Weight: 3}, "b": {Weight: 1}, "c": {Weight: 1}}}
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	q.models.Models[0].MaxInFlight = 1
	cuda := q.models.Models[0]
	cuda.Name, cuda.Platforms = "cuda-only", []string{"cuda"}
	q.models.Models = append(q.models.Models, cuda)
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	a := &ModelCall{Model: "test", Project: "a", Priority: "production"}
	b := &ModelCall{Model: "test", Project: "b", Priority: "experiment"}
	incompatible := &ModelCall{Model: "cuda-only", Project: "c", Priority: "production"}
	for _, call := range []*ModelCall{a, b, incompatible} {
		if err := q.addCall(call); err != nil {
			t.Fatal(err)
		}
	}
	for project, seconds := range map[string]float64{"a": 60, "b": 10, "c": 1000} {
		q.s.FairUsage[fairUsageKey("shared", "gpu", project)] = FairUsage{Seconds: seconds, At: clock.Now()}
	}
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var selected string
	for _, j := range snapshot.Jobs {
		if j.QueueOrder == 1 {
			selected = j.ID
		}
	}
	if selected != b.ID {
		t.Fatalf("status selected %s, want b=%s", selected, b.ID)
	}
	reportModel(t, q, m, "ready")
	if b.Machine != m.ID || a.Machine != "" || incompatible.Machine != "" {
		t.Fatalf("status selected b, dispatch: a=%q b=%q incompatible=%q", a.Machine, b.Machine, incompatible.Machine)
	}
}
