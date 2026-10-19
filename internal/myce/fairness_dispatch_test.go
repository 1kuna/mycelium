package myce

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

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

// Use the real check-in admission path, not a comparison of two planner calls.
// One device has one batch-sized reservation or one model call slot, so the
// first status candidate has an unambiguous corresponding dispatch decision.
func TestTierFairnessStatusEqualsDispatchSeeded(t *testing.T) {
	const seed = 98
	rng := rand.New(rand.NewSource(seed))
	for _, kind := range []string{"batch", "calls", "mixed"} {
		batchWinners, callWinners := 0, 0
		for trial := 0; trial < 40; trial++ {
			t.Run(fmt.Sprintf("%s/%02d", kind, trial), func(t *testing.T) {
				q, clock, m := modelFixture(t)
				q.models.Models[0].MaxInFlight = 1
				cuda := q.models.Models[0]
				cuda.Name, cuda.Platforms = "cuda-only", []string{"cuda"}
				q.models.Models = append(q.models.Models, cuda)
				m.ModelConfigHash = modelConfigHash(q.models)
				check(t, q, m, nil)
				policy := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "shared", Selectors: []string{"*/*"}}}, Projects: map[string]ProjectPolicy{}}
				for _, project := range []string{"a", "b", "c"} {
					policy.Projects[project] = ProjectPolicy{Weight: float64(1 + rng.Intn(4))}
				}
				if _, err := q.SetPriorityPolicy(0, policy); err != nil {
					t.Fatal(err)
				}
				n := 3 + rng.Intn(5)
				for i := 0; i < n; i++ {
					project := string(rune('a' + rng.Intn(3)))
					priority := []string{"production", "experiment"}[rng.Intn(2)]
					isCall := kind == "calls" || kind == "mixed" && i%2 == 0
					incompatible := i > 1 && rng.Intn(3) == 0
					id := fmt.Sprintf("candidate-%02d", i)
					queued := clock.Now().Add(-time.Duration(rng.Intn(20)) * time.Second)
					if isCall {
						model := "test"
						if incompatible {
							model = "cuda-only"
						}
						q.calls[id] = &ModelCall{ID: id, Model: model, Project: project, Priority: priority, Queued: queued}
					} else {
						s := projectSpec(project)
						s.Priority = priority
						if incompatible {
							s.Needs.Platform = Platform{Required: []string{"cuda"}}
						}
						q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: queued, Checkpoint: filepath.Join(q.root, id)}
					}
				}
				for _, project := range []string{"a", "b", "c"} {
					q.s.FairUsage[fairUsageKey("shared", "gpu", project)] = FairUsage{Seconds: float64(rng.Intn(1500)), At: clock.Now().Add(-time.Duration(rng.Intn(72)) * time.Hour)}
				}
				// Observe ready inventory without running maintenance: the following
				// real check-in must dispatch exactly this status winner.
				if kind != "batch" {
					q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", Attempt: "seeded-load", Priority: "production", LastUsed: clock.Now()}
					q.s.Machines[m.ID].FreeMemoryGB = 0
					q.s.Machines[m.ID].Models = []string{"test"}
				}
				order, _ := q.queueProjection()
				var selected string
				for id, rank := range order {
					if rank == 1 {
						selected = id
					}
				}
				if selected == "" {
					t.Fatalf("seed=%d no eligible candidate", seed)
				}
				if q.calls[selected] == nil {
					batchWinners++
				} else {
					callWinners++
				}
				var reply Reply
				if kind == "batch" {
					reply = check(t, q, m, nil)
				} else {
					reply = reportModel(t, q, m, "ready")
					// A batch winner first reclaims the idle model and waits for its exact
					// unload acknowledgement before the worker can start it.
					if q.calls[selected] == nil && reply.Job == nil {
						reply = reportModel(t, q, m, "unloaded")
					}
				}
				dispatched := ""
				if reply.Job != nil {
					dispatched = reply.Job.ID
				}
				for id, call := range q.calls {
					if call.Machine != "" {
						if dispatched != "" {
							t.Fatal("multiple exclusive candidates dispatched")
						}
						dispatched = id
					}
				}
				if dispatched != selected {
					t.Fatalf("seed=%d status=%s dispatch=%s weights=%v usage=%v", seed, selected, dispatched, policy.Projects, q.s.FairUsage)
				}
			})
		}
		if kind == "mixed" && (batchWinners == 0 || callWinners == 0) {
			t.Fatalf("seed=%d mixed coverage: batch=%d calls=%d", seed, batchWinners, callWinners)
		}
	}
}
