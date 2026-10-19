package myce

import (
	"context"
	"testing"
	"time"
)

// A heartbeat can snapshot "loading" just before the load fails. Its delayed
// reply must not restart that same attempt before the failure is reported.
func TestRuntimeFailedAttemptRejectsDelayedLoadingPlan(t *testing.T) {
	model := Model{Name: "test", Engine: "fake", MemoryGB: 1}
	old := &workerModel{model: model, attempt: "failed-attempt", state: "failed", err: "verification failed"}
	creations := 0
	rt := &modelRuntime{models: map[string]*workerModel{"test": old}, session: "session", config: ModelConfig{Wait: time.Second, Models: []Model{model}}, memorySource: func(context.Context) (float64, error) { return 8, nil }, engineFactory: func(Model) Engine { creations++; return &FakeEngine{} }}
	plan := []ModelSlot{{Model: model.Name, Session: rt.session, Attempt: old.attempt, State: "loading"}}
	rt.sync(plan)
	rt.wg.Wait()
	if creations != 0 || rt.models[model.Name] != old || old.state != "failed" {
		t.Fatal("delayed loading reply restarted failed attempt", creations, rt.models[model.Name].state)
	}
	plan[0].Attempt = "fresh-attempt"
	rt.sync(plan)
	rt.wg.Wait()
	if creations != 1 || rt.models[model.Name].attempt != "fresh-attempt" || rt.models[model.Name].state != "ready" {
		t.Fatal("fresh queue attempt did not load")
	}
}
