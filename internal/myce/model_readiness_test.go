package myce

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeLoadAdmissionDoesNotConsumeModelSilence(t *testing.T) {
	for _, phase := range []string{"memory", "authorization"} {
		t.Run(phase, func(t *testing.T) {
			rt, m, ctx := runtimeLoadFixture(t, &FakeEngine{})
			clock := &manualWorkerClock{now: time.Unix(1, 0)}
			rt.clock, rt.dir = clock, t.TempDir()
			identity := m.model
			identity.Platforms = []string{rt.platform}
			mergeReadinessHistory(modelReadinessFile(rt.dir, identity), readinessHistory{Load: readinessLatency{Mean: 100 * time.Millisecond}})
			expectedBudget := readReadinessHistory(modelReadinessFile(rt.dir, identity)).Load.budget()
			waitForAdmission := func(ctx context.Context) {
				// A blocked control request is not a stalled model process.
				clock.Advance(time.Second)
				if _, stalled := readinessFromContext(ctx).state(); stalled {
					t.Error("admission wait classified as model-load silence")
				}
			}
			if phase == "memory" {
				rt.memorySource = func(ctx context.Context) (float64, error) {
					waitForAdmission(ctx)
					return 8, nil
				}
			} else {
				ctx = withLaunchGate(ctx, func(ctx context.Context) error { waitForAdmission(ctx); return nil })
			}
			rt.load(ctx, m)
			if m.state != "ready" {
				t.Fatalf("admitted model did not become ready: %s %s", m.state, m.err)
			}
			if budget, _ := m.progress.state(); budget != expectedBudget {
				t.Fatalf("admission wait enlarged the model silence budget: %s", budget)
			}
		})
	}
}
