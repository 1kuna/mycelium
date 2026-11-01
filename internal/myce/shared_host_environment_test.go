package myce

import (
	"context"
	"slices"
	"testing"
)

// A prepared cache entry is discovered by the worker, not configured in its
// initial Machine. Queue placement and local shared-host admission must agree.
func TestSharedHostWorkerAdmitsAdvertisedEnvironment(t *testing.T) {
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	w := harnessWorker(t, h)
	w.Machine.SharedHost = &SharedHostProfile{InteractiveReserveGB: .125}
	w.sharedCPUDev = true
	w.SharedPressureSource = func(context.Context) (SharedHostPressure, error) {
		return SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}, nil
	}
	w.EnvironmentRoot, w.EnvironmentLimit = t.TempDir(), 1<<20
	r := binaryRecipe(t, "cpu")
	cache := EnvironmentCache{Root: w.EnvironmentRoot, Limit: w.EnvironmentLimit, Clock: RealClock{}}
	_, release, err := cache.Acquire(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	release()
	j := harnessJob(t, "prepared-cpu", "", "exit 0")
	j.Spec.Environment.Name = "tools"
	j.EnvironmentRecipes = map[string]EnvironmentRecipe{"cpu": r}
	j.Spec.Run.Command = []string{"gpu-task"}
	h.jobs = []Job{j}
	startHarness(t, w, h)
	await(t, func() bool { return h.completed(j.ID) })
	h.mu.Lock()
	advertised := slices.ContainsFunc(h.reports, func(c Checkin) bool {
		return slices.Contains(c.Machine.Environments, r.Key)
	})
	h.mu.Unlock()
	if !advertised {
		t.Fatal("prepared cache entry was not advertised")
	}
	if result := h.result(j.ID); !result.Success {
		t.Fatalf("advertised prepared environment refused locally: %+v", result)
	}
}
