package main

import (
	"context"
	"runtime"
	"testing"

	"mycelium/internal/myce"
)

// Exercise both in-process installSyntheticHost and the tagged CLI child with
// the real worker/API paths. Their hardware-probe guard remains enabled.
func assertSyntheticResourceTelemetry(t *testing.T, ctx context.Context, client myce.Client, id string) {
	t.Helper()
	j, err := client.GetJob(ctx, id)
	if err != nil || len(j.ResourceAttempts) != 1 || j.ResourceAttempts[0].Resources == nil {
		t.Fatalf("synthetic attempt telemetry: %+v %v", j, err)
	}
	cpu := j.ResourceAttempts[0].Resources.CPU
	if runtime.GOOS == "darwin" {
		if cpu.UsageSeconds == nil || *cpu.UsageSeconds < 1.25 || cpu.Source != "sampled_process_tree" || !cpu.LowerBound {
			t.Fatalf("synthetic CPU probe not used: %+v", cpu)
		}
	} else if runtime.GOOS == "linux" && (cpu.UsageSeconds != nil || cpu.Unavailable != "no_local_cgroup") {
		t.Fatalf("uncontained fixture must not read a real CPU cgroup: %+v", cpu)
	}
	wait(t, ctx, func() bool {
		s, err := status(ctx, client)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range s.Machines {
			if p := m.IOPressure; p != nil && p.Some != nil && p.Some.TotalUS == 1234 && p.Full != nil && !p.SampledAt.IsZero() {
				return true
			}
		}
		return false
	})
}
