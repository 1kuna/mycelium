//go:build placement_cost

package myce

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// Time the public check-in, including durable state and every maintenance pass.
// This is an evidence probe, not a shared-host latency assertion. Deterministic
// work counts in TestValuePlanningWorkCounts protect the control path.
// The timing probe also runs unchanged on base.
func TestValueCheckinCost(t *testing.T) {
	for _, count := range []int{200, 800, 1600} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			samples := make([]time.Duration, 3)
			for trial := range samples {
				q, _ := fixture(t)
				accelerator := cpuMachine("a-accelerator", "accelerator", "fake", 4, 64)
				check(t, q, accelerator, nil)
				for i := range 4 {
					id := fmt.Sprintf("cpu-%d", i)
					check(t, q, cpuMachine(id, id, "cpu", 4, 16), nil)
				}
				template := submit(t, q, cpuSpec(1, 1))
				// Bulk-create canonical queued work; setup time is not measured.
				if err := q.update(func() error {
					for i := 1; i < count; i++ {
						copy := cloneJob(template)
						copy.ID = fmt.Sprintf("cost-%06d", i)
						q.s.Jobs[copy.ID] = &copy
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				start := time.Now() // Measurement only; queue decisions use fake Clock.
				check(t, q, accelerator, nil)
				samples[trial] = time.Since(start)
			}
			slices.Sort(samples)
			t.Logf("queued=%d checkin median=%s min=%s max=%s", count, samples[1], samples[0], samples[2])
		})
	}
}
