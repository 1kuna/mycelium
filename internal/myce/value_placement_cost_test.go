package myce

import (
	"fmt"
	"runtime/debug"
	"slices"
	"testing"
	"time"
)

// Time the public check-in, including durable state and every maintenance pass.
// Fixture uses pre-existing helpers so it can also be measured unchanged on base.
func TestValueCheckinCost(t *testing.T) {
	for _, count := range []int{50, 200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			samples := make([]time.Duration, 5)
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
			// Four times rounded-up paired base medians on this laptop.
			// Race instrumentation has its own measured baseline; docs/simulator.md
			// records both. Five fresh trials limit noise without timing replays.
			base := map[int]time.Duration{50: 18 * time.Millisecond, 200: 90 * time.Millisecond}[count]
			if info, ok := debug.ReadBuildInfo(); ok {
				for _, setting := range info.Settings {
					if setting.Key == "-race" && setting.Value == "true" {
						base = map[int]time.Duration{50: 60 * time.Millisecond, 200: 325 * time.Millisecond}[count]
					}
				}
			}
			if limit := 4 * base; samples[2] > limit {
				t.Errorf("check-in median %s exceeds four-times-base bound %s", samples[2], limit)
			}
			t.Logf("queued=%d checkin median=%s min=%s max=%s", count, samples[2], samples[0], samples[4])
		})
	}
}
