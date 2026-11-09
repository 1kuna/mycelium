package myce

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinBenchmarkOptIn(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, mode := range []string{"default-empty", "enabled"} {
			t.Run(mode, func(t *testing.T) {
				q, _ := fixture(t)
				root := newTestStateRoot(t)
				registered := make(chan struct{})
				admitted := make(chan struct{})
				var registerOnce sync.Once
				var once sync.Once
				handler := API{Queue: q, Token: "test"}.Handler()
				server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/api/workers/checkin" {
						registerOnce.Do(func() { close(registered) })
					}
					handler.ServeHTTP(w, r)
					snap, err := q.Snapshot()
					if err == nil && len(snap.Machines) > 0 && !snap.Machines[0].BenchmarkPending {
						once.Do(func() { close(admitted) })
					}
				}))
				defer server.Close()
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				defer func() {
					cancel()
					if err := <-done; err != nil {
						t.Error(err)
					}
				}()
				var called atomic.Bool
				w := Worker{EnvironmentLimit: 1 << 30, InventoryProbe: benchmarkTestInventory, CPUIsolationProbe: func(context.Context) error { return nil }, OnboardingBenchmark: mode == "enabled", StateDir: root, Client: Client{URL: server.URL, Token: "test"}, Machine: machine("dedicated", "xpu"), Interval: time.Millisecond,
					OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} },
					Benchmark: func(_ context.Context, _ BenchmarkConfig, p *CapabilityProfile) error {
						called.Store(true)
						if mode != "enabled" {
							t.Error("benchmark ran without opt-in")
						}
						snap, _ := q.Snapshot()
						if len(snap.Machines) != 1 || !snap.Machines[0].BenchmarkPending {
							t.Error("benchmark ran without queue registration")
						}
						p.Onboarding = "measured; qualified reference replay"
						p.Benchmark = &BenchmarkResult{State: "partial", Measurements: []Throughput{{Resource: "cpu", Rate: 100, Unit: "bytes/s"}, {Resource: "accelerator", Reason: "no qualified reference work"}}}
						return saveWorkerJSON(root, "capability-profile.json", p)
					},
				}
				go func() { done <- w.Run(ctx) }()
				// Joining's admission assertion starts at queue registration, after local
				// profile/store/model-server startup, which this test does not measure.
				select {
				case <-registered:
				case err := <-done:
					done <- err // Cleanup still owns the worker join.
					t.Fatalf("worker stopped before registration: %v", err)
				case <-time.After(testWaitTimeout):
					t.Fatal("worker never registered")
				}
				select {
				case <-admitted:
				case <-time.After(2 * time.Second):
					t.Fatal("join did not admit within two seconds")
				}
				if mode == "enabled" && !called.Load() {
					t.Fatal("opted-in benchmark never ran")
				}

				if mode == "enabled" {
					snap, err := q.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					p := snap.Machines[0].CapabilityProfile
					if p == nil || p.Benchmark == nil || p.Benchmark.State != "partial" || len(p.Benchmark.Measurements) != 2 || p.Benchmark.Measurements[0].Rate != 100 || p.Onboarding != "measured; qualified reference replay" {
						t.Fatalf("completed benchmark lost from status: %+v", p)
					}
				}
			})
		}

	})

	t.Run("PlacementWithoutBenchmark", func(t *testing.T) {
		for _, profile := range []bool{false, true} {
			q, clock := fixture(t)
			m := machine("dedicated", "fake")
			if profile {
				// Hardware facts remain required even when throughput is unmeasured.
				m.CapabilityProfile.LogicalCPUs = 8
			}
			check(t, q, m, nil)
			j := submit(t, q, spec("experiment"))
			clock.Advance(time.Second)
			if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
				t.Fatal("job not placed without benchmark")
			}
		}

	})
}
