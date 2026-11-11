package myce

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestJoinBenchmarkOptIn(t *testing.T) {
	for _, mode := range []string{"default-empty", "default-cached", "enabled"} {
		t.Run(mode, func(t *testing.T) {
			q, _ := fixture(t)
			root := newTestStateRoot(t)
			cache := filepath.Join(root, "capability-profile.json")
			original := []byte(`{"benchmark":{"state":"partial"}}`)
			if mode == "default-cached" {
				if err := os.WriteFile(cache, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			registered := make(chan struct{})
			admitted := make(chan struct{})
			benchmarkEntered := make(chan struct{})
			benchmarkRelease := make(chan struct{})
			var releaseOnce sync.Once
			releaseBenchmark := func() { releaseOnce.Do(func() { close(benchmarkRelease) }) }
			var registerOnce sync.Once
			var once sync.Once
			handler := API{Queue: q, Token: "test"}.Handler()
			server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r)
				snap, err := q.Snapshot()
				if err == nil && len(snap.Machines) > 0 {
					registerOnce.Do(func() { close(registered) })
					if !snap.Machines[0].BenchmarkPending {
						once.Do(func() { close(admitted) })
					}
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			defer func() {
				cancel()
				releaseBenchmark()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			var calls atomic.Int32
			w := Worker{EnvironmentLimit: 1 << 30, InventoryProbe: benchmarkTestInventory, CPUIsolationProbe: func(context.Context) error { return nil }, OnboardingBenchmark: mode == "enabled", StateDir: root, Client: Client{URL: server.URL, Token: "test"}, Machine: machine("dedicated", "xpu"), Interval: time.Millisecond,
				OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} },
				Benchmark: func(ctx context.Context, _ BenchmarkConfig, p *CapabilityProfile) error {
					calls.Add(1)
					snap, _ := q.Snapshot()
					if len(snap.Machines) != 1 || !snap.Machines[0].BenchmarkPending {
						t.Error("benchmark ran without queue registration")
					}
					close(benchmarkEntered)
					select {
					case <-benchmarkRelease:
					case <-ctx.Done():
						return ctx.Err()
					}
					p.Onboarding = "measured; qualified reference replay"
					p.Benchmark = &BenchmarkResult{State: "partial", Measurements: []Throughput{{Resource: "cpu", Rate: 100, Unit: "bytes/s"}, {Resource: "accelerator", Reason: "no qualified reference work"}}}
					return saveWorkerJSON(root, "capability-profile.json", p)
				},
			}
			go func() { done <- w.Run(ctx) }()
			wait := func(signal <-chan struct{}, phase string) {
				t.Helper()
				select {
				case <-signal:
				case err := <-done:
					done <- err // Cleanup still owns the worker join.
					t.Fatalf("worker stopped before %s: %v", phase, err)
				case <-time.After(testWaitTimeout):
					t.Fatalf("worker never reached %s", phase)
				}
			}
			wait(registered, "queue registration")
			if mode == "enabled" {
				wait(benchmarkEntered, "benchmark entry")
				select {
				case <-admitted:
					t.Fatal("join admitted while benchmark was held")
				default:
				}
				releaseBenchmark()
			}
			wait(admitted, "admission after benchmark release")
			want := int32(0)
			if mode == "enabled" {
				want = 1
			}
			if calls.Load() != want {
				t.Fatalf("benchmark calls=%d, want %d", calls.Load(), want)
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
			data, err := os.ReadFile(cache)
			switch mode {
			case "default-empty":
				if !os.IsNotExist(err) {
					t.Fatalf("default join wrote cache: %v", err)
				}
			case "default-cached":
				if err != nil || string(data) != string(original) {
					t.Fatalf("default join changed cache: %v", err)
				}
			case "enabled":
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestPlacementWithoutBenchmark(t *testing.T) {
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
}
