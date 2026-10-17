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
			root := t.TempDir()
			cache := filepath.Join(root, "capability-profile.json")
			original := []byte(`{"benchmark":{"state":"partial"}}`)
			if mode == "default-cached" {
				if err := os.WriteFile(cache, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			admitted := make(chan struct{})
			var once sync.Once
			handler := API{Queue: q, Token: "test"}.Handler()
			server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r)
				snap, err := q.Snapshot()
				if err == nil && len(snap.Machines) > 0 {
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
			var calls atomic.Int32
			w := Worker{OnboardingBenchmark: mode == "enabled", StateDir: root, Client: Client{URL: server.URL, Token: "test"}, Machine: machine("dedicated", "xpu"), Interval: time.Millisecond,
				OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} },
				Benchmark: func(_ context.Context, _ BenchmarkConfig, p *CapabilityProfile) error {
					calls.Add(1)
					snap, _ := q.Snapshot()
					if len(snap.Machines) != 0 {
						t.Error("benchmark ran after admission")
					}
					p.Benchmark = &BenchmarkResult{State: "partial"}
					return saveWorkerJSON(root, "capability-profile.json", p)
				},
			}
			go func() { done <- w.Run(ctx) }()
			select {
			case <-admitted:
			case <-time.After(2 * time.Second):
				t.Fatal("join did not admit within two seconds")
			}
			want := int32(0)
			if mode == "enabled" {
				want = 1
			}
			if calls.Load() != want {
				t.Fatalf("benchmark calls=%d, want %d", calls.Load(), want)
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
			m.CapabilityProfile = &CapabilityProfile{LogicalCPUs: 8}
		}
		check(t, q, m, nil)
		j := submit(t, q, spec("experiment"))
		clock.Advance(time.Second)
		if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
			t.Fatal("job not placed without benchmark")
		}
	}
}
