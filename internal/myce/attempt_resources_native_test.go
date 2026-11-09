package myce

import (
	"context"
	"mycelium/internal/probeguard"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"
)

// Read-only OS qualification is deliberately separate from synthetic gates.
// No cgroups, workers, services or configuration are created or changed.
func TestAttemptResourcesNativeRead(t *testing.T) {
	if os.Getenv("MYCE_NATIVE_RESOURCE_TEST") != "1" {
		t.Skip("requires explicit native read-only telemetry probe")
	}
	before := probeguard.Before
	probeguard.Before = nil
	defer func() { probeguard.Before = before }()
	clock := RealClock{}
	host := localIOPressure(clock)
	if runtime.GOOS == "darwin" && host.Unavailable != "unsupported_os" {
		t.Fatal(host)
	}
	if runtime.GOOS == "linux" && host.Some == nil {
		t.Fatal(host)
	}
	pid := os.Getpid()
	birth, err := processBirth(pid)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]float64{}
	var costs []time.Duration
	for i := 0; i < 20; i++ {
		at := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		cpu, err := localResourceCPU(ctx, map[int]string{pid: birth}, seen)
		cancel()
		costs = append(costs, time.Since(at))
		if err != nil || cpu < 0 {
			t.Fatalf("CPU read: %v %v", cpu, err)
		}
	}
	sort.Slice(costs, func(i, j int) bool { return costs[i] < costs[j] })
	t.Logf("OS=%s host IO unavailable=%s; 20 process-tree CPU probes median=%s p95=%s max=%s", runtime.GOOS, host.Unavailable, costs[10], costs[18], costs[19])
}
