//go:build smoke

package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	"mycelium/internal/bench"
)

func TestFleetBenchmarkConservativeSmoke(t *testing.T) {
	configPath := os.Getenv("MYCELIUM_BENCHMARK_CONFIG")
	if configPath == "" {
		t.Skip("set MYCELIUM_BENCHMARK_CONFIG for real fleet benchmark smoke")
	}
	out := os.Getenv("MYCELIUM_BENCHMARK_OUT")
	if out == "" {
		out = t.TempDir()
	}
	cfg, err := bench.LoadFleetConfig(configPath)
	if err != nil {
		t.Fatalf("LoadFleetConfig: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	result, err := bench.RunFleet(ctx, cfg, bench.FleetRunOptions{
		Profile:    bench.FleetProfileConservative,
		OutputRoot: out,
	})
	if err != nil {
		t.Fatalf("RunFleet: %v output=%s failures=%+v", err, result.OutputDir, result.Failures)
	}
	if !result.Preflight.Passed || len(result.Results) == 0 || len(result.Resources) == 0 || len(result.Metrics) == 0 {
		t.Fatalf("incomplete benchmark proof: output=%s preflight=%+v results=%d resources=%d metrics=%d", result.OutputDir, result.Preflight, len(result.Results), len(result.Resources), len(result.Metrics))
	}
	t.Logf("fleet benchmark artifact: %s", result.OutputDir)
}
