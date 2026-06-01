//go:build smoke

package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/node"
	"mycelium/test/fixtures"
)

func TestPhase3CatalogMaterializedPresetLoadsInNode(t *testing.T) {
	binary := os.Getenv("MYCELIUM_LLAMA_CPP_BINARY")
	model := os.Getenv("MYCELIUM_LLAMA_CPP_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_LLAMA_CPP_BINARY and MYCELIUM_LLAMA_CPP_MODEL for Phase 3 catalog smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	result, err := catalog.NewInstaller(t.TempDir()).Install(ctx, catalog.InstallRequest{
		Source:        model,
		ID:            "smoke-catalog",
		Model:         "smoke-catalog",
		ContextLength: 2048,
		Quant:         "test",
	})
	if err != nil {
		t.Fatalf("install local model: %v", err)
	}
	if result.Provenance.MaterializedPath != result.Preset.ModelRef {
		t.Fatalf("provenance = %+v preset = %+v", result.Provenance, result.Preset)
	}

	agent := node.NewAgent(
		fixtures.MakeNode(),
		newSmokeAdapter(binary),
		clock.System{},
		node.WithListenAddr(freeAddr(t)),
		node.WithAllocator(lease.NewAllocator()),
	)
	inst, err := agent.Load(ctx, domain.LoadRequest{Preset: result.Preset, Claim: fixtures.MakeClaim(result.Preset.EstWeightsMB, 1), AcceleratorSet: []int{0}})
	if err != nil {
		t.Fatalf("load materialized preset: %v", err)
	}
	if err := agent.Unload(ctx, inst.ID); err != nil {
		t.Fatalf("unload materialized preset: %v", err)
	}
}
