package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func RunNodeAgentConformance(t *testing.T, name string, newAgent func() ports.NodeAgent, p domain.Preset) {
	t.Run(name+"/snapshot_load_unload", func(t *testing.T) {
		agent := newAgent()
		before, err := agent.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot before load: %v", err)
		}
		inst, err := agent.Load(context.Background(), p)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if inst.ID == "" || inst.PresetID != p.ID {
			t.Fatalf("loaded invalid instance: %+v", inst)
		}
		after, err := agent.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot after load: %v", err)
		}
		if len(after.Instances) != len(before.Instances)+1 {
			t.Fatalf("load did not add one instance: before=%d after=%d", len(before.Instances), len(after.Instances))
		}
		if err := agent.Unload(context.Background(), inst.ID); err != nil {
			t.Fatalf("Unload: %v", err)
		}
		final, err := agent.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot after unload: %v", err)
		}
		if len(final.Instances) != len(before.Instances) {
			t.Fatalf("unload did not restore instance count: before=%d final=%d", len(before.Instances), len(final.Instances))
		}
	})

	t.Run(name+"/inspect_model_returns_metadata", func(t *testing.T) {
		agent := newAgent()
		metadata, err := agent.InspectModel(context.Background(), p)
		if err != nil {
			t.Fatalf("InspectModel: %v", err)
		}
		if metadata.ModelRef == "" || metadata.WeightsMB <= 0 {
			t.Fatalf("invalid metadata: %+v", metadata)
		}
	})
}
