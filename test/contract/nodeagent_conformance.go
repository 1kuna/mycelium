package contract

import (
	"context"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract/assert"
)

func RunNodeAgentConformance(t *testing.T, name string, newAgent func() ports.NodeAgent, p domain.Preset) {
	t.Run(name+"/snapshot_load_unload", func(t *testing.T) {
		agent := newAgent()
		before, err := agent.Snapshot(context.Background())
		assert.NoError(t, "Snapshot before load", err)
		inst, err := agent.Load(context.Background(), p)
		assert.NoError(t, "Load", err)
		assert.True(t, inst.ID != "" && inst.PresetID == p.ID, "loaded invalid instance: %+v", inst)
		assert.NoError(t, "BeginRequest", agent.BeginRequest(context.Background(), inst.ID))
		assert.NoError(t, "EndRequest", agent.EndRequest(context.Background(), inst.ID))
		after, err := agent.Snapshot(context.Background())
		assert.NoError(t, "Snapshot after load", err)
		assert.True(t, len(after.Instances) == len(before.Instances)+1, "load did not add one instance: before=%d after=%d", len(before.Instances), len(after.Instances))
		assert.NoError(t, "Unload", agent.Unload(context.Background(), inst.ID))
		final, err := agent.Snapshot(context.Background())
		assert.NoError(t, "Snapshot after unload", err)
		assert.True(t, len(final.Instances) == len(before.Instances), "unload did not restore instance count: before=%d final=%d", len(before.Instances), len(final.Instances))
	})

	t.Run(name+"/inspect_model_returns_metadata", func(t *testing.T) {
		agent := newAgent()
		metadata, err := agent.InspectModel(context.Background(), p)
		assert.NoError(t, "InspectModel", err)
		assert.True(t, metadata.ModelRef != "" && metadata.WeightsMB > 0, "invalid metadata: %+v", metadata)
	})
}
