package mocks

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type NodeAgent struct {
	NodeVal    domain.Node
	Instances  []domain.ModelInstance
	Metadata   domain.ModelMetadata
	LoadErr    error
	UnloadErr  error
	InspectErr error
	Calls      []string
	nextID     int
}

func NewNodeAgent(node domain.Node) *NodeAgent {
	return &NodeAgent{NodeVal: node}
}

func (m *NodeAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	m.Calls = append(m.Calls, "snapshot")
	return domain.NodeSnapshot{Node: m.NodeVal, Instances: append([]domain.ModelInstance(nil), m.Instances...)}, nil
}

func (m *NodeAgent) Load(_ context.Context, p domain.Preset) (domain.ModelInstance, error) {
	m.Calls = append(m.Calls, "load:"+p.ID)
	if m.LoadErr != nil {
		return domain.ModelInstance{}, m.LoadErr
	}
	m.nextID++
	inst := domain.ModelInstance{
		ID:             fmt.Sprintf("inst_%d", m.nextID),
		PresetID:       p.ID,
		NodeID:         m.NodeVal.ID,
		AcceleratorSet: []int{0},
		State:          domain.InstReady,
		Claim:          domain.Claim{WeightsMB: p.EstWeightsMB},
		Addr:           fmt.Sprintf("127.0.0.1:%d", 60000+m.nextID),
	}
	m.Instances = append(m.Instances, inst)
	return inst, nil
}

func (m *NodeAgent) Unload(_ context.Context, id string) error {
	m.Calls = append(m.Calls, "unload:"+id)
	if m.UnloadErr != nil {
		return m.UnloadErr
	}
	out := m.Instances[:0]
	for _, inst := range m.Instances {
		if inst.ID != id {
			out = append(out, inst)
		}
	}
	m.Instances = out
	return nil
}

func (m *NodeAgent) InspectModel(_ context.Context, p domain.Preset) (domain.ModelMetadata, error) {
	m.Calls = append(m.Calls, "inspect:"+p.ID)
	if m.InspectErr != nil {
		return domain.ModelMetadata{}, m.InspectErr
	}
	if m.Metadata != (domain.ModelMetadata{}) {
		return m.Metadata, nil
	}
	return domain.ModelMetadata{
		ModelRef:      p.ModelRef,
		Format:        "gguf",
		WeightsMB:     p.EstWeightsMB,
		KVPerTokenMB:  p.KVPerTokenMB,
		ContextLength: p.ContextLength,
	}, nil
}

var _ ports.NodeAgent = (*NodeAgent)(nil)
