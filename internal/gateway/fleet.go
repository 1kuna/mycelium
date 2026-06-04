package gateway

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type NodeDirectory struct {
	Agents map[string]ports.NodeAgent
}

func (d NodeDirectory) Snapshot(ctx context.Context) (domain.FleetSnapshot, error) {
	var fleet domain.FleetSnapshot
	for _, agent := range d.Agents {
		snap, err := agent.Snapshot(ctx)
		if err != nil {
			return domain.FleetSnapshot{}, err
		}
		fleet.Nodes = append(fleet.Nodes, snap.Node)
		fleet.Instances = append(fleet.Instances, snap.Instances...)
	}
	return fleet, nil
}

func (d NodeDirectory) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	agent, ok := d.Agents[nodeID]
	if !ok {
		return nil, fmt.Errorf("node agent %q is not registered", nodeID)
	}
	return agent, nil
}

func (d NodeDirectory) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	agent, err := d.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	admission, ok := agent.(ports.AdmissionController)
	if !ok {
		return nil, fmt.Errorf("node agent %q does not expose admission", nodeID)
	}
	return admission, nil
}

func (d NodeDirectory) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	agent, err := d.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	inspector, ok := agent.(ports.LeaseInspector)
	if !ok {
		return nil, fmt.Errorf("node agent %q does not expose lease inspection", nodeID)
	}
	return inspector, nil
}

func (d NodeDirectory) JobStatusInspector(nodeID string) (ports.JobStatusInspector, error) {
	agent, err := d.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	inspector, ok := agent.(ports.JobStatusInspector)
	if !ok {
		return nil, fmt.Errorf("node agent %q does not expose job status inspection", nodeID)
	}
	return inspector, nil
}

var _ FleetSource = NodeDirectory{}
var _ NodeResolver = NodeDirectory{}
