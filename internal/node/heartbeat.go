package node

import (
	"context"
	"fmt"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const (
	defaultHeartbeatInterval  = 5 * time.Second
	defaultHeartbeatMaxMisses = 3
)

type HeartbeatPolicy struct {
	Interval  time.Duration
	MaxMisses int
}

type HeartbeatTracker struct {
	clock  ports.Clock
	policy HeartbeatPolicy
	nodes  map[string]domain.Node
}

func NewHeartbeatTracker(clock ports.Clock, policy HeartbeatPolicy) *HeartbeatTracker {
	return &HeartbeatTracker{
		clock:  clock,
		policy: normalizeHeartbeatPolicy(policy),
		nodes:  map[string]domain.Node{},
	}
}

func (a *Agent) Heartbeat(ctx context.Context) (domain.Node, error) {
	if err := ctx.Err(); err != nil {
		return domain.Node{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.node.HeartbeatAt = a.clock.Now()
	return a.node, nil
}

func (t *HeartbeatTracker) Observe(node domain.Node) {
	t.nodes[node.ID] = node
}

func (t *HeartbeatTracker) Node(id string) (domain.Node, error) {
	node, ok := t.nodes[id]
	if !ok {
		return domain.Node{}, fmt.Errorf("unknown node %q", id)
	}
	return t.withReachability(node), nil
}

func (t *HeartbeatTracker) Fleet() []domain.Node {
	out := make([]domain.Node, 0, len(t.nodes))
	for _, node := range t.nodes {
		out = append(out, t.withReachability(node))
	}
	return out
}

func (t *HeartbeatTracker) withReachability(node domain.Node) domain.Node {
	if node.Status == domain.NodeMaintenance || node.Status == domain.NodeDraining {
		return node
	}
	if t.clock.Now().Sub(node.HeartbeatAt) > t.policy.Interval*time.Duration(t.policy.MaxMisses) {
		node.Status = domain.NodeUnreachable
	}
	return node
}

func normalizeHeartbeatPolicy(policy HeartbeatPolicy) HeartbeatPolicy {
	if policy.Interval <= 0 {
		policy.Interval = defaultHeartbeatInterval
	}
	if policy.MaxMisses <= 0 {
		policy.MaxMisses = defaultHeartbeatMaxMisses
	}
	return policy
}
