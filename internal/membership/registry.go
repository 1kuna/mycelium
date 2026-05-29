package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"mycelium/internal/domain"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/ports"
)

type Registry struct {
	mu     sync.Mutex
	tokens *TokenManager
	tunnel ports.Tunnel
	agents map[string]ports.NodeAgent
	nodes  map[string]domain.Node
}

func NewRegistry(tokens *TokenManager, tunnel ports.Tunnel) *Registry {
	if tunnel == nil {
		tunnel = NewLANTunnel()
	}
	return &Registry{
		tokens: tokens,
		tunnel: tunnel,
		agents: map[string]ports.NodeAgent{},
		nodes:  map[string]domain.Node{},
	}
}

func (r *Registry) Join(ctx context.Context, req JoinRequest) (domain.Node, error) {
	if r.tokens == nil {
		return domain.Node{}, fmt.Errorf("join token manager is not configured")
	}
	if err := r.tokens.Validate(req.Token); err != nil {
		return domain.Node{}, err
	}
	node := req.Node
	addr, err := r.tunnel.Open(ctx, node)
	if err != nil {
		return domain.Node{}, err
	}
	node.Address = addr
	node.Status = domain.NodeReady
	client := nodeagent.NewHTTPClient("http://" + addr)
	if snap, err := client.Snapshot(ctx); err == nil {
		node = snap.Node
		node.Address = addr
		node.Status = domain.NodeReady
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[node.ID] = node
	r.agents[node.ID] = client
	return node, nil
}

func (r *Registry) Snapshot(ctx context.Context) (domain.FleetSnapshot, error) {
	r.mu.Lock()
	agents := make(map[string]ports.NodeAgent, len(r.agents))
	nodes := make(map[string]domain.Node, len(r.nodes))
	for id, agent := range r.agents {
		agents[id] = agent
	}
	for id, node := range r.nodes {
		nodes[id] = node
	}
	r.mu.Unlock()

	fleet := domain.FleetSnapshot{Nodes: []domain.Node{}, Instances: []domain.ModelInstance{}}
	for id, agent := range agents {
		snap, err := agent.Snapshot(ctx)
		if err != nil {
			node := nodes[id]
			node.Status = domain.NodeUnreachable
			fleet.Nodes = append(fleet.Nodes, node)
			continue
		}
		node := snap.Node
		node.Address = nodes[id].Address
		fleet.Nodes = append(fleet.Nodes, node)
		fleet.Instances = append(fleet.Instances, snap.Instances...)
	}
	return fleet, nil
}

func (r *Registry) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	agent, ok := r.agents[nodeID]
	if !ok {
		return nil, fmt.Errorf("node agent %q is not registered", nodeID)
	}
	return agent, nil
}

func (r *Registry) Announce(context.Context, domain.Node) error {
	return fmt.Errorf("membership announce requires an explicit join token")
}

func (r *Registry) Discover(ctx context.Context) ([]domain.Node, error) {
	fleet, err := r.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return fleet.Nodes, nil
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/join":
		var join JoinRequest
		if err := json.NewDecoder(req.Body).Decode(&join); err != nil {
			writeMembershipError(w, http.StatusBadRequest, err.Error())
			return
		}
		node, err := r.Join(req.Context(), join)
		if err != nil {
			writeMembershipError(w, http.StatusForbidden, err.Error())
			return
		}
		writeMembershipJSON(w, JoinResponse{Node: node})
	case req.Method == http.MethodGet && req.URL.Path == "/nodes":
		fleet, err := r.Snapshot(req.Context())
		if err != nil {
			writeMembershipError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeMembershipJSON(w, fleet.Nodes)
	default:
		writeMembershipError(w, http.StatusNotFound, "not found")
	}
}

func writeMembershipJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		panic(err)
	}
}

func writeMembershipError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

var _ ports.Discovery = (*Registry)(nil)
