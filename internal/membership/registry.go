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
	store  NodeStore
	agents map[string]ports.NodeAgent
	nodes  map[string]domain.Node
}

type NodeStore interface {
	SaveNode(ctx context.Context, node domain.Node) error
	ListNodes(ctx context.Context) ([]domain.Node, error)
}

const LabelAdvertisedAddress = "mycelium.node.advertised_addr"

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

func NewPersistentRegistry(ctx context.Context, tokens *TokenManager, tunnel ports.Tunnel, store NodeStore) (*Registry, error) {
	registry := NewRegistry(tokens, tunnel)
	registry.store = store
	if store == nil {
		return registry, nil
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if node.ID == "" || node.Address == "" {
			continue
		}
		runtimeNode, agent, err := registry.runtimeNode(ctx, node)
		if err != nil {
			return nil, err
		}
		registry.nodes[runtimeNode.ID] = runtimeNode
		registry.agents[runtimeNode.ID] = agent
	}
	return registry, nil
}

func (r *Registry) Join(ctx context.Context, req JoinRequest) (domain.Node, error) {
	if r.tokens == nil {
		return domain.Node{}, fmt.Errorf("join token manager is not configured")
	}
	if err := r.tokens.Validate(req.Token); err != nil {
		return domain.Node{}, err
	}
	node, agent, err := r.runtimeNode(ctx, req.Node)
	if err != nil {
		return domain.Node{}, err
	}
	r.mu.Lock()
	r.nodes[node.ID] = node
	r.agents[node.ID] = agent
	r.mu.Unlock()
	if r.store != nil {
		if err := r.store.SaveNode(ctx, persistedNode(node)); err != nil {
			return domain.Node{}, err
		}
	}
	return node, nil
}

func (r *Registry) runtimeNode(ctx context.Context, node domain.Node) (domain.Node, ports.NodeAgent, error) {
	advertised := advertisedAddress(node)
	addr, err := r.tunnel.Open(ctx, node)
	if err != nil {
		return domain.Node{}, nil, err
	}
	runtimeNode := node
	runtimeNode.Address = addr
	runtimeNode.Status = domain.NodeReady
	runtimeNode.Labels = withAdvertisedAddress(runtimeNode.Labels, advertised)
	client := nodeagent.NewHTTPClient("http://" + addr)
	if snap, err := client.Snapshot(ctx); err == nil {
		runtimeNode = snap.Node
		runtimeNode.Address = addr
		runtimeNode.Status = domain.NodeReady
		runtimeNode.Labels = withAdvertisedAddress(runtimeNode.Labels, advertised)
	}
	return runtimeNode, client, nil
}

func persistedNode(node domain.Node) domain.Node {
	out := node
	if advertised := advertisedAddress(node); advertised != "" {
		out.Address = advertised
	}
	return out
}

func advertisedAddress(node domain.Node) string {
	if node.Labels != nil && node.Labels[LabelAdvertisedAddress] != "" {
		return node.Labels[LabelAdvertisedAddress]
	}
	return node.Address
}

func withAdvertisedAddress(labels map[string]string, address string) map[string]string {
	out := map[string]string{}
	for key, value := range labels {
		out[key] = value
	}
	if address != "" {
		out[LabelAdvertisedAddress] = address
	}
	return out
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

func (r *Registry) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	agent, err := r.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	admission, ok := agent.(ports.AdmissionController)
	if !ok {
		return nil, fmt.Errorf("node agent %q does not expose admission", nodeID)
	}
	return admission, nil
}

func (r *Registry) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	agent, err := r.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	inspector, ok := agent.(ports.LeaseInspector)
	if !ok {
		return nil, fmt.Errorf("node agent %q does not expose lease inspection", nodeID)
	}
	return inspector, nil
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
