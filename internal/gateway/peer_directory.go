package gateway

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"mycelium/internal/domain"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/ports"
)

type PeerAgentFactory func(address string) ports.NodeAgent

type PeerNodeStore interface {
	SaveNode(ctx context.Context, node domain.Node) error
}

type PeerDirectory struct {
	Discovery ports.PeerDiscovery
	Factory   PeerAgentFactory
	Tunnel    ports.Tunnel
	Store     PeerNodeStore
	SelfID    string
	AuthToken string
	Client    *http.Client

	mu          sync.Mutex
	agents      map[string]ports.NodeAgent
	peerAgents  map[string]cachedPeerAgent
	peersByNode map[string]domain.Peer
}

type cachedPeerAgent struct {
	sourceAddress string
	resolvedPeer  domain.Peer
	agent         ports.NodeAgent
}

func (d *PeerDirectory) Snapshot(ctx context.Context) (domain.FleetSnapshot, error) {
	if d.Discovery == nil {
		return domain.FleetSnapshot{}, fmt.Errorf("peer discovery is not configured")
	}
	peers, err := d.Discovery.Peers(ctx)
	if err != nil {
		return domain.FleetSnapshot{}, err
	}
	type peerAgent struct {
		peer         domain.Peer
		resolvedPeer domain.Peer
		agent        ports.NodeAgent
	}
	candidates := []peerAgent{}
	for _, peer := range peers {
		if d.SelfID != "" && peer.ID == d.SelfID {
			continue
		}
		if !peer.Compute {
			continue
		}
		agent, resolvedPeer, err := d.agentFor(ctx, peer)
		if err != nil {
			return domain.FleetSnapshot{}, err
		}
		candidates = append(candidates, peerAgent{peer: peer, resolvedPeer: resolvedPeer, agent: agent})
	}
	type snapshotResult struct {
		peer         domain.Peer
		resolvedPeer domain.Peer
		agent        ports.NodeAgent
		snap         domain.NodeSnapshot
		err          error
	}
	results := make(chan snapshotResult, len(candidates))
	for _, candidate := range candidates {
		candidate := candidate
		go func() {
			snap, err := candidate.agent.Snapshot(ctx)
			results <- snapshotResult{peer: candidate.peer, resolvedPeer: candidate.resolvedPeer, agent: candidate.agent, snap: snap, err: err}
		}()
	}
	agents := map[string]ports.NodeAgent{}
	peersByNode := map[string]domain.Peer{}
	var fleet domain.FleetSnapshot
	for range candidates {
		result := <-results
		if result.err != nil {
			log.Printf("mycelium peer snapshot failed: peer=%s address=%s error=%v", result.peer.ID, result.peer.Addresses[0], result.err)
			fleet.Nodes = append(fleet.Nodes, unreachablePeerNode(result.peer))
			continue
		}
		fleet.Nodes = append(fleet.Nodes, result.snap.Node)
		fleet.Instances = append(fleet.Instances, result.snap.Instances...)
		agents[result.snap.Node.ID] = result.agent
		peersByNode[result.snap.Node.ID] = result.resolvedPeer
	}
	sort.Slice(fleet.Nodes, func(i, j int) bool { return fleet.Nodes[i].ID < fleet.Nodes[j].ID })
	sort.Slice(fleet.Instances, func(i, j int) bool {
		if fleet.Instances[i].NodeID == fleet.Instances[j].NodeID {
			return fleet.Instances[i].ID < fleet.Instances[j].ID
		}
		return fleet.Instances[i].NodeID < fleet.Instances[j].NodeID
	})
	for _, node := range fleet.Nodes {
		if err := d.saveNode(ctx, node); err != nil {
			return domain.FleetSnapshot{}, err
		}
	}
	d.mu.Lock()
	d.agents = agents
	d.peersByNode = peersByNode
	d.mu.Unlock()
	return fleet, nil
}

func (d *PeerDirectory) NodeAgent(nodeID string) (ports.NodeAgent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	agent, ok := d.agents[nodeID]
	if !ok {
		return nil, fmt.Errorf("peer node agent %q is not registered", nodeID)
	}
	return agent, nil
}

func (d *PeerDirectory) AdmissionController(nodeID string) (ports.AdmissionController, error) {
	agent, err := d.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	admission, ok := agent.(ports.AdmissionController)
	if !ok {
		return nil, fmt.Errorf("peer node agent %q does not expose admission", nodeID)
	}
	return admission, nil
}

func (d *PeerDirectory) LeaseInspector(nodeID string) (ports.LeaseInspector, error) {
	agent, err := d.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	inspector, ok := agent.(ports.LeaseInspector)
	if !ok {
		return nil, fmt.Errorf("peer node agent %q does not expose lease inspection", nodeID)
	}
	return inspector, nil
}

func (d *PeerDirectory) JobStatusInspector(nodeID string) (ports.JobStatusInspector, error) {
	agent, err := d.NodeAgent(nodeID)
	if err != nil {
		return nil, err
	}
	inspector, ok := agent.(ports.JobStatusInspector)
	if !ok {
		return nil, fmt.Errorf("peer node agent %q does not expose job status inspection", nodeID)
	}
	return inspector, nil
}

func (d *PeerDirectory) PeerForNode(nodeID string) (domain.Peer, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	peer, ok := d.peersByNode[nodeID]
	return peer, ok
}

func (d *PeerDirectory) agentFor(ctx context.Context, peer domain.Peer) (ports.NodeAgent, domain.Peer, error) {
	if peer.ID == "" {
		return nil, domain.Peer{}, fmt.Errorf("discovered peer is missing id")
	}
	if len(peer.Addresses) == 0 {
		return nil, domain.Peer{}, fmt.Errorf("discovered peer %q has no reachable address", peer.ID)
	}
	sourceAddress := peer.Addresses[0]
	d.mu.Lock()
	if cached, ok := d.peerAgents[peer.ID]; ok && cached.sourceAddress == sourceAddress {
		d.mu.Unlock()
		return cached.agent, cached.resolvedPeer, nil
	}
	d.mu.Unlock()

	address := sourceAddress
	if d.Tunnel != nil {
		loopback, err := d.Tunnel.Open(ctx, domain.Node{ID: peer.ID, Address: address})
		if err != nil {
			return nil, domain.Peer{}, err
		}
		address = loopback
	}
	resolvedPeer := peer
	resolvedPeer.Addresses = append([]string{address}, peer.Addresses[1:]...)
	factory := d.Factory
	if factory == nil {
		factory = func(address string) ports.NodeAgent {
			client := nodeagent.NewHTTPClient(peerAgentBaseURL(address))
			client.AuthToken = d.AuthToken
			client.Client = d.Client
			if client.Client == nil {
				client.Client = &http.Client{Transport: &http.Transport{
					Proxy: nil,
					DialContext: (&net.Dialer{
						Timeout:   5 * time.Second,
						KeepAlive: 30 * time.Second,
					}).DialContext,
				}}
			}
			return client
		}
	}
	agent := factory(address)
	d.mu.Lock()
	if d.peerAgents == nil {
		d.peerAgents = map[string]cachedPeerAgent{}
	}
	d.peerAgents[peer.ID] = cachedPeerAgent{sourceAddress: sourceAddress, resolvedPeer: resolvedPeer, agent: agent}
	d.mu.Unlock()
	return agent, resolvedPeer, nil
}

func (d *PeerDirectory) saveNode(ctx context.Context, node domain.Node) error {
	if d.Store == nil {
		return nil
	}
	return d.Store.SaveNode(ctx, node)
}

func unreachablePeerNode(peer domain.Peer) domain.Node {
	node := domain.Node{ID: peer.ID, Name: peer.ID, Status: domain.NodeUnreachable}
	if len(peer.Addresses) > 0 {
		node.Address = peer.Addresses[0]
	}
	return node
}

func peerAgentBaseURL(address string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return address
	}
	return "http://" + address
}

var _ FleetSource = (*PeerDirectory)(nil)
var _ NodeResolver = (*PeerDirectory)(nil)
var _ TelemetryPeerResolver = (*PeerDirectory)(nil)
