package gateway

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
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

	mu     sync.Mutex
	agents map[string]ports.NodeAgent
}

func (d *PeerDirectory) Snapshot(ctx context.Context) (domain.FleetSnapshot, error) {
	if d.Discovery == nil {
		return domain.FleetSnapshot{}, fmt.Errorf("peer discovery is not configured")
	}
	peers, err := d.Discovery.Peers(ctx)
	if err != nil {
		return domain.FleetSnapshot{}, err
	}
	agents := map[string]ports.NodeAgent{}
	var fleet domain.FleetSnapshot
	for _, peer := range peers {
		if d.SelfID != "" && peer.ID == d.SelfID {
			continue
		}
		if !peer.Compute {
			continue
		}
		agent, err := d.agentFor(ctx, peer)
		if err != nil {
			return domain.FleetSnapshot{}, err
		}
		snap, err := agent.Snapshot(ctx)
		if err != nil {
			log.Printf("mycelium peer snapshot failed: peer=%s address=%s error=%v", peer.ID, peer.Addresses[0], err)
			node := unreachablePeerNode(peer)
			if err := d.saveNode(ctx, node); err != nil {
				return domain.FleetSnapshot{}, err
			}
			fleet.Nodes = append(fleet.Nodes, node)
			continue
		}
		if err := d.saveNode(ctx, snap.Node); err != nil {
			return domain.FleetSnapshot{}, err
		}
		fleet.Nodes = append(fleet.Nodes, snap.Node)
		fleet.Instances = append(fleet.Instances, snap.Instances...)
		agents[snap.Node.ID] = agent
	}
	d.mu.Lock()
	d.agents = agents
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

func (d *PeerDirectory) agentFor(ctx context.Context, peer domain.Peer) (ports.NodeAgent, error) {
	if peer.ID == "" {
		return nil, fmt.Errorf("discovered peer is missing id")
	}
	if len(peer.Addresses) == 0 {
		return nil, fmt.Errorf("discovered peer %q has no reachable address", peer.ID)
	}
	address := peer.Addresses[0]
	if d.Tunnel != nil {
		loopback, err := d.Tunnel.Open(ctx, domain.Node{ID: peer.ID, Address: address})
		if err != nil {
			return nil, err
		}
		address = loopback
	}
	factory := d.Factory
	if factory == nil {
		factory = func(address string) ports.NodeAgent {
			client := nodeagent.NewHTTPClient(peerAgentBaseURL(address))
			client.AuthToken = d.AuthToken
			client.Client = &http.Client{Transport: &http.Transport{
				Proxy: nil,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			}}
			return client
		}
	}
	return factory(address), nil
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
