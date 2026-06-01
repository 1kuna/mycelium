package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	nodeagent "mycelium/internal/node"
	"mycelium/internal/ports"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPeerDirectoryBuildsFleetFromComputePeers(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	inst := fixtures.MakeInstance(fixtures.OnNode(node.ID))
	agent := admittingAgent{NodeAgent: mocks.NewNodeAgent(node), AdmissionController: &mocks.AdmissionController{}}
	agent.Instances = []domain.ModelInstance{inst}
	seenAddress := ""
	store := &recordingPeerNodeStore{}
	tunnel := &mocks.Tunnel{Addr: "127.0.0.1:6000"}
	directory := &PeerDirectory{
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{
			{ID: "self", Addresses: []string{"127.0.0.1:9"}, Compute: true},
			{ID: "thin", Addresses: []string{"127.0.0.1:1"}, Compute: false},
			{ID: "compute", Addresses: []string{"127.0.0.1:2"}, Compute: true},
		}},
		Store:  store,
		SelfID: "self",
		Tunnel: tunnel,
		Factory: func(address string) ports.NodeAgent {
			seenAddress = address
			return agent
		},
	}

	fleet, err := directory.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if seenAddress != "127.0.0.1:6000" || len(fleet.Nodes) != 1 || fleet.Nodes[0].ID != node.ID || len(fleet.Instances) != 1 {
		t.Fatalf("address=%s fleet=%+v", seenAddress, fleet)
	}
	if len(tunnel.Nodes) != 1 || tunnel.Nodes[0].ID != "compute" || tunnel.Nodes[0].Address != "127.0.0.1:2" {
		t.Fatalf("tunnel nodes = %+v", tunnel.Nodes)
	}
	if len(store.nodes) != 1 || store.nodes[0].ID != node.ID {
		t.Fatalf("stored nodes = %+v", store.nodes)
	}
	if got, err := directory.NodeAgent(node.ID); err != nil || got == nil {
		t.Fatalf("NodeAgent: %v", err)
	}
	if _, err := directory.AdmissionController(node.ID); err != nil {
		t.Fatalf("AdmissionController: %v", err)
	}
	if _, err := directory.LeaseInspector(node.ID); err != nil {
		t.Fatalf("LeaseInspector: %v", err)
	}
}

func TestPeerDirectoryErrors(t *testing.T) {
	boom := errors.New("boom")
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	checks := []struct {
		name      string
		directory *PeerDirectory
		want      string
		wantErr   error
	}{
		{name: "missing discovery", directory: &PeerDirectory{}, want: "not configured"},
		{name: "discovery", directory: &PeerDirectory{Discovery: &mocks.PeerDiscovery{Err: boom}}, wantErr: boom},
		{name: "missing id", directory: &PeerDirectory{Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{Addresses: []string{"127.0.0.1:1"}, Compute: true}}}}, want: "missing id"},
		{name: "missing address", directory: &PeerDirectory{Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Compute: true}}}}, want: "reachable address"},
		{name: "tunnel", directory: &PeerDirectory{Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}}}, Tunnel: &mocks.Tunnel{Err: boom}}, wantErr: boom},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			_, err := check.directory.Snapshot(context.Background())
			switch {
			case check.wantErr != nil && !errors.Is(err, check.wantErr):
				t.Fatalf("err = %v", err)
			case check.want != "" && (err == nil || !strings.Contains(err.Error(), check.want)):
				t.Fatalf("err = %v", err)
			}
		})
	}

	directory := &PeerDirectory{}
	if _, err := directory.NodeAgent("missing"); err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("missing NodeAgent err = %v", err)
	}
	plain := mocks.NewNodeAgent(node)
	directory.agents = map[string]ports.NodeAgent{node.ID: plain}
	if _, err := directory.AdmissionController(node.ID); err == nil || !strings.Contains(err.Error(), "admission") {
		t.Fatalf("plain admission err = %v", err)
	}
	if _, err := directory.LeaseInspector(node.ID); err == nil || !strings.Contains(err.Error(), "lease inspection") {
		t.Fatalf("plain lease err = %v", err)
	}
	if got := peerAgentBaseURL("127.0.0.1:1"); got != "http://127.0.0.1:1" {
		t.Fatalf("base = %s", got)
	}
	if got := peerAgentBaseURL("https://example.test"); got != "https://example.test" {
		t.Fatalf("base https = %s", got)
	}
}

func TestPeerDirectoryMarksUnreachablePeers(t *testing.T) {
	boom := errors.New("boom")
	store := &recordingPeerNodeStore{}
	directory := &PeerDirectory{
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}}},
		Store:     store,
		Factory: func(string) ports.NodeAgent {
			return failingSnapshotAgent{NodeAgent: mocks.NewNodeAgent(fixtures.MakeNode()), err: boom}
		},
	}

	fleet, err := directory.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(fleet.Nodes) != 1 || fleet.Nodes[0].ID != "peer-a" || fleet.Nodes[0].Status != domain.NodeUnreachable {
		t.Fatalf("fleet = %+v", fleet)
	}
	if len(store.nodes) != 1 || store.nodes[0].Status != domain.NodeUnreachable {
		t.Fatalf("stored nodes = %+v", store.nodes)
	}
	if _, err := directory.NodeAgent("peer-a"); err == nil {
		t.Fatal("unreachable peer registered an agent")
	}
}

func TestPeerDirectoryDefaultFactorySendsAuthToken(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithNodeID("node-a"))
	server := directUpstream(nodeagent.HTTPServer{Agent: mocks.NewNodeAgent(node), AuthToken: "rpc-secret"})
	directory := &PeerDirectory{
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Addresses: []string{server}, Compute: true}}},
		AuthToken: "rpc-secret",
		Client:    testUpstreams.client(),
	}

	fleet, err := directory.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(fleet.Nodes) != 1 || fleet.Nodes[0].ID != node.ID {
		t.Fatalf("fleet = %+v", fleet)
	}
}

func TestPeerDirectoryStoreErrorsFailLoudly(t *testing.T) {
	boom := errors.New("boom")
	directory := &PeerDirectory{
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}}},
		Store:     &recordingPeerNodeStore{err: boom},
		Factory: func(string) ports.NodeAgent {
			return mocks.NewNodeAgent(fixtures.MakeNode(fixtures.WithNodeID("node-a")))
		},
	}
	if _, err := directory.Snapshot(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("store err = %v", err)
	}
}

func TestPeerDirectorySnapshotsPeersInParallelAndSorts(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan string, 2)
	agents := map[string]ports.NodeAgent{
		"peer-b": blockingSnapshotAgent{node: fixtures.MakeNode(fixtures.WithNodeID("node-b")), entered: entered, release: release},
		"peer-a": blockingSnapshotAgent{node: fixtures.MakeNode(fixtures.WithNodeID("node-a")), entered: entered, release: release},
	}
	directory := &PeerDirectory{
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{
			{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}, Compute: true},
			{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true},
		}},
	}
	directory.Factory = func(address string) ports.NodeAgent {
		switch address {
		case "127.0.0.1:1":
			return agents["peer-a"]
		case "127.0.0.1:2":
			return agents["peer-b"]
		default:
			t.Fatalf("unexpected address %s", address)
			return nil
		}
	}

	done := make(chan struct{})
	var fleet domain.FleetSnapshot
	var err error
	go func() {
		fleet, err = directory.Snapshot(context.Background())
		close(done)
	}()
	wantEntered := map[string]bool{}
	for len(wantEntered) < 2 {
		select {
		case id := <-entered:
			wantEntered[id] = true
		case <-time.After(time.Second):
			t.Fatal("snapshot did not fan out to both peers")
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("snapshot did not finish after peers were released")
	}
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(fleet.Nodes) != 2 || fleet.Nodes[0].ID != "node-a" || fleet.Nodes[1].ID != "node-b" {
		t.Fatalf("nodes not sorted: %+v", fleet.Nodes)
	}
	if got, err := directory.NodeAgent("node-a"); err != nil || got == nil {
		t.Fatalf("NodeAgent node-a: %v", err)
	}
}

type failingSnapshotAgent struct {
	ports.NodeAgent
	err error
}

func (a failingSnapshotAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{}, a.err
}

type blockingSnapshotAgent struct {
	node    domain.Node
	entered chan<- string
	release <-chan struct{}
}

func (a blockingSnapshotAgent) Snapshot(ctx context.Context) (domain.NodeSnapshot, error) {
	a.entered <- a.node.ID
	select {
	case <-ctx.Done():
		return domain.NodeSnapshot{}, ctx.Err()
	case <-a.release:
		return domain.NodeSnapshot{Node: a.node}, nil
	}
}

func (a blockingSnapshotAgent) Load(context.Context, domain.LoadRequest) (domain.ModelInstance, error) {
	return domain.ModelInstance{}, errors.New("not implemented")
}

func (a blockingSnapshotAgent) Unload(context.Context, string) error {
	return errors.New("not implemented")
}

func (a blockingSnapshotAgent) InspectModel(context.Context, domain.Preset) (domain.ModelMetadata, error) {
	return domain.ModelMetadata{}, errors.New("not implemented")
}

func (a blockingSnapshotAgent) BeginRequest(context.Context, string) error {
	return nil
}

func (a blockingSnapshotAgent) EndRequest(context.Context, string) error {
	return nil
}

type recordingPeerNodeStore struct {
	nodes []domain.Node
	err   error
}

func (s *recordingPeerNodeStore) SaveNode(_ context.Context, node domain.Node) error {
	if s.err != nil {
		return s.err
	}
	s.nodes = append(s.nodes, node)
	return nil
}
