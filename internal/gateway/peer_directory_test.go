package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"

	"mycelium/internal/domain"
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
	directory := &PeerDirectory{
		Discovery: &mocks.PeerDiscovery{PeersVal: []domain.Peer{
			{ID: "self", Addresses: []string{"127.0.0.1:9"}, Compute: true},
			{ID: "thin", Addresses: []string{"127.0.0.1:1"}, Compute: false},
			{ID: "compute", Addresses: []string{"127.0.0.1:2"}, Compute: true},
		}},
		Store:  store,
		SelfID: "self",
		Factory: func(address string) ports.NodeAgent {
			seenAddress = address
			return agent
		},
	}

	fleet, err := directory.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if seenAddress != "127.0.0.1:2" || len(fleet.Nodes) != 1 || fleet.Nodes[0].ID != node.ID || len(fleet.Instances) != 1 {
		t.Fatalf("address=%s fleet=%+v", seenAddress, fleet)
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

type failingSnapshotAgent struct {
	ports.NodeAgent
	err error
}

func (a failingSnapshotAgent) Snapshot(context.Context) (domain.NodeSnapshot, error) {
	return domain.NodeSnapshot{}, a.err
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
