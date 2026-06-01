package peer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestPreSendNegotiatorDisabledDoesNothing(t *testing.T) {
	decision := domain.PlacementDecision{JobID: "job-a", NodeID: "node-a"}
	got, err := (PreSendNegotiator{}).Negotiate(context.Background(), fixtures.MakeJob(fixtures.WithJobID("job-a")), decision, domain.FleetSnapshot{})
	if err != nil || !reflect.DeepEqual(got, decision) {
		t.Fatalf("disabled negotiate = %+v %v", got, err)
	}
}

func TestPreSendNegotiatorAsksPeersInDeterministicOrder(t *testing.T) {
	ctx := context.Background()
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	decision := domain.PlacementDecision{JobID: job.ID, NodeID: "node-a", Action: domain.ActionLoadedNew}
	client := &recordingPreSendClient{advice: map[string]PreSendAdvice{
		"peer-b": {PeerID: "peer-b", Accepted: true},
		"peer-c": {PeerID: "peer-c", Accepted: true},
	}}
	negotiator := PreSendNegotiator{
		Enabled: true,
		SelfID:  "peer-a",
		Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{
			{ID: "peer-c"},
			{ID: "peer-a"},
			{ID: "peer-b"},
		}},
		Client: client,
		Clock:  mocks.NewFakeClock(time.Unix(100, 0).UTC()),
	}

	got, err := negotiator.Negotiate(ctx, job, decision, domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode(fixtures.WithNodeID("node-a"))}})
	if err != nil {
		t.Fatalf("Negotiate: %v", err)
	}
	if !reflect.DeepEqual(got, decision) {
		t.Fatalf("decision changed = %+v", got)
	}
	if !reflect.DeepEqual(client.calls, []string{"peer-b:job-a", "peer-c:job-a"}) {
		t.Fatalf("calls = %+v", client.calls)
	}
	if client.proposals[0].From != "peer-a" || client.proposals[0].CreatedAt != time.Unix(100, 0).UTC() {
		t.Fatalf("proposal = %+v", client.proposals[0])
	}
}

func TestPreSendNegotiatorRejectsBadInputsAndAdvice(t *testing.T) {
	ctx := context.Background()
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	decision := domain.PlacementDecision{JobID: job.ID, NodeID: "node-a", Action: domain.ActionLoadedNew}
	fleet := domain.FleetSnapshot{Nodes: []domain.Node{fixtures.MakeNode(fixtures.WithNodeID("node-a"))}}
	boom := errors.New("boom")
	base := func() PreSendNegotiator {
		return PreSendNegotiator{
			Enabled: true,
			SelfID:  "peer-a",
			Peers:   &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}},
			Client:  &recordingPreSendClient{advice: map[string]PreSendAdvice{"peer-b": {PeerID: "peer-b", Accepted: true}}},
			Clock:   mocks.NewFakeClock(time.Unix(101, 0).UTC()),
		}
	}
	checks := []struct {
		name       string
		negotiator PreSendNegotiator
		job        domain.Job
		decision   domain.PlacementDecision
		fleet      domain.FleetSnapshot
		want       string
		wantErr    error
	}{
		{name: "unconfigured", negotiator: PreSendNegotiator{Enabled: true}, job: job, decision: decision, fleet: fleet, want: "not fully configured"},
		{name: "empty job", negotiator: base(), job: domain.Job{}, decision: decision, fleet: fleet, want: "job id"},
		{name: "mismatch", negotiator: base(), job: job, decision: domain.PlacementDecision{JobID: "other"}, fleet: fleet, want: "does not match"},
		{name: "missing node", negotiator: base(), job: job, decision: domain.PlacementDecision{JobID: job.ID, Action: domain.ActionLoadedNew}, fleet: fleet, want: "no node"},
		{name: "node absent", negotiator: base(), job: job, decision: decision, fleet: domain.FleetSnapshot{}, want: "not in the fleet"},
		{name: "discovery", negotiator: PreSendNegotiator{Enabled: true, SelfID: "peer-a", Peers: &mocks.PeerDiscovery{Err: boom}, Client: &recordingPreSendClient{}, Clock: mocks.NewFakeClock(time.Now())}, job: job, decision: decision, fleet: fleet, wantErr: boom},
		{name: "client", negotiator: PreSendNegotiator{Enabled: true, SelfID: "peer-a", Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}}, Client: &recordingPreSendClient{err: boom}, Clock: mocks.NewFakeClock(time.Now())}, job: job, decision: decision, fleet: fleet, wantErr: boom},
		{name: "wrong peer", negotiator: PreSendNegotiator{Enabled: true, SelfID: "peer-a", Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}}, Client: &recordingPreSendClient{advice: map[string]PreSendAdvice{"peer-b": {PeerID: "peer-z", Accepted: true}}}, Clock: mocks.NewFakeClock(time.Now())}, job: job, decision: decision, fleet: fleet, want: "came from"},
		{name: "rejected", negotiator: PreSendNegotiator{Enabled: true, SelfID: "peer-a", Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}}, Client: &recordingPreSendClient{advice: map[string]PreSendAdvice{"peer-b": {PeerID: "peer-b", Reason: "busy"}}}, Clock: mocks.NewFakeClock(time.Now())}, job: job, decision: decision, fleet: fleet, want: "busy"},
		{name: "rejected no reason", negotiator: PreSendNegotiator{Enabled: true, SelfID: "peer-a", Peers: &mocks.PeerDiscovery{PeersVal: []domain.Peer{{ID: "peer-b"}}}, Client: &recordingPreSendClient{advice: map[string]PreSendAdvice{"peer-b": {PeerID: "peer-b"}}}, Clock: mocks.NewFakeClock(time.Now())}, job: job, decision: decision, fleet: fleet, want: "rejected by peer"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			got, err := check.negotiator.Negotiate(ctx, check.job, check.decision, check.fleet)
			if check.wantErr != nil && !errors.Is(err, check.wantErr) {
				t.Fatalf("err = %v", err)
			}
			if check.want != "" && (err == nil || !strings.Contains(err.Error(), check.want)) {
				t.Fatalf("err = %v", err)
			}
			if check.wantErr == nil && check.want == "" && (err != nil || !reflect.DeepEqual(got, check.decision)) {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}

func TestPreSendNegotiatorSkipsQueuedDecision(t *testing.T) {
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	decision := domain.PlacementDecision{JobID: job.ID, Action: domain.ActionQueued}
	got, err := (PreSendNegotiator{
		Enabled: true,
		SelfID:  "peer-a",
		Peers:   &mocks.PeerDiscovery{Err: errors.New("should not be called")},
		Client:  &recordingPreSendClient{},
		Clock:   mocks.NewFakeClock(time.Now()),
	}).Negotiate(context.Background(), job, decision, domain.FleetSnapshot{})
	if err != nil || !reflect.DeepEqual(got, decision) {
		t.Fatalf("queued negotiate = %+v %v", got, err)
	}
}

func TestRegistryPreSendAdvisorRejectsDuplicateCoordinator(t *testing.T) {
	ctx := context.Background()
	registry := NewJobRegistry()
	duplicate := registryRecord("job-a", "peer-b", domain.JobRunning, time.Unix(110, 0).UTC())
	if err := registry.Put(ctx, duplicate); err != nil {
		t.Fatalf("Put duplicate: %v", err)
	}
	advisor := RegistryPreSendAdvisor{SelfID: "peer-c", Registry: registry}
	proposal := PreSendProposal{
		From:     "peer-a",
		Job:      fixtures.MakeJob(fixtures.WithJobID("job-a")),
		Decision: domain.PlacementDecision{JobID: "job-a", NodeID: "node-a", Action: domain.ActionLoadedNew},
	}

	advice, err := advisor.AdvisePreSend(ctx, proposal)
	if err != nil {
		t.Fatalf("AdvisePreSend: %v", err)
	}
	if advice.Accepted || advice.PeerID != "peer-c" || !strings.Contains(advice.Reason, "peer-b") {
		t.Fatalf("advice = %+v", advice)
	}
	duplicate.Status = domain.JobDone
	duplicate.UpdatedAt = duplicate.UpdatedAt.Add(time.Second)
	if err := registry.Put(ctx, duplicate); err != nil {
		t.Fatalf("Put done: %v", err)
	}
	advice, err = advisor.AdvisePreSend(ctx, proposal)
	if err != nil || !advice.Accepted {
		t.Fatalf("done advice = %+v %v", advice, err)
	}
}

func TestRegistryPreSendAdvisorErrors(t *testing.T) {
	ctx := context.Background()
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	proposal := PreSendProposal{From: "peer-a", Job: job, Decision: domain.PlacementDecision{JobID: job.ID}}
	boom := errors.New("boom")
	checks := []struct {
		name    string
		advisor RegistryPreSendAdvisor
		propose PreSendProposal
		want    string
		wantErr error
	}{
		{name: "unconfigured", advisor: RegistryPreSendAdvisor{}, propose: proposal, want: "not fully configured"},
		{name: "from", advisor: RegistryPreSendAdvisor{SelfID: "peer-b", Registry: NewJobRegistry()}, propose: PreSendProposal{Job: job, Decision: domain.PlacementDecision{JobID: job.ID}}, want: "coordinator"},
		{name: "job", advisor: RegistryPreSendAdvisor{SelfID: "peer-b", Registry: NewJobRegistry()}, propose: PreSendProposal{From: "peer-a", Decision: domain.PlacementDecision{JobID: job.ID}}, want: "job id"},
		{name: "mismatch", advisor: RegistryPreSendAdvisor{SelfID: "peer-b", Registry: NewJobRegistry()}, propose: PreSendProposal{From: "peer-a", Job: job, Decision: domain.PlacementDecision{JobID: "other"}}, want: "does not match"},
		{name: "snapshot", advisor: RegistryPreSendAdvisor{SelfID: "peer-b", Registry: &failingRegistry{err: boom}}, propose: proposal, wantErr: boom},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if _, err := check.advisor.AdvisePreSend(ctx, check.propose); check.wantErr != nil && !errors.Is(err, check.wantErr) {
				t.Fatalf("err = %v", err)
			} else if check.want != "" && (err == nil || !strings.Contains(err.Error(), check.want)) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

type recordingPreSendClient struct {
	advice    map[string]PreSendAdvice
	err       error
	calls     []string
	proposals []PreSendProposal
}

func (c *recordingPreSendClient) AdvisePreSend(_ context.Context, peer domain.Peer, proposal PreSendProposal) (PreSendAdvice, error) {
	c.calls = append(c.calls, peer.ID+":"+proposal.Job.ID)
	c.proposals = append(c.proposals, proposal)
	if c.err != nil {
		return PreSendAdvice{}, c.err
	}
	return c.advice[peer.ID], nil
}
