package peer

import (
	"context"
	"fmt"
	"sort"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type PreSendProposal struct {
	From      string                   `json:"from"`
	Job       domain.Job               `json:"job"`
	Decision  domain.PlacementDecision `json:"decision"`
	CreatedAt time.Time                `json:"created_at"`
}

type PreSendAdvice struct {
	PeerID   string `json:"peer_id"`
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

type PreSendClient interface {
	AdvisePreSend(ctx context.Context, peer domain.Peer, proposal PreSendProposal) (PreSendAdvice, error)
}

type PreSendNegotiator struct {
	Enabled bool
	SelfID  string
	Peers   RegistryPeerSource
	Client  PreSendClient
	Clock   ports.Clock
}

func (n PreSendNegotiator) Negotiate(ctx context.Context, job domain.Job, decision domain.PlacementDecision, fleet domain.FleetSnapshot) (domain.PlacementDecision, error) {
	if !n.Enabled {
		return decision, nil
	}
	if err := n.validate(); err != nil {
		return domain.PlacementDecision{}, err
	}
	if job.ID == "" {
		return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation job id is required")
	}
	if decision.JobID != job.ID {
		return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation decision job %q does not match job %q", decision.JobID, job.ID)
	}
	if decision.Action == domain.ActionQueued {
		return decision, nil
	}
	if decision.NodeID == "" {
		return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation decision for job %q has no node", job.ID)
	}
	if !fleetHasNode(fleet, decision.NodeID) {
		return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation node %q is not in the fleet snapshot", decision.NodeID)
	}
	peers, err := n.Peers.Peers(ctx)
	if err != nil {
		return domain.PlacementDecision{}, err
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	proposal := PreSendProposal{
		From:      n.SelfID,
		Job:       job,
		Decision:  decision,
		CreatedAt: n.Clock.Now().UTC(),
	}
	for _, candidate := range peers {
		if candidate.ID == n.SelfID {
			continue
		}
		advice, err := n.Client.AdvisePreSend(ctx, candidate, proposal)
		if err != nil {
			return domain.PlacementDecision{}, err
		}
		if advice.PeerID != candidate.ID {
			return domain.PlacementDecision{}, fmt.Errorf("pre-send advice for peer %q came from %q", candidate.ID, advice.PeerID)
		}
		if !advice.Accepted {
			if advice.Reason == "" {
				return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation rejected by peer %q", candidate.ID)
			}
			return domain.PlacementDecision{}, fmt.Errorf("pre-send negotiation rejected by peer %q: %s", candidate.ID, advice.Reason)
		}
	}
	return decision, nil
}

func (n PreSendNegotiator) validate() error {
	if n.SelfID == "" || n.Peers == nil || n.Client == nil || n.Clock == nil {
		return fmt.Errorf("pre-send negotiator is not fully configured")
	}
	return nil
}

type RegistryPreSendAdvisor struct {
	SelfID   string
	Registry ports.JobRegistry
}

func (a RegistryPreSendAdvisor) AdvisePreSend(ctx context.Context, proposal PreSendProposal) (PreSendAdvice, error) {
	if a.SelfID == "" || a.Registry == nil {
		return PreSendAdvice{}, fmt.Errorf("pre-send advisor is not fully configured")
	}
	if proposal.From == "" {
		return PreSendAdvice{}, fmt.Errorf("pre-send proposal coordinator is required")
	}
	if proposal.Job.ID == "" {
		return PreSendAdvice{}, fmt.Errorf("pre-send proposal job id is required")
	}
	if proposal.Decision.JobID != proposal.Job.ID {
		return PreSendAdvice{}, fmt.Errorf("pre-send proposal decision job %q does not match job %q", proposal.Decision.JobID, proposal.Job.ID)
	}
	records, err := a.Registry.Snapshot(ctx)
	if err != nil {
		return PreSendAdvice{}, err
	}
	for _, rec := range records {
		if rec.JobID == proposal.Job.ID && rec.Coordinator != "" && rec.Coordinator != proposal.From && unfinished(rec.Status) {
			return PreSendAdvice{
				PeerID:   a.SelfID,
				Accepted: false,
				Reason:   fmt.Sprintf("job %q is already coordinated by %q", proposal.Job.ID, rec.Coordinator),
			}, nil
		}
	}
	return PreSendAdvice{PeerID: a.SelfID, Accepted: true}, nil
}

func fleetHasNode(fleet domain.FleetSnapshot, nodeID string) bool {
	for _, node := range fleet.Nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}

var _ ports.PreSendNegotiator = PreSendNegotiator{}
