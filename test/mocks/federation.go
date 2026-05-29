package mocks

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type AdmissionController struct {
	OfferVal         domain.LeaseOffer
	LeaseVal         domain.Lease
	OfferErr         error
	CommitErr        error
	ReleaseErr       error
	PreemptErr       error
	LeaseForJobVal   domain.Lease
	LeaseForJobFound bool
	LeaseForJobErr   error
	Offers           map[string]domain.LeaseOffer
	Calls            []string
}

func (m *AdmissionController) Offer(_ context.Context, job domain.Job, claim domain.Claim) (domain.LeaseOffer, error) {
	m.Calls = append(m.Calls, "offer:"+job.ID)
	if m.OfferErr != nil {
		return domain.LeaseOffer{}, m.OfferErr
	}
	if m.OfferVal.OfferID != "" {
		m.recordOffer(m.OfferVal)
		return m.OfferVal, nil
	}
	offer := domain.LeaseOffer{
		OfferID: fmt.Sprintf("offer_%s", job.ID),
		JobID:   job.ID,
		NodeID:  "node_test",
		Claim:   claim,
		Fence:   1,
	}
	m.recordOffer(offer)
	return offer, nil
}

func (m *AdmissionController) Commit(_ context.Context, offerID string, fence uint64) (domain.Lease, error) {
	m.Calls = append(m.Calls, fmt.Sprintf("commit:%s:%d", offerID, fence))
	if m.CommitErr != nil {
		return domain.Lease{}, m.CommitErr
	}
	if m.LeaseVal.ID != "" {
		return m.LeaseVal, nil
	}
	offer := m.Offers[offerID]
	return domain.Lease{ID: "lease_" + offerID, JobID: offer.JobID, NodeID: offer.NodeID, Claim: offer.Claim}, nil
}

func (m *AdmissionController) Release(_ context.Context, leaseID string) error {
	m.Calls = append(m.Calls, "release:"+leaseID)
	return m.ReleaseErr
}

func (m *AdmissionController) Preempt(_ context.Context, leaseID, reason string) error {
	m.Calls = append(m.Calls, "preempt:"+leaseID+":"+reason)
	return m.PreemptErr
}

func (m *AdmissionController) LeaseForJob(_ context.Context, jobID string) (domain.Lease, bool, error) {
	m.Calls = append(m.Calls, "lease-for-job:"+jobID)
	return m.LeaseForJobVal, m.LeaseForJobFound, m.LeaseForJobErr
}

func (m *AdmissionController) recordOffer(offer domain.LeaseOffer) {
	if m.Offers == nil {
		m.Offers = map[string]domain.LeaseOffer{}
	}
	m.Offers[offer.OfferID] = offer
}

type Coordinator struct {
	Decision   domain.PlacementDecision
	Lease      domain.Lease
	Err        error
	PlanErr    error
	CommitErr  error
	ReleaseErr error
	Calls      []string
}

func (m *Coordinator) ClaimJob(_ context.Context, jobID string) error {
	m.Calls = append(m.Calls, "claim:"+jobID)
	return m.Err
}

func (m *Coordinator) Plan(_ context.Context, jobID string) (domain.PlacementDecision, error) {
	m.Calls = append(m.Calls, "plan:"+jobID)
	if m.PlanErr != nil {
		return domain.PlacementDecision{}, m.PlanErr
	}
	return m.Decision, nil
}

func (m *Coordinator) Commit(_ context.Context, plan domain.PlacementDecision) (domain.Lease, error) {
	m.Calls = append(m.Calls, "commit:"+plan.JobID)
	if m.CommitErr != nil {
		return domain.Lease{}, m.CommitErr
	}
	return m.Lease, nil
}

func (m *Coordinator) Release(_ context.Context, jobID string) error {
	m.Calls = append(m.Calls, "release:"+jobID)
	return m.ReleaseErr
}

type JobRegistry struct {
	Records  []domain.JobRecord
	Err      error
	WatchErr error
	WatchCh  chan domain.JobRecord
	Calls    []string
}

func (m *JobRegistry) Put(_ context.Context, rec domain.JobRecord) error {
	m.Calls = append(m.Calls, "put:"+rec.JobID)
	if m.Err != nil {
		return m.Err
	}
	m.Records = append(m.Records, rec)
	return nil
}

func (m *JobRegistry) Watch(_ context.Context, fromCursor string) (<-chan domain.JobRecord, error) {
	m.Calls = append(m.Calls, "watch:"+fromCursor)
	if m.WatchErr != nil {
		return nil, m.WatchErr
	}
	if m.WatchCh != nil {
		return m.WatchCh, nil
	}
	ch := make(chan domain.JobRecord)
	close(ch)
	return ch, nil
}

func (m *JobRegistry) Snapshot(context.Context) ([]domain.JobRecord, error) {
	m.Calls = append(m.Calls, "snapshot")
	if m.Err != nil {
		return nil, m.Err
	}
	return append([]domain.JobRecord(nil), m.Records...), nil
}

type PeerDiscovery struct {
	PeersVal []domain.Peer
	Err      error
	WatchErr error
	WatchCh  chan domain.Peer
	Calls    []string
}

func (m *PeerDiscovery) Advertise(_ context.Context, self domain.Peer) error {
	m.Calls = append(m.Calls, "advertise:"+self.ID)
	if m.Err != nil {
		return m.Err
	}
	m.PeersVal = append(m.PeersVal, self)
	return nil
}

func (m *PeerDiscovery) Peers(context.Context) ([]domain.Peer, error) {
	m.Calls = append(m.Calls, "peers")
	if m.Err != nil {
		return nil, m.Err
	}
	return append([]domain.Peer(nil), m.PeersVal...), nil
}

func (m *PeerDiscovery) WatchPeers(context.Context) (<-chan domain.Peer, error) {
	m.Calls = append(m.Calls, "watch-peers")
	if m.WatchErr != nil {
		return nil, m.WatchErr
	}
	if m.WatchCh != nil {
		return m.WatchCh, nil
	}
	ch := make(chan domain.Peer)
	close(ch)
	return ch, nil
}

var _ ports.AdmissionController = (*AdmissionController)(nil)
var _ ports.LeaseInspector = (*AdmissionController)(nil)
var _ ports.Coordinator = (*Coordinator)(nil)
var _ ports.JobRegistry = (*JobRegistry)(nil)
var _ ports.PeerDiscovery = (*PeerDiscovery)(nil)
