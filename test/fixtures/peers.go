package fixtures

import (
	"time"

	"mycelium/internal/domain"
)

func MakePeer(opts ...func(*domain.Peer)) domain.Peer {
	p := domain.Peer{
		ID:        "peer_test",
		Addresses: []string{"127.0.0.1:51847"},
		Compute:   true,
		LastSeen:  time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
		Version:   "test",
	}
	for _, opt := range opts {
		opt(&p)
	}
	return p
}

func WithPeerID(id string) func(*domain.Peer) {
	return func(p *domain.Peer) { p.ID = id }
}

func ComputeOff(p *domain.Peer) {
	p.Compute = false
}

func WithPeerAddress(addr string) func(*domain.Peer) {
	return func(p *domain.Peer) { p.Addresses = []string{addr} }
}

func MakeLeaseOffer(opts ...func(*domain.LeaseOffer)) domain.LeaseOffer {
	o := domain.LeaseOffer{
		OfferID:   "offer_test",
		JobID:     "job_test",
		NodeID:    "node_test",
		Claim:     MakeClaim(1, 2),
		Fence:     1,
		ExpiresAt: time.Date(2026, 5, 29, 12, 1, 0, 0, time.UTC),
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func WithOfferID(id string) func(*domain.LeaseOffer) {
	return func(o *domain.LeaseOffer) { o.OfferID = id }
}

func WithOfferFence(fence uint64) func(*domain.LeaseOffer) {
	return func(o *domain.LeaseOffer) { o.Fence = fence }
}

func MakeJobRecord(opts ...func(*domain.JobRecord)) domain.JobRecord {
	r := domain.JobRecord{
		JobID:       "job_test",
		Coordinator: "peer_test",
		Status:      domain.JobQueued,
		Request:     []byte(`{"model":"test"}`),
		Fence:       1,
		UpdatedAt:   time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC),
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

func WithRecordJobID(id string) func(*domain.JobRecord) {
	return func(r *domain.JobRecord) { r.JobID = id }
}
