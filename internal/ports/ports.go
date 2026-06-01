package ports

import (
	"context"
	"time"

	"mycelium/internal/domain"
)

type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type ResourceEstimator interface {
	Estimate(ctx context.Context, p domain.Preset, contextLen, concurrency int) (domain.Claim, error)
}

type Allocator interface {
	Fits(node domain.Node, acc []int, existing []domain.ModelInstance, want domain.Claim) bool
	CanStackLoad(node domain.Node, acc []int, existing []domain.ModelInstance) bool
}

type BackendAdapter interface {
	Name() string
	Launch(ctx context.Context, p domain.Preset, addr string) (Handle, error)
	WaitReady(ctx context.Context, addr string) error
	Stop(ctx context.Context, h Handle) error
}

type Handle struct {
	PID  int
	Addr string
	Kind string
	Ref  string
}

type NodeAgent interface {
	Snapshot(ctx context.Context) (domain.NodeSnapshot, error)
	Load(ctx context.Context, p domain.Preset) (domain.ModelInstance, error)
	Unload(ctx context.Context, instanceID string) error
	InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error)
	BeginRequest(ctx context.Context, instanceID string) error
	EndRequest(ctx context.Context, instanceID string) error
}

type Placer interface {
	Place(ctx context.Context, job domain.Job, fleet domain.FleetSnapshot) (domain.PlacementDecision, error)
}

type AdmissionController interface {
	Offer(ctx context.Context, req domain.Job, claim domain.Claim) (domain.LeaseOffer, error)
	Commit(ctx context.Context, offerID string, fence uint64) (domain.Lease, error)
	Release(ctx context.Context, leaseID string) error
	Preempt(ctx context.Context, leaseID, reason string) error
}

type LeaseInspector interface {
	LeaseForJob(ctx context.Context, jobID string) (domain.Lease, bool, error)
	LeaseForInstance(ctx context.Context, instanceID string) (domain.Lease, bool, error)
}

type LeaseBinder interface {
	BindInstance(ctx context.Context, leaseID, instanceID string) error
}

type Coordinator interface {
	ClaimJob(ctx context.Context, jobID string) error
	Plan(ctx context.Context, jobID string) (domain.PlacementDecision, error)
	Commit(ctx context.Context, plan domain.PlacementDecision) (domain.Lease, error)
	Release(ctx context.Context, jobID string) error
}

type JobRegistry interface {
	Put(ctx context.Context, rec domain.JobRecord) error
	Watch(ctx context.Context, fromCursor string) (<-chan domain.JobRecord, error)
	Snapshot(ctx context.Context) ([]domain.JobRecord, error)
}

type PeerDiscovery interface {
	Advertise(ctx context.Context, self domain.Peer) error
	Peers(ctx context.Context) ([]domain.Peer, error)
	WatchPeers(ctx context.Context) (<-chan domain.Peer, error)
}

type TelemetrySink interface {
	Record(ctx context.Context, m domain.RunMetric) error
}

type ModelRegistry interface {
	Resolve(ctx context.Context, model string) (domain.Preset, error)
}

type Catalog interface {
	Materialize(ctx context.Context, ref string) (domain.Preset, error)
}

type TelemetryStore interface {
	TelemetrySink
	Metrics(ctx context.Context, project string) ([]domain.RunMetric, error)
}

type TelemetryPeerClient interface {
	Metrics(ctx context.Context, peer domain.Peer) ([]domain.RunMetric, error)
	PushMetrics(ctx context.Context, peer domain.Peer, metrics []domain.RunMetric) error
	Recommendations(ctx context.Context, peer domain.Peer) ([]domain.RecommendationRecord, error)
	PushRecommendations(ctx context.Context, peer domain.Peer, recs []domain.RecommendationRecord) error
}

type Optimizer interface {
	Recommend(ctx context.Context, project domain.Project) ([]domain.TraceStep, error)
}

type Tunnel interface {
	Open(ctx context.Context, node domain.Node) (string, error)
	Close(ctx context.Context, nodeID string) error
}

type Store interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type HardwareDetector interface {
	Detect(ctx context.Context, seed domain.Node) (domain.Node, error)
}
