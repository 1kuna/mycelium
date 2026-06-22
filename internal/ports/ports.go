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

type UnitResourceEstimator interface {
	EstimateForUnit(ctx context.Context, p domain.Preset, contextLen, concurrency int, node domain.Node, acceleratorSet []int) (domain.Claim, error)
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

type DynamicBackendAdapter interface {
	LaunchDynamic(ctx context.Context, p domain.Preset, addr string) (Handle, error)
}

type Handle struct {
	PID       int
	PGID      int
	Addr      string
	Kind      string
	Ref       string
	Binary    string
	Args      []string
	StartedAt time.Time
}

type NodeAgent interface {
	Snapshot(ctx context.Context) (domain.NodeSnapshot, error)
	Load(ctx context.Context, req domain.LoadRequest) (domain.ModelInstance, error)
	Unload(ctx context.Context, instanceID string) error
	InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error)
	BeginRequest(ctx context.Context, instanceID string) error
	EndRequest(ctx context.Context, instanceID string) error
}

type Placer interface {
	Place(ctx context.Context, job domain.Job, fleet domain.FleetSnapshot) (domain.PlacementDecision, error)
}

type AdmissionController interface {
	Offer(ctx context.Context, req domain.AdmissionRequest) (domain.LeaseOffer, error)
	Commit(ctx context.Context, offerID string, fence uint64) (domain.Lease, error)
	Release(ctx context.Context, leaseID string) error
	Preempt(ctx context.Context, leaseID, reason string) error
}

type PolicyPreempter interface {
	PreemptForJob(ctx context.Context, req domain.Job, leaseID, reason string) error
}

type LeaseInspector interface {
	LeaseForJob(ctx context.Context, jobID string) (domain.Lease, bool, error)
	LeaseForInstance(ctx context.Context, instanceID string) (domain.Lease, bool, error)
}

type JobStatusInspector interface {
	JobStatus(ctx context.Context, jobID string) (domain.JobStatus, bool, error)
}

type LeaseBinder interface {
	BindInstance(ctx context.Context, leaseID, instanceID string) error
}

type Coordinator interface {
	ClaimJob(ctx context.Context, jobID string) error
	Plan(ctx context.Context, jobID string) (domain.PlacementDecision, error)
	Commit(ctx context.Context, plan domain.PlacementDecision) (domain.CommitOutcome, error)
	MarkRunning(ctx context.Context, jobID string) error
	Release(ctx context.Context, jobID string) error
	Complete(ctx context.Context, jobID string) error
	Fail(ctx context.Context, jobID string, err error) error
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
	RecordSample(ctx context.Context, m domain.SessionMetric) error
}

type ModelRegistry interface {
	Resolve(ctx context.Context, model string) (domain.Preset, error)
}

type Catalog interface {
	Materialize(ctx context.Context, ref string) (domain.Preset, error)
}

type ModelInventory interface {
	SaveModelLocality(ctx context.Context, locality domain.ModelLocality) error
	ListModelLocalities(ctx context.Context) ([]domain.ModelLocality, error)
	DeleteModelLocality(ctx context.Context, id string) error
}

type LocalityPlanStore interface {
	SaveLocalityPlan(ctx context.Context, plan domain.LocalityPlan) error
	LocalityPlan(ctx context.Context, id string) (domain.LocalityPlan, error)
	ListLocalityPlans(ctx context.Context) ([]domain.LocalityPlan, error)
}

type PeerCatalogStager interface {
	StageModel(ctx context.Context, peer domain.Peer, preset domain.Preset) (domain.ModelLocality, error)
}

type TelemetryStore interface {
	TelemetrySink
	Metrics(ctx context.Context, project string) ([]domain.RunMetric, error)
	Samples(ctx context.Context, query domain.SessionMetricQuery) ([]domain.SessionMetric, error)
}

type TelemetryPeerClient interface {
	Metrics(ctx context.Context, peer domain.Peer) ([]domain.RunMetric, error)
	PushMetrics(ctx context.Context, peer domain.Peer, metrics []domain.RunMetric) error
	Samples(ctx context.Context, peer domain.Peer, query domain.SessionMetricQuery) ([]domain.SessionMetric, error)
	PushSamples(ctx context.Context, peer domain.Peer, samples []domain.SessionMetric) error
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

type HostDetector interface {
	DetectHost(ctx context.Context, seed domain.Node) (domain.HostFacts, error)
}

type EngineDetector interface {
	DetectEngines(ctx context.Context, host domain.HostFacts) ([]domain.EngineProfile, error)
}

type BootstrapPlanner interface {
	PlanBootstrap(ctx context.Context, req domain.BootstrapRequest, host domain.HostFacts, detections []domain.EngineProfile) (domain.BootstrapPlan, error)
}

type EngineInstaller interface {
	ApplyBootstrapPlan(ctx context.Context, plan domain.BootstrapPlan, progress func(domain.BootstrapEvent)) (domain.BootstrapResult, error)
}

type EngineVerifier interface {
	VerifyEngine(ctx context.Context, profile domain.EngineProfile) (domain.EngineVerification, error)
}

type EngineRegistry interface {
	SaveEngineProfile(ctx context.Context, profile domain.EngineProfile) error
	ListEngineProfiles(ctx context.Context) ([]domain.EngineProfile, error)
	MarkEngineProfileUnready(ctx context.Context, profileID, reason string) error
}

type EngineReadinessChecker interface {
	CheckEngineReadiness(ctx context.Context, node domain.Node, preset domain.Preset) (domain.EngineReadinessCheck, error)
}

type BootstrapPlanStore interface {
	SaveBootstrapPlan(ctx context.Context, plan domain.BootstrapPlan) error
	BootstrapPlan(ctx context.Context, id string) (domain.BootstrapPlan, error)
	ListBootstrapPlans(ctx context.Context) ([]domain.BootstrapPlan, error)
}
