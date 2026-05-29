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
}

type Placer interface {
	Place(ctx context.Context, job domain.Job, fleet domain.FleetSnapshot) (domain.PlacementDecision, error)
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

type Optimizer interface {
	Recommend(ctx context.Context, project domain.Project) ([]domain.TraceStep, error)
}

type Discovery interface {
	Announce(ctx context.Context, node domain.Node) error
	Discover(ctx context.Context) ([]domain.Node, error)
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
