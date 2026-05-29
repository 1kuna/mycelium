package gateway

import (
	"context"
	"fmt"

	"mycelium/internal/domain"
)

type InstanceFailureStore interface {
	Instance(ctx context.Context, id string) (domain.ModelInstance, error)
	SaveInstance(ctx context.Context, inst domain.ModelInstance) error
	DeleteInstance(ctx context.Context, id string) error
}

type InstanceFailureReporter struct {
	Store InstanceFailureStore
	Nodes NodeResolver
}

func (r InstanceFailureReporter) ReportInstanceFailure(ctx context.Context, instanceID string, cause error) error {
	if r.Store == nil || r.Nodes == nil {
		return fmt.Errorf("instance failure reporter is not fully configured")
	}
	if instanceID == "" {
		return fmt.Errorf("failed instance id is required")
	}
	inst, err := r.Store.Instance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("load failed instance %q: %w", instanceID, err)
	}
	agent, err := r.Nodes.NodeAgent(inst.NodeID)
	if err != nil {
		if markErr := r.markError(ctx, inst); markErr != nil {
			return fmt.Errorf("resolve node for failed instance %q: %v; mark error: %w", instanceID, err, markErr)
		}
		return fmt.Errorf("resolve node for failed instance %q: %w", instanceID, err)
	}
	if err := agent.Unload(ctx, instanceID); err != nil {
		if markErr := r.markError(ctx, inst); markErr != nil {
			return fmt.Errorf("unload failed instance %q: %v; mark error: %w", instanceID, err, markErr)
		}
		return fmt.Errorf("unload failed instance %q: %w", instanceID, err)
	}
	if err := r.Store.DeleteInstance(ctx, instanceID); err != nil {
		return fmt.Errorf("delete failed instance %q after unload: %w", instanceID, err)
	}
	return nil
}

func (r InstanceFailureReporter) markError(ctx context.Context, inst domain.ModelInstance) error {
	inst.State = domain.InstError
	inst.Loading = false
	return r.Store.SaveInstance(ctx, inst)
}

var _ FailureReporter = InstanceFailureReporter{}
