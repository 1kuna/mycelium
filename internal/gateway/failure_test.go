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

func TestInstanceFailureReporterUnloadsAndDeletes(t *testing.T) {
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode(node.ID))
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	store := newFailureStore(inst)
	reporter := InstanceFailureReporter{
		Store: store,
		Nodes: NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: agent}},
	}

	if err := reporter.ReportInstanceFailure(context.Background(), inst.ID, errors.New("upstream failed")); err != nil {
		t.Fatalf("ReportInstanceFailure: %v", err)
	}
	if strings.Join(agent.Calls, ",") != "unload:inst-a" {
		t.Fatalf("agent calls = %+v", agent.Calls)
	}
	if strings.Join(store.deleted, ",") != inst.ID {
		t.Fatalf("deleted = %+v", store.deleted)
	}
	if _, ok := store.instances[inst.ID]; ok {
		t.Fatalf("instance was not deleted: %+v", store.instances)
	}
}

func TestInstanceFailureReporterMarksErrorWhenUnloadFails(t *testing.T) {
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode(node.ID))
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	agent.UnloadErr = errors.New("stuck")
	store := newFailureStore(inst)
	reporter := InstanceFailureReporter{
		Store: store,
		Nodes: NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: agent}},
	}

	err := reporter.ReportInstanceFailure(context.Background(), inst.ID, errors.New("upstream failed"))
	if err == nil || !strings.Contains(err.Error(), "unload failed instance") {
		t.Fatalf("err = %v", err)
	}
	if got := store.instances[inst.ID]; got.State != domain.InstError || got.Loading {
		t.Fatalf("stored instance = %+v", got)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("deleted = %+v", store.deleted)
	}
}

func TestInstanceFailureReporterValidationAndStoreErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		reporter InstanceFailureReporter
		id       string
		want     string
	}{
		{name: "unconfigured", reporter: InstanceFailureReporter{}, id: "inst-a", want: "not fully configured"},
		{name: "empty id", reporter: InstanceFailureReporter{Store: newFailureStore(), Nodes: NodeDirectory{}}, want: "failed instance id"},
		{name: "missing instance", reporter: InstanceFailureReporter{Store: newFailureStore(), Nodes: NodeDirectory{}}, id: "missing", want: "load failed instance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.reporter.ReportInstanceFailure(context.Background(), tc.id, errors.New("upstream failed"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestInstanceFailureReporterResolverAndPersistenceFailures(t *testing.T) {
	node := fixtures.MakeNode()
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.OnNode(node.ID), fixtures.Loading)

	resolverStore := newFailureStore(inst)
	resolverErr := InstanceFailureReporter{Store: resolverStore, Nodes: NodeDirectory{}}
	err := resolverErr.ReportInstanceFailure(context.Background(), inst.ID, errors.New("upstream failed"))
	if err == nil || !strings.Contains(err.Error(), "resolve node for failed instance") {
		t.Fatalf("resolver err = %v", err)
	}
	if got := resolverStore.instances[inst.ID]; got.State != domain.InstError || got.Loading {
		t.Fatalf("resolver stored instance = %+v", got)
	}

	deleteStore := newFailureStore(inst)
	deleteStore.deleteErr = errors.New("delete failed")
	agent := mocks.NewNodeAgent(node)
	agent.Instances = []domain.ModelInstance{inst}
	deleteErr := InstanceFailureReporter{
		Store: deleteStore,
		Nodes: NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: agent}},
	}
	err = deleteErr.ReportInstanceFailure(context.Background(), inst.ID, errors.New("upstream failed"))
	if err == nil || !strings.Contains(err.Error(), "delete failed instance") {
		t.Fatalf("delete err = %v", err)
	}

	saveStore := newFailureStore(inst)
	saveStore.saveErr = errors.New("save failed")
	saveAgent := mocks.NewNodeAgent(node)
	saveAgent.UnloadErr = errors.New("stuck")
	saveErr := InstanceFailureReporter{
		Store: saveStore,
		Nodes: NodeDirectory{Agents: map[string]ports.NodeAgent{node.ID: saveAgent}},
	}
	err = saveErr.ReportInstanceFailure(context.Background(), inst.ID, errors.New("upstream failed"))
	if err == nil || !strings.Contains(err.Error(), "mark error") {
		t.Fatalf("save err = %v", err)
	}
}

type failureStore struct {
	instances map[string]domain.ModelInstance
	deleted   []string
	saveErr   error
	deleteErr error
}

func newFailureStore(instances ...domain.ModelInstance) *failureStore {
	store := &failureStore{instances: map[string]domain.ModelInstance{}}
	for _, inst := range instances {
		store.instances[inst.ID] = inst
	}
	return store
}

func (s *failureStore) Instance(_ context.Context, id string) (domain.ModelInstance, error) {
	inst, ok := s.instances[id]
	if !ok {
		return domain.ModelInstance{}, errors.New("missing instance")
	}
	return inst, nil
}

func (s *failureStore) SaveInstance(_ context.Context, inst domain.ModelInstance) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.instances[inst.ID] = inst
	return nil
}

func (s *failureStore) DeleteInstance(_ context.Context, id string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.instances, id)
	s.deleted = append(s.deleted, id)
	return nil
}
