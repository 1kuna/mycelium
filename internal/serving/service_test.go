package serving

import (
	"context"
	"errors"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/reconcile"
)

func TestEnsureDemandCreatesAndBumpsModelService(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	now := time.Unix(10, 0).UTC()
	service, err := EnsureDemand(ctx, store, Demand{
		Model:               "vision-model",
		Project:             "project-a",
		ContextRequest:      49152,
		ExpectedConcurrency: 2,
		MaxReplicas:         2,
		Priority:            domain.PriorityNormal,
		CandidatePresetIDs:  []string{"preset-b", "preset-a", "preset-a"},
		LastDemandJobID:     "job-a",
		Reason:              "no instance available",
		Now:                 now,
	})
	if err != nil {
		t.Fatalf("EnsureDemand: %v", err)
	}
	if service.ID == "" || service.Status != domain.ModelServiceActive || service.MinReplicas != 1 || service.MaxReplicas != 2 || service.DemandCount != 1 {
		t.Fatalf("service = %+v", service)
	}
	if got := service.CandidatePresetIDs; len(got) != 2 || got[0] != "preset-a" || got[1] != "preset-b" {
		t.Fatalf("candidate preset ids = %+v", got)
	}

	bumped, err := EnsureDemand(ctx, store, Demand{
		Model:               "vision-model",
		Project:             "project-a",
		ContextRequest:      49152,
		ExpectedConcurrency: 3,
		MaxReplicas:         3,
		CandidatePresetIDs:  []string{"preset-c"},
		LastDemandJobID:     "job-b",
		Reason:              "capacity limited",
		Now:                 now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("EnsureDemand bump: %v", err)
	}
	if bumped.ID != service.ID || bumped.DemandCount != 2 || bumped.ExpectedConcurrency != 3 || bumped.MaxReplicas != 3 || bumped.LastDemandJobID != "job-b" {
		t.Fatalf("bumped service = %+v", bumped)
	}
	if got := bumped.CandidatePresetIDs; len(got) != 3 || got[2] != "preset-c" {
		t.Fatalf("bumped candidate presets = %+v", got)
	}
}

func TestReconcileCreatesPrewarmTasksForMissingServiceReplicas(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(20, 0).UTC()
	store := &fakeStore{services: []domain.ModelService{{
		ID:                 "svc-a",
		Model:              "vision-model",
		ContextRequest:     49152,
		MinReplicas:        2,
		MaxReplicas:        2,
		CandidatePresetIDs: []string{"preset-a", "preset-b"},
		Status:             domain.ModelServiceActive,
		CreatedAt:          now,
		UpdatedAt:          now,
	}}}
	report, err := Reconcile(ctx, store, ReconcileOptions{Now: now})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Created != 2 || len(store.tasks) != 2 {
		t.Fatalf("report=%+v tasks=%+v", report, store.tasks)
	}
	for _, task := range store.tasks {
		if task.JobID != "svc-a" || task.Kind != string(reconcile.TaskPrewarmReplica) || task.Status != domain.ReconciliationTaskAllowed || task.Source != "model_service" {
			t.Fatalf("task = %+v", task)
		}
		if task.ContextRequest != 49152 || task.ExpectedConcurrency != 0 {
			t.Fatalf("task = %+v", task)
		}
	}
}

func TestReconcileDoesNotCreatePrewarmWhenServiceAlreadyHasRunningLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(30, 0).UTC()
	store := &fakeStore{
		services: []domain.ModelService{{
			ID:                 "svc-a",
			Model:              "vision-model",
			MinReplicas:        1,
			MaxReplicas:        1,
			CandidatePresetIDs: []string{"preset-a"},
			Status:             domain.ModelServiceActive,
			CreatedAt:          now,
			UpdatedAt:          now,
		}},
		jobs: []domain.Job{{
			ID:       "held-a",
			ParentID: "svc-a",
			Status:   domain.JobRunning,
			Result:   &domain.JobResult{LeaseID: "lease-a"},
		}},
		leases: []domain.Lease{{
			ID:        "lease-a",
			JobID:     "held-a",
			ExpiresAt: now.Add(time.Minute),
		}},
	}
	report, err := Reconcile(ctx, store, ReconcileOptions{Now: now})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Created != 0 || len(store.tasks) != 0 {
		t.Fatalf("report=%+v tasks=%+v", report, store.tasks)
	}
}

func TestReconcileReplacesServiceJobWithoutActiveLease(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(35, 0).UTC()
	tests := map[string][]domain.Lease{
		"missing": {},
		"expired": {{
			ID:        "lease-a",
			JobID:     "held-a",
			ExpiresAt: now.Add(-time.Second),
		}},
		"wrong job": {{
			ID:        "lease-a",
			JobID:     "other-job",
			ExpiresAt: now.Add(time.Minute),
		}},
	}
	for name, leases := range tests {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{
				services: []domain.ModelService{{
					ID:                 "svc-a",
					Model:              "vision-model",
					MinReplicas:        1,
					MaxReplicas:        1,
					CandidatePresetIDs: []string{"preset-a"},
					Status:             domain.ModelServiceActive,
					CreatedAt:          now,
					UpdatedAt:          now,
				}},
				jobs: []domain.Job{{
					ID:       "held-a",
					ParentID: "svc-a",
					Status:   domain.JobRunning,
					Result:   &domain.JobResult{LeaseID: "lease-a"},
				}},
				leases: leases,
			}
			report, err := Reconcile(ctx, store, ReconcileOptions{Now: now})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if report.Created != 1 || len(store.tasks) != 1 || store.tasks[0].Kind != string(reconcile.TaskPrewarmReplica) {
				t.Fatalf("report=%+v tasks=%+v", report, store.tasks)
			}
		})
	}
}

func TestEnsureDemandValidationAndDefaults(t *testing.T) {
	if _, err := EnsureDemand(context.Background(), nil, Demand{Model: "m"}); err == nil {
		t.Fatal("nil store should fail")
	}
	if _, err := EnsureDemand(context.Background(), &fakeStore{}, Demand{}); err == nil {
		t.Fatal("missing model should fail")
	}
	store := &fakeStore{}
	service, err := EnsureDemand(context.Background(), store, Demand{Model: "m", Now: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatalf("EnsureDemand defaults: %v", err)
	}
	if service.MinReplicas != 1 || service.MaxReplicas != 1 || service.ExpectedConcurrency != 1 || service.Priority != domain.PriorityBackground || service.Preemption != domain.PreemptSoft {
		t.Fatalf("defaults = %+v", service)
	}
}

func TestReconcileSkipsExistingNonTerminalTaskAndPausedServices(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(40, 0).UTC()
	service := domain.ModelService{
		ID:                 "svc-a",
		Model:              "vision-model",
		MinReplicas:        1,
		MaxReplicas:        1,
		CandidatePresetIDs: []string{"preset-a"},
		Status:             domain.ModelServiceActive,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	existing := prewarmTaskForService(service, "preset-a", now)
	store := &fakeStore{
		services: []domain.ModelService{
			service,
			{ID: "paused", Model: "m", MinReplicas: 1, MaxReplicas: 1, Status: domain.ModelServicePaused, CreatedAt: now, UpdatedAt: now},
		},
		tasks: []domain.ReconciliationTaskRecord{existing},
	}
	report, err := Reconcile(ctx, store, ReconcileOptions{Now: now})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Created != 0 || report.Skipped == 0 || len(store.tasks) != 1 {
		t.Fatalf("report=%+v tasks=%+v", report, store.tasks)
	}

	store.tasks[0].Status = domain.ReconciliationTaskDone
	report, err = Reconcile(ctx, store, ReconcileOptions{Now: now, Limit: 1})
	if err != nil {
		t.Fatalf("Reconcile after terminal: %v", err)
	}
	if report.Created != 1 || len(store.tasks) != 1 || store.tasks[0].Status != domain.ReconciliationTaskAllowed {
		t.Fatalf("report=%+v tasks=%+v", report, store.tasks)
	}
}

func TestReconcileSurfacesStoreErrors(t *testing.T) {
	want := errors.New("store failed")
	if _, err := Reconcile(context.Background(), nil, ReconcileOptions{}); err == nil {
		t.Fatal("nil store should fail")
	}
	if _, err := Reconcile(context.Background(), &fakeStore{listServicesErr: want}, ReconcileOptions{}); !errors.Is(err, want) {
		t.Fatalf("list services err = %v", err)
	}
	if _, err := Reconcile(context.Background(), &fakeStore{services: []domain.ModelService{{ID: "svc", Model: "m", MinReplicas: 1, Status: domain.ModelServiceActive}}, listJobsErr: want}, ReconcileOptions{}); !errors.Is(err, want) {
		t.Fatalf("list jobs err = %v", err)
	}
	if _, err := Reconcile(context.Background(), &fakeStore{services: []domain.ModelService{{ID: "svc", Model: "m", MinReplicas: 1, Status: domain.ModelServiceActive}}, listLeasesErr: want}, ReconcileOptions{}); !errors.Is(err, want) {
		t.Fatalf("list leases err = %v", err)
	}
	if _, err := Reconcile(context.Background(), &fakeStore{services: []domain.ModelService{{ID: "svc", Model: "m", MinReplicas: 1, Status: domain.ModelServiceActive, CandidatePresetIDs: []string{"p"}}}, listTasksErr: want}, ReconcileOptions{}); !errors.Is(err, want) {
		t.Fatalf("list tasks err = %v", err)
	}
	if _, err := Reconcile(context.Background(), &fakeStore{services: []domain.ModelService{{ID: "svc", Model: "m", MinReplicas: 1, Status: domain.ModelServiceActive, CandidatePresetIDs: []string{"p"}}}, saveTaskErr: want}, ReconcileOptions{}); !errors.Is(err, want) {
		t.Fatalf("save task err = %v", err)
	}
}

type fakeStore struct {
	services        []domain.ModelService
	tasks           []domain.ReconciliationTaskRecord
	jobs            []domain.Job
	leases          []domain.Lease
	listServicesErr error
	saveServiceErr  error
	listTasksErr    error
	saveTaskErr     error
	listJobsErr     error
	listLeasesErr   error
}

func (s *fakeStore) ListModelServices(context.Context) ([]domain.ModelService, error) {
	if s.listServicesErr != nil {
		return nil, s.listServicesErr
	}
	return append([]domain.ModelService(nil), s.services...), nil
}

func (s *fakeStore) SaveModelService(_ context.Context, service domain.ModelService) error {
	if s.saveServiceErr != nil {
		return s.saveServiceErr
	}
	for i := range s.services {
		if s.services[i].ID == service.ID {
			s.services[i] = service
			return nil
		}
	}
	s.services = append(s.services, service)
	return nil
}

func (s *fakeStore) ListReconciliationTasks(_ context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	if s.listTasksErr != nil {
		return nil, s.listTasksErr
	}
	var out []domain.ReconciliationTaskRecord
	for _, task := range s.tasks {
		if jobID == "" || task.JobID == jobID {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *fakeStore) SaveReconciliationTask(_ context.Context, task domain.ReconciliationTaskRecord) error {
	if s.saveTaskErr != nil {
		return s.saveTaskErr
	}
	for i := range s.tasks {
		if s.tasks[i].ID == task.ID {
			s.tasks[i] = task
			return nil
		}
	}
	s.tasks = append(s.tasks, task)
	return nil
}

func (s *fakeStore) ListJobs(context.Context) ([]domain.Job, error) {
	if s.listJobsErr != nil {
		return nil, s.listJobsErr
	}
	return append([]domain.Job(nil), s.jobs...), nil
}

func (s *fakeStore) ListLeases(context.Context) ([]domain.Lease, error) {
	if s.listLeasesErr != nil {
		return nil, s.listLeasesErr
	}
	return append([]domain.Lease(nil), s.leases...), nil
}
