package serving

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/reconcile"
)

type Store interface {
	ListModelServices(ctx context.Context) ([]domain.ModelService, error)
	SaveModelService(ctx context.Context, service domain.ModelService) error
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
	ListJobs(ctx context.Context) ([]domain.Job, error)
}

type Demand struct {
	Model               string
	Project             string
	ContextRequest      int
	ExpectedConcurrency int
	MinReplicas         int
	MaxReplicas         int
	Priority            domain.Priority
	Preemption          domain.Preemption
	CandidatePresetIDs  []string
	LastDemandJobID     string
	Reason              string
	Now                 time.Time
}

type ReconcileOptions struct {
	Now   time.Time
	Limit int
}

type ReconcileReport struct {
	Services int      `json:"services"`
	Created  int      `json:"created"`
	Skipped  int      `json:"skipped"`
	Tasks    []string `json:"tasks,omitempty"`
}

func EnsureDemand(ctx context.Context, store Store, demand Demand) (domain.ModelService, error) {
	if store == nil {
		return domain.ModelService{}, fmt.Errorf("model service store is required")
	}
	demand.Model = strings.TrimSpace(demand.Model)
	if demand.Model == "" {
		return domain.ModelService{}, fmt.Errorf("model service demand requires a model")
	}
	if demand.MinReplicas <= 0 {
		demand.MinReplicas = 1
	}
	if demand.MaxReplicas <= 0 || demand.MaxReplicas < demand.MinReplicas {
		demand.MaxReplicas = demand.MinReplicas
	}
	if demand.ExpectedConcurrency <= 0 {
		demand.ExpectedConcurrency = 1
	}
	now := demand.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	id := ServiceID(demand.Project, demand.Model, demand.ContextRequest)
	services, err := store.ListModelServices(ctx)
	if err != nil {
		return domain.ModelService{}, err
	}
	for _, service := range services {
		if service.ID != id {
			continue
		}
		service.Status = domain.ModelServiceActive
		service.DemandCount++
		service.LastDemandJobID = demand.LastDemandJobID
		service.LastReason = demand.Reason
		service.UpdatedAt = now
		service.MinReplicas = maxInt(service.MinReplicas, demand.MinReplicas)
		service.MaxReplicas = maxInt(service.MaxReplicas, demand.MaxReplicas)
		service.ExpectedConcurrency = maxInt(service.ExpectedConcurrency, demand.ExpectedConcurrency)
		service.CandidatePresetIDs = mergeStrings(service.CandidatePresetIDs, demand.CandidatePresetIDs)
		service.Events = append(service.Events, domain.ModelServiceEvent{Status: service.Status, Message: "demand observed: " + demand.Reason, At: now})
		if err := store.SaveModelService(ctx, service); err != nil {
			return domain.ModelService{}, err
		}
		return service, nil
	}
	service := domain.ModelService{
		ID:                  id,
		Model:               demand.Model,
		Project:             demand.Project,
		ContextRequest:      demand.ContextRequest,
		ExpectedConcurrency: demand.ExpectedConcurrency,
		MinReplicas:         demand.MinReplicas,
		MaxReplicas:         demand.MaxReplicas,
		Priority:            firstPriority(demand.Priority, domain.PriorityBackground),
		Preemption:          firstPreemption(demand.Preemption, domain.PreemptSoft),
		CandidatePresetIDs:  uniqueStrings(demand.CandidatePresetIDs),
		Status:              domain.ModelServiceActive,
		DemandCount:         1,
		LastDemandJobID:     demand.LastDemandJobID,
		LastReason:          demand.Reason,
		CreatedAt:           now,
		UpdatedAt:           now,
		Events: []domain.ModelServiceEvent{{
			Status:  domain.ModelServiceActive,
			Message: "created from gateway demand: " + demand.Reason,
			At:      now,
		}},
	}
	if err := store.SaveModelService(ctx, service); err != nil {
		return domain.ModelService{}, err
	}
	return service, nil
}

func ServiceID(project, model string, contextRequest int) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSpace(project),
		strings.TrimSpace(model),
		fmt.Sprint(contextRequest),
	}, "\x00")))
	return "model-service-" + hex.EncodeToString(sum[:8])
}

func Reconcile(ctx context.Context, store Store, opts ReconcileOptions) (ReconcileReport, error) {
	if store == nil {
		return ReconcileReport{}, fmt.Errorf("model service store is required")
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	services, err := store.ListModelServices(ctx)
	if err != nil {
		return ReconcileReport{}, err
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return ReconcileReport{}, err
	}
	report := ReconcileReport{Services: len(services)}
	for _, service := range services {
		if service.Status != domain.ModelServiceActive {
			report.Skipped++
			continue
		}
		have := runningServiceJobs(service.ID, jobs)
		want := service.MinReplicas - have
		if want <= 0 {
			report.Skipped++
			continue
		}
		if opts.Limit > 0 && report.Created >= opts.Limit {
			report.Skipped++
			continue
		}
		remaining := want
		for _, presetID := range service.CandidatePresetIDs {
			if remaining <= 0 {
				break
			}
			if opts.Limit > 0 && report.Created >= opts.Limit {
				report.Skipped++
				break
			}
			task := prewarmTaskForService(service, presetID, now)
			existing, err := store.ListReconciliationTasks(ctx, service.ID)
			if err != nil {
				return report, err
			}
			if hasNonTerminalTask(existing, task.ID) {
				report.Skipped++
				continue
			}
			if err := store.SaveReconciliationTask(ctx, task); err != nil {
				return report, err
			}
			report.Created++
			report.Tasks = append(report.Tasks, task.ID)
			remaining--
		}
		if remaining > 0 && len(service.CandidatePresetIDs) == 0 {
			report.Skipped++
		}
	}
	return report, nil
}

func prewarmTaskForService(service domain.ModelService, presetID string, now time.Time) domain.ReconciliationTaskRecord {
	task := reconcile.Task{
		Kind:               reconcile.TaskPrewarmReplica,
		PresetID:           presetID,
		Model:              service.Model,
		Reason:             "model service requires warm held capacity",
		AutomaticCandidate: true,
		Next:               "admit and hold a replica for the model service",
	}
	return domain.ReconciliationTaskRecord{
		ID:                 serviceTaskID(service.ID, presetID),
		JobID:              service.ID,
		Kind:               string(task.Kind),
		Status:             domain.ReconciliationTaskAllowed,
		PresetID:           task.PresetID,
		Model:              task.Model,
		Reason:             task.Reason,
		AutomaticCandidate: true,
		RequiresApproval:   false,
		Next:               task.Next,
		Source:             "model_service",
		CreatedAt:          now,
		UpdatedAt:          now,
		Events: []domain.ReconciliationTaskEvent{{
			Status:  domain.ReconciliationTaskAllowed,
			Message: "created from model service desired state",
			At:      now,
		}},
	}
}

func serviceTaskID(serviceID, presetID string) string {
	sum := sha256.Sum256([]byte(serviceID + "\x00" + presetID))
	return "service-prewarm-" + hex.EncodeToString(sum[:8])
}

func runningServiceJobs(serviceID string, jobs []domain.Job) int {
	count := 0
	for _, job := range jobs {
		if job.ParentID == serviceID && job.Status == domain.JobRunning && job.Result != nil && !job.Result.Terminal && job.Result.LeaseID != "" {
			count++
		}
	}
	return count
}

func hasNonTerminalTask(tasks []domain.ReconciliationTaskRecord, id string) bool {
	for _, task := range tasks {
		if task.ID != id {
			continue
		}
		return task.Status != domain.ReconciliationTaskDone && task.Status != domain.ReconciliationTaskFailed
	}
	return false
}

func mergeStrings(a, b []string) []string {
	return uniqueStrings(append(append([]string(nil), a...), b...))
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func firstPriority(values ...domain.Priority) domain.Priority {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstPreemption(values ...domain.Preemption) domain.Preemption {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
