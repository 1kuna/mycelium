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
	"mycelium/internal/modelruntime"
	"mycelium/internal/reconcile"
)

type Store interface {
	ListModelServices(ctx context.Context) ([]domain.ModelService, error)
	SaveModelService(ctx context.Context, service domain.ModelService) error
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
	ListJobs(ctx context.Context) ([]domain.Job, error)
	ListLeases(ctx context.Context) ([]domain.Lease, error)
}

type Demand struct {
	Model               string
	ModelSource         string
	ModelSourceContract *domain.ModelSourceContract
	Backend             domain.Backend
	Project             string
	ContextRequest      int
	ExpectedConcurrency int
	MinReplicas         int
	MaxReplicas         int
	Priority            domain.Priority
	Preemption          domain.Preemption
	CandidatePresetIDs  []string
	ReplaceCandidates   bool
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

type presetLister interface {
	ListPresets(ctx context.Context) ([]domain.Preset, error)
}

type serviceJobLister interface {
	ListModelServiceJobs(ctx context.Context) ([]domain.Job, error)
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
		if strings.TrimSpace(demand.ModelSource) != "" {
			service.ModelSource = strings.TrimSpace(demand.ModelSource)
		}
		if modelSourceContractPresent(demand.ModelSourceContract) {
			service.ModelSourceContract = cloneModelSourceContract(demand.ModelSourceContract)
		}
		if demand.Backend != "" {
			service.Backend = demand.Backend
		}
		service.UpdatedAt = now
		service.MinReplicas = maxInt(service.MinReplicas, demand.MinReplicas)
		service.MaxReplicas = maxInt(service.MaxReplicas, demand.MaxReplicas)
		service.ExpectedConcurrency = maxInt(service.ExpectedConcurrency, demand.ExpectedConcurrency)
		if demand.ReplaceCandidates {
			service.CandidatePresetIDs = uniqueStrings(demand.CandidatePresetIDs)
		} else {
			service.CandidatePresetIDs = mergeStrings(service.CandidatePresetIDs, demand.CandidatePresetIDs)
		}
		service.Events = append(service.Events, domain.ModelServiceEvent{Status: service.Status, Message: "demand observed: " + demand.Reason, At: now})
		if err := store.SaveModelService(ctx, service); err != nil {
			return domain.ModelService{}, err
		}
		return service, nil
	}
	service := domain.ModelService{
		ID:                  id,
		Model:               demand.Model,
		ModelSource:         strings.TrimSpace(demand.ModelSource),
		ModelSourceContract: cloneModelSourceContract(demand.ModelSourceContract),
		Backend:             demand.Backend,
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
	jobs, err := serviceJobs(ctx, store)
	if err != nil {
		return ReconcileReport{}, err
	}
	leases, err := store.ListLeases(ctx)
	if err != nil {
		return ReconcileReport{}, err
	}
	report := ReconcileReport{Services: len(services)}
	for _, service := range services {
		if service.Status != domain.ModelServiceActive {
			report.Skipped++
			continue
		}
		service, err = refreshServiceCandidates(ctx, store, service, now)
		if err != nil {
			return report, err
		}
		heldPresets := runningServicePresets(service.ID, jobs, leases, now)
		inflightPresets := inFlightServicePresets(service.ID, jobs)
		have := len(heldPresets)
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
			if heldPresets[presetID] || inflightPresets[presetID] {
				report.Skipped++
				continue
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
			inflightPresets[presetID] = true
			remaining--
		}
		if remaining > 0 && len(service.CandidatePresetIDs) == 0 {
			task := catalogRefreshTaskForService(service, now)
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
		}
	}
	return report, nil
}

func serviceJobs(ctx context.Context, store Store) ([]domain.Job, error) {
	if lister, ok := store.(serviceJobLister); ok {
		return lister.ListModelServiceJobs(ctx)
	}
	return store.ListJobs(ctx)
}

func refreshServiceCandidates(ctx context.Context, store Store, service domain.ModelService, now time.Time) (domain.ModelService, error) {
	lister, ok := store.(presetLister)
	if !ok {
		return service, nil
	}
	presets, err := lister.ListPresets(ctx)
	if err != nil {
		return domain.ModelService{}, err
	}
	if len(presets) == 0 && len(service.CandidatePresetIDs) > 0 {
		return service, nil
	}
	var candidates []string
	for _, preset := range presets {
		if !presetMatchesService(preset, service) {
			continue
		}
		candidates = append(candidates, preset.ID)
	}
	candidates = uniqueStrings(candidates)
	if len(candidates) == 0 {
		if len(service.CandidatePresetIDs) > 0 {
			service.CandidatePresetIDs = nil
			service.UpdatedAt = now
			service.Events = append(service.Events, domain.ModelServiceEvent{
				Status:  service.Status,
				Message: "candidate presets cleared because none match the catalog/context envelope",
				At:      now,
			})
			if err := store.SaveModelService(ctx, service); err != nil {
				return domain.ModelService{}, err
			}
		}
		return service, nil
	}
	if equalStringSlices(service.CandidatePresetIDs, candidates) {
		return service, nil
	}
	service.CandidatePresetIDs = candidates
	service.UpdatedAt = now
	service.Events = append(service.Events, domain.ModelServiceEvent{
		Status:  service.Status,
		Message: "candidate presets refreshed from catalog",
		At:      now,
	})
	if err := store.SaveModelService(ctx, service); err != nil {
		return domain.ModelService{}, err
	}
	return service, nil
}

func presetMatchesService(preset domain.Preset, service domain.ModelService) bool {
	if preset.ID == "" || !presetMatchesModel(preset, service.Model) {
		return false
	}
	if service.ContextRequest > 0 && modelruntime.EffectiveContextLength(preset) < service.ContextRequest {
		return false
	}
	return true
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
		ID:                  serviceTaskID(service.ID, presetID),
		JobID:               service.ID,
		Kind:                string(task.Kind),
		Status:              domain.ReconciliationTaskAllowed,
		PresetID:            task.PresetID,
		Model:               task.Model,
		ModelSource:         service.ModelSource,
		Backend:             service.Backend,
		Project:             service.Project,
		ContextRequest:      service.ContextRequest,
		ExpectedConcurrency: service.ExpectedConcurrency,
		Reason:              task.Reason,
		AutomaticCandidate:  true,
		RequiresApproval:    false,
		Next:                task.Next,
		Source:              "model_service",
		CreatedAt:           now,
		UpdatedAt:           now,
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

func catalogRefreshTaskForService(service domain.ModelService, now time.Time) domain.ReconciliationTaskRecord {
	sum := sha256.Sum256([]byte(service.ID + "\x00refresh_catalog\x00" + service.Model + "\x00" + service.ModelSource))
	reason := "model service has no candidate presets"
	next := "refresh the configured catalog so model service candidates can be resolved"
	if strings.TrimSpace(service.ModelSource) != "" {
		reason = "model service source has no candidate presets"
		next = "materialize the model source into the configured catalog, then resolve service candidates"
	}
	return domain.ReconciliationTaskRecord{
		ID:                  "service-refresh-" + hex.EncodeToString(sum[:8]),
		JobID:               service.ID,
		Kind:                string(reconcile.TaskRefreshCatalog),
		Status:              domain.ReconciliationTaskAllowed,
		Model:               service.Model,
		ModelSource:         service.ModelSource,
		ModelSourceContract: service.ModelSourceContract,
		Backend:             service.Backend,
		Project:             service.Project,
		ContextRequest:      service.ContextRequest,
		ExpectedConcurrency: service.ExpectedConcurrency,
		Reason:              reason,
		AutomaticCandidate:  true,
		RequiresApproval:    false,
		Next:                next,
		Source:              "model_service",
		CreatedAt:           now,
		UpdatedAt:           now,
		Events: []domain.ReconciliationTaskEvent{{
			Status:  domain.ReconciliationTaskAllowed,
			Message: "created from model service desired state",
			At:      now,
		}},
	}
}

func modelSourceContractPresent(contract *domain.ModelSourceContract) bool {
	return contract != nil && (contract.Format != "" ||
		contract.SizeMB != 0 ||
		contract.KVPerTokenMB != 0 ||
		contract.Quant != "" ||
		contract.EngineProfile != "" ||
		len(contract.Capabilities) != 0)
}

func cloneModelSourceContract(contract *domain.ModelSourceContract) *domain.ModelSourceContract {
	if contract == nil {
		return nil
	}
	out := *contract
	out.Capabilities = append([]domain.Capability(nil), contract.Capabilities...)
	return &out
}

func presetMatchesModel(preset domain.Preset, model string) bool {
	if model == "" {
		return true
	}
	if preset.ID == model || preset.ModelRef == model {
		return true
	}
	for _, alias := range preset.Aliases {
		if alias == model {
			return true
		}
	}
	return false
}

func runningServicePresets(serviceID string, jobs []domain.Job, leases []domain.Lease, now time.Time) map[string]bool {
	activeLeases := map[string]domain.Lease{}
	for _, lease := range leases {
		if lease.ID == "" {
			continue
		}
		if !lease.ExpiresAt.IsZero() && !lease.ExpiresAt.After(now) {
			continue
		}
		activeLeases[lease.ID] = lease
	}
	held := map[string]bool{}
	for _, job := range jobs {
		if job.ParentID != serviceID || job.Status != domain.JobRunning || job.Result == nil || job.Result.Terminal || job.Result.LeaseID == "" {
			continue
		}
		lease, ok := activeLeases[job.Result.LeaseID]
		if !ok {
			continue
		}
		if lease.JobID != "" && lease.JobID != job.ID {
			continue
		}
		if job.PresetID != "" {
			held[job.PresetID] = true
		}
	}
	return held
}

func inFlightServicePresets(serviceID string, jobs []domain.Job) map[string]bool {
	inFlight := map[string]bool{}
	for _, job := range jobs {
		if job.ParentID != serviceID || job.PresetID == "" || jobStatusTerminal(job.Status) {
			continue
		}
		inFlight[job.PresetID] = true
	}
	return inFlight
}

func jobStatusTerminal(status domain.JobStatus) bool {
	return status == domain.JobDone || status == domain.JobFailed || status == domain.JobPreempted
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

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
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
