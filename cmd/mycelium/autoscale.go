package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/modelruntime"
	"mycelium/internal/pool"
	"mycelium/internal/ports"
	"mycelium/internal/scheduler"
)

const defaultAutoScaleInterval = 5 * time.Second

type autoScaleStore interface {
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
	SaveInstance(ctx context.Context, inst domain.ModelInstance) error
}

type autoScaleRuntime interface {
	SubmitWithPayload(ctx context.Context, job domain.Job, payload []byte, hooks ...scheduler.SubmitHooks) (scheduler.Result, error)
	FinishJob(ctx context.Context, job domain.Job, lease domain.Lease, cause error) error
}

type autoScaler struct {
	Store     autoScaleStore
	Runtime   autoScaleRuntime
	Fleet     scheduler.FleetSource
	Estimator ports.ResourceEstimator
	Allocator pool.Allocator
	Clock     ports.Clock
	Policy    domain.AutonomyPolicy
	Presets   map[string]domain.Preset
	Project   string

	failures map[autoScaleKey]autoScaleFailure
}

type autoScaleKey struct {
	PresetID string
	NodeID   string
}

type autoScaleFailure struct {
	Count       int
	LastFailure time.Time
	LastSeen    time.Time
}

type autoScaleDemandGroup struct {
	Model       string
	ContextLen  int
	Concurrency int
	Tasks       []domain.ReconciliationTaskRecord
}

type autoScaleReport struct {
	DemandTasks int
	Planned     int
	Scaled      int
	Refused     int
	Failed      int
}

func startAutoScaler(ctx context.Context, runtime *scheduler.Service, store autoScaleStore, fleet scheduler.FleetSource, estimator ports.ResourceEstimator, alloc pool.Allocator, policy domain.AutonomyPolicy, clk ports.Clock, interval time.Duration, presets map[string]domain.Preset, project string) {
	if !policy.AutoScale || runtime == nil || store == nil || fleet == nil || estimator == nil || alloc == nil || clk == nil {
		return
	}
	if interval <= 0 {
		interval = defaultAutoScaleInterval
	}
	scaler := &autoScaler{
		Store:     store,
		Runtime:   runtime,
		Fleet:     fleet,
		Estimator: estimator,
		Allocator: alloc,
		Clock:     clk,
		Policy:    policy,
		Presets:   presets,
		Project:   firstNonEmpty(project, "autoscale"),
		failures:  map[autoScaleKey]autoScaleFailure{},
	}
	go func() {
		for {
			timer := clk.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			if _, err := scaler.Tick(ctx); err != nil {
				log.Printf("mycelium autoscale tick failed: %v", err)
			}
		}
	}()
}

func (s *autoScaler) Tick(ctx context.Context) (autoScaleReport, error) {
	if s == nil || !s.Policy.AutoScale {
		return autoScaleReport{}, nil
	}
	if s.Store == nil || s.Runtime == nil || s.Fleet == nil || s.Estimator == nil || s.Allocator == nil || s.Clock == nil {
		return autoScaleReport{}, fmt.Errorf("autoscaler is not fully configured")
	}
	tasks, err := s.Store.ListReconciliationTasks(ctx, "")
	if err != nil {
		return autoScaleReport{}, err
	}
	open := openAutoScaleDemand(tasks)
	report := autoScaleReport{DemandTasks: len(open)}
	if len(open) == 0 {
		return report, nil
	}
	groups := s.groupDemand(open)
	fleet, err := s.Fleet.Snapshot(ctx)
	if err != nil {
		return report, err
	}
	for _, group := range groups {
		groupReport, err := s.scaleGroup(ctx, group, fleet)
		report.Planned += groupReport.Planned
		report.Scaled += groupReport.Scaled
		report.Refused += groupReport.Refused
		report.Failed += groupReport.Failed
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

func openAutoScaleDemand(tasks []domain.ReconciliationTaskRecord) []domain.ReconciliationTaskRecord {
	var open []domain.ReconciliationTaskRecord
	for _, task := range tasks {
		if task.Source != "demand_signal" || task.Status != domain.ReconciliationTaskAllowed {
			continue
		}
		if task.PresetID == "" && task.Model == "" {
			continue
		}
		open = append(open, task)
	}
	sort.Slice(open, func(i, j int) bool {
		if open[i].Model != open[j].Model {
			return open[i].Model < open[j].Model
		}
		if open[i].PresetID != open[j].PresetID {
			return open[i].PresetID < open[j].PresetID
		}
		if open[i].NodeID != open[j].NodeID {
			return open[i].NodeID < open[j].NodeID
		}
		return open[i].ID < open[j].ID
	})
	return open
}

func (s *autoScaler) groupDemand(tasks []domain.ReconciliationTaskRecord) []autoScaleDemandGroup {
	byModel := map[string]*autoScaleDemandGroup{}
	for _, task := range tasks {
		model := s.taskModel(task)
		group := byModel[model]
		if group == nil {
			group = &autoScaleDemandGroup{Model: model}
			byModel[model] = group
		}
		group.Tasks = append(group.Tasks, task)
		if task.ContextLength > group.ContextLen {
			group.ContextLen = task.ContextLength
		}
		if task.ExpectedConcurrency > group.Concurrency {
			group.Concurrency = task.ExpectedConcurrency
		}
	}
	groups := make([]autoScaleDemandGroup, 0, len(byModel))
	for _, group := range byModel {
		if group.Concurrency <= 0 {
			group.Concurrency = 1
		}
		groups = append(groups, *group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Model < groups[j].Model })
	return groups
}

func (s *autoScaler) taskModel(task domain.ReconciliationTaskRecord) string {
	if strings.TrimSpace(task.Model) != "" {
		return task.Model
	}
	if preset, ok := s.Presets[task.PresetID]; ok {
		return firstNonEmpty(preset.ModelRef, firstAlias(preset.Aliases), preset.ID)
	}
	return task.PresetID
}

func (s *autoScaler) scaleGroup(ctx context.Context, group autoScaleDemandGroup, fleet domain.FleetSnapshot) (autoScaleReport, error) {
	report := autoScaleReport{}
	presets := autoscalePresetsForModel(s.Presets, group.Model)
	if len(presets) == 0 {
		err := fmt.Sprintf("no presets match autoscale demand model %q", group.Model)
		if saveErr := s.closeTasks(ctx, group.Tasks, domain.ReconciliationTaskFailed, err, "autoscale refused demand with no registered preset", "autoscale_no_preset"); saveErr != nil {
			return report, saveErr
		}
		report.Refused += len(group.Tasks)
		return report, nil
	}
	if group.ContextLen <= 0 {
		group.ContextLen = modelruntime.EffectiveContextLength(presets[0])
	}
	existing := existingAutoscaleModelReplicas(fleet.Instances, presets)
	maxTotal := 0
	if s.Policy.MaxReplicasPerModel > 0 {
		maxTotal = s.Policy.MaxReplicasPerModel - existing
		if maxTotal <= 0 {
			msg := fmt.Sprintf("autoscale cap reached for model %q: existing=%d max=%d", group.Model, existing, s.Policy.MaxReplicasPerModel)
			if err := s.closeTasks(ctx, group.Tasks, domain.ReconciliationTaskFailed, msg, "autoscale refused demand because the model replica cap is reached", "autoscale_cap_reached"); err != nil {
				return report, err
			}
			report.Refused += len(group.Tasks)
			return report, nil
		}
	}
	plan, err := pool.BuildPlan(ctx, s.Estimator, s.Allocator, group.Model, presets, fleet, pool.Options{
		ContextLength: group.ContextLen,
		Concurrency:   group.Concurrency,
		MaxPerNode:    s.Policy.MaxReplicasPerModelPerNode,
		MaxTotal:      maxTotal,
	})
	if err != nil {
		if saveErr := s.recordFailureForTasks(ctx, group.Tasks, err); saveErr != nil {
			return report, saveErr
		}
		report.Failed += len(group.Tasks)
		return report, nil
	}
	scaledNodes := map[string]autoScaleLaunchResult{}
	refusals := map[string]string{}
	for _, nodePlan := range plan.Nodes {
		if nodePlan.Refusal != "" {
			refusals[nodePlan.NodeID] = firstNonEmpty(nodePlan.SetupGap, nodePlan.Refusal)
			continue
		}
		for i := 0; i < nodePlan.Additional; i++ {
			report.Planned++
			key := autoScaleKey{PresetID: nodePlan.PresetID, NodeID: nodePlan.NodeID}
			if s.coolingDown(key, group.Tasks) {
				continue
			}
			result, err := s.launchReplica(ctx, group, nodePlan, i)
			if err != nil {
				taskErr := s.recordFailure(ctx, key, group.Tasks, err)
				report.Failed++
				if taskErr != nil {
					return report, taskErr
				}
				continue
			}
			delete(s.failures, key)
			scaledNodes[nodePlan.NodeID] = result
			report.Scaled++
		}
	}
	if err := s.closeGroupTasks(ctx, group.Tasks, scaledNodes, refusals, plan.TotalAdditional == 0); err != nil {
		return report, err
	}
	report.Refused += countClosedRefusals(group.Tasks, scaledNodes, refusals, plan.TotalAdditional == 0)
	return report, nil
}

type autoScaleLaunchResult struct {
	Job      domain.Job
	Instance domain.ModelInstance
	Lease    domain.Lease
}

func (s *autoScaler) launchReplica(ctx context.Context, group autoScaleDemandGroup, nodePlan pool.NodePlan, index int) (autoScaleLaunchResult, error) {
	job := domain.Job{
		ID:                  autoscaleJobID(group.Model, nodePlan.NodeID, nodePlan.PresetID, index, s.Clock.Now()),
		TaskType:            string(domain.CapabilityChat),
		Model:               group.Model,
		PresetID:            nodePlan.PresetID,
		NodeSelector:        map[string]string{domain.LabelNodeID: nodePlan.NodeID},
		Project:             s.Project,
		Priority:            domain.PriorityBackground,
		SpeedPref:           domain.SpeedThroughput,
		ForceNewInstance:    true,
		ContextRequest:      nodePlan.ContextLength,
		ExpectedConcurrency: group.Concurrency,
		Preemption:          domain.PreemptSoft,
		Status:              domain.JobQueued,
	}
	result, err := s.Runtime.SubmitWithPayload(ctx, job, nil, scheduler.SubmitHooks{InstanceOrigin: domain.InstanceOriginAutoscale})
	if err != nil {
		return autoScaleLaunchResult{}, err
	}
	if result.Instance.ID == "" {
		return autoScaleLaunchResult{}, fmt.Errorf("autoscale prewarm job %q returned no instance", job.ID)
	}
	if result.Lease.ID == "" {
		return autoScaleLaunchResult{}, fmt.Errorf("autoscale prewarm job %q returned no lease to release", job.ID)
	}
	now := s.Clock.Now()
	result.Instance.Origin = domain.InstanceOriginAutoscale
	if result.Instance.CreatedAt.IsZero() {
		result.Instance.CreatedAt = now
	}
	if result.Instance.LastActivityAt.IsZero() {
		result.Instance.LastActivityAt = result.Instance.CreatedAt
	}
	if err := s.Store.SaveInstance(ctx, result.Instance); err != nil {
		return autoScaleLaunchResult{}, err
	}
	if err := s.Runtime.FinishJob(ctx, job, result.Lease, nil); err != nil {
		return autoScaleLaunchResult{}, err
	}
	return autoScaleLaunchResult{Job: job, Instance: result.Instance, Lease: result.Lease}, nil
}

func (s *autoScaler) coolingDown(key autoScaleKey, tasks []domain.ReconciliationTaskRecord) bool {
	failure, ok := s.failures[key]
	if !ok || s.Policy.ScaleCooldownMS <= 0 {
		return false
	}
	now := s.Clock.Now()
	deadline := failure.LastFailure.Add(time.Duration(s.Policy.ScaleCooldownMS) * time.Millisecond)
	if !now.Before(deadline) {
		for _, task := range tasks {
			if task.PresetID == key.PresetID && (task.NodeID == "" || task.NodeID == key.NodeID) && task.LastSeen.After(failure.LastFailure) {
				delete(s.failures, key)
				return false
			}
		}
		return false
	}
	for _, task := range tasks {
		if task.PresetID == key.PresetID && (task.NodeID == "" || task.NodeID == key.NodeID) && task.LastSeen.After(failure.LastFailure) {
			return true
		}
	}
	return true
}

func (s *autoScaler) recordFailure(ctx context.Context, key autoScaleKey, tasks []domain.ReconciliationTaskRecord, cause error) error {
	now := s.Clock.Now()
	failure := s.failures[key]
	failure.Count++
	failure.LastFailure = now
	for _, task := range tasks {
		if task.PresetID == key.PresetID && (task.NodeID == "" || task.NodeID == key.NodeID) && task.LastSeen.After(failure.LastSeen) {
			failure.LastSeen = task.LastSeen
		}
	}
	s.failures[key] = failure
	status := domain.ReconciliationTaskAllowed
	blocker := ""
	message := fmt.Sprintf("autoscale attempt %d/3 failed on node %s: %v", failure.Count, key.NodeID, cause)
	next := "retry after autoscale cooldown"
	if failure.Count >= 3 {
		status = domain.ReconciliationTaskFailed
		blocker = "autoscale_circuit_breaker"
		message = fmt.Sprintf("autoscale circuit breaker opened after 3 failures on node %s: %v", key.NodeID, cause)
		next = "wait for new demand after cooldown or clear the failed task after fixing the owner"
	}
	for _, task := range tasks {
		if task.PresetID != key.PresetID || (task.NodeID != "" && task.NodeID != key.NodeID) {
			continue
		}
		updated := transitionAutoScaleTask(task, status, message, message, now)
		updated.Blocker = blocker
		updated.Next = next
		if err := s.Store.SaveReconciliationTask(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

func (s *autoScaler) recordFailureForTasks(ctx context.Context, tasks []domain.ReconciliationTaskRecord, cause error) error {
	for _, task := range tasks {
		key := autoScaleKey{PresetID: task.PresetID, NodeID: task.NodeID}
		if err := s.recordFailure(ctx, key, []domain.ReconciliationTaskRecord{task}, cause); err != nil {
			return err
		}
	}
	return nil
}

func (s *autoScaler) closeGroupTasks(ctx context.Context, tasks []domain.ReconciliationTaskRecord, scaled map[string]autoScaleLaunchResult, refusals map[string]string, noAdditional bool) error {
	now := s.Clock.Now()
	for _, task := range tasks {
		if _, ok := scaled[task.NodeID]; ok || (task.NodeID == "" && len(scaled) > 0) {
			msg := "autoscale warmed replica and released its lease"
			if task.NodeID != "" {
				if result, ok := scaled[task.NodeID]; ok {
					msg = fmt.Sprintf("autoscale warmed instance %s on %s and released lease %s", result.Instance.ID, task.NodeID, result.Lease.ID)
				}
			}
			if err := s.Store.SaveReconciliationTask(ctx, transitionAutoScaleTask(task, domain.ReconciliationTaskDone, "", msg, now)); err != nil {
				return err
			}
			continue
		}
		if reason, ok := refusals[task.NodeID]; ok || (task.NodeID == "" && len(refusals) > 0) {
			if task.NodeID == "" {
				reason = joinedRefusals(refusals)
			}
			updated := transitionAutoScaleTask(task, domain.ReconciliationTaskFailed, reason, "autoscale refused demand: "+reason, now)
			updated.Blocker = "autoscale_setup_gap"
			if err := s.Store.SaveReconciliationTask(ctx, updated); err != nil {
				return err
			}
			continue
		}
		if noAdditional {
			msg := "autoscale planner found no safe additional replicas"
			updated := transitionAutoScaleTask(task, domain.ReconciliationTaskFailed, msg, msg, now)
			updated.Blocker = "autoscale_no_capacity"
			if err := s.Store.SaveReconciliationTask(ctx, updated); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *autoScaler) closeTasks(ctx context.Context, tasks []domain.ReconciliationTaskRecord, status domain.ReconciliationTaskStatus, errText, eventMessage, blocker string) error {
	now := s.Clock.Now()
	for _, task := range tasks {
		updated := transitionAutoScaleTask(task, status, errText, eventMessage, now)
		updated.Blocker = blocker
		if err := s.Store.SaveReconciliationTask(ctx, updated); err != nil {
			return err
		}
	}
	return nil
}

func transitionAutoScaleTask(task domain.ReconciliationTaskRecord, status domain.ReconciliationTaskStatus, errText, message string, now time.Time) domain.ReconciliationTaskRecord {
	task.Status = status
	task.UpdatedAt = now
	task.Error = errText
	task.Events = append(task.Events, domain.ReconciliationTaskEvent{Status: status, Message: message, At: now})
	return task
}

func countClosedRefusals(tasks []domain.ReconciliationTaskRecord, scaled map[string]autoScaleLaunchResult, refusals map[string]string, noAdditional bool) int {
	count := 0
	for _, task := range tasks {
		if _, ok := scaled[task.NodeID]; ok || (task.NodeID == "" && len(scaled) > 0) {
			continue
		}
		if _, ok := refusals[task.NodeID]; ok || (task.NodeID == "" && len(refusals) > 0) || noAdditional {
			count++
		}
	}
	return count
}

func autoscalePresetsForModel(presets map[string]domain.Preset, model string) []domain.Preset {
	seen := map[string]struct{}{}
	var out []domain.Preset
	for _, preset := range presets {
		if _, ok := seen[preset.ID]; ok {
			continue
		}
		if autoscalePresetMatches(preset, model) {
			seen[preset.ID] = struct{}{}
			out = append(out, preset)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func autoscalePresetMatches(preset domain.Preset, model string) bool {
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

func existingAutoscaleModelReplicas(instances []domain.ModelInstance, presets []domain.Preset) int {
	ids := map[string]struct{}{}
	for _, preset := range presets {
		ids[preset.ID] = struct{}{}
	}
	count := 0
	for _, inst := range instances {
		if _, ok := ids[inst.PresetID]; ok {
			count++
		}
	}
	return count
}

func autoscaleJobID(model, nodeID, presetID string, index int, now time.Time) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		model,
		nodeID,
		presetID,
		fmt.Sprintf("%d", index),
		fmt.Sprintf("%d", now.UnixNano()),
	}, "\x00")))
	return "autoscale-" + hex.EncodeToString(sum[:8])
}

func joinedRefusals(refusals map[string]string) string {
	keys := make([]string, 0, len(refusals))
	for nodeID := range refusals {
		keys = append(keys, nodeID)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, nodeID := range keys {
		parts = append(parts, nodeID+": "+refusals[nodeID])
	}
	return strings.Join(parts, "; ")
}

func firstAlias(aliases []string) string {
	if len(aliases) == 0 {
		return ""
	}
	return aliases[0]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
