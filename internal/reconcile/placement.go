package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"mycelium/internal/domain"
)

type TaskKind string

const (
	TaskWaitCapacity          TaskKind = "wait_capacity"
	TaskPrewarmReplica        TaskKind = "prewarm_replica"
	TaskVerifyEngineReadiness TaskKind = "verify_engine_readiness"
	TaskStageModelArtifact    TaskKind = "stage_model_artifact"
	TaskRepairOwner           TaskKind = "repair_owner"
	TaskRecordIncompatibility TaskKind = "record_incompatibility"
	TaskRefreshCatalog        TaskKind = "refresh_catalog"
)

type StandingPolicy struct {
	AllowWaitCapacity       bool `json:"allow_wait_capacity"`
	AllowPrewarmReplica     bool `json:"allow_prewarm_replica"`
	AllowEngineVerification bool `json:"allow_engine_verification"`
	AllowArtifactStaging    bool `json:"allow_artifact_staging"`
	AllowOwnerRepair        bool `json:"allow_owner_repair"`
	AllowCatalogRefresh     bool `json:"allow_catalog_refresh"`
}

func DefaultStandingPolicy() StandingPolicy {
	return StandingPolicy{
		AllowWaitCapacity:       true,
		AllowPrewarmReplica:     true,
		AllowEngineVerification: true,
		AllowArtifactStaging:    true,
		AllowOwnerRepair:        false,
		AllowCatalogRefresh:     true,
	}
}

type Task struct {
	Kind               TaskKind `json:"kind"`
	NodeID             string   `json:"node_id,omitempty"`
	PresetID           string   `json:"preset_id,omitempty"`
	Model              string   `json:"model,omitempty"`
	Reason             string   `json:"reason"`
	AutomaticCandidate bool     `json:"automatic_candidate"`
	RequiresApproval   bool     `json:"requires_approval"`
	Next               string   `json:"next"`
}

type AuditRecord struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

type PlacementPlan struct {
	JobID   string                 `json:"job_id,omitempty"`
	Model   string                 `json:"model,omitempty"`
	Preset  string                 `json:"preset_id,omitempty"`
	Action  domain.PlacementAction `json:"action,omitempty"`
	Summary string                 `json:"summary"`
	Tasks   []Task                 `json:"tasks,omitempty"`
	Audit   []AuditRecord          `json:"audit,omitempty"`
}

func PlanPlacement(decision domain.PlacementDecision, policy StandingPolicy) PlacementPlan {
	plan := PlacementPlan{
		JobID:  decision.JobID,
		Model:  firstNonEmpty(decision.Preset.ModelRef, firstAlias(decision.Preset.Aliases)),
		Preset: decision.Preset.ID,
		Action: decision.Action,
	}
	seen := map[string]struct{}{}
	add := func(task Task) {
		task.PresetID = firstNonEmpty(task.PresetID, decision.Preset.ID)
		task.Model = firstNonEmpty(task.Model, plan.Model)
		applyPolicy(&task, policy)
		key := fmt.Sprintf("%s|%s|%s|%s", task.Kind, task.NodeID, task.PresetID, task.Reason)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		plan.Tasks = append(plan.Tasks, task)
	}

	for _, step := range decision.Trace {
		switch step.Step {
		case "resolve":
			for _, reason := range rejectedPresetReasons(step) {
				add(taskForReason("", reason))
			}
			if strings.Contains(step.Result, "unknown model") || strings.Contains(step.Result, "no preset") {
				add(Task{Kind: TaskRefreshCatalog, Reason: step.Result, Next: "refresh or add the requested model route before admitting workload"})
			}
		case "filter":
			for nodeID, reason := range droppedNodes(step) {
				add(taskForReason(nodeID, reason))
			}
			if strings.Contains(step.Result, "context cap") {
				add(Task{Kind: TaskRecordIncompatibility, Reason: step.Result, Next: "choose a larger envelope or split the workload before retrying"})
			}
		case "admit":
			if strings.Contains(step.Result, "queued") || decision.Action == domain.ActionQueued {
				add(Task{Kind: TaskWaitCapacity, Reason: firstNonEmpty(step.Result, "queued"), Next: "drain the explicit workload queue when owner capacity is available"})
			}
		}
	}
	if decision.Action == domain.ActionQueued && len(plan.Tasks) == 0 {
		add(Task{Kind: TaskWaitCapacity, Reason: "queued", Next: "inspect owner capacity and drain the explicit workload queue"})
	}
	sort.Slice(plan.Tasks, func(i, j int) bool {
		if plan.Tasks[i].RequiresApproval != plan.Tasks[j].RequiresApproval {
			return !plan.Tasks[i].RequiresApproval
		}
		if plan.Tasks[i].Kind != plan.Tasks[j].Kind {
			return plan.Tasks[i].Kind < plan.Tasks[j].Kind
		}
		if plan.Tasks[i].NodeID != plan.Tasks[j].NodeID {
			return plan.Tasks[i].NodeID < plan.Tasks[j].NodeID
		}
		return plan.Tasks[i].Reason < plan.Tasks[j].Reason
	})
	plan.Summary = placementSummary(plan)
	plan.Audit = []AuditRecord{{
		Kind:    "report_only",
		Message: "placement reconciliation tasks are proposed from scheduler evidence and do not mutate live routing or services",
	}}
	return plan
}

func RecordsFromPlan(plan PlacementPlan, now time.Time) []domain.ReconciliationTaskRecord {
	out := make([]domain.ReconciliationTaskRecord, 0, len(plan.Tasks))
	for _, task := range plan.Tasks {
		status := domain.ReconciliationTaskAllowed
		blocker := ""
		if task.RequiresApproval {
			status = domain.ReconciliationTaskBlocked
			blocker = "operator_approval_required"
		}
		out = append(out, domain.ReconciliationTaskRecord{
			ID:                 taskRecordID(plan, task),
			JobID:              plan.JobID,
			Kind:               string(task.Kind),
			Status:             status,
			NodeID:             task.NodeID,
			PresetID:           task.PresetID,
			Model:              task.Model,
			Reason:             task.Reason,
			AutomaticCandidate: task.AutomaticCandidate,
			RequiresApproval:   task.RequiresApproval,
			Next:               task.Next,
			Blocker:            blocker,
			Source:             "placement_decision",
			CreatedAt:          now,
			UpdatedAt:          now,
		})
	}
	return out
}

func taskRecordID(plan PlacementPlan, task Task) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		plan.JobID,
		string(task.Kind),
		task.NodeID,
		task.PresetID,
		task.Model,
		task.Reason,
	}, "\x00")))
	return "recon-" + hex.EncodeToString(sum[:8])
}

func taskForReason(nodeID, reason string) Task {
	switch {
	case reason == "load_in_flight", reason == "dedicated_unit":
		return Task{Kind: TaskWaitCapacity, NodeID: nodeID, Reason: reason, Next: "wait for loading or dedicated work to release, then drain queued work"}
	case reason == "fit":
		return Task{Kind: TaskPrewarmReplica, NodeID: nodeID, Reason: reason, Next: "find a fitting owner, prewarm an allowed replica, or record capacity incompatibility"}
	case reason == "disk.free", reason == "disk.free_after_model":
		return Task{Kind: TaskStageModelArtifact, NodeID: nodeID, Reason: reason, Next: "stage the artifact only if disk headroom policy remains satisfied"}
	case reason == "preset.node_id":
		return Task{Kind: TaskRecordIncompatibility, NodeID: nodeID, Reason: reason, Next: "use a shared model group or add a preset envelope for this owner"}
	case strings.HasPrefix(reason, "label."+domain.LabelPeerBackend), strings.HasPrefix(reason, "label."+domain.LabelPeerBackends):
		return Task{Kind: TaskVerifyEngineReadiness, NodeID: nodeID, Reason: reason, Next: "verify or apply an engine profile for the requested backend"}
	case strings.HasPrefix(reason, "label."):
		return Task{Kind: TaskRecordIncompatibility, NodeID: nodeID, Reason: reason, Next: "record the capability mismatch or update node labels through the normal config path"}
	case reason == string(domain.NodeUnreachable):
		return Task{Kind: TaskRepairOwner, NodeID: nodeID, Reason: reason, Next: "restore peer reachability before scheduling work here"}
	case reason == string(domain.NodeMaintenance), reason == string(domain.NodeDraining):
		return Task{Kind: TaskWaitCapacity, NodeID: nodeID, Reason: reason, Next: "wait for the owner to return to ready state"}
	default:
		if strings.Contains(reason, "context cap") {
			return Task{Kind: TaskRecordIncompatibility, NodeID: nodeID, Reason: reason, Next: "choose a larger envelope or split the workload before retrying"}
		}
		return Task{Kind: TaskRecordIncompatibility, NodeID: nodeID, Reason: reason, Next: "record the placement incompatibility before retrying"}
	}
}

func applyPolicy(task *Task, policy StandingPolicy) {
	switch task.Kind {
	case TaskWaitCapacity:
		task.AutomaticCandidate = policy.AllowWaitCapacity
	case TaskPrewarmReplica:
		task.AutomaticCandidate = policy.AllowPrewarmReplica
	case TaskVerifyEngineReadiness:
		task.AutomaticCandidate = policy.AllowEngineVerification
	case TaskStageModelArtifact:
		task.AutomaticCandidate = policy.AllowArtifactStaging
	case TaskRepairOwner:
		task.AutomaticCandidate = policy.AllowOwnerRepair
	case TaskRefreshCatalog:
		task.AutomaticCandidate = policy.AllowCatalogRefresh
	case TaskRecordIncompatibility:
		task.AutomaticCandidate = true
	}
	task.RequiresApproval = !task.AutomaticCandidate
}

func droppedNodes(step domain.TraceStep) map[string]string {
	raw, ok := step.Data["dropped"]
	if !ok || raw == nil {
		return nil
	}
	out := map[string]string{}
	switch dropped := raw.(type) {
	case map[string]string:
		for node, reason := range dropped {
			out[node] = reason
		}
	case map[string]any:
		for node, reason := range dropped {
			if text, ok := reason.(string); ok {
				out[node] = text
			}
		}
	}
	return out
}

func rejectedPresetReasons(step domain.TraceStep) []string {
	raw, ok := step.Data["rejected_presets"]
	if !ok || raw == nil {
		return nil
	}
	var out []string
	switch rejected := raw.(type) {
	case []string:
		out = append(out, rejected...)
	case []any:
		for _, item := range rejected {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
	}
	return out
}

func placementSummary(plan PlacementPlan) string {
	if len(plan.Tasks) == 0 {
		return "no reconciliation tasks"
	}
	automatic := 0
	for _, task := range plan.Tasks {
		if task.AutomaticCandidate {
			automatic++
		}
	}
	if automatic == len(plan.Tasks) {
		return fmt.Sprintf("%d automatic reconciliation candidate(s)", len(plan.Tasks))
	}
	return fmt.Sprintf("%d reconciliation task(s), %d require approval", len(plan.Tasks), len(plan.Tasks)-automatic)
}

func firstAlias(aliases []string) string {
	if len(aliases) == 0 {
		return ""
	}
	return aliases[0]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
