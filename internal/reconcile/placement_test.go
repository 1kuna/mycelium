package reconcile

import (
	"testing"

	"mycelium/internal/domain"
)

func TestPlanPlacementTurnsDropReasonsIntoTasks(t *testing.T) {
	decision := domain.PlacementDecision{
		JobID:  "job-a",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a"},
		Trace: []domain.TraceStep{
			{
				Step: "filter",
				Data: map[string]any{"dropped": map[string]string{
					"node-loading": "load_in_flight",
					"node-engine":  "label." + domain.LabelPeerBackends,
					"node-disk":    "disk.free_after_model",
					"node-down":    string(domain.NodeUnreachable),
				}},
			},
			{Step: "admit", Result: "queued: no fit"},
		},
	}

	plan := PlanPlacement(decision, DefaultStandingPolicy())
	if plan.Summary != "5 reconciliation task(s), 1 require approval" {
		t.Fatalf("summary = %q", plan.Summary)
	}
	if got := findTask(plan.Tasks, TaskWaitCapacity, "node-loading"); got == nil || !got.AutomaticCandidate {
		t.Fatalf("wait task = %+v", got)
	}
	if got := findTask(plan.Tasks, TaskVerifyEngineReadiness, "node-engine"); got == nil || got.RequiresApproval {
		t.Fatalf("engine task = %+v", got)
	}
	if got := findTask(plan.Tasks, TaskStageModelArtifact, "node-disk"); got == nil || got.RequiresApproval {
		t.Fatalf("stage task = %+v", got)
	}
	if got := findTask(plan.Tasks, TaskRepairOwner, "node-down"); got == nil || !got.RequiresApproval {
		t.Fatalf("repair task = %+v", got)
	}
	if len(plan.Audit) != 1 || plan.Audit[0].Kind != "report_only" {
		t.Fatalf("audit = %+v", plan.Audit)
	}
}

func TestPlanPlacementClassifiesContextAndCatalogGaps(t *testing.T) {
	decision := domain.PlacementDecision{
		JobID:  "job-context",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "small", Aliases: []string{"shared-model"}},
		Trace: []domain.TraceStep{
			{
				Step:   "resolve",
				Result: "model group",
				Data: map[string]any{"rejected_presets": []any{
					`small:preset "small" context cap 16384 is below requested context 49152`,
				}},
			},
		},
	}

	plan := PlanPlacement(decision, DefaultStandingPolicy())
	if plan.Model != "shared-model" {
		t.Fatalf("model = %q", plan.Model)
	}
	if len(plan.Tasks) != 1 {
		t.Fatalf("tasks = %+v", plan.Tasks)
	}
	if task := plan.Tasks[0]; task.Kind != TaskRecordIncompatibility || task.Next != "choose a larger envelope or split the workload before retrying" {
		t.Fatalf("task = %+v", task)
	}

	missing := PlanPlacement(domain.PlacementDecision{
		JobID: "job-missing",
		Trace: []domain.TraceStep{{
			Step:   "resolve",
			Result: `unknown model "missing"`,
		}},
	}, DefaultStandingPolicy())
	if len(missing.Tasks) != 1 || missing.Tasks[0].Kind != TaskRefreshCatalog {
		t.Fatalf("missing route tasks = %+v", missing.Tasks)
	}
}

func TestPlanPlacementCoversDropReasonTaxonomy(t *testing.T) {
	decision := domain.PlacementDecision{
		JobID:  "job-taxonomy",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-taxonomy", ModelRef: "model-taxonomy"},
		Trace: []domain.TraceStep{{
			Step: "filter",
			Data: map[string]any{"dropped": map[string]any{
				"dedicated":   "dedicated_unit",
				"capacity":    "fit",
				"disk":        "disk.free",
				"pinned":      "preset.node_id",
				"label":       "label.gpu.vendor",
				"maintenance": string(domain.NodeMaintenance),
				"unknown":     "custom_reason",
			}},
		}},
	}
	policy := DefaultStandingPolicy()
	policy.AllowPrewarmReplica = false
	policy.AllowArtifactStaging = false

	plan := PlanPlacement(decision, policy)
	want := map[string]TaskKind{
		"dedicated":   TaskWaitCapacity,
		"capacity":    TaskPrewarmReplica,
		"disk":        TaskStageModelArtifact,
		"pinned":      TaskRecordIncompatibility,
		"label":       TaskRecordIncompatibility,
		"maintenance": TaskWaitCapacity,
		"unknown":     TaskRecordIncompatibility,
	}
	for node, kind := range want {
		task := findTask(plan.Tasks, kind, node)
		if task == nil {
			t.Fatalf("missing %s/%s in %+v", node, kind, plan.Tasks)
		}
		if (kind == TaskPrewarmReplica || kind == TaskStageModelArtifact) && !task.RequiresApproval {
			t.Fatalf("policy did not mark %s as approval-required: %+v", node, task)
		}
	}
}

func findTask(tasks []Task, kind TaskKind, nodeID string) *Task {
	for _, task := range tasks {
		if task.Kind == kind && task.NodeID == nodeID {
			copy := task
			return &copy
		}
	}
	return nil
}
