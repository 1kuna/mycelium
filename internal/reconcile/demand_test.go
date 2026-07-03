package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestRecordDemandDeduplicatesBurstByPresetNodeReason(t *testing.T) {
	ctx := context.Background()
	store := newDemandMemoryStore()
	start := time.Unix(100, 0).UTC()
	decision := domain.PlacementDecision{
		JobID:  "job-a",
		Action: domain.ActionQueued,
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a", ContextLength: 4096},
		Trace: []domain.TraceStep{
			{Step: "estimate", Result: "weights=10MB kv=20MB @ctx4096 x3"},
			{
				Step: "filter",
				Data: map[string]any{"dropped": map[string]string{
					"node-a": "fit",
					"node-b": "load_in_flight",
				}},
			},
			{Step: "admit", Result: "queued: no fit"},
		},
	}

	for i := 0; i < 10000; i++ {
		now := start.Add(time.Duration(i) * time.Millisecond)
		next := decision
		next.JobID = "job-burst"
		if err := RecordDemand(ctx, store, next, "queued", now); err != nil {
			t.Fatalf("RecordDemand %d: %v", i, err)
		}
	}

	tasks, err := store.ListReconciliationTasks(ctx, "")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("tasks = %+v", tasks)
	}
	for _, task := range tasks {
		if task.JobID != "" || task.Source != "demand_signal" || task.PresetID != "preset-a" || task.Model != "model-a" || task.ContextLength != 4096 || task.ExpectedConcurrency != 3 {
			t.Fatalf("task metadata = %+v", task)
		}
		if task.Hits != 10000 || !task.LastSeen.Equal(start.Add(9999*time.Millisecond)) {
			t.Fatalf("pressure = hits %d last %s task=%+v", task.Hits, task.LastSeen, task)
		}
		if len(task.Events) != 1 {
			t.Fatalf("events should not grow with hits: %+v", task.Events)
		}
		switch task.NodeID {
		case "node-a":
			if task.Kind != string(TaskPrewarmReplica) || task.Reason != "fit" {
				t.Fatalf("node-a task = %+v", task)
			}
		case "node-b":
			if task.Kind != string(TaskWaitCapacity) || task.Reason != "load_in_flight" {
				t.Fatalf("node-b task = %+v", task)
			}
		default:
			t.Fatalf("unexpected task = %+v", task)
		}
	}
}

func TestRecordDemandFallbackForOwnerNoFit(t *testing.T) {
	ctx := context.Background()
	store := newDemandMemoryStore()
	now := time.Unix(200, 0).UTC()
	decision := domain.PlacementDecision{
		JobID:  "job-a",
		Action: domain.ActionLoadedNew,
		NodeID: "node-a",
		Preset: domain.Preset{ID: "preset-a", ModelRef: "model-a", ContextLength: 8192},
	}
	if err := RecordDemand(ctx, store, decision, domain.ErrNoFit.Error(), now); err != nil {
		t.Fatalf("RecordDemand: %v", err)
	}
	tasks, err := store.ListReconciliationTasks(ctx, "")
	if err != nil {
		t.Fatalf("ListReconciliationTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Kind != string(TaskPrewarmReplica) || tasks[0].NodeID != "node-a" || tasks[0].Hits != 1 || tasks[0].ContextLength != 8192 || tasks[0].ExpectedConcurrency != 1 {
		t.Fatalf("tasks = %+v", tasks)
	}
}

func TestRecordDemandNoopsForNonCapacityDecision(t *testing.T) {
	store := newDemandMemoryStore()
	err := RecordDemand(context.Background(), store, domain.PlacementDecision{
		JobID:  "job-a",
		Action: domain.ActionWarmInstance,
		Preset: domain.Preset{ID: "preset-a"},
	}, "", time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatalf("RecordDemand: %v", err)
	}
	if len(store.tasks) != 0 {
		t.Fatalf("client/non-capacity path produced demand: %+v", store.tasks)
	}
}

func TestRecordDemandStoreErrors(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	decision := domain.PlacementDecision{Action: domain.ActionQueued, Preset: domain.Preset{ID: "preset-a"}}
	if err := RecordDemand(context.Background(), nil, decision, "queued", now); err == nil {
		t.Fatal("nil store accepted")
	}
	if err := RecordDemand(context.Background(), newDemandMemoryStore(), decision, "queued", time.Time{}); err == nil {
		t.Fatal("zero timestamp accepted")
	}
	listErr := errors.New("list")
	if err := RecordDemand(context.Background(), &demandMemoryStore{listErr: listErr}, decision, "queued", now); !errors.Is(err, listErr) {
		t.Fatalf("list err = %v", err)
	}
	saveErr := errors.New("save")
	if err := RecordDemand(context.Background(), &demandMemoryStore{saveErr: saveErr}, decision, "queued", now); !errors.Is(err, saveErr) {
		t.Fatalf("save err = %v", err)
	}
}

type demandMemoryStore struct {
	tasks   map[string]domain.ReconciliationTaskRecord
	listErr error
	saveErr error
}

func newDemandMemoryStore() *demandMemoryStore {
	return &demandMemoryStore{tasks: map[string]domain.ReconciliationTaskRecord{}}
}

func (s *demandMemoryStore) ListReconciliationTasks(_ context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]domain.ReconciliationTaskRecord, 0, len(s.tasks))
	for _, task := range s.tasks {
		if jobID == "" || task.JobID == jobID {
			out = append(out, task)
		}
	}
	return out, nil
}

func (s *demandMemoryStore) SaveReconciliationTask(_ context.Context, task domain.ReconciliationTaskRecord) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.tasks == nil {
		s.tasks = map[string]domain.ReconciliationTaskRecord{}
	}
	s.tasks[task.ID] = task
	return nil
}
