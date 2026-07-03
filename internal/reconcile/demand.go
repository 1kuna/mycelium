package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/domain"
)

type DemandStore interface {
	ListReconciliationTasks(ctx context.Context, jobID string) ([]domain.ReconciliationTaskRecord, error)
	SaveReconciliationTask(ctx context.Context, task domain.ReconciliationTaskRecord) error
}

func RecordDemand(ctx context.Context, store DemandStore, decision domain.PlacementDecision, reason string, now time.Time) error {
	if store == nil {
		return fmt.Errorf("demand store is required")
	}
	if now.IsZero() {
		return fmt.Errorf("demand timestamp is required")
	}
	records := demandRecords(decision, reason, now)
	if len(records) == 0 {
		return nil
	}
	existing, err := store.ListReconciliationTasks(ctx, "")
	if err != nil {
		return err
	}
	byID := make(map[string]domain.ReconciliationTaskRecord, len(existing))
	for _, task := range existing {
		byID[task.ID] = task
	}
	for _, record := range records {
		if current, ok := byID[record.ID]; ok {
			record = mergeDemandRecord(current, record, now)
		}
		if err := store.SaveReconciliationTask(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func demandRecords(decision domain.PlacementDecision, reason string, now time.Time) []domain.ReconciliationTaskRecord {
	if decision.Action != domain.ActionQueued && reason == "" {
		return nil
	}
	plan := PlanPlacement(decision, DefaultStandingPolicy())
	if len(plan.Tasks) == 0 && decision.Preset.ID != "" && decision.NodeID != "" {
		plan.Tasks = append(plan.Tasks, Task{
			Kind:     TaskPrewarmReplica,
			NodeID:   decision.NodeID,
			PresetID: decision.Preset.ID,
			Model:    firstNonEmpty(decision.Preset.ModelRef, firstAlias(decision.Preset.Aliases)),
			Reason:   firstNonEmpty(reason, "capacity"),
			Next:     "prewarm an allowed replica or leave demand queued until owner capacity is available",
		})
	}
	records := RecordsFromPlan(plan, now)
	records = demandScope(records, reason)
	contextLength, concurrency := demandEnvelope(decision)
	for i := range records {
		records[i].ID = demandTaskRecordID(records[i])
		records[i].JobID = ""
		records[i].Source = "demand_signal"
		records[i].ContextLength = contextLength
		records[i].ExpectedConcurrency = concurrency
		records[i].Hits = 1
		records[i].LastSeen = now
		if len(records[i].Events) > 0 {
			records[i].Events[0].Message = "created from live demand signal"
		}
	}
	return records
}

func demandEnvelope(decision domain.PlacementDecision) (int, int) {
	contextLength := decision.Preset.ContextLength
	concurrency := 0
	for _, step := range decision.Trace {
		if step.Step != "estimate" {
			continue
		}
		if parsedContext, parsedConcurrency, ok := parseEstimateEnvelope(step.Result); ok {
			if contextLength == 0 {
				contextLength = parsedContext
			}
			concurrency = parsedConcurrency
			break
		}
	}
	if concurrency == 0 {
		concurrency = 1
	}
	return contextLength, concurrency
}

func parseEstimateEnvelope(result string) (int, int, bool) {
	ctxIndex := strings.Index(result, "@ctx")
	if ctxIndex < 0 {
		return 0, 0, false
	}
	rest := result[ctxIndex+len("@ctx"):]
	fields := strings.Fields(rest)
	if len(fields) < 2 || !strings.HasPrefix(fields[1], "x") {
		return 0, 0, false
	}
	contextLength, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, 0, false
	}
	concurrency, err := strconv.Atoi(strings.TrimPrefix(fields[1], "x"))
	if err != nil {
		return 0, 0, false
	}
	return contextLength, concurrency, true
}

func demandScope(records []domain.ReconciliationTaskRecord, reason string) []domain.ReconciliationTaskRecord {
	hasNodeScoped := false
	for _, record := range records {
		if record.NodeID != "" {
			hasNodeScoped = true
			break
		}
	}
	out := records[:0]
	for _, record := range records {
		if hasNodeScoped && record.NodeID == "" {
			continue
		}
		if record.Reason == "" || record.Reason == "queued" || strings.HasPrefix(record.Reason, "queued:") {
			record.Reason = firstNonEmpty(reason, record.Reason, "capacity")
		}
		out = append(out, record)
	}
	return out
}

func mergeDemandRecord(current, next domain.ReconciliationTaskRecord, now time.Time) domain.ReconciliationTaskRecord {
	next.CreatedAt = current.CreatedAt
	next.Events = current.Events
	if current.Hits > 0 {
		next.Hits = current.Hits + 1
	}
	if current.Status != domain.ReconciliationTaskDone && current.Status != domain.ReconciliationTaskFailed {
		next.Status = current.Status
		next.Blocker = current.Blocker
		next.Error = current.Error
	}
	next.LastSeen = now
	next.UpdatedAt = now
	return next
}

func demandTaskRecordID(task domain.ReconciliationTaskRecord) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		task.Kind,
		task.PresetID,
		task.NodeID,
		task.Reason,
	}, "\x00")))
	return "demand-" + hex.EncodeToString(sum[:8])
}
