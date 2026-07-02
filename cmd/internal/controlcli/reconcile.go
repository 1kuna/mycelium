package controlcli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/reconcile"
	storesqlite "mycelium/internal/store/sqlite"
)

func runReconcile(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce reconcile <placement|status>")
	}
	switch args[0] {
	case "placement":
		return runReconcilePlacement(args[1:])
	case "status":
		return runReconcileStatus(args[1:])
	default:
		return fmt.Errorf("usage: myce reconcile <placement|status>")
	}
}

func runReconcilePlacement(args []string) error {
	fs := flag.NewFlagSet("reconcile placement", flag.ContinueOnError)
	decisionFile := fs.String("decision-file", "", "placement decision, job, or wrapper JSON")
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	save := fs.Bool("save", false, "persist reconciliation task records")
	jsonOut := fs.Bool("json", false, "print JSON")
	allowOwnerRepair := fs.Bool("allow-owner-repair", false, "treat owner repair as allowed by standing policy")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || strings.TrimSpace(*decisionFile) == "" {
		return fmt.Errorf("usage: myce reconcile placement --decision-file <path> [--json]")
	}
	decision, err := loadPlacementDecisionFile(*decisionFile)
	if err != nil {
		return err
	}
	policy := reconcile.DefaultStandingPolicy()
	policy.AllowOwnerRepair = *allowOwnerRepair
	plan := reconcile.PlanPlacement(decision, policy)
	if *save {
		if err := saveReconciliationPlan(context.Background(), *dbPath, plan); err != nil {
			return err
		}
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, plan)
	}
	printReconcilePlacement(os.Stdout, plan)
	return nil
}

func runReconcileStatus(args []string) error {
	fs := flag.NewFlagSet("reconcile status", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	jobID := fs.String("job", "", "filter by job id")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: myce reconcile status [--db path] [--job id] [--json]")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	tasks, err := store.ListReconciliationTasks(context.Background(), *jobID)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, tasks)
	}
	printReconcileStatus(os.Stdout, tasks)
	return nil
}

func saveReconciliationPlan(ctx context.Context, dbPath string, plan reconcile.PlacementPlan) error {
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	now := time.Now().UTC()
	for _, task := range reconcile.RecordsFromPlan(plan, now) {
		if err := store.SaveReconciliationTask(ctx, task); err != nil {
			return err
		}
	}
	return nil
}

func loadPlacementDecisionFile(path string) (domain.PlacementDecision, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return domain.PlacementDecision{}, fmt.Errorf("read placement decision: %w", err)
	}
	decision, err := decodePlacementDecision(data)
	if err != nil {
		return domain.PlacementDecision{}, fmt.Errorf("decode placement decision: %w", err)
	}
	return decision, nil
}

func decodePlacementDecision(data []byte) (domain.PlacementDecision, error) {
	var decision domain.PlacementDecision
	if err := json.Unmarshal(data, &decision); err == nil && (decision.JobID != "" || decision.Action != "" || len(decision.Trace) > 0) {
		return decision, nil
	}
	var job domain.Job
	if err := json.Unmarshal(data, &job); err == nil && job.Result != nil {
		return job.Result.Decision, nil
	}
	var wrapper struct {
		Decision domain.PlacementDecision `json:"decision"`
		Result   *domain.JobResult        `json:"result,omitempty"`
		Job      *domain.Job              `json:"job,omitempty"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return domain.PlacementDecision{}, err
	}
	switch {
	case wrapper.Decision.JobID != "" || wrapper.Decision.Action != "" || len(wrapper.Decision.Trace) > 0:
		return wrapper.Decision, nil
	case wrapper.Result != nil:
		return wrapper.Result.Decision, nil
	case wrapper.Job != nil && wrapper.Job.Result != nil:
		return wrapper.Job.Result.Decision, nil
	default:
		return domain.PlacementDecision{}, fmt.Errorf("JSON does not contain a placement decision")
	}
}

func printReconcilePlacement(w io.Writer, plan reconcile.PlacementPlan) {
	fmt.Fprintf(w, "reconcile\tjob=%s\tmodel=%s\tpreset=%s\taction=%s\tsummary=%s\n", emptyDash(plan.JobID), emptyDash(plan.Model), emptyDash(plan.Preset), emptyDash(string(plan.Action)), plan.Summary)
	for _, task := range plan.Tasks {
		fmt.Fprintf(w, "task\tkind=%s\tnode=%s\tpreset=%s\tmodel=%s\treason=%s\tautomatic=%t\trequires_approval=%t\tnext=%s\n",
			task.Kind,
			emptyDash(task.NodeID),
			emptyDash(task.PresetID),
			emptyDash(task.Model),
			emptyDash(task.Reason),
			task.AutomaticCandidate,
			task.RequiresApproval,
			emptyDash(task.Next),
		)
	}
	for _, audit := range plan.Audit {
		fmt.Fprintf(w, "audit\tkind=%s\tmessage=%s\n", audit.Kind, audit.Message)
	}
}

func printReconcileStatus(w io.Writer, tasks []domain.ReconciliationTaskRecord) {
	fmt.Fprintf(w, "reconcile-status\ttasks=%d\n", len(tasks))
	for _, task := range tasks {
		fmt.Fprintf(w, "task\tid=%s\tjob=%s\tstatus=%s\tkind=%s\tnode=%s\tpreset=%s\tmodel=%s\treason=%s\tautomatic=%t\trequires_approval=%t\tblocker=%s\tnext=%s\n",
			task.ID,
			emptyDash(task.JobID),
			task.Status,
			task.Kind,
			emptyDash(task.NodeID),
			emptyDash(task.PresetID),
			emptyDash(task.Model),
			emptyDash(task.Reason),
			task.AutomaticCandidate,
			task.RequiresApproval,
			emptyDash(task.Blocker),
			emptyDash(task.Next),
		)
	}
}
