package controlcli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/reconcile"
)

func runReconcile(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce reconcile <placement>")
	}
	switch args[0] {
	case "placement":
		return runReconcilePlacement(args[1:])
	default:
		return fmt.Errorf("usage: myce reconcile <placement>")
	}
}

func runReconcilePlacement(args []string) error {
	fs := flag.NewFlagSet("reconcile placement", flag.ContinueOnError)
	decisionFile := fs.String("decision-file", "", "placement decision, job, or wrapper JSON")
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
	if *jsonOut {
		return writePrettyJSON(os.Stdout, plan)
	}
	printReconcilePlacement(os.Stdout, plan)
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
