package myce

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// DeadlineGuidance is a read-only receipt projection, separate from accepted
// Spec and hold state. PriorityPolicy on Job describes the current match.
type DeadlineGuidance struct {
	Code             string        `json:"code"`
	DeadlinePolicy   PriorityMatch `json:"deadline_policy"`
	PrecedenceChange string        `json:"precedence_change"`
	Blockers         []string      `json:"blockers,omitempty"`
	ETA              string        `json:"eta"`
	Example          string        `json:"example"`
	NextAction       string        `json:"next_action"`
	Message          string        `json:"message"`
}

// Submission advice needs no cancellation ancestry or retained-history copy.
// Read only policy/planner facts under the lock; no filesystem or network I/O.
// The accepted receipt is already detached and retains all submission fields.
func (q *Queue) submissionDeadlineGuidance(j *Job, ctx context.Context) {
	unlock := requestLock(ctx, &q.mu)
	defer unlock()
	copy := *j
	if copy.State == "queued" {
		copy.Reason = q.waitReason(&copy)
		copy.Reason = q.statusWaitingReason(&copy)
	}
	j.PriorityPolicy = q.priorityMatch(j.Spec)
	j.DeadlineGuidance = q.deadlineGuidance(&copy)
}

// Caller holds q.mu or owns a detached observation. Only reads planner facts.
func (q *Queue) deadlineGuidance(j *Job) *DeadlineGuidance {
	candidate := j.Spec
	candidate.Class, candidate.Priority = "deadline", "deadline"
	current, proposed := q.priorityMatch(j.Spec), q.priorityMatch(candidate)
	change := "same"
	if proposed.Index < current.Index {
		change = "higher"
	}
	if proposed.Index > current.Index {
		change = "lower"
	}
	d := &DeadlineGuidance{
		Code: "deadline_priority_guidance", DeadlinePolicy: proposed, PrecedenceChange: change,
		ETA:        "unknown: queue start and completion time are not assured",
		Example:    "class: deadline\ndeadline: " + j.Queued.Add(24*time.Hour).UTC().Format(time.RFC3339),
		NextAction: "For deadline work, use class: deadline plus an RFC3339 deadline in a new submission (replace legacy priority if present). Use a new submission_key for keyed work; accepted class and submission identity stay unchanged. Choose the desired deadline and check the current owner policy match before submitting.",
		Message:    "A deadline is an ordering hint, not a completion guarantee or production start_by. Deadline demand does not preempt non-resumable commands. The deadline-class policy match may give higher, equal or lower precedence; owner policy controls ordering.",
	}
	if j.State != "queued" {
		return d
	}
	if j.Reason != "" {
		d.Blockers = append(d.Blockers, j.Reason)
	}
	// Explain the deadline interruption contract only on otherwise fitting
	// targets with a reserved command slot. General slot diagnostics remain #537.
	for _, m := range q.plannerMachines() {
		if !q.jobAllowsMachine(j, m.ID) || !fits(j, m, max(m.TotalMemoryGB, m.FreeMemoryGB)) {
			continue
		}
		for _, r := range q.s.Jobs {
			if r.ID == j.ID || !batchReservesCapacity(r) || !batchOnTarget(r, m) || r.Spec.Resumable {
				continue
			}
			timing := "ETA unknown: no comparable duration observation"
			if r.Timing != nil && r.Timing.Reason == "" && r.Timing.PredictedSeconds != nil && r.StartedAt != nil {
				remaining := *r.Timing.PredictedSeconds - q.clock.Now().Sub(*r.StartedAt).Seconds()
				if remaining > 0 {
					timing = fmt.Sprintf("observed duration estimate: %.0fs remaining; not assured", remaining)
				} else {
					timing = "ETA unknown: observed duration estimate overrun"
				}
			}
			d.Blockers = append(d.Blockers, fmt.Sprintf("non-resumable command %s on %s (tier %s); deadline demand waits for release; %s", r.ID, m.ID, q.priorityMatch(r.Spec).Tier, timing))
		}
	}
	slices.Sort(d.Blockers)
	d.Blockers = slices.Compact(d.Blockers)
	return d
}

// DeadlineGuidanceText preserves submit's job-ID stdout and wait's final-state
// output while sharing the API's current-policy advice with CLI observers.
func DeadlineGuidanceText(j *Job) string {
	d := j.DeadlineGuidance
	if d == nil {
		return ""
	}
	p := j.PriorityPolicy
	return fmt.Sprintf("priority advisory: tier %s selector %s revision %d; deadline class matches tier %s selector %s revision %d (%s tier precedence). Blockers: %s. ETA %s. %s %s Example: %s", p.Tier, p.Selector, p.Revision, d.DeadlinePolicy.Tier, d.DeadlinePolicy.Selector, d.DeadlinePolicy.Revision, d.PrecedenceChange, strings.Join(d.Blockers, "; "), d.ETA, d.Message, d.NextAction, strings.ReplaceAll(d.Example, "\n", "; "))
}
