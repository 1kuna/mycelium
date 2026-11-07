package myce

import "time"

// AlertRecovery is an episode's action receipt, not release or retry authority.
// Receipts are replaced by value: detached status views and notifier deliveries
// can retain the old immutable value while verification commits a new one.
type AlertRecovery struct {
	Episode           string     `json:"episode"`
	Action            string     `json:"action"`
	State             string     `json:"state"`
	Cause             string     `json:"cause"`
	Summary           string     `json:"summary"`
	NextStep          string     `json:"next_step,omitempty"`
	Machine           string     `json:"machine"`
	Session           string     `json:"session,omitempty"`
	JobID             string     `json:"job_id"`
	Attempt           int        `json:"attempt"`
	AdmissionRevision uint64     `json:"admission_revision,omitempty"`
	PreviousGB        float64    `json:"previous_gb,omitempty"`
	AvailableGB       float64    `json:"available_gb,omitempty"`
	At                time.Time  `json:"at"`
	VerifiedAt        *time.Time `json:"verified_at,omitempty"`
	VerifiedAttempt   int        `json:"verified_attempt,omitempty"`
}

// The worker's live_host_* availability uses the same operator-reserve-adjusted
// units as Machine.HostAvailableMemoryGB. Required in live_host_growth includes
// the other attempts' remaining growth; never use it as the new job's size or
// rewrite any reservation, learned requirement, containment or configured cap.
type alertMemoryEvidence struct {
	AdmissionRevision uint64  `json:"admission_revision"`
	AvailableGB       float64 `json:"available_gb"`
	Cause             string  `json:"cause"`
}

func memoryRefusalEvidence(j *Job, o AttemptOutcome) *alertMemoryEvidence {
	c := o.ExecutionCause
	if j.Attempt != o.Attempt || j.StartedAt != nil || c == nil || c.Operation != "launch" || c.Class != "resource_limit" || !c.PrelaunchRefused {
		return nil
	}
	d := safeAdmissionDetail(c.Admission)
	if d == nil || d.Unavailable || d.Available == nil || *d.Available >= d.Required || (d.Subcheck != "live_host_ram" && d.Subcheck != "live_host_growth") {
		return nil
	}
	return &alertMemoryEvidence{AdmissionRevision: j.AdmissionRevision, AvailableGB: *d.Available, Cause: d.Subcheck}
}

// Called only when the detector opens an episode, inside its transaction. A
// delivery is never an action command. Additional classes join here with their
// own evidence adapter; #834 keeps exclusive ownership of host/mount recovery.
func (q *Queue) recoverHealthAlert(e HealthAlert) *AlertRecovery {
	if e.Kind != "infrastructure_loss_streak" {
		return e.Recovery
	}
	r := AlertRecovery{
		Episode: e.DeliveryID, Action: "none", State: "owner_required",
		Cause: e.Reason, At: e.At, Machine: e.Machine, JobID: e.JobID, Attempt: e.Attempt,
		Summary:  "Repeated infrastructure failures need attention; no supported automatic repair matches the retained cause",
		NextStep: "Ask the worker owner to inspect the retained attempt causes and restore the failing dependency; keep the same job and submission key",
	}
	endings := q.s.Health.Conditions["attempt-endings/"+e.Machine].Endings
	if len(endings) == 0 {
		return &r
	}
	last := endings[len(endings)-1]
	r.Session = last.Session
	j := q.s.Jobs[e.JobID]
	m := q.s.Machines[e.Machine]
	proof := last.Memory
	if proof != nil {
		r.Cause = proof.Cause
		r.AdmissionRevision, r.AvailableGB = proof.AdmissionRevision, proof.AvailableGB
	}
	// A later check-in is authoritative, even at the very same injected-clock
	// timestamp. Replayed endings, replaced sessions and later executions cannot
	// downgrade its live memory evidence. Old assignments have no generation.
	if proof != nil && proof.Cause == "live_host_growth" {
		// #868/#909: the required side can be an unbounded growth forecast,
		// rather than stale availability. AdmissionDetail has no paired owner
		// forecast terms to distinguish them; a generation fence cannot do so.
		r.Summary = "Repeated prelaunch memory refusals include a growth forecast whose cause is unverified; Mycelium left capacity unchanged"
		r.NextStep = "Ask the Mycelium maintenance owner to inspect forecast bounds and paired queue/worker memory snapshots; keep the same job and submission key"
	} else if proof != nil && j != nil && j.Attempt == e.Attempt && j.State == "queued" && j.Machine == "" && !cancelled(j) &&
		last.JobID == e.JobID && last.Attempt == e.Attempt && m != nil && !m.Gone && !m.Leaving &&
		m.Session == last.Session && proof.AdmissionRevision != 0 && m.AdmissionRevision == proof.AdmissionRevision &&
		m.HostMemoryMeasured && m.HostMemoryError == "" && proof.AvailableGB < m.HostAvailableMemoryGB {
		r.Action, r.State = "refresh_live_host_memory", "verifying"
		r.PreviousGB = m.HostAvailableMemoryGB
		r.Summary = "Mycelium corrected an older free-RAM snapshot after repeated prelaunch memory refusals; a successful later start is still unverified"
		r.NextStep = "Keep this job queued; Mycelium will use fresh capacity reports and the normal placement rules"
		m.HostAvailableMemoryGB = proof.AvailableGB
		q.refreshHostMemory()
	} else if proof != nil {
		r.Summary = "Mycelium left current capacity unchanged because the refused attempt's memory evidence no longer matches a fresh, current assignment snapshot"
		r.NextStep = "Ask the worker owner to inspect current memory telemetry and the retained admission comparison; no budget increase or manual retry is authorized by this alert"
	}
	if j != nil && j.Attempt == e.Attempt {
		q.touchJob(j.ID).HealthRecovery = &r
	}
	return &r
}

// Progress, not elapsed time, settles recovery. No timeout, retry loop or extra
// probe is introduced. A subsequent positive worker start verifies its ordinary
// launch checks. Another matching refusal stops this adapter and emits one
// actionable finding through the same existing notifier. A session change or
// cancelled/terminal demand cannot prove host recovery.
func (q *Queue) verifyHealthRecoveries() {
	var findings []HealthAlert
	for i := range q.s.Health.Events {
		e := q.s.Health.Events[i]
		if e.Recovery == nil || e.Recovery.State != "verifying" {
			continue
		}
		r := *e.Recovery
		j, m := q.s.Jobs[r.JobID], q.s.Machines[r.Machine]
		failed := false
		switch {
		case j == nil || m == nil || m.Session != r.Session || cancelled(j):
			r.State, r.Summary, r.NextStep = "unverified", "The original recovery no longer has matching live demand and worker-session evidence; host recovery is unverified", ""
		case j.Attempt > r.Attempt && j.Machine == r.Machine && j.Session == r.Session && j.StartedAt != nil:
			now := q.clock.Now()
			r.State, r.VerifiedAt, r.VerifiedAttempt = "verified", &now, j.Attempt
			r.Summary, r.NextStep = "Mycelium corrected stale free-RAM evidence and verified that a later attempt passed launch checks and started", ""
		default:
			for _, ending := range q.s.Health.Conditions["attempt-endings/"+r.Machine].Endings {
				if ending.JobID == r.JobID && ending.Session == r.Session && ending.Attempt > r.Attempt && ending.Memory != nil {
					r.State, failed = "owner_required", true
					r.Summary = "Refreshing the free-RAM snapshot did not resolve a later prelaunch memory refusal; Mycelium will not repeat this recovery for the episode"
					r.NextStep = "Ask the worker owner to compare current memory telemetry with the retained admission comparisons; keep the same job and do not raise shared budgets or clear custody"
					break
				}
			}
			if !failed && terminal(j.State) {
				r.State, r.Summary, r.NextStep = "unverified", "The job ended before a matching later start verified this recovery; host recovery is unverified", ""
			}
		}
		if r.State == "verifying" {
			continue
		}
		e.Recovery = &r
		q.s.Health.Events[i] = e
		if j != nil && j.HealthRecovery != nil && j.HealthRecovery.Episode == r.Episode {
			q.touchJob(j.ID).HealthRecovery = &r
		}
		if failed {
			findings = append(findings, HealthAlert{Kind: "alert_recovery_action_required", Host: e.Host, Machine: r.Machine, JobID: r.JobID, Attempt: r.Attempt, Reason: r.Cause, Recovery: &r})
		}
	}
	for _, finding := range findings {
		q.appendHealthAlert(finding)
	}
}
