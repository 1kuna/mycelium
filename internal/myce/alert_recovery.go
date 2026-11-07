package myce

import "time"

// AlertRecovery is an episode's decision receipt, not release or retry authority.
// It is immutable after publication so detached status and notifier deliveries
// retain the exact evidence and decision. No automatic action is qualified yet.
type AlertRecovery struct {
	Episode   string           `json:"episode"`
	Action    string           `json:"action"`
	State     string           `json:"state"`
	Cause     string           `json:"cause"`
	Summary   string           `json:"summary"`
	NextStep  string           `json:"next_step,omitempty"`
	Machine   string           `json:"machine"`
	Session   string           `json:"session,omitempty"`
	JobID     string           `json:"job_id"`
	Attempt   int              `json:"attempt"`
	Admission *AdmissionDetail `json:"admission,omitempty"`
	At        time.Time        `json:"at"`
}

func memoryRefusalEvidence(j *Job, o AttemptOutcome) *AdmissionDetail {
	c := o.ExecutionCause
	if j.Attempt != o.Attempt || j.StartedAt != nil || c == nil || c.Operation != "launch" || c.Class != "resource_limit" || c.Predicate != "memory_capacity" || !c.PrelaunchRefused {
		return nil
	}
	d := safeAdmissionDetail(c.Admission)
	if d == nil || (d.Subcheck != "live_host_ram" && d.Subcheck != "live_host_growth") {
		return nil
	}
	return d
}

// Called only when the detector opens an episode, inside its transaction. A
// delivery is never an action command. Future actions require a recorded causal
// incident, fresh execution/episode fences and post-action verification through
// their existing lifecycle. #834 retains ownership of host/mount recovery.
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
	if last.JobID != e.JobID || last.Attempt != e.Attempt {
		return &r
	}
	r.Session = last.Session
	r.Admission = last.Memory
	if r.Admission != nil {
		r.Cause = r.Admission.Subcheck
		r.Summary = "Repeated prelaunch memory refusals need diagnosis; Mycelium left capacity unchanged"
		r.NextStep = "Ask the Mycelium maintenance owner to compare the retained admission refusal with current RAM telemetry, job needs and paired queue/worker snapshots; keep the same job and submission key"
		if r.Cause == "live_host_growth" {
			// #868/#909: the required side can be an unbounded growth forecast,
			// rather than stale availability. A comparison alone proves neither
			// forecast validity nor that correcting availability would help.
			r.Summary = "Repeated prelaunch memory refusals include a growth forecast whose cause is unverified; Mycelium left capacity unchanged"
			r.NextStep = "Ask the Mycelium maintenance owner to inspect forecast bounds and paired queue/worker memory snapshots; keep the same job and submission key"
		}
	}
	if j := q.s.Jobs[e.JobID]; j != nil && j.Attempt == e.Attempt {
		q.touchJob(j.ID).HealthRecovery = &r
	}
	return &r
}
