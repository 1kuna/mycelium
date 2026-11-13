package myce

import "slices"

const stagingHostRetryHold = "staging_host_retry"

// Hosts where this job lost an attempt to input staging. Retained attempt
// history is the evidence, so the preference survives a queue restart without
// new state.
func (q *Queue) stagingFailedHosts(j *Job) []string {
	var hosts []string
	for _, h := range j.AttemptHistory {
		if !h.Interrupted || h.ExecutionCause == nil || h.ExecutionCause.Operation != "input_staging" {
			continue
		}
		host := h.Machine
		if m := q.s.Machines[h.Machine]; m != nil {
			host = hostID(m)
		}
		if !slices.Contains(hosts, host) {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// A staging failure is host-local evidence (its disk, cache or mount). Retry
// first on another host that could run the job, instead of spending every
// infrastructure retry where staging already failed. A host is held only
// while some untried compatible host exists, so a job with one possible host
// still retries there, and nothing waits on a host that is out of contact,
// leaving or under maintenance.
func (q *Queue) stagingHostRetryReason(j *Job, m *Machine) string {
	if m == nil || len(j.AttemptHistory) == 0 {
		return ""
	}
	failed := q.stagingFailedHosts(j)
	if !slices.Contains(failed, hostID(m)) {
		return ""
	}
	now := q.clock.Now()
	for _, other := range q.plannerMachines() {
		if other.Leaving || !q.contactFresh(other, now, false) || q.maintenanceBlocked(hostID(other)) || slices.Contains(failed, hostID(other)) || !q.jobAllowsMachine(j, other.ID) {
			continue
		}
		if fits(j, other, max(other.TotalMemoryGB, other.FreeMemoryGB)) {
			return "input staging failed on this host; retrying on another eligible host"
		}
	}
	return ""
}
