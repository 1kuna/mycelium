package myce

import (
	"fmt"
	"slices"
)

const stagingHostRetryHold = "staging_host_retry"
const stagingStorageHold = "staging_storage_full"

func stagingStorageFull(c *RuntimeCause) bool {
	return c != nil && c.Operation == "input_staging" && c.Predicate == "" &&
		(c.Class == "storage_full" || c.Class == "resource_limit" && c.Staging != nil &&
			(c.Staging.Error == "cache_space" || c.Staging.Error == "cache_limit"))
}

func stagingStorageFailure(j *Job, h *AttemptOutcome) bool {
	return h.Attempt > j.StagingStorageRetryAfter && h.State == "queued" &&
		h.Interrupted && stagingStorageFull(h.ExecutionCause)
}

func pendingStagingStorageFailure(j *Job) bool {
	for i := range j.AttemptHistory {
		if stagingStorageFailure(j, &j.AttemptHistory[i]) {
			return true
		}
	}
	return false
}

// Keep the refusal on the physical host across worker and queue restarts.
// Neither a new session nor aggregate free space proves that the refusing
// cache, filesystem or quota has room for this input.
func (q *Queue) stagingStorageHostHold(j *Job, m *Machine) *Hold {
	for i := len(j.AttemptHistory) - 1; i >= 0; i-- {
		h := &j.AttemptHistory[i]
		if !stagingStorageFailure(j, h) {
			continue
		}
		host := h.Machine
		if h.Placement != nil && h.Placement.Host != "" {
			host = h.Placement.Host
		} else if worker := q.s.Machines[host]; worker != nil {
			host = hostID(worker)
		}
		if host == hostID(m) {
			return newHold(stagingStorageHold, fmt.Sprintf("%s: attempt %d: %s", host, h.Attempt, h.ExecutionCause))
		}
	}
	return nil
}

func (q *Queue) stagingStorageWait(j *Job, machines []*Machine) *Hold {
	if !pendingStagingStorageFailure(j) {
		return nil
	}
	var hold *Hold
	for _, m := range machines {
		if q.maintenanceBlocked(hostID(m)) || !q.structuralDemandPolicyIgnoringMaintenance(j, m) ||
			!fitsWithoutHostMemory(j, m, max(m.TotalMemoryGB, m.FreeMemoryGB)) {
			continue
		}
		h := q.stagingStorageHostHold(j, m)
		if h == nil {
			return nil // An alternate route can still accept this job.
		}
		hold = h
	}
	return hold
}

// An explicit owner resume after repair grants one new staging try per host.
// Keep the original causes in attempt history.
func clearStagingStorageHolds(j *Job) {
	if pendingStagingStorageFailure(j) {
		j.StagingStorageRetryAfter = j.Attempt
	}
}

// Hosts where this job lost an attempt to input staging or checkpoint storage. Retained attempt
// history is the evidence, so the preference survives a queue restart without
// new state.
func (q *Queue) stagingFailedHosts(j *Job) []string {
	var hosts []string
	for _, h := range j.AttemptHistory {
		if !h.Interrupted || h.ExecutionCause == nil || (h.ExecutionCause.Operation != "input_staging" && !checkpointStorageInterruption(h.ExecutionCause)) {
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

// A storage/staging failure is host-local evidence (its disk, cache or mount). Retry
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
			return "storage or input staging failed on this host; retrying on another eligible host"
		}
	}
	return ""
}
