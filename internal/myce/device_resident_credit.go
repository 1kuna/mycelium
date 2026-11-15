package myce

import "time"

// Cgroup anonymous pages are unique across attempts, but CUDA counters can
// overlap those pages and each other. The physical union is at least the larger
// of all credited host pages and ONE process's device counter. Assign only that
// difference to its exact owner; never sum device counters, even across jobs.
type deviceResidentUnion struct {
	host, device float64
	owner        attemptKey
}

func (u *deviceResidentUnion) observe(j *Job, m *Machine, s hostMemorySample, now time.Time, launching bool) {
	_, lend := lendingFloor(j, m, s, now)
	rss := m.MemoryEnforcement == darwinRSSGuardStatus && (lend == lendByUse || chargedBelowLimit(j))
	u.host += memoryResidentCredit(s, now, rss)
	if j.Spec.Needs.cpuOnly() || len(j.AllocatedDevices) > 1 || !launching && j.StartedAt == nil && !s.started {
		return
	}
	device := helperDeviceResident(m, s, now)
	key := jobKey(*j)
	if device > u.device || device > 0 && device == u.device && key.job < u.owner.job {
		u.device, u.owner = device, key
	}
}

func (u deviceResidentUnion) credit(key attemptKey) float64 {
	if key != u.owner {
		return 0
	}
	return max(0, u.device-u.host)
}

func residentUnion(jobs []Job, m *Machine, samples map[attemptKey]hostMemorySample, now time.Time, launching ...attemptKey) deviceResidentUnion {
	var u deviceResidentUnion
	for _, j := range jobs {
		if !j.ReleaseConfirmed {
			own := len(launching) == 1 && jobKey(j) == launching[0]
			u.observe(&j, allocationMachine(m, j.AllocatedDevices), samples[jobKey(j)], now, own)
		}
	}
	return u
}

func (q *Queue) deviceResidentCredit(j *Job, m *Machine) float64 {
	now := q.clock.Now()
	if helperDeviceResident(m, q.hostMemorySamples[j.Machine][jobKey(*j)], now) == 0 {
		return 0
	}
	var u deviceResidentUnion
	for _, live := range q.liveJobs() {
		owner := q.planningAllocationMachine(q.s.Machines[live.Machine], live.AllocatedDevices)
		if !live.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (live.State == "running" || live.State == "stopping") {
			u.observe(live, owner, q.hostMemorySamples[live.Machine][jobKey(*live)], now, false)
		}
	}
	return u.credit(jobKey(*j))
}

// Call with rt.mu held, using the same exact actors as final launch admission.
func (rt *modelRuntime) deviceResidentUnionLocked(samples map[attemptKey]hostMemorySample, now time.Time, launching ...attemptKey) deviceResidentUnion {
	var u deviceResidentUnion
	for _, j := range rt.batchJobs {
		if a := rt.batchSupervisors[jobKey(j)]; a != nil {
			j = a.admissionJob()
		}
		if !j.ReleaseConfirmed {
			own := len(launching) == 1 && jobKey(j) == launching[0]
			u.observe(&j, allocationMachine(&rt.memoryMachine, j.AllocatedDevices), samples[jobKey(j)], now, own)
		}
	}
	return u
}
