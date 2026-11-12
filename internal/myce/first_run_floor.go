package myce

import (
	"strings"
	"time"
)

// A worker with this capability stops an adaptive-guarded attempt above its
// charge only on measured host pressure, never on memory PSI growth alone
// (#1110, #1484). A first run charged its floor there grows into the room it
// measures, so the queue charges it the floor instead of offering it the
// host's whole free room. Older workers keep the whole-room probe (MY-42).
const firstRunFloorCapability = "first_run_floor_v1"

// The envelope source part of a first run charged its floor.
const firstRunFloorSource = "charged its floor, grows into measured room"

// chargesFirstRunFloor says target m charges a guarded CPU first run its
// floor (member caps, constrained lower bound, OOM floor, any foreign peak)
// rather than the host's whole free room. The whole-room probe stays where
// there is no evidence the floor is as safe (#1484):
//   - production, which keeps its protection;
//   - the shared interactive host (MY-45);
//   - a host without a cgroup v2 or Darwin RSS guard, or without measured
//     host memory and an allowance;
//   - OOM recovery, which charges its measured peak plus growth;
//   - a worker without firstRunFloorCapability, or without the adaptive guard.
//
// GPU first runs never reach this: the caller sizes CPU work only.
func (q *Queue) chargesFirstRunFloor(j *Job, m *Machine) bool {
	n := j.Spec.Needs
	return q.learnedMemory() && n.cpuOnly() && n.Memory == "auto" && j.OOMRetries == 0 && j.OOMCapacity == nil &&
		m.SharedHost == nil && m.HostMemoryMeasured && m.HostMemoryError == "" && hostMemoryAllowanceGB(m) > 0 &&
		(m.MemoryEnforcement == cgroupMemoryEnforcement || m.MemoryEnforcement == darwinRSSGuardStatus) &&
		contains(m.Capabilities, firstRunFloorCapability) && contains(m.Capabilities, learnedMemoryGuardCapability) &&
		q.priorityMatch(j.Spec).Tier != "production"
}

// floorCharged says e charged a first run its floor.
func floorCharged(e *JobMemoryEnvelope) bool {
	return e != nil && strings.Contains(e.Source, firstRunFloorSource)
}

// firstRunFloorRoom is the guard limit of a first run charged its floor: its
// own use plus the room the worker's ledger leaves it, which is what the queue
// could still give it. Its charge says nothing about what it needs, so it is
// never stopped at its floor on host pressure: on a working Linux host,
// memory PSI grew in 28% of 200 ms intervals and swap-out in 4%, with 14 GiB
// or more available (#1484). It is stopped only once its measured use, plus
// the growth until a stop lands, outgrows that room:
//   - enforced: the adaptive ceiling (allowance within physical RAM) less
//     every other attempt's and model's charge, less this run's own growth;
//   - physical: live available less the reserve, less what others are charged
//     but have not yet used, less the host's measured loss of room, which
//     includes every neighbour's growth as well as this run's.
//
// Growth until a stop lands is the rate just measured over one sampling
// interval plus the guard's sustained-overage grace. The worker computes this
// every sample from its own ledger, so the room is never older than one
// check-in; a queue-sent grant would be as old as the check-in round trip,
// measured at 1–3 s and up to 40 s (#1484).
type firstRunFloorRoom struct {
	others          func() (charge, pending float64)
	at              time.Time
	used, available float64
}

func (r *firstRunFloorRoom) limitBytes(now time.Time, bytes uint64, host *gpuHostState, reserve, capacityGB float64) int64 {
	used, available := float64(bytes)/(1<<30), -1.0
	if host != nil && validMemoryValue(host.available) {
		available = host.available
	}
	own, lost := 0.0, 0.0
	if !r.at.IsZero() && now.After(r.at) {
		interval := now.Sub(r.at)
		horizon := (interval + processMemoryGrace).Seconds() / interval.Seconds()
		own = max(0, used-r.used) * horizon
		if available >= 0 && r.available >= 0 {
			lost = max(0, r.available-available) * horizon
		}
	}
	r.at, r.used, r.available = now, used, available
	charge, pending := r.others()
	room := capacityGB - charge - own
	if available >= 0 {
		room = min(room, used+available-reserve-pending-lost)
	}
	if !validRate(room) {
		return 0
	}
	return int64(room * (1 << 30))
}

func (r *firstRunFloorRoom) reset() {
	if r != nil {
		*r = firstRunFloorRoom{others: r.others}
	}
}
