package myce

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"
)

// A worker with this capability stops an adaptive-guarded attempt above its
// charge only on measured host pressure, never on memory PSI growth alone
// (#1110, #1484). A first run charged its floor there grows into the room it
// measures, so the queue charges it the floor instead of offering it the
// host's whole free room. Other workers contain the small charge with their
// strict attempt guard.
const firstRunFloorCapability = "first_run_floor_v1"

// The envelope source part of a first run charged its floor.
const firstRunFloorSource = "charged its floor, grows into measured room"

// Native shell startup measured on Linux and Darwin (2026-11-14): the larger
// of total memcg peak 3,895,296 bytes and RSS peak + measured growth
// 3,375,104 + 2,621,440 bytes, rounded up to the supported 64 KiB page size.
// This seeds strictly contained first runs, not arbitrary workload demand.
// Configured launch slack already contributes to that startup envelope.
const firstRunStartupGB = float64(92*65536) / (1 << 30)

// chargesFirstRunFloor says target m charges a guarded CPU first run its
// floor (member caps, constrained lower bound, OOM floor, any foreign peak)
// rather than the host's whole free room. Strict containment stays where
// room lending has not been qualified (#1484, #1500):
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
	charge, pending float64
	ledgerKnown     bool
	decision        *MemoryRoomDecision
}

// MemoryRoomDecision preserves the actual bound, independently of the launch
// accounting floor. Only numeric inputs and fixed source labels cross the wire.
type MemoryRoomDecision struct {
	RoomGB           float64  `json:"room_gb"`
	FloorGB          float64  `json:"floor_gb"`
	AllowanceGB      float64  `json:"allowance_gb"`
	CapacityGB       float64  `json:"capacity_gb"`
	CapacitySource   string   `json:"capacity_source"`
	ChargeGB         float64  `json:"charges_gb"`
	PendingGB        float64  `json:"pending_gb"`
	AvailableGB      *float64 `json:"available_gb,omitempty"`
	ReserveGB        float64  `json:"reserve_gb"`
	OwnGrowthGB      float64  `json:"own_growth_gb"`
	HostLossGB       float64  `json:"host_loss_gb"`
	LedgerReadFailed bool     `json:"ledger_read_failed,omitempty"`
}

func memoryCapacitySource(m *Machine) string {
	if p := m.CapabilityProfile; p != nil && p.SystemMemoryBytes > 0 && (m.HostMemoryBudgetGB <= 0 || float64(p.SystemMemoryBytes)/(1<<30) < m.HostMemoryBudgetGB) {
		return "physical RAM"
	}
	if m.HostMemoryBudgetGB > 0 {
		return "host allowance"
	}
	return "unavailable"
}

func safeMemoryRoom(d *MemoryRoomDecision) *MemoryRoomDecision {
	if d == nil || math.IsNaN(d.RoomGB) || math.IsInf(d.RoomGB, 0) {
		return nil
	}
	for _, v := range []float64{d.FloorGB, d.AllowanceGB, d.CapacityGB, d.ChargeGB, d.PendingGB, d.ReserveGB, d.OwnGrowthGB, d.HostLossGB} {
		if !validMemoryValue(v) {
			return nil
		}
	}
	if d.CapacitySource != "physical RAM" && d.CapacitySource != "host allowance" && d.CapacitySource != "unavailable" {
		return nil
	}
	out := *d
	if d.AvailableGB != nil {
		if !validMemoryValue(*d.AvailableGB) {
			return nil
		}
		v := *d.AvailableGB
		out.AvailableGB = &v
	}
	return &out
}

func custodianHostCharges(dir, id string) func(attemptKey) (float64, float64) {
	return func(attemptKey) (float64, float64) {
		var ledger struct {
			Charge  *float64 `json:"charge"`
			Pending *float64 `json:"pending"`
		}
		if readCustodianJSON(dir, custodianName(id, "ledger"), &ledger) != nil || ledger.Charge == nil || ledger.Pending == nil || !validMemoryValue(*ledger.Charge) || !validMemoryValue(*ledger.Pending) {
			return math.NaN(), math.NaN() // Unknown, never fabricated full-allowance charges.
		}
		return *ledger.Charge, *ledger.Pending
	}
}

// The existing final launch gates call this again after preparation. An offer
// of the accounting floor is never permission to start a floor-only guard.
func (w Worker) firstRunMemoryAdmission(ctx context.Context, j Job) (*MemoryRoomDecision, error) {
	if !floorCharged(j.MemoryEnvelope) {
		return nil, nil
	}
	floor := hostNeed(&w.Machine, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	capacity := adaptiveMemoryCeilingGB(&j, &w.Machine)
	d := &MemoryRoomDecision{FloorGB: floor, CapacityGB: capacity, AllowanceGB: w.Machine.HostMemoryBudgetGB, CapacitySource: memoryCapacitySource(&w.Machine), ReserveGB: w.HostMemoryReserveGB}
	refuse := func(check string, unavailable bool) (*MemoryRoomDecision, error) {
		err := refuseAdmission(check, "GiB", floor, max(0, d.RoomGB), unavailable).(*admissionError)
		err.detail.MemoryRoom = d
		return d, err
	}
	if capacity <= floor {
		return refuse("exploratory_capacity_unavailable", true)
	}
	if w.hostChargesExcept != nil {
		d.ChargeGB, d.PendingGB = w.hostChargesExcept(jobKey(j))
	}
	if !validMemoryValue(d.ChargeGB) || !validMemoryValue(d.PendingGB) {
		d.ChargeGB, d.PendingGB, d.LedgerReadFailed = 0, 0, true
		d, err := refuse("custodian_ledger_read_failed", true)
		return d, errors.Join(errWorkerCheckpoint, err)
	}
	if w.HostMemorySource == nil {
		return refuse("exploratory_host_memory_unavailable", true)
	}
	_, raw, err := w.sampleHostMemory(ctx)
	if err != nil || !validMemoryValue(raw) {
		return refuse("exploratory_host_memory_unavailable", true)
	}
	d.AvailableGB = &raw
	d.RoomGB = min(capacity-d.ChargeGB, raw-d.ReserveGB-d.PendingGB)
	if d.RoomGB <= floor {
		d, err := refuse("exploratory_memory_room", false)
		return d, errors.Join(errWorkerCheckpoint, err)
	}
	return d, nil
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
	failed := !validMemoryValue(charge) || !validMemoryValue(pending)
	if !failed {
		r.charge, r.pending, r.ledgerKnown = charge, pending, true
	}
	// A failed read is not a full-allowance charge. Retain the admitted ledger
	// while the physical-room and hard guards keep running during replacement.
	charge, pending = r.charge, r.pending
	room := capacityGB - charge - own
	if available >= 0 {
		room = min(room, used+available-reserve-pending-lost)
	}
	d := MemoryRoomDecision{RoomGB: room, CapacityGB: capacityGB, ChargeGB: charge, PendingGB: pending, ReserveGB: reserve, OwnGrowthGB: own, HostLossGB: lost, LedgerReadFailed: failed}
	if previous := r.decision; previous != nil {
		d.FloorGB, d.AllowanceGB, d.CapacitySource = previous.FloorGB, previous.AllowanceGB, previous.CapacitySource
	}
	if available >= 0 {
		d.AvailableGB = &available
	}
	r.decision = &d
	return int64(max(0, room) * (1 << 30))
}

func (r *firstRunFloorRoom) reset() {
	if r != nil {
		*r = firstRunFloorRoom{others: r.others, charge: r.charge, pending: r.pending, ledgerKnown: r.ledgerKnown, decision: r.decision}
	}
}
