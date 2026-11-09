package myce

import (
	"cmp"
	"reflect"
	"slices"
	"time"
)

// This is one retained target comparison, not a claim that RAM is the job's
// only blocker. Its ledger is immutable and may be shared by waiting jobs.
type MemoryAdmissionSnapshot struct {
	CapturedAt     time.Time                    `json:"captured_at"`
	WaitingSince   time.Time                    `json:"waiting_since"`
	Attempt        int                          `json:"attempt"`
	Machine        string                       `json:"machine"`
	Session        string                       `json:"session"`
	Host           string                       `json:"host"`
	Device         string                       `json:"device,omitempty"`
	Target         ExecutionTarget              `json:"target"`
	Envelope       JobMemoryEnvelope            `json:"envelope"`
	DriftReserveGB float64                      `json:"drift_reserve_gb"`
	Reason         string                       `json:"reason"`
	Ledger         *HostMemoryLedger            `json:"ledger"`
	Reservations   []MemoryAdmissionReservation `json:"reservations"`
}

type MemoryAdmissionReservation struct {
	ID                string     `json:"id"`
	Job               string     `json:"job"`
	State             string     `json:"state"`
	ReadyAt           *time.Time `json:"ready_at,omitempty"`
	PendingKnown      bool       `json:"pending_known"`
	PendingObservedAt *time.Time `json:"pending_observed_at,omitempty"`
	Pending           []string   `json:"pending"`
}

type HostMemoryLedger struct {
	CapturedAt     time.Time         `json:"captured_at"`
	ObservedAt     time.Time         `json:"observed_at"`
	AvailableKnown bool              `json:"available_known"`
	AvailableGB    float64           `json:"available_gb"`
	AllowanceGB    float64           `json:"allowance_gb"`
	ReservedGB     float64           `json:"reserved_gb"`
	GrowthGB       float64           `json:"growth_gb"`
	SignedRoomGB   float64           `json:"signed_room_gb"`
	DebtGB         float64           `json:"debt_gb"`
	FreeGB         float64           `json:"free_gb"`
	Owners         []HostMemoryOwner `json:"owners"`
}

type HostMemoryOwner struct {
	Kind         string     `json:"kind"`
	Execution    *Execution `json:"execution,omitempty"`
	Machine      string     `json:"machine"`
	Session      string     `json:"session"`
	Model        string     `json:"model,omitempty"`
	ModelAttempt string     `json:"model_attempt,omitempty"`
	State        string     `json:"state,omitempty"`
	ChargeGB     float64    `json:"charge_gb"`
	GrowthGB     float64    `json:"growth_gb"`
}

// Canonical owner enumeration for refreshHostMemory. A current exact attempt
// covers its nominal release receipt once; orphan custody remains fully charged.
func (q *Queue) hostMemoryLedger(m *Machine) *HostMemoryLedger {
	l := &HostMemoryLedger{CapturedAt: q.clock.Now(), Owners: []HostMemoryOwner{}}
	add := func(o HostMemoryOwner) {
		l.ReservedGB += o.ChargeGB
		l.GrowthGB += o.GrowthGB
		l.Owners = append(l.Owners, o)
	}
	for _, j := range q.liveJobs() {
		owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		if !j.ReleaseConfirmed && owner != nil && hostID(owner) == hostID(m) && (j.State == "running" || j.State == "stopping") {
			need := hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
			charge, growth := jobHostMemoryCharge(j, owner, need, q.hostMemorySamples[j.Machine][jobKey(*j)], l.CapturedAt)
			add(HostMemoryOwner{Kind: "attempt", Execution: &Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, State: j.State, ChargeGB: charge, GrowthGB: growth})
		}
	}
	for _, r := range q.s.HostReleases {
		if r.Host != hostID(m) {
			continue
		}
		j := q.s.Jobs[r.Job]
		if j != nil && j.Machine == r.Machine && j.Session == r.Session && j.Attempt == r.Attempt && batchReservesCapacity(j) {
			continue
		}
		add(HostMemoryOwner{Kind: "release", Execution: &Execution{ID: r.Job, Attempt: r.Attempt}, Machine: r.Machine, Session: r.Session, State: "release_pending", ChargeGB: r.HostChargeGB, GrowthGB: r.HostChargeGB})
	}
	for _, s := range q.s.ModelSlots {
		owner := slotMachine(q.s.Machines[s.Machine], s)
		if owner != nil && slotReservesMemory(s) && hostID(owner) == hostID(m) {
			model, _ := q.model(s.Model)
			need := hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
			add(HostMemoryOwner{Kind: "model", Model: s.Model, ModelAttempt: s.Attempt, Machine: s.Machine, Session: s.Session, State: s.State, ChargeGB: need, GrowthGB: need})
		}
	}
	slices.SortFunc(l.Owners, func(a, b HostMemoryOwner) int {
		x, y := Execution{}, Execution{}
		if a.Execution != nil {
			x = *a.Execution
		}
		if b.Execution != nil {
			y = *b.Execution
		}
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Machine, b.Machine), cmp.Compare(a.Session, b.Session), cmp.Compare(x.ID, y.ID), cmp.Compare(x.Attempt, y.Attempt), cmp.Compare(a.Model, b.Model), cmp.Compare(a.ModelAttempt, b.ModelAttempt))
	})
	return l
}

type memoryAdmissionDecision struct {
	snapshot   MemoryAdmissionSnapshot
	machine    *Machine
	generation uint64
}
type memoryAdmissionCapture struct {
	decisions map[string]memoryAdmissionDecision
	drift     map[string]float64
	// Populated only by existing reservation accounting, never by a candidate.
	reservations map[string]MemoryAdmissionReservation
}

func memoryAdmissionReservation(r Reservation, j *Job, pending []string, known bool) MemoryAdmissionReservation {
	s := MemoryAdmissionReservation{ID: r.ID, Job: r.Job, ReadyAt: r.ReadyAt, State: "unknown", PendingKnown: known, Pending: slices.Clone(pending)}
	switch {
	case r.ReleasedAt != nil:
		s.State = "released"
	case r.ExpiredAt != nil:
		s.State = "expired"
	case j == nil || terminal(j.State):
		s.State = "waiting_for_release"
	case j.StartedAt != nil && (j.State == "running" || j.State == "stopping"):
		s.State = "in_use"
	case known && len(pending) > 0:
		s.State = "draining"
	case known:
		s.State = "ready"
	}
	return s
}

func (q *Queue) observeReservationPending(r Reservation, pending []string) {
	if c := q.memoryAdmissionCapture; c != nil {
		if c.reservations == nil {
			c.reservations = map[string]MemoryAdmissionReservation{}
		}
		s := memoryAdmissionReservation(r, q.retainedJob(r.Job), pending, true)
		now := q.clock.Now()
		s.PendingObservedAt = &now
		c.reservations[r.ID] = s
	}
}

func memoryAdmissionKey(j *Job, m *Machine) string { return j.ID + "/" + targetKey(m) }

func (q *Queue) observeMemoryAdmissionDrift(j *Job, m *Machine, gb float64) {
	if c := q.memoryAdmissionCapture; c != nil {
		c.drift[memoryAdmissionKey(j, m)] = gb
	}
}

func (q *Queue) observeMemoryAdmission(j *Job, m *Machine) {
	c := q.memoryAdmissionCapture
	if c == nil || j.State != "queued" || j.MemoryEnvelope == nil || !q.jobAllowsMachine(j, m.ID) {
		return
	}
	if stored := q.s.Jobs[j.ID]; stored == nil || stored.State != "queued" {
		return
	}
	why := hostFitFailure(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if why == "" {
		why = j.MemoryEnvelope.Refusal
	}
	if why == "" {
		return
	}
	// Do not attribute an incompatible OS, device, runtime or recipe to RAM.
	capacity := q.qualificationCapacity(m)
	var fit targetFit
	fitsResourcesChecked(j, &capacity, capacity.TotalMemoryGB, &fit)
	for _, failed := range fit.failed {
		if failed != "memory envelope" {
			return
		}
	}
	ledger := q.hostMemoryLedgers[m.ID]
	if ledger == nil {
		return
	} // No ledger is unknown, never reconstructed from nominal receipts.
	if m.FreeHostMemoryGB != ledger.FreeGB || m.HostMemoryInUseGB != ledger.ReservedGB {
		return // Qualification and hypothetical release projections are not live comparisons.
	}
	s := MemoryAdmissionSnapshot{CapturedAt: q.clock.Now(), WaitingSince: j.WaitingSince, Attempt: j.Attempt, Machine: m.ID, Session: m.Session, Host: hostID(m), Device: m.Device, Target: machineTarget(m), Envelope: *j.MemoryEnvelope, DriftReserveGB: c.drift[memoryAdmissionKey(j, m)], Reason: why, Ledger: ledger}
	// Keep one deterministic representative: the least RAM shortfall, then
	// target identity. It records a comparison, never a placement promise.
	if old, ok := c.decisions[j.ID]; ok && old.generation == q.memoryLedgerGeneration {
		deficit := func(v MemoryAdmissionSnapshot) float64 { return max(0, v.Envelope.HostGB-v.Ledger.FreeGB) }
		if cmp.Or(cmp.Compare(deficit(old.snapshot), deficit(s)), cmp.Compare(targetKey(old.machine), targetKey(m))) <= 0 {
			return
		}
	}
	copy := *m
	s.Reservations = []MemoryAdmissionReservation{}
	for _, r := range q.s.Reservations {
		if r.active() && (r.Job == j.ID || r.Target.matchesTarget(m)) {
			p, known := c.reservations[r.ID]
			if !known {
				p = memoryAdmissionReservation(r, q.retainedJob(r.Job), nil, false)
			} else {
				observed := p.PendingObservedAt
				p = memoryAdmissionReservation(r, q.retainedJob(r.Job), p.Pending, true)
				p.PendingObservedAt = observed
			}
			s.Reservations = append(s.Reservations, p)
		}
	}
	slices.SortFunc(s.Reservations, func(a, b MemoryAdmissionReservation) int { return cmp.Compare(a.ID, b.ID) })
	c.decisions[j.ID] = memoryAdmissionDecision{snapshot: s, machine: &copy, generation: q.memoryLedgerGeneration}
}

func (q *Queue) retainMemoryAdmissions() {
	for id, d := range q.memoryAdmissionCapture.decisions {
		j := q.s.Jobs[id]
		if j == nil || j.Attempt != d.snapshot.Attempt || !j.WaitingSince.Equal(d.snapshot.WaitingSince) {
			continue
		}
		s := d.snapshot
		// Avoid rewriting every waiting record on identical comparisons. A
		// changed decision replaces it; a terminal startup capture retains its
		// final timestamp. Existing job history bounds storage, not a new log.
		if old := j.LastMemoryAdmission; old != nil && old.Ledger != nil && !terminal(j.State) {
			before, after := *old, s
			before.CapturedAt = after.CapturedAt
			left, right := *before.Ledger, *after.Ledger
			left.CapturedAt = right.CapturedAt
			left.ObservedAt = right.ObservedAt
			before.Ledger, after.Ledger = &left, &right
			if len(before.Reservations) == len(after.Reservations) {
				before.Reservations = slices.Clone(before.Reservations)
				for i := range before.Reservations {
					before.Reservations[i].PendingObservedAt = after.Reservations[i].PendingObservedAt
				}
			}
			if reflect.DeepEqual(before, after) {
				continue
			}
		}
		q.touchJob(id).LastMemoryAdmission = &s
	}
}
