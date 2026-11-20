package myce

// A planned replacement is distinct from sudden host loss. Unknown custodian
// ownership survives an unhealthy update and operation rerun; a timeout is not
// permission to duplicate still-running non-resumable work on another host.
type workerHandover struct {
	Session  string                `json:"session"`
	PID      int                   `json:"pid"`
	Attempts []Execution           `json:"attempts"`
	Models   []HostEngineExecution `json:"models,omitempty"`
}

func (q *Queue) beginWorkerHandover(h *fleetUpgradeHost) {
	m := q.s.Machines[h.Machine]
	if m == nil || m.Fleet == nil || m.Fleet.Adoption == nil {
		return
	}
	v := workerHandover{Session: m.Session, PID: m.Fleet.PID}
	for _, o := range q.hostObligations(h.Host) {
		if o.Engine != nil {
			v.Models = append(v.Models, *o.Engine)
			continue
		}
		if o.Call != nil {
			continue // Calls share the exact detached engine/bridge owner.
		}
		v.Attempts = append(v.Attempts, Execution{ID: o.Result.ID, Attempt: o.Result.Attempt})
	}
	if len(v.Attempts) == 0 && len(v.Models) == 0 {
		return
	}
	if q.s.WorkerHandovers == nil {
		q.s.WorkerHandovers = map[string]workerHandover{}
	}
	q.s.WorkerHandovers[m.ID] = v
}

// Does not refresh timestamps, inventory or admission. It retains only exact
// pre-replacement executions until a replacement proves adoption or those
// executions positively release. Operator cancellation remains authoritative.
func (q *Queue) retainWorkerHandover(m *Machine) bool {
	v, ok := q.s.WorkerHandovers[m.ID]
	if !ok {
		return false
	}
	pending := false
	for _, e := range v.Attempts {
		for _, o := range q.hostObligations(hostID(m)) {
			pending = pending || o.Result.Machine == m.ID && o.Result.Session == v.Session && o.Result.ID == e.ID && o.Result.Attempt == e.Attempt
		}
	}
	adopted := m.Session == v.Session && q.fleetProofFresh(m.Fleet) && m.Fleet.PID != v.PID && m.Fleet.Adoption != nil && m.Fleet.Adoption.Protocol == custodianProtocol && m.Fleet.Adoption.Reason == ""
	if adopted {
		for _, e := range v.Attempts {
			found := false
			for _, a := range m.Fleet.Adoption.Attempts {
				found = found || a.ID == e.ID && a.Attempt == e.Attempt
			}
			// A released execution no longer needs live adoption evidence.
			if !found {
				for _, o := range q.hostObligations(hostID(m)) {
					if o.Result.Machine == m.ID && o.Result.Session == v.Session && o.Result.ID == e.ID && o.Result.Attempt == e.Attempt {
						adopted = false
					}
				}
			}
		}
	}
	for _, e := range v.Models {
		for _, o := range q.hostObligations(hostID(m)) {
			if o.Engine != nil && *o.Engine == e {
				pending = true
				if q.modelAdoptionReason(m, o) != "" {
					adopted = false
				}
			}
		}
	}
	if !pending || adopted {
		delete(q.s.WorkerHandovers, m.ID)
		return false
	}
	return true
}
