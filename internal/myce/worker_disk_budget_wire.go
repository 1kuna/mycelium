package myce

const workerDiskBudgetProtocol = "worker-disk-budget-v1"

// Queues advertising this accept the foreign-headroom fields on disk budgets.
const workerDiskHeadroomProtocol = "worker-disk-headroom-v1"

// The previous release strictly decodes check-ins and predates disk headroom
// telemetry. Only additive fields wait for an advertisement on the serialized
// check-in lane; a strict refusal retries once without them.
type workerDiskBudgetWire struct {
	machine, session    string
	supported, headroom bool
}

func (w Worker) diskBudgetCheckin(c Checkin) Checkin {
	if r := w.diskBudgetWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerDiskBudgetWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if r.supported && r.headroom {
			return c
		}
		if r.supported {
			return withoutDiskHeadroomFields(c)
		}
	}
	return withoutDiskBudgetFields(c)
}

// Preserve the baseline collection and durable local evidence on old queues.
func withoutDiskBudgetFields(c Checkin) Checkin {
	if c.Machine.HygieneCollection != nil {
		r := *c.Machine.HygieneCollection
		r.DiskBudget, r.Scratch, r.CompilerWarning = nil, nil, ""
		r.UnmanagedCompilerBytes = 0
		c.Machine.HygieneCollection = &r
	}
	return c
}

// Copies the collection; local evidence keeps its headroom fields.
func withoutDiskHeadroomFields(c Checkin) Checkin {
	if c.Machine.HygieneCollection == nil || len(c.Machine.HygieneCollection.DiskBudget) == 0 {
		return c
	}
	r := *c.Machine.HygieneCollection
	r.DiskBudget = append([]DiskBudgetVolume(nil), r.DiskBudget...)
	for i := range r.DiskBudget {
		v := &r.DiskBudget[i]
		v.ForeignBytes, v.ForeignPeakBytes, v.ForeignPeakGrowthBytes, v.ForeignHeadroomBytes = 0, 0, 0, 0
	}
	c.Machine.HygieneCollection = &r
	return c
}

func hasDiskBudgetWireFields(c Checkin) bool {
	r := c.Machine.HygieneCollection
	return r != nil && (len(r.DiskBudget) > 0 || r.Scratch != nil || r.CompilerWarning != "" || r.UnmanagedCompilerBytes != 0)
}
