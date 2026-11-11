package myce

const workerDiskBudgetProtocol = "worker-disk-budget-v1"

// r6.8 strictly decodes check-ins and predates disk-budget telemetry. Only
// these new fields wait for an advertisement on the serialized check-in lane.
type workerDiskBudgetWire struct {
	machine, session string
	supported        bool
}

func (w Worker) diskBudgetCheckin(c Checkin) Checkin {
	if r := w.diskBudgetWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerDiskBudgetWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if r.supported {
			return c
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

func hasDiskBudgetWireFields(c Checkin) bool {
	r := c.Machine.HygieneCollection
	return r != nil && (len(r.DiskBudget) > 0 || r.Scratch != nil || r.CompilerWarning != "" || r.UnmanagedCompilerBytes != 0)
}
