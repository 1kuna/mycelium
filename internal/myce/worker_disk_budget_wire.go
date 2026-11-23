package myce

import "slices"

const workerDiskBudgetProtocol = "worker-disk-budget-v1"

// Queues advertising this accept the foreign-headroom fields on disk budgets.
const workerDiskHeadroomProtocol = "worker-disk-headroom-v1"

// Queues advertising this accept each volume's decline age and cause (#834).
const workerDiskTrendProtocol = "worker-disk-trend-v1"

// Additive disk fields wait together for an advertisement on the serialized
// check-in lane. Foreign-consumer detail fields negotiate on the same lane.
type workerDiskBudgetWire struct {
	machine, session   string
	supported, foreign bool
}

func (w Worker) diskBudgetCheckin(c Checkin) Checkin {
	if r := w.diskBudgetWire; r == nil || !r.foreign || r.machine != c.Machine.ID || r.session != c.Machine.Session {
		c = withoutForeignDetailFields(c)
	}
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

// Copies host health; local evidence keeps the detail for the next queue.
func withoutForeignDetailFields(c Checkin) Checkin {
	h := c.Machine.HostHealth
	if h == nil || len(h.Foreign) == 0 && len(h.ForeignServices) == 0 {
		return c
	}
	copy := *h
	copy.ForeignServices = nil
	copy.Foreign = append([]ForeignConsumer(nil), h.Foreign...)
	for i := range copy.Foreign {
		copy.Foreign[i].User, copy.Foreign[i].AgeSeconds, copy.Foreign[i].GPUBytes = "", 0, 0
	}
	c.Machine.HostHealth = &copy
	return c
}

func hasForeignDetailFields(c Checkin) bool {
	h := c.Machine.HostHealth
	return h != nil && (len(h.ForeignServices) > 0 || slices.ContainsFunc(h.Foreign, func(f ForeignConsumer) bool { return f.User != "" || f.AgeSeconds != 0 || f.GPUBytes != 0 }))
}

func hasDiskBudgetWireFields(c Checkin) bool {
	r := c.Machine.HygieneCollection
	return r != nil && (len(r.DiskBudget) > 0 || r.Scratch != nil || r.CompilerWarning != "" || r.UnmanagedCompilerBytes != 0)
}
