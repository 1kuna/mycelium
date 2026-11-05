package myce

const hostHygieneProtocol = "host-hygiene-v1"

// Negotiated by the serialized check-in lane. Older queues strictly decode
// requests, so local evidence stays durable without sending unknown fields.
type workerHygieneWire struct {
	machine, session string
	supported        bool
}

func (w Worker) hygieneCheckin(c Checkin) Checkin {
	if r := w.hygieneWire; r != nil {
		if r.machine != c.Machine.ID || r.session != c.Machine.Session {
			*r = workerHygieneWire{machine: c.Machine.ID, session: c.Machine.Session}
		}
		if r.supported {
			return c
		}
	}
	return w.legacyHygieneCheckin(c)
}

func (w Worker) legacyHygieneCheckin(c Checkin) Checkin {
	if c.Machine.HostHealth != nil && c.Machine.HostHealth.DrainReason != "" {
		// The old leaving signal withholds admission without releasing custody.
		c.Machine.Leaving = true
	}
	c.Machine.HostHealth = nil
	return c
}

func (w Worker) acceptHygieneReply(reply Reply) {
	if w.hygieneWire != nil {
		w.hygieneWire.supported = reply.HostHygieneProtocol == hostHygieneProtocol
	}
}
