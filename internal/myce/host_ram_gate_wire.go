package myce

const hostRAMGateHeader = "X-Myce-Host-RAM-Gate"
const hostRAMGateProtocol = "host-ram-gate-v1"

// Workers roll out before the queue, and the previous queue strictly decodes
// worker requests. Send the RAM gate, native startup evidence and pool-stop
// telemetry only while the queue advertises them. Local copies keep them.
func hostRAMGateWireInput(in any, supported bool) (any, bool) {
	switch c := in.(type) {
	case *Checkin:
		if c != nil {
			return hostRAMGateWireInput(*c, supported)
		}
	case *Result:
		if c != nil {
			return hostRAMGateWireInput(*c, supported)
		}
	case *workerLaunchRequest:
		if c != nil {
			return hostRAMGateWireInput(*c, supported)
		}
	case *HostLaunch:
		if c != nil {
			return hostRAMGateWireInput(*c, supported)
		}
	case Checkin:
		// Worker.checkin retries a refusal once with all its additive fields
		// removed, so a rollback costs one refused check-in, not two.
		if !supported {
			c.Machine.HostRAMGate = nil
		}
		return c, false
	case workerLaunchRequest:
		l, present := hostRAMGateWireInput(c.HostLaunch, supported)
		c.HostLaunch = l.(HostLaunch)
		return c, present
	case HostLaunch:
		if c.NativeStartup == "" && c.NativeResidentGB == nil || supported {
			return c, c.NativeStartup != "" || c.NativeResidentGB != nil
		}
		c.NativeStartup, c.NativeResidentGB = "", nil
		return c, false
	case Result:
		startup := func(cause *RuntimeCause) bool {
			return cause != nil && cause.Admission != nil && cause.Admission.StartupGB != 0
		}
		stop := c.PriorityStop != nil && (c.PriorityStop.MemoryPool != nil || c.PriorityStop.DevicePool != nil)
		present := stop || startup(c.ExecutionCause) || startup(c.CleanupCause)
		if !present || supported {
			return c, present
		}
		strip := func(cause *RuntimeCause) *RuntimeCause {
			if !startup(cause) {
				return cause
			}
			copy, admission := *cause, *cause.Admission
			admission.StartupGB = 0
			copy.Admission = &admission
			return &copy
		}
		c.ExecutionCause, c.CleanupCause = strip(c.ExecutionCause), strip(c.CleanupCause)
		if stop {
			copy := *c.PriorityStop
			copy.MemoryPool, copy.DevicePool = nil, nil
			c.PriorityStop = &copy
		}
		return c, false
	}
	return in, false
}
