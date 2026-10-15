package hostbridge

// Session contains only opaque session ID and connection state, no user names,
// station names, process inventory, window contents, or input events.
type Session struct{ ID, State uint32 }

// Covered deliberately supports only one active local console session. RDP,
// fast user switching, disconnected sessions, and uncertain transitions withhold.
// State 6 is a listener, not a user. Session zero never supplies owner idle.
func Covered(self, console uint32, sessions []Session) bool {
	if self == 0 || self != console || console == 0xffffffff {
		return false
	}
	found := false
	for _, s := range sessions {
		if s.ID == 0 || s.State == 6 {
			continue
		}
		if s.ID != self || s.State != 0 || found {
			return false
		}
		found = true
	}
	return found
}

// Idle handles GetLastInputInfo's 32-bit tick wrap. A future/nonmonotonic
// anomaly cannot turn into a huge idle age. Longer-than-half-wrap is unknown.
func Idle(now, last uint32) (float64, bool) {
	elapsed := now - last
	if elapsed > 0x7fffffff {
		return 0, false
	}
	return float64(elapsed) / 1000, true
}
func Power(ac, battery, saver byte) (string, bool) {
	laptop := battery != 128
	if battery == 255 || (battery != 128 && battery & ^byte(15) != 0) || ac > 1 || saver > 1 {
		return "unknown", laptop
	}
	if ac == 0 || saver == 1 {
		return "battery", laptop
	}
	return "ac", laptop
}

// Continuity caps idle at the continuously observed awake interval. Startup,
// missing coverage, wake/large sample gaps and clock rollback require five new
// observed minutes even if GetLastInputInfo includes time spent asleep.
// now is GetTickCount64 in milliseconds (including suspend), never wall time.
type Continuity struct {
	initialized bool
	start, last uint64
	input       uint32
}

func (c *Continuity) Observe(now uint64, lastInput uint32) (float64, bool) {
	raw, known := Idle(uint32(now), lastInput)
	if !known {
		*c = Continuity{}
		return 0, false
	}
	if !c.initialized || now < c.last || now-c.last > 15000 || lastInput != c.input {
		c.start = now
		c.initialized = true
	}
	c.last = now
	c.input = lastInput
	return min(raw, float64(now-c.start)/1000), true
}
