package myce

// Receipt history shares the measured observation envelope with the latest
// cache. Only this helper admits variable receipt payloads to the recorder.
// A flusher owns its immutable batch through flushMu; capture never waits on its
// SQL. Refusal leaves the latest cache readable and counts missing durability.
func (c *controlRecorder) appendDecision(e ControlEvent, budget int64) {
	c.decisionLimit.Store(budget)
	if !c.flushMu.TryLock() {
		if e.Decision != nil {
			c.mu.Lock()
			c.dropped++
			c.mu.Unlock()
		}
		return
	}
	defer c.flushMu.Unlock()
	c.retainDecision(e, c.decisionLimit.Load())
}

// Caller owns flushMu; no immutable flusher batch can retain shed payloads.
func (c *controlRecorder) retainDecision(e ControlEvent, budget int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decisionCapacity = budget
	buffers := [][]ControlEvent{c.pending, c.backlog, c.recent}
	c.decisionBytes = 0
	for _, events := range buffers {
		for _, event := range events {
			if event.Decision != nil {
				// Charge repeated references conservatively; no scratch map.
				c.decisionBytes += decisionCost(event.Decision)
			}
		}
	}
	cost, value := int64(0), 3
	if e.Decision != nil {
		cost, value = decisionCost(e.Decision), decisionValue(*e.Decision)
	}
	for c.decisionBytes+cost > budget {
		var victim *ControlEvent
		for _, events := range buffers {
			for i := range events {
				item := &events[i]
				r := item.Decision
				if r == nil || decisionValue(*r) > value {
					continue
				}
				if victim == nil || decisionValue(*r) < decisionValue(*victim.Decision) || decisionValue(*r) == decisionValue(*victim.Decision) && r.At.Before(victim.Decision.At) {
					victim = item
				}
			}
		}
		if victim == nil {
			break
		}
		c.decisionBytes -= decisionCost(victim.Decision)
		victim.Decision, victim.DecisionOmitted = nil, true
		c.dropped++
	}
	if e.Decision == nil {
		return
	}
	if cost > budget-c.decisionBytes || len(c.pending) == cap(c.pending) {
		c.dropped++
		return
	}
	c.appendLocked(e)
	c.decisionBytes += cost
}
