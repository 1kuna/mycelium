package myce

import (
	"slices"
	"time"
)

// Receipt history shares the measured envelope with the latest cache.
// The flusher owns payload mutations through flushMu. Captures never wait for
// its SQL; c.mu protects only constant-size handoffs and individual mutations.
func (c *controlRecorder) appendDecision(e ControlEvent, budget int64) {
	c.mu.Lock()
	c.decisionLimit = budget
	owned := c.flushMu.TryLock()
	if !owned && e.Decision != nil {
		c.dropped++
	}
	c.mu.Unlock()
	if !owned {
		return
	}
	defer c.releaseDecisionFlush()
	c.retainDecision(e, c.decisionBudgetLimit())
}

func (c *controlRecorder) decisionBudgetLimit() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.decisionLimit
}

// Publishing a withdrawal and releasing flush ownership use the same mutex.
// A withdrawal either belongs to this owner's final trim or acquires/joins the
// next owner; it cannot fall between the final limit check and flushMu.Unlock.
func (c *controlRecorder) releaseDecisionFlush() {
	for {
		budget := c.decisionBudgetLimit()
		c.retainDecision(ControlEvent{}, budget)
		if c.decisionReleasePrepared != nil {
			c.decisionReleasePrepared()
		}
		c.mu.Lock()
		if c.decisionLimit == budget {
			c.flushMu.Unlock()
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

// Caller owns flushMu. Ordinary producers only append beyond these immutable
// prefixes; snapshotting three slice headers needs no payload scan under c.mu.
func (c *controlRecorder) retainDecision(e ControlEvent, budget int64) {
	c.mu.Lock()
	buffers := [3][]ControlEvent{c.pending, c.backlog, c.recent}
	c.mu.Unlock()
	type victim struct {
		event *ControlEvent
		cost  int64
		value int
		at    time.Time
	}
	var victims []victim
	used := int64(0)
	for _, events := range buffers {
		for i := range events {
			if r := events[i].Decision; r != nil {
				cost := decisionCost(r)
				used += cost // Repeated references are conservatively charged.
				victims = append(victims, victim{&events[i], cost, decisionValue(*r), r.At})
			}
		}
	}
	cost, value := int64(0), 3
	if e.Decision != nil {
		cost, value = decisionCost(e.Decision), decisionValue(*e.Decision)
	}
	// All scanning, sizing and ordering occur outside the shared control mutex.
	// Victim metadata fits the retained versions' conservative two-copy charge.
	slices.SortFunc(victims, func(a, b victim) int {
		if a.value != b.value {
			return a.value - b.value
		}
		return a.at.Compare(b.at)
	})
	if c.decisionRetentionPrepared != nil {
		c.decisionRetentionPrepared()
	}
	c.mu.Lock()
	c.decisionCapacity, c.decisionBytes = budget, used
	c.mu.Unlock()
	for _, v := range victims {
		if used+cost <= budget || v.value > value {
			break
		}
		c.mu.Lock()
		v.event.Decision, v.event.DecisionOmitted = nil, true
		c.decisionBytes -= v.cost
		c.dropped++
		c.mu.Unlock()
		used -= v.cost
	}
	if e.Decision == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if cost > budget-used || len(c.pending) == cap(c.pending) {
		c.dropped++
		return
	}
	c.appendLocked(e)
	c.decisionBytes += cost
}
