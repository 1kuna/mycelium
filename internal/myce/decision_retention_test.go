package myce

import (
	"testing"
	"time"
)

func TestDecisionRetentionDeferredHistoryBudget(t *testing.T) {
	q, clock, _ := decisionFixture(t)
	budget := 6 * decisionCost(&DecisionReceipt{})
	// Hold only the diagnostic writer, so successful capture cannot flush its
	// changing history. Its backlog and pending buffers must share the budget.
	q.control.writeGate.Lock()
	defer q.control.writeGate.Unlock()
	for i := 0; i < 256; i++ {
		clock.Advance(time.Millisecond)
		r := &DecisionReceipt{JobID: "queued", Worker: "worker", Target: "target", Admission: "project_cap", Memory: DecisionMemory{Evaluated: true, HostGB: float64(i)}}
		c := &decisionCapture{rows: map[decisionKey]*DecisionReceipt{{"queued", "target"}: r}, revision: uint64(i + 1), at: clock.Now(), early: "none"}
		q.publishDecisions(c, Reply{}, "reply_ready", budget)
		if err := q.control.flush(q.db); err != nil {
			t.Fatal(err)
		}
		q.control.mu.Lock()
		seen := map[*DecisionReceipt]bool{}
		used := int64(0)
		for _, events := range [][]ControlEvent{q.control.pending, q.control.backlog, q.control.recent, q.control.drainBuffer, q.control.spare[:cap(q.control.spare)]} {
			for _, event := range events {
				if event.Decision != nil && !seen[event.Decision] {
					seen[event.Decision] = true
					used += decisionCost(event.Decision)
				}
			}
		}
		q.control.mu.Unlock()
		if used > budget/2 || used+q.decisions.bytes > budget {
			t.Fatalf("deferred history escaped share: recorder=%d cache=%d budget=%d", used, q.decisions.bytes, budget)
		}
	}
	q.publishDecisions(&decisionCapture{}, Reply{}, "reply_ready", 0)
	if q.control.decisionBytes != 0 || q.decisions.bytes != 0 || q.control.dropped == 0 {
		t.Fatal("withdrawn capacity retained payloads or hid shedding")
	}
}

func TestDecisionRetentionBusyFlusherDoesNotWait(t *testing.T) {
	q, _, _ := decisionFixture(t)
	q.control.flushMu.Lock()
	defer q.control.flushMu.Unlock()
	before := q.control.dropped
	q.control.appendDecision(ControlEvent{Decision: &DecisionReceipt{}}, 8<<20)
	if q.control.dropped != before+1 {
		t.Fatal("busy diagnostic flusher did not report refused durability")
	}
}

func TestDecisionRetentionWithdrawalDuringFlush(t *testing.T) {
	q, _, _ := decisionFixture(t)
	c := q.control
	c.appendDecision(ControlEvent{Kind: "decision", Decision: &DecisionReceipt{JobID: "queued"}}, 8<<20)
	c.storageProbe = func() (controlDiskBudget, error) {
		// This executes with flushMu held, before SQL. The withdrawal must
		// neither wait on that mutex nor leave the old payload behind.
		c.appendDecision(ControlEvent{}, 0)
		return controlDiskBudget{}, nil
	}
	if err := c.flush(q.db); err != nil {
		t.Fatal(err)
	}
	if c.decisionBytes != 0 || c.decisionCapacity != 0 {
		t.Fatalf("flush retained withdrawn capacity: bytes=%d capacity=%d", c.decisionBytes, c.decisionCapacity)
	}
}
