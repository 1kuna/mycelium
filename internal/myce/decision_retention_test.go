package myce

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDecisionRetentionFinalWithdrawalHandoff(t *testing.T) {
	q, _, _ := decisionFixture(t)
	c := q.control
	c.appendDecision(ControlEvent{Kind: "decision", Decision: &DecisionReceipt{JobID: "queued"}}, 8<<20)
	prepared, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.decisionReleasePrepared = func() { once.Do(func() { close(prepared); <-resume }) }
	done := make(chan struct{})
	go func() {
		c.appendDecision(ControlEvent{}, 8<<20)
		close(done)
	}()
	<-prepared // The old limit was read and its trim finished.
	c.appendDecision(ControlEvent{}, 0)
	close(resume)
	<-done
	c.decisionReleasePrepared = nil
	if c.decisionCapacity != 0 || c.decisionBytes != 0 {
		t.Fatal("final release lost the concurrent withdrawal")
	}
}

func TestDecisionRetentionSelectionAllowsControlProgress(t *testing.T) {
	q, _, _ := decisionFixture(t)
	c := q.control
	for i := 0; i < 128; i++ {
		c.appendDecision(ControlEvent{Kind: "decision", Decision: &DecisionReceipt{JobID: ID()}}, 8<<20)
	}
	prepared, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.decisionRetentionPrepared = func() { once.Do(func() { close(prepared); <-resume }) }
	done := make(chan struct{})
	go func() { c.appendDecision(ControlEvent{}, 0); close(done) }()
	<-prepared
	progress := make(chan struct{})
	go func() { q.PriorityPolicy(); close(progress) }()
	select {
	case <-progress:
	case <-time.After(2 * time.Second):
		close(resume)
		<-done
		t.Fatal("history selection held the shared control mutex")
	}
	close(resume)
	<-done
	c.decisionRetentionPrepared = nil
	if c.decisionBytes != 0 {
		t.Fatal("withdrawal did not shed history")
	}
}

func TestDecisionRetentionGeneralControlReadOmitsPayload(t *testing.T) {
	q, _, _ := decisionFixture(t)
	r := &DecisionReceipt{JobID: "queued", Target: "target", CheckinWorker: "worker", Reply: DecisionReply{Directives: []DecisionDirective{{Execution: Execution{ID: "offered", Attempt: 1}, Offer: true}}}}
	q.control.appendDecision(ControlEvent{Kind: "decision", Operation: "checkin", Decision: r}, 8<<20)
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	page, err := q.controlPage(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range page.Entries {
		if e.Kind == "decision" {
			found = true
			if e.Decision != nil || !e.DecisionOmitted {
				t.Fatal("durable general read hydrated payload")
			}
		}
	}
	for _, e := range page.Telemetry.Recent {
		if e.Decision != nil {
			t.Fatal("general telemetry retained payload in its response")
		}
	}
	targeted, err := q.DecisionReceipts(context.Background(), "queued")
	if err != nil || !found || len(targeted.Receipts) != 1 || len(targeted.Receipts[0].Reply.Directives) != 1 {
		t.Fatalf("targeted durable evidence was lost: %+v %v", targeted, err)
	}
}

func TestDecisionRetentionStatusRefreshAfterCapacityWithdrawal(t *testing.T) {
	q, clock, client := decisionFixture(t)
	r := &DecisionReceipt{JobID: "queued"}
	for i := 0; i < 1024; i++ {
		r.Reply.Directives = append(r.Reply.Directives, DecisionDirective{Execution: Execution{ID: ID(), Attempt: 1}, Offer: true})
	}
	q.control.appendDecision(ControlEvent{Kind: "decision", Decision: r}, 8<<20)
	if err := q.control.flush(q.db); err != nil {
		t.Fatal(err)
	}
	read := func() Snapshot {
		t.Helper()
		var out Snapshot
		if err := client.Call(context.Background(), "GET", "/api/status", nil, &out); err != nil {
			t.Fatal(err)
		}
		if out.ControlTelemetry == nil {
			t.Fatal("missing general telemetry")
		}
		found := false
		for _, e := range out.ControlTelemetry.Recent {
			if e.Decision != nil {
				t.Fatal("status hydrated historical receipt payload")
			}
			if e.Kind == "decision" {
				found = true
				if !e.DecisionOmitted {
					t.Fatal("status hid payload omission")
				}
			}
		}
		if !found {
			t.Fatal("missing retained decision marker")
		}
		return out
	}
	first := read()
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 64 << 10, nil }
	clock.Advance(2 * time.Second) // Expire the general status cache generation.
	second := read()
	if !second.Freshness.CapturedAt.After(first.Freshness.CapturedAt) {
		t.Fatal("status did not refresh")
	}
	// Withdrawal affects the read's current share; no subsequent check-in has
	// trimmed the recorder's historical, larger-budget payload. It must stay
	// absent from both refreshed status and its encoded cache.
	foundPayload := false
	for _, e := range q.control.recent {
		foundPayload = foundPayload || e.Decision != nil
	}
	if !foundPayload {
		t.Fatal("fixture lost historical payload before refresh")
	}
	read()
}

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
