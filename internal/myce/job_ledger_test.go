package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func ledgerCustody(t *testing.T, q *Queue, j *Job, id, phase string) {
	t.Helper()
	if err := q.NativeCustody(NativeCustodyReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, ProcessID: fmt.Sprintf("exec-%d", j.Attempt), CustodyID: id, Phase: phase}); err != nil {
		t.Fatal(err)
	}
}

// #298: every planned requeue and generation is retained beyond both diagnostic
// caps, with a stable snapshot while new generations arrive between pages.
func TestJobLedgerRequeuesCustodyPaginationRestart(t *testing.T) {
	q, clock := fixture(t)
	m := machine("one", "fake")
	receipt := submit(t, q, spec("experiment"))
	var j *Job
	for attempt := 1; attempt <= 18; attempt++ {
		j = check(t, q, m, nil).Job
		if j == nil || j.Attempt != attempt {
			t.Fatalf("assignment %d: %+v", attempt, j)
		}
		for gen := 1; gen <= 2; gen++ {
			id := fmt.Sprintf("custody-%d-%d", attempt, gen)
			ledgerCustody(t, q, j, id, "reserved")
			clock.Advance(time.Second)
			ledgerCustody(t, q, j, id, "started")
			if gen == 1 {
				ledgerCustody(t, q, j, id, "released")
			}
		}
		r := result(j, false)
		if err := q.Started(r); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		r.Interrupted, r.OperatorStop = true, true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	if len(q.s.Jobs[j.ID].AttemptHistory) != attemptHistoryLimit {
		t.Fatal("diagnostic cap changed")
	}
	q = reopenCancellationQueue(t, q, clock)
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	page, err := client.JobLedger(context.Background(), receipt.ID, 0, -1, 7)
	if err != nil || !page.Complete || !page.HasMore || page.HighWater <= 32 {
		t.Fatalf("first page: %+v %v", page, err)
	}
	high := page.HighWater
	// A new assignment/custody cannot extend an already chosen snapshot.
	j = check(t, q, m, nil).Job
	ledgerCustody(t, q, j, "after-snapshot", "reserved")
	var entries []JobLedgerEntry
	for {
		entries = append(entries, page.Entries...)
		if !page.HasMore {
			break
		}
		page, err = client.JobLedger(context.Background(), receipt.ID, page.Next, high, 7)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(entries) != high || page.Next != high {
		t.Fatal("snapshot incomplete", len(entries), high)
	}
	counts := map[string]int{}
	for i, e := range entries {
		if e.Sequence != i+1 || e.Machine != m.ID || e.Session != m.Session || e.Reason == "" || e.At.IsZero() {
			t.Fatal("lost identity/ordering", e)
		}
		counts[e.Kind]++
		if e.CustodyID != "" && e.ExecutionID == "" {
			t.Fatal("custody lost execution", e)
		}
		if e.Kind == "custody_started" && e.CustodyStart == nil {
			t.Fatal("missing start", e)
		}
		if e.Kind == "custody_released" && (e.CustodyEnd == nil || e.Release != "confirmed") {
			t.Fatal("missing release", e)
		}
	}
	if counts["assigned"] != 18 || counts["started"] != 18 || counts["outcome"] != 18 || counts["custody_reserved"] != 36 || counts["custody_started"] != 36 || counts["custody_released"] != 36 || counts["released"] != 18 {
		t.Fatal("lost transition", counts)
	}
	got, err := client.GetJob(context.Background(), receipt.ID)
	if err != nil || got.Ledger == nil || !got.Ledger.Complete || got.Ledger.HighWater <= high || len(got.LedgerEntries) != 0 {
		t.Fatal("GET summary", got, err)
	}
	for _, args := range [][3]int{{-1, high, 1}, {0, high + 100, 1}, {0, high, 257}, {high + 1, high, 1}} {
		if _, err := client.JobLedger(context.Background(), receipt.ID, args[0], args[1], args[2]); err == nil {
			t.Fatal("invalid cursor accepted", args)
		}
	}
}

func TestJobLedgerCustodyFencesAndRollback(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	j := check(t, q, machine("one", "fake"), nil).Job
	r := NativeCustodyReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, ProcessID: "exec-1", CustodyID: "group", Phase: "reserved"}
	if err := q.NativeCustody(r); err != nil {
		t.Fatal(err)
	}
	before := len(q.s.Jobs[j.ID].LedgerEntries)
	if err := q.NativeCustody(r); err != nil || len(q.s.Jobs[j.ID].LedgerEntries) != before {
		t.Fatal("duplicate reservation", err)
	}
	r.Session = "stale"
	if err := q.NativeCustody(r); !errors.Is(err, ErrStale) {
		t.Fatal("stale custody accepted", err)
	}
	r.Session = j.Session
	r.Phase = "started"
	if _, err := q.db.Exec(`CREATE TRIGGER ledger_fail BEFORE UPDATE ON queue_state BEGIN SELECT RAISE(ABORT,'ledger failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.NativeCustody(r); err == nil {
		t.Fatal("failed commit permitted execution")
	}
	if len(q.s.Jobs[j.ID].LedgerEntries) != before {
		t.Fatal("failed commit retained transition")
	}
	if _, err := q.db.Exec(`DROP TRIGGER ledger_fail`); err != nil {
		t.Fatal(err)
	}
	if err := q.NativeCustody(r); err != nil {
		t.Fatal(err)
	}
	// Release-only proof after loss settles the exact old generation, without
	// accepting its stale output as a current result.
	old := result(j, true)
	if err := q.update(func() error { q.requeue(q.touchJob(j.ID), "planned", false); return nil }); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	next := check(t, q, machine("two", "fake"), nil).Job
	if next == nil {
		t.Fatal("no replacement")
	}
	if err := q.Complete(old); !errors.Is(err, ErrStale) {
		t.Fatal("stale result accepted", err)
	}
	page, err := q.JobLedger(j.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	for _, e := range page.Entries {
		if e.CustodyID == "group" && e.Release == "confirmed" {
			released = true
		}
	}
	if !released || q.s.Jobs[j.ID].Attempt != next.Attempt {
		t.Fatal("old release lost or replacement changed")
	}
	ledgerCustody(t, q, next, "cancelled-replay", "reserved")
	if _, err := q.CancelJob(next.ID); err != nil {
		t.Fatal(err)
	}
	replay := NativeCustodyReport{Execution: Execution{ID: next.ID, Attempt: next.Attempt}, Machine: next.Machine, Session: next.Session, ProcessID: fmt.Sprintf("exec-%d", next.Attempt), CustodyID: "cancelled-replay", Phase: "reserved"}
	if err := q.NativeCustody(replay); !errors.Is(err, ErrStale) {
		t.Fatal("reservation replay authorized after cancellation", err)
	}

}
