package myce

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestJobLedgerRetentionAndExpiry(t *testing.T) {
	q, clock := fixture(t)
	s := keyedSpec()
	s.Priority = "experiment"
	j := submit(t, q, s)
	a := check(t, q, machine("one", "fake"), nil).Job
	ledgerCustody(t, q, a, "held", "reserved")
	ledgerCustody(t, q, a, "held", "started")
	r := result(a, true)
	r.ReleasePending = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * jobHistoryRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if page, err := q.JobLedger(j.ID, 0, -1, 256); err != nil || len(page.Entries) == 0 {
		t.Fatal("unresolved ledger pruned", page, err)
	}
	r.ReleaseOnly = true
	r.ReleasePending = false
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Jobs[j.ID] != nil {
		t.Fatal("released job not pruned")
	}
	page, err := q.JobLedger(j.ID, 0, -1, 256)
	if err != nil || len(page.Entries) == 0 {
		t.Fatal("keyed replay ledger lost", page, err)
	}
	q = reopenCancellationQueue(t, q, clock)
	again, err := q.JobLedger(j.ID, 0, page.HighWater, 256)
	if err != nil || len(again.Entries) != len(page.Entries) {
		t.Fatal("replay restart lost ledger", again, err)
	}
	clock.Advance(submissionRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	_, err = q.JobLedger(j.ID, 0, -1, 256)
	var api *HTTPError
	if !errors.As(err, &api) || api.Status != 410 || api.Code != "job_ledger_unavailable" {
		t.Fatal("expiry reported as empty history", err)
	}
	// Older retained jobs never claim complete history merely because arrays are empty.
	if err := q.update(func() error { q.putJob("legacy", &Job{ID: "legacy", Attempt: 1, State: "running"}); return nil }); err != nil {
		t.Fatal(err)
	}
	legacy, err := q.JobLedger("legacy", 0, -1, 256)
	if err != nil || legacy.Complete {
		t.Fatal("legacy history manufactured", legacy, err)
	}
	data, _ := json.Marshal(again)
	if strings.Contains(string(data), "ledger_entries") {
		t.Fatal("private storage shape exposed")
	}
}
