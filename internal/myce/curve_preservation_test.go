package myce

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func preservationSnapshot(t *testing.T, q *Queue) Snapshot {
	t.Helper()
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCurvePreservationAcceptedLearning(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	empty := preservationSnapshot(t, q)
	j := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	peak := 2.0
	r := result(&j, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	first := preservationSnapshot(t, q)
	pages := []JobLedgerPage{}
	addPage := func(id string) {
		p, err := q.JobLedger(id, 0, -1, 256)
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, p)
	}
	addPage(j.ID)
	receipt, err := VerifyCurvePreservation(empty, first, pages)
	if err != nil || len(receipt.Changes) != 1 || receipt.Changes[0].Evidence[0].Job != j.ID {
		t.Fatal(receipt, err)
	}
	// A new accepted timed sample changes rate, totals, provenance and peak.
	second := startObservation(t, q, m, s)
	clock.Advance(3 * time.Second)
	peak = 3
	r = result(&second, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	measured := preservationSnapshot(t, q)
	addPage(second.ID)
	if _, err = VerifyCurvePreservation(first, measured, pages); err != nil {
		t.Fatal(err)
	}
	// Occupancy is unavailable, but the accepted memory sample still learns.
	third := startObservation(t, q, m, s)
	q.s.Jobs[third.ID].Timing.Reason = "occupancy unavailable"
	clock.Advance(time.Second)
	r = result(&third, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	memory := preservationSnapshot(t, q)
	addPage(third.ID)
	a, b := measured.WorkCurves[0], memory.WorkCurves[0]
	if a.Rate != b.Rate || a.Job != b.Job || !a.RecordedAt.Equal(b.RecordedAt) || b.Samples != a.Samples+1 || !b.MemoryRecordedAt.After(a.MemoryRecordedAt) {
		t.Fatal("not a memory-only update", a, b)
	}
	if _, err = VerifyCurvePreservation(measured, memory, pages); err != nil {
		t.Fatal(err)
	}
	if _, err = VerifyCurvePreservation(first, memory, pages); err != nil {
		t.Fatal("multi-sample chain", err)
	}
	// Verify accepted provenance survives durable queue serialization.
	var stored Job
	var data []byte
	if err = q.db.QueryRow("SELECT data FROM queue_state WHERE key=?", "jobs/"+third.ID).Scan(&data); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range stored.LedgerEntries {
		if e.WorkLearning != nil {
			found = true
		}
	}
	if !found {
		t.Fatal("accepted evidence not persisted")
	}
	// Fail closed on actual loss/reset, unexplained changes and missing evidence.
	for name, mutate := range map[string]func(*Snapshot){
		"removed":     func(s *Snapshot) { s.WorkCurves = nil },
		"reset":       func(s *Snapshot) { s.WorkCurves[0].Samples = 0 },
		"clock":       func(s *Snapshot) { s.WorkCurves[0].RecordedAt = a.RecordedAt.Add(-time.Second) },
		"unexplained": func(s *Snapshot) { s.WorkCurves[0].Rate += 10 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := memory
			changed.WorkCurves = append([]WorkObservation(nil), memory.WorkCurves...)
			mutate(&changed)
			if _, err := VerifyCurvePreservation(measured, changed, pages); err == nil {
				t.Fatal("unsafe preservation accepted")
			}
		})
	}
	if _, err = VerifyCurvePreservation(measured, memory, nil); err == nil {
		t.Fatal("missing provenance accepted")
	}
	// A later local save/installer time is not the server snapshot boundary.
	later := measured
	later.Now = memory.Now.Add(time.Second)
	if _, err = VerifyCurvePreservation(later, memory, pages); err == nil {
		t.Fatal("wrong boundary accepted")
	}
	// Only derived availability labels may differ without learning.
	labels := measured
	labels.WorkCurves = append([]WorkObservation(nil), measured.WorkCurves...)
	labels.WorkCurves[0].StaleReason = "component unavailable"
	if _, err = VerifyCurvePreservation(measured, labels, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(measured.WorkCurves[0], a) {
		t.Fatal("guard mutated input")
	}
}

func TestCurvePreservationFailureInvalidation(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Priority = "production"
	before := preservationSnapshot(t, q)
	j := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&j, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	after := preservationSnapshot(t, q)
	// This explicit-resource fixture ends failed, with a new zero-sample key.
	if len(after.WorkCurves) != 1 || !after.WorkCurves[0].Forbidden {
		t.Fatal(after.WorkCurves)
	}
	verify := func(s Snapshot) error { _, err := VerifyCurvePreservation(before, s, nil); return err }
	if err := verify(after); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Snapshot){
		"missing job":             func(s *Snapshot) { s.Jobs = nil },
		"key":                     func(s *Snapshot) { s.WorkCurves[0].Key.Work = "other" },
		"attempt":                 func(s *Snapshot) { s.WorkCurves[0].Attempt++ },
		"machine":                 func(s *Snapshot) { s.WorkCurves[0].Machine = "other" },
		"session":                 func(s *Snapshot) { s.WorkCurves[0].Session = "other" },
		"precutover":              func(s *Snapshot) { s.WorkCurves[0].RecordedAt = before.Now },
		"unexplained measurement": func(s *Snapshot) { s.WorkCurves[0].Rate = 10 },
		"unexplained failure": func(s *Snapshot) {
			s.Jobs[0].AttemptHistory[0].OutOfMemory = false
			s.Jobs[0].AttemptHistory[0].ExecutionCause = nil
		},
		"old failure": func(s *Snapshot) { s.Jobs[0].AttemptHistory[0].EndedAt = before.Now },
	} {
		t.Run(name, func(t *testing.T) {
			data, _ := json.Marshal(after)
			var changed Snapshot
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if err := verify(changed); err == nil {
				t.Fatal("unproven failure accepted")
			}
		})
	}
	data, _ := json.Marshal(after)
	var ceiling Snapshot
	json.Unmarshal(data, &ceiling)
	ceiling.Jobs[0].AttemptHistory[0].OutOfMemory = false
	ceiling.Jobs[0].AttemptHistory[0].ExecutionCause = &RuntimeCause{Class: "resource_limit"}
	if err := verify(ceiling); err != nil {
		t.Fatal(err)
	}
}
