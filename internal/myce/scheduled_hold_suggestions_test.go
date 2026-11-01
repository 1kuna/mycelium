package myce

import (
	"testing"
	"time"
)

func TestScheduledHoldSuggestionsRequireOwnerActivation(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake", "host-one")
	for range 2 {
		s := spec("production")
		s.Project = "beneficiary"
		submit(t, q, s)
		run := check(t, q, m, nil).Job
		if run == nil {
			t.Fatal("no production sample")
		}
		if err := q.Started(result(run, true)); err != nil {
			t.Fatal(err)
		}
		c.Advance(20 * time.Second)
		finish(t, q, run, true)
	}
	q = reopenCancellationQueue(t, q, c)
	suggestions, err := q.HoldSuggestions()
	if err != nil || len(suggestions) != 1 {
		t.Fatal(suggestions, err)
	}
	got := suggestions[0]
	if got.Rule.Enabled || got.Rule.Beneficiary != "beneficiary/production" || got.Rule.Timezone != "UTC" || len(got.Evidence) != 2 || got.Uncertainty == "" {
		t.Fatal("suggestion lacks evidence/uncertainty or activates ownership", got)
	}
	if q.PriorityPolicy().Revision != 0 || len(q.PriorityPolicy().Policy.ScheduledHolds) != 0 {
		t.Fatal("history changed ownership")
	}
	p := defaultPriorityPolicy()
	p.ScheduledHolds = []ScheduledHold{got.Rule}
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	peer := submit(t, q, spec("experiment"))
	if run := check(t, q, m, nil).Job; run == nil || run.ID != peer.ID {
		t.Fatal("disabled suggestion withheld peer", run)
	}
}
