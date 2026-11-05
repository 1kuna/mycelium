package myce

import "testing"

func TestObservationLimitPrefixPriorityUpdates(t *testing.T) {
	q, _, l := adaptiveObservationFixture(t, 2, 6500)
	prefix := "job-0000"
	if _, protected := l.index.observationPriority(prefix); protected {
		t.Fatal("nonproduction prefix protected")
	}
	id := "job-00000000"
	if err := q.update(func() error { q.touchJob(id).Spec = spec("production"); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, protected := l.index.observationPriority(prefix); !protected {
		t.Fatal("production prefix not protected")
	}
	if err := q.update(func() error { q.deleteJob(id); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, protected := l.index.observationPriority(prefix); protected {
		t.Fatal("removed production still protects prefix")
	}
	if tier, protected := l.index.observationPriority("job-00000001"); protected || tier != 2 {
		t.Fatal("remaining exact priority", tier, protected)
	}
}
