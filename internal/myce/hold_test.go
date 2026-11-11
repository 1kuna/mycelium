package myce

import (
	"strings"
	"testing"
	"time"
)

// Status shows each hold as one reason and one next action, so every kind
// must supply both, plus who clears it and when.
func TestHoldKindsAreComplete(t *testing.T) {
	for kind, k := range holdKinds {
		if k.owner == "" || k.reason == "" || k.clears == "" || k.next == "" || strings.Contains(k.reason, ";") {
			t.Fatalf("%s: incomplete or multi-part hold kind %+v", kind, k)
		}
	}
	h := newHold("not-a-kind", "evidence")
	if h.Owner != ownerOperator || h.Detail != "evidence" {
		t.Fatalf("unknown kind must default to an operator-owned hold: %+v", h)
	}
	until := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := pauseHold(&Job{ID: "job-1", State: "paused", Pause: &JobPause{Reason: "waiting on data", Until: &until}})
	if p.Kind != "paused" || p.Next != "myce resume job-1 when it should run" || p.Expires == nil || !strings.Contains(p.Text(), "; next: ") {
		t.Fatalf("pause hold: %+v", p)
	}
}
