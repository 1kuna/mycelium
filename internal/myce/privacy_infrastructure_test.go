package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// #937: status redaction must explain why retry ended, while withholding the
// private transport text attached to a pre-execution authorization interruption.
func TestPrivacyInfrastructureExhaustionExplainsRetryEnd(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	j := submit(t, q, spec("experiment"))
	for attempt := 1; attempt <= 3; attempt++ {
		a := check(t, q, m, nil).Job
		if a == nil || a.Attempt != attempt {
			t.Fatalf("missing attempt %d: %+v", attempt, a)
		}
		clock.Advance(time.Second)
		r := result(a, false)
		r.Interrupted = true
		r.Error = "synthetic-private-transport-detail"
		r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "io_error", Predicate: "launch_authorization", Reason: r.Error}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	got, err := client.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := "stopped after 3 infrastructure interruptions (Mycelium-side, not a command failure); last: start_authorization: io_error; launch_authorization: queue launch authorization or assignment was refused; refresh job and host status; next: resubmit with a new submission key"
	if got.State != "failed" || got.Losses != 3 || got.Attempt != 3 || got.StartedAt != nil || !got.ReleaseConfirmed || got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now()) {
		t.Fatalf("infrastructure charging or terminal state changed: %+v", got)
	}
	if got.Reason != want {
		t.Fatalf("budget exhaustion hidden: %q", got.Reason)
	}
	if got.ExecutionCause == nil || got.ExecutionCause.Operation != "start_authorization" || got.ExecutionCause.Class != "io_error" || got.ExecutionCause.Predicate != "launch_authorization" {
		t.Fatalf("authorization diagnostic lost: %+v", got.ExecutionCause)
	}
	last := got.AttemptHistory[len(got.AttemptHistory)-1]
	if last.Reason != want || !last.Interrupted {
		t.Fatalf("terminal history lost infrastructure exhaustion: %+v", last)
	}
	page, err := client.JobLedger(context.Background(), j.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	if last := page.Entries[len(page.Entries)-1]; last.Kind != "outcome" || last.Reason != want {
		t.Fatalf("terminal ledger lost exhaustion: %+v", last)
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "synthetic-private") {
		t.Fatal("public job disclosed transport detail")
	}
}
