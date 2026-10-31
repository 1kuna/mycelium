package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestAttemptHistoryStatusCLI(t *testing.T) {
	exit := 1
	snapshot := myce.Snapshot{Now: time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC), Jobs: []*myce.Job{
		{ID: "1234567890abcdef", State: "failed", Attempt: 2, Starts: 2, AttemptHistory: []myce.AttemptOutcome{
			{Attempt: 1, ExecutionCause: &myce.RuntimeCause{Operation: "launch", Class: "launch_refused", Predicate: "native_workdir_containment"}, Machine: "one", Session: "worker-session", State: "queued", Interrupted: true, OperatorStop: true, Reason: "requeued: stopped by operator"},
			{Attempt: 2, Machine: "one", Session: "worker-session", State: "failed", ExecutionCause: &myce.RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &exit}},
		}},
		{ID: "unrelated-job", State: "queued", Spec: myce.Spec{Name: "unrelated"}},
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/12345678" {
			t.Errorf("unexpected API path %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(snapshot.Jobs[0])
	}))
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test")
	out := captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", server.URL, "12345678"}) })
	for _, want := range []string{"attempt 1", "state=queued", "operator=true", "attempt 2", "command_exit (exit 1)", "native_workdir_containment", "workdir beneath {work}"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q: %s", want, out)
		}
	}
	if strings.Contains(out, "unrelated") {
		t.Fatal("status ignored job selector", out)
	}
}
