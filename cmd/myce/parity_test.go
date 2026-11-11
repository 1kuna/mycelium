package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// A project lists, widens and narrows its own production parity hosts with
// its submit authority; additions without passing evidence are refused.
func TestParityCLI(t *testing.T) {
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), &bindClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureHosts(map[string]myce.HostPolicy{"approved-host": {}, "candidate-host": {}}); err != nil {
		t.Fatal(err)
	}
	// A startup project config seeds the project-owned list.
	if err := q.ConfigureProjects(map[string]myce.ProjectPolicy{"test": {Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"approved-host"}}}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "fleet-test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "fleet-test")
	ctx := context.Background()
	var out bytes.Buffer
	if err := parityCommand(ctx, []string{"add", "--project", "test", "--work", "kind:sample", "--host", "candidate-host"}, &out); err == nil || !strings.Contains(err.Error(), "parity_evidence_required") || !strings.Contains(err.Error(), "next: run a check or experiment of kind:sample as project test on host candidate-host") {
		t.Fatal("addition without evidence was not refused with its next action:", err)
	}
	if err := parityCommand(ctx, []string{"remove", "--project", "test", "--work", "kind:sample", "--host", "approved-host"}, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := parityCommand(ctx, []string{"list", "--project", "test"}, &out); err != nil {
		t.Fatal(err)
	}
	var record myce.ProjectParityHosts
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || record.Project != "test" || record.Hosts["kind:sample"] == nil || len(record.Hosts["kind:sample"]) != 0 {
		t.Fatalf("list after removing the last host: %s %v", out.String(), err)
	}
	if err := parityCommand(ctx, []string{"remove", "--project", "test", "--work", "kind:sample", "--host", "approved-host", "--evidence", "job"}, &out); err == nil {
		t.Fatal("remove accepted unused evidence")
	}
}
