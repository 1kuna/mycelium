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

// A project lists, approves and withdraws machine kinds for its production
// with its submit authority; approvals without passing evidence are refused,
// and the CLI never asks for a host.
func TestParityCLI(t *testing.T) {
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), &bindClock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureHosts(map[string]myce.HostPolicy{"approved-host": {}}); err != nil {
		t.Fatal(err)
	}
	// A startup project config that still names a host is accepted and kept
	// until that host reports its kind (MY-42).
	if err := q.ConfigureProjects(map[string]myce.ProjectPolicy{"test": {Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"approved-host"}}}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "fleet-test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "fleet-test")
	ctx := context.Background()
	var out bytes.Buffer
	if err := parityCommand(ctx, []string{"add", "--project", "test", "--work", "kind:sample", "--kind", "cuda"}, &out); err == nil || !strings.Contains(err.Error(), "parity_evidence_required") || !strings.Contains(err.Error(), "next: run a check or experiment of kind:sample as project test that compares outputs") {
		t.Fatal("approval without evidence was not refused with its next action:", err)
	}
	if err := parityCommand(ctx, []string{"add", "--project", "test", "--work", "kind:sample", "--kind", "linux,mac", "--evidence", "job"}, &out); err == nil || !strings.Contains(err.Error(), "two values") {
		t.Fatal("ambiguous kind accepted:", err)
	}
	out.Reset()
	if err := parityCommand(ctx, []string{"list", "--project", "test"}, &out); err != nil {
		t.Fatal(err)
	}
	var record myce.ProjectParity
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || len(record.Kinds["kind:sample"]) != 1 || record.Kinds["kind:sample"][0].PendingHost != "approved-host" || len(record.Warnings) != 1 {
		t.Fatalf("list of a pending host approval: %s %v", out.String(), err)
	}
	if err := parityCommand(ctx, []string{"remove", "--project", "test", "--work", "kind:sample", "--host", "approved-host"}, &out); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := parityCommand(ctx, []string{"list", "--project", "test"}, &out); err != nil {
		t.Fatal(err)
	}
	record = myce.ProjectParity{}
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || record.Project != "test" || record.Kinds["kind:sample"] == nil || len(record.Kinds["kind:sample"]) != 0 {
		t.Fatalf("list after removing the last approval: %s %v", out.String(), err)
	}
	for _, args := range [][]string{
		{"remove", "--project", "test", "--work", "kind:sample", "--kind", "cuda", "--evidence", "job"},
		{"remove", "--project", "test", "--work", "kind:sample"},
		{"list", "--project", "test", "--kind", "cuda"},
	} {
		if err := parityCommand(ctx, args, &out); err == nil {
			t.Fatal("usage error accepted:", args)
		}
	}
	if strings.Contains(parityUsage, "--host") {
		t.Fatal("usage asks for a host")
	}
}
