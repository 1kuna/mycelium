package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// The CLI relays the queue's named supersession refusal and needs both an
// operation and an audit reason before contacting the queue (#588).
func TestFleetSupersedeCLI(t *testing.T) {
	root := t.TempDir()
	q, err := myce.Open(filepath.Join(root, "queue.db"), filepath.Join(root, "checkpoints"), myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	var out bytes.Buffer
	authority := []string{"--queue", server.URL, "--token", "test"}
	err = fleetCommand(context.Background(), append([]string{"supersede", "--operation", "op-1"}, authority...), &out)
	if err == nil || !strings.Contains(err.Error(), "myce fleet supersede --operation ID --reason TEXT") {
		t.Fatalf("missing reason accepted: %v", err)
	}
	err = fleetCommand(context.Background(), append([]string{"supersede", "--operation", "op-1", "--reason", "retire"}, authority...), &out)
	if err == nil || !strings.Contains(err.Error(), "fleet_operation_mismatch") || !strings.Contains(err.Error(), `operation "op-1" is not the current fleet operation; current: none`) {
		t.Fatalf("refusal not relayed: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("refused supersession printed a record: %s", out.String())
	}
}
