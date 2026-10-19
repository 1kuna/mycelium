package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestPolicyCLI(t *testing.T) {
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), &bindClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "owner-test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "owner-test")
	var out bytes.Buffer
	if err := policyCommand([]string{"get", "--yaml"}, &out); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(file, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{out.String(), "version: 1\ntiers:\n  - name: shared\n    selectors: ['*/*']\nprojects:\n  test: {}\n"} {
		if _, err := myce.ReadPriorityPolicy(strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	out.Reset()
	if err := policyCommand([]string{"set", file, "--if-revision", "0"}, &out); err != nil {
		t.Fatal(err)
	}
	var record myce.PriorityPolicyRecord
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || record.Revision != 1 {
		t.Fatalf("%s %v", out.String(), err)
	}
	if err := policyCommand([]string{"set", file, "--if-revision", "0"}, &out); err == nil {
		t.Fatal("stale CLI overwrite accepted")
	}
	if err := policyCommand([]string{"set", file}, &out); err == nil {
		t.Fatal("missing CAS accepted")
	}
	if _, err := myce.ReadPriorityPolicy(strings.NewReader("version: 1\ntires: []")); err == nil {
		t.Fatal("unknown YAML accepted")
	}
}
