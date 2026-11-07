package main

import (
	"encoding/json"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestFirstRunMessages(t *testing.T) {
	j := &myce.Job{State: "queued", Isolation: "unsandboxed: plain native job defaults to unsandboxed in v1", Blocker: &myce.Blocker{
		Code: "unknown_observation", Dependency: "worker inventory", Owner: "queue owner",
		Action: "restore fresh worker observations", Evidence: "inventory is missing or stale",
	}}
	before, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	text := jobReasonText(j)
	for _, want := range []string{"Waiting for worker information", "inventory is missing or stale", "run myce status", "worker account's files", "isolation: sandboxed"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q: %s", want, text)
		}
	}
	if strings.Contains(text, "unknown_observation") {
		t.Errorf("unexplained code: %s", text)
	}
	after, _ := json.Marshal(j)
	if string(before) != string(after) || j.Blocker.Code != "unknown_observation" {
		t.Fatal("human formatting changed the JSON observation")
	}
}
