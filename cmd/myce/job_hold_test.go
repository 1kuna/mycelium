package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestJobHoldCLI(t *testing.T) {
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), &bindClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	j, err := q.Submit(myce.Spec{Name: "job", Project: "sample", Run: myce.RunSpec{Command: []string{"echo", "done"}}, Needs: myce.ResourceNeeds{Platform: myce.Platform{Any: true}, MemoryGB: 1}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "owner-test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "owner-test")
	var out bytes.Buffer
	if err := jobHoldCommand("pause", []string{j.ID, "--reason", "waiting for review", "--until", "2026-10-19T13:00:00Z", "--if-revision", "0"}, &out); err != nil {
		t.Fatal(err)
	}
	var hold myce.JobHold
	if err := json.Unmarshal(out.Bytes(), &hold); err != nil || !hold.Active || hold.Revision != 1 || hold.Until == nil {
		t.Fatal(out.String(), err)
	}
	if err := jobHoldCommand("resume", []string{j.ID, "--if-revision", "0"}, &out); err == nil {
		t.Fatal("stale resume succeeded")
	}
	out.Reset()
	if err := jobHoldCommand("resume", []string{"--if-revision", "1", j.ID}, &out); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out.Bytes(), &hold); err != nil || hold.Active || hold.Paused || hold.Revision != 2 {
		t.Fatal(out.String(), err)
	}
	if err := jobHoldCommand("pause", []string{j.ID, "--reason", "review"}, &out); err == nil {
		t.Fatal("missing revision accepted")
	}
	if err := jobHoldCommand("resume", []string{j.ID, "--if-revision", "2", "--until", "2026-10-19T14:00:00Z"}, &out); err == nil {
		t.Fatal("resume accepted until")
	}
}
