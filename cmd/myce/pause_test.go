package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/myce"
)

type pauseCLIClock struct{ now time.Time }

func (c pauseCLIClock) Now() time.Time { return c.now }

func TestPauseResumeCLI(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test")
	clock := pauseCLIClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	s := myce.Spec{Project: "test", Name: "review", Run: myce.RunSpec{Command: []string{"echo", "done"}}}
	s.Needs.Platform.Any = true
	s.Needs.MemoryGB = 1
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	until := clock.Now().Add(time.Hour).Format(time.RFC3339)
	if err = pauseCommand(context.Background(), "pause", []string{j.ID, "--reason", "review input", "--until", until, "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	var paused myce.Job
	if err = json.Unmarshal(out.Bytes(), &paused); err != nil || paused.State != "paused" || paused.Pause == nil || paused.Pause.Reason != "review input" || paused.Pause.Until.Format(time.RFC3339) != until {
		t.Fatal("bad pause reply", out.String(), err)
	}
	out.Reset()
	if err = pauseCommand(context.Background(), "resume", []string{j.ID, "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	var resumed myce.Job
	if err = json.Unmarshal(out.Bytes(), &resumed); err != nil || resumed.ID != j.ID || resumed.State != "queued" || resumed.Pause != nil || resumed.Queued != j.Queued {
		t.Fatal("bad resume reply", out.String(), err)
	}
}
