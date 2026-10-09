package main

import (
	"mycelium/internal/myce"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestModelPinCLI(t *testing.T) {
	dir := t.TempDir()
	q, e := myce.Open(filepath.Join(dir, "q.db"), dir, myce.RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	if e = q.ConfigureModels(myce.ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []myce.Model{{Name: "m", Path: "fake", Engine: "fake", MemoryGB: 1, Platforms: []string{"fake"}}}}); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test")
	if e := run([]string{"model", "pin", "m", "--project", "atlas", "--platform", "fake", "--queue", server.URL, "--weekdays", "1,2,3,4,5", "--start", "08:00", "--end", "20:00"}); e != nil {
		t.Fatal(e)
	}
	snap, e := q.Snapshot()
	if e != nil || len(snap.Pins) != 1 || snap.Pins[0].Platform != "fake" {
		t.Fatal(snap.Pins, e)
	}
	if e := run([]string{"model", "unpin", "m", "--project", "atlas", "--queue", server.URL}); e != nil {
		t.Fatal(e)
	}
	snap, e = q.Snapshot()
	if e != nil || len(snap.Pins) != 0 {
		t.Fatal("pin not removed")
	}
	if e := run([]string{"model", "fetch", "../outside", "--model-root", dir}); e == nil {
		t.Fatal("fetch traversal accepted")
	}
}
