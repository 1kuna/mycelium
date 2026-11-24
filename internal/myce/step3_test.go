package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDifferentWorkerCheckpointRoots(t *testing.T) {
	q, _ := fixture(t)
	shared := q.root
	aliases := []string{filepath.Join(t.TempDir(), "linux-nas"), filepath.Join(t.TempDir(), "mac-nas")}
	for _, a := range aliases {
		if e := os.Symlink(shared, a); e != nil {
			t.Fatal(e)
		}
	}
	j := submit(t, q, spec("experiment"))
	first := checkRelative(t, q, machine("linux", "fake"))
	if filepath.IsAbs(first.Checkpoint) {
		t.Fatal("assignment leaked queue mount path")
	}
	first.Checkpoint = filepath.Join(aliases[0], first.Checkpoint)
	dir, e := prepareCheckpoint(*first)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "saved"), []byte("progress"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = q.Leave("linux", true); e != nil {
		t.Fatal(e)
	}
	r := result(first, false)
	r.Interrupted = true
	if e = q.Complete(r); e != nil {
		t.Fatal(e)
	}
	second := checkRelative(t, q, machine("mac", "fake"))
	second.Checkpoint = filepath.Join(aliases[1], second.Checkpoint)
	dir, e = prepareCheckpoint(*second)
	if e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(filepath.Join(dir, "saved"))
	if e != nil || string(data) != "progress" {
		t.Fatalf("%s lost progress: %q %v", j.ID, data, e)
	}
}
func TestWorkerRechecksMemoryBeforeCommand(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("production"))
	j.Attempt = 1
	var calls atomic.Int32
	w := Worker{Clock: clock, StateDir: newTestStateRoot(t), Machine: machine("memory", "fake"), MemorySource: func(context.Context) (float64, error) { calls.Add(1); return 2, nil }}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	s, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	await(t, func() bool {
		_, e = w.start(s, *j)
		return !errors.Is(e, errWorkerCheckpoint) || !strings.Contains(e.Error(), "input copy pending")
	})
	if !errors.Is(e, errWorkerMemory) || calls.Load() != 1 {
		t.Fatalf("memory recheck: %v calls=%d", e, calls.Load())
	}
	if s.state.Job != nil {
		t.Fatal("insufficient-memory command entered spool")
	}
}
