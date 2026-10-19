package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPriorityLossFakeClock(t *testing.T) {
	for _, tc := range []struct {
		p    string
		wait time.Duration
	}{{"production", 5 * time.Second}, {"deadline", 20 * time.Second}, {"experiment", 60 * time.Second}} {
		t.Run(tc.p, func(t *testing.T) {
			q, c := fixture(t)
			j := submit(t, q, spec(tc.p))
			m := machine("lost", "fake")
			check(t, q, m, nil)
			peer := observer()
			check(t, q, peer, nil)
			c.Advance(tc.wait - time.Nanosecond)
			check(t, q, peer, nil)
			requireState(t, q, j.ID, "running")
			c.Advance(time.Nanosecond)
			check(t, q, peer, nil)
			requireState(t, q, j.ID, "queued")
		})
	}
	t.Run("custom", func(t *testing.T) {
		q, c := fixture(t)
		if err := q.SetLossTimeouts(9*time.Second, 2*time.Second, time.Second); err != nil {
			t.Fatal(err)
		}
		j := submit(t, q, spec("production"))
		check(t, q, machine("lost", "fake"), nil)
		peer := observer()
		check(t, q, peer, nil)
		c.Advance(8 * time.Second)
		check(t, q, peer, nil)
		requireState(t, q, j.ID, "running")
		c.Advance(time.Second)
		check(t, q, peer, nil)
		requireState(t, q, j.ID, "queued")
	})
}
func TestMajoritySilenceProtectsProductionFakeClock(t *testing.T) {
	q, c := fixture(t)
	for _, id := range []string{"a", "b", "c"} {
		s := spec("production")
		s.AllowedMachines = []string{id}
		submit(t, q, s)
		check(t, q, machine(id, "fake"), nil)
	}
	c.Advance(5 * time.Second)
	m := machine("c", "fake")
	q.mu.Lock()
	q.s.Machines[m.ID].LastSeen = c.Now()
	q.mu.Unlock()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	snap, _ := q.Snapshot()
	if !strings.Contains(snap.Warning, "network") {
		t.Fatal("majority loss not held")
	}
	for _, j := range snap.Jobs {
		if j.State != "running" {
			t.Fatal("fleet job moved during majority outage")
		}
	}
}
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
	first := check(t, q, machine("linux", "fake"), nil).Job
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
	if e = os.WriteFile(filepath.Join(dir, "generation.json"), []byte(`{"myce_checkpoint":{"generation":1}}`), 0600); e != nil {
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
	second := check(t, q, machine("mac", "fake"), nil).Job
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
	q, _ := fixture(t)
	j := submit(t, q, spec("production"))
	j.Attempt = 1
	var calls atomic.Int32
	w := Worker{StateDir: t.TempDir(), Machine: machine("memory", "fake"), MemorySource: func(context.Context) (float64, error) { calls.Add(1); return 2, nil }}
	s, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	_, e = w.start(s, *j)
	if !errors.Is(e, errWorkerMemory) || calls.Load() != 1 {
		t.Fatalf("memory recheck: %v calls=%d", e, calls.Load())
	}
	if s.state.Job != nil {
		t.Fatal("insufficient-memory command entered spool")
	}
}

func TestProductionModelLoadingLossFakeClock(t *testing.T) {
	q, c, m := modelFixture(t)
	addModelCall(t, q, "production")
	peer := observer()
	check(t, q, peer, nil)
	c.Advance(ProductionLossAfter - time.Nanosecond)
	check(t, q, peer, nil)
	if q.s.Machines[m.ID].Gone {
		t.Fatal("loading production worker lost early")
	}
	c.Advance(time.Nanosecond)
	check(t, q, peer, nil)
	if !q.s.Machines[m.ID].Gone {
		t.Fatal("loading production model ignored production loss threshold")
	}
}

func TestMeasuredCheckinDoesNotDoubleCountResidentModel(t *testing.T) {
	q, _, m := modelFixture(t)
	c := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	m.MemoryMeasured = true
	m.FreeMemoryGB = 3
	reportModel(t, q, m, "ready")
	if free := q.s.Machines[m.ID].FreeMemoryGB; free != 3 {
		t.Fatalf("measured model memory subtracted twice: %v", free)
	}
	if e := q.finishCall(c.ID); e != nil {
		t.Fatal(e)
	}
	m.MemoryMeasured = false
	m.FreeMemoryGB = 11
	reportModel(t, q, m, "ready")
	if free := q.s.Machines[m.ID].FreeMemoryGB; free != 3 {
		t.Fatalf("configured model reservation missing: %v", free)
	}
}
