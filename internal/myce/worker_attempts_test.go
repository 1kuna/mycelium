package myce

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAttemptReleasesWhenRAMDropsBeforeEntrypoint(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "entrypoint")
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	h.jobs = []Job{harnessJob(t, "ram-drop", "", "touch "+marker)}
	h.jobs[0].Spec.Needs.HostMemoryGB = 22
	w := harnessWorker(t, h)
	var free atomic.Uint64
	free.Store(math.Float64bits(64))
	w.HostMemorySource = func(context.Context) (float64, error) {
		return math.Float64frombits(free.Load()), nil
	}
	checkpoint := t.TempDir()
	w.checkpointPrepare = func(Job) (string, error) {
		// Admission succeeded; RAM changed while inputs/checkpoints were staged.
		free.Store(math.Float64bits(21.9))
		return checkpoint, nil
	}
	startHarness(t, w, h)
	deadline := time.After(3 * time.Second)
	for !h.completed("ram-drop") {
		select {
		case <-deadline:
			t.Fatal("RAM refusal retained assigned custody without starting or releasing")
		case <-time.After(5 * time.Millisecond):
		}
	}
	r := h.result("ram-drop")
	if r.Success || r.ReleasePending || r.ExecutionCause == nil || r.ExecutionCause.Operation != "launch" || r.ExecutionCause.Class != "resource_limit" || !strings.Contains(r.Error, "host RAM need 22.0 GiB, available 21.9 GiB") {
		t.Fatalf("RAM refusal must release with its admission diagnostic: %+v", r)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("entrypoint ran without its RAM reservation", err)
	}

	q, _ := fixture(t)
	m := machine("ram-worker", "fake", "ram-session")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	s := spec("experiment")
	s.Needs.HostMemoryGB = 22
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("fixture was not assigned")
	}
	r.Execution, r.Machine, r.Session = Execution{ID: j.ID, Attempt: assigned.Attempt}, m.ID, m.Session
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || !got.ReleaseConfirmed || got.StartedAt != nil || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("prelaunch RAM refusal must wait in the queue without a loss: %+v", got)
	}
}
