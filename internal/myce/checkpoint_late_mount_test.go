package myce

import (
	"os"
	"path/filepath"
	"testing"
)

// The directory appears when the simulated external automount becomes usable.
// Systemd's own start-limit behavior requires the separate native boot check.
func TestCheckpointAdmissionAfterLateMount(t *testing.T) {
	root := filepath.Join(t.TempDir(), "late-mount", "checkpoints")
	q, _ := fixture(t)
	m := machine("late-mount-worker", "fake")
	j := submit(t, q, spec("experiment"))
	var probe checkpointAvailability
	observe := func() {
		available, _ := probe.runProgress(root, func(progress func()) (bool, error) {
			err := workerCheckpointAvailable(root, progress)
			return err == nil, err
		})
		m.CheckpointUnavailable = !available
	}
	observe()
	if !m.CheckpointUnavailable {
		t.Fatal("missing checkpoint mount reported available")
	}
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("checkpoint-dependent work admitted while mount is absent")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	observe()
	if m.CheckpointUnavailable {
		t.Fatal("checkpoint mount remained unavailable after arrival")
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("checkpoint-dependent work did not become eligible after mount arrival")
	}
}
