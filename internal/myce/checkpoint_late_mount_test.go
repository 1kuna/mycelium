package myce

import (
	"context"
	"encoding/json"
	"net/http"
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

func TestWorkerHeartbeatAfterLateCheckpointMount(t *testing.T) {
	root := filepath.Join(t.TempDir(), "late-mount", "checkpoints")
	w := recoveryWorker(t)
	w.EnvironmentLimit, w.CheckpointRoot = 1<<20, root
	heartbeats := 0
	recovered := false
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/workers/checkin" {
			http.NotFound(rw, req)
			return
		}
		var c Checkin
		if err := json.NewDecoder(req.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		heartbeats++
		if heartbeats <= 2 && !c.Machine.CheckpointUnavailable {
			t.Error("worker admitted missing checkpoint root")
		}
		if heartbeats == 2 {
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Error(err)
			}
		}
		if heartbeats > 2 && !c.Machine.CheckpointUnavailable {
			recovered = true
		}
		if err := json.NewEncoder(rw).Encode(attemptReply(c, Reply{Leave: recovered})); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !recovered || heartbeats < 3 {
		t.Fatal("same worker did not heartbeat through mount recovery")
	}
}
