package myce

import (
	"context"
	"encoding/json"
	"errors"
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
	// Wait for each local filesystem observation before changing the mount.
	// The heartbeat deadline is exercised by the checkpoint I/O tests.
	probe.timeout = -1
	observe := func() error {
		available, err := probe.runProgress(root, func(progress func()) (bool, error) {
			err := workerCheckpointAvailable(root, progress)
			return err == nil, err
		})
		m.CheckpointUnavailable = !available
		return err
	}
	if err := observe(); !errors.Is(err, os.ErrNotExist) || !m.CheckpointUnavailable {
		t.Fatalf("missing checkpoint mount availability=%t: %v", !m.CheckpointUnavailable, err)
	}
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("checkpoint-dependent work admitted while mount is absent")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := observe(); err != nil || m.CheckpointUnavailable {
		t.Fatalf("late checkpoint mount availability=%t: %v", !m.CheckpointUnavailable, err)
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("checkpoint-dependent work did not become eligible after mount arrival")
	}
}

func TestWorkerHeartbeatAfterLateCheckpointMount(t *testing.T) {
	root := filepath.Join(t.TempDir(), "late-mount", "checkpoints")
	w := recoveryWorker(t)
	w.EnvironmentLimit, w.CheckpointRoot = 1<<20, root
	clock := &workerFakeClock{now: w.Clock.Now()}
	w.Clock = clock
	heartbeats := 0
	recovered := false
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/api/workers/"+w.Machine.ID+"/leave" {
			_ = json.NewEncoder(rw).Encode(map[string]bool{"ok": true})
			return
		}
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
			// Failed metadata probes are cached for five minutes. Exercise
			// the next normal observation without outrunning the I/O deadline.
			clock.Advance(checkpointMetadataInterval)
		}
		if heartbeats > 2 && !c.Machine.CheckpointUnavailable {
			recovered = true
		}
		if err := json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol, Leave: recovered}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	done := make(chan struct{})
	var runErr error
	go func() { runErr = w.Run(ctx); close(done) }()
	wait := func() {
		await(t, func() bool {
			clock.Advance(w.Interval)
			select {
			case <-done:
				return true
			default:
				return false
			}
		})
	}
	defer func() { cancel(); wait() }()
	wait()
	if runErr != nil {
		t.Fatal(runErr)
	}
	if !recovered || heartbeats < 3 {
		t.Fatal("same worker did not heartbeat through mount recovery")
	}
}
