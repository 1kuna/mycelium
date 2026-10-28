package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDockerRuntimePlacement(t *testing.T) {
	image := dockerJob()
	native := Job{Spec: spec("experiment")}
	for _, tc := range []struct {
		name, os string
		enabled  bool
		fits     bool
	}{
		{"native-only", "linux", false, false},
		{"enabled", "linux", true, true},
		{"unsupported", "darwin", true, false},
		{"unknown-os", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := machine(tc.name, "fake")
			m.CapabilityProfile.OS = tc.os
			if tc.enabled {
				m.Capabilities = append(m.Capabilities, "docker")
			}
			if got := fits(&image, &m, 8); got != tc.fits {
				t.Fatalf("image fits=%v, want %v", got, tc.fits)
			}
			if !fits(&native, &m, 8) {
				t.Fatal("native command requires Docker")
			}
		})
	}
	q, _ := fixture(t)
	m := machine("native", "fake")
	m.CapabilityProfile.OS = "linux"
	queued := submit(t, q, image.Spec)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("assigned image to native-only worker")
	}
	if got := job(t, q, queued.ID).Reason; got != "needs docker runtime; no worker has it enabled" {
		t.Fatal(got)
	}
	s := submit(t, q, native.Spec)
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != s.ID {
		t.Fatal("native job blocked behind image job")
	}
	m = machine("image", "fake")
	m.CapabilityProfile.OS = "linux"
	m.Capabilities = append(m.Capabilities, "docker")
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != queued.ID {
		t.Fatal("enabled Linux worker did not receive image")
	}
}

func TestDockerRuntimeDisabledRejectsLaunchAndRecovery(t *testing.T) {
	w, store, _, root := dockerFixture(t)
	w.DockerEndpoint = ""
	t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
	if _, err := w.startDocker(store, dockerJob(), "", nil); err == nil || !strings.Contains(err.Error(), "Docker runtime disabled") {
		t.Fatalf("disabled launch: %v", err)
	}
	j := dockerJob()
	store.state.Job, store.state.Identity = &j, "saved"
	state := dockerExecution{Identity: "saved", Endpoint: "unix:///saved.sock"}
	if err := saveWorkerJSON(store.dir, "saved.docker.json", state); err != nil {
		t.Fatal(err)
	}
	if _, err := w.restoreDocker(store); err == nil || !strings.Contains(err.Error(), "Docker runtime disabled") {
		t.Fatalf("disabled recovery: %v", err)
	}
	// Already proven release can recover its result without an enabled runtime.
	state.Released = true
	if err := saveWorkerJSON(store.dir, "saved.docker.json", state); err != nil {
		t.Fatal(err)
	}
	if p, err := w.restoreDocker(store); err != nil || p != nil || store.state.Result == nil {
		t.Fatalf("released recovery required Docker: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(root, "calls")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled runtime called Docker: %s (%v)", b, err)
	}
}

func TestDockerRuntimeDisabledWorkerMakesNoCalls(t *testing.T) {
	_, _, _, root := dockerFixture(t) // Fake CLI records every invocation.
	t.Setenv("DOCKER_HOST", "unix:///ambient.sock")
	q, initial := fixture(t)
	q.clock = &workerFakeClock{now: initial.Now()}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	state := newTestStateRoot(t)
	// Due retained custody must stay intact without contacting its old daemon.
	if err := saveWorkerJSON(state, "old.docker-ack.json", dockerAcknowledgement{Worker: "native", At: q.clock.Now().Add(-48 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(state, "old.docker.json", dockerExecution{Identity: "old", Endpoint: "unix:///old.sock", Released: true}); err != nil {
		t.Fatal(err)
	}
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: state, Clock: q.clock, Client: Client{URL: server.URL, Token: "test"}, Machine: machine("native", "fake"), Interval: time.Millisecond}
	w.Machine.HostID = "test-host"
	// A stale caller-supplied capability cannot enable an unconfigured runtime.
	w.Machine.Capabilities = append(w.Machine.Capabilities, "docker")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		await(t, func() bool {
			q.clock.(*workerFakeClock).Advance(time.Millisecond)
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				return true
			default:
				return false
			}
		})
		if b, err := os.ReadFile(filepath.Join(root, "calls")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("disabled runtime called Docker: %s (%v)", b, err)
		}
		if _, err := os.Stat(filepath.Join(state, "old.docker.json")); err != nil {
			t.Error("disabled runtime removed retained custody", err)
		}
	})
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", "echo native"}
	j := submit(t, q, s)
	await(t, func() bool {
		q.clock.(*workerFakeClock).Advance(time.Millisecond)
		return job(t, q, j.ID).State == "succeeded"
	})
	// Real API health and status reads must remain Docker-free too.
	var observation HostMaintenanceStatus
	if err := w.Client.Call(context.Background(), "GET", "/api/hosts/"+hostID(&w.Machine)+"/maintenance", nil, &observation); err != nil || len(observation.Workers) != 1 || !observation.Healthy {
		t.Fatal("native worker unhealthy", err, observation.Workers)
	}
	for _, path := range []string{"/api/status"} {
		var response map[string]any
		if err := w.Client.Call(context.Background(), "GET", path, nil, &response); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := q.Snapshot()
	if err != nil || len(snapshot.Machines) != 1 || contains(snapshot.Machines[0].Capabilities, "docker") {
		t.Fatal("disabled worker advertised Docker", err)
	}
}
