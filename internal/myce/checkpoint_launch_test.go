package myce

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMaintenanceResumeWaitsForCheckpointCopy(t *testing.T) {
	q, clock := fixture(t)
	w := recoveryWorker(t)
	w.Clock = clock
	w.Machine.HostID = "resume-host"
	handler := (API{Queue: q, Token: "test"}).Handler()
	w.Client = Client{URL: "http://queue.invalid", Token: "test", HTTP: &http.Client{Transport: handlerTestTransport(handler)}}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	live := inventoryReviewDevices()[:1]
	w.Machine.Devices = live
	var copying atomic.Bool
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) {
		if copying.Load() {
			return nil, errors.New("storage-contended discovery")
		}
		return live, nil
	}
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `grep -q '"generation":7' "$MYCE_CHECKPOINT_DIR/state.json"`}
	original := submit(t, q, s)
	first := check(t, q, w.Machine, nil).Job
	if first == nil || first.ID != original.ID {
		t.Fatal("missing original assignment")
	}
	writeSave(t, attemptCheckpoint(*first), "7")
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance("resume-host", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, first)
	if _, err := q.ChangeHostMaintenance("resume-host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	resumed := check(t, q, w.Machine, nil).Job
	if resumed == nil || resumed.Attempt != 2 || resumed.MaintenanceResume == nil {
		t.Fatal("missing maintenance resume")
	}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = resumed.Session
	store.nasPrepare.timeout = time.Millisecond
	release, finished := make(chan struct{}), make(chan struct{})
	var copies atomic.Int32
	w.checkpointPrepare = func(j Job) (string, error) {
		copies.Add(1)
		copying.Store(true)
		defer copying.Store(false)
		defer close(finished)
		<-release
		return prepareCheckpoint(j)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-finished
	}()
	start := func() (*process, error) {
		for {
			p, err := w.startAssignedAttempt(context.Background(), store, *resumed)
			if !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending") {
				return p, err
			}
			store.extensionFiles.mu.Lock()
			pending := store.extensionFiles.pending
			store.extensionFiles.mu.Unlock()
			if pending == nil {
				t.Fatal("lost input writer")
			}
			<-pending.done
		}
	}
	p, err := start()
	if p != nil || !errors.Is(err, errWorkerCheckpoint) {
		t.Fatal("copy should remain owned", err)
	}
	// The same attempt must wait for its writer before running admission again.
	clock.Advance(time.Second)
	p, err = start()
	if p != nil || !errors.Is(err, errWorkerCheckpoint) {
		t.Fatal("pending resume was refused during its own copy", err)
	}
	if store.checkpointUnavailable {
		t.Fatal("pending copy fenced healthy checkpoint storage")
	}
	close(release)
	<-finished
	p, err = start()
	if err != nil {
		t.Fatal("resume did not start", err)
	}
	r := d5Finish(t, p)
	if !r.Success || copies.Load() != 1 {
		t.Fatal("lost checkpoint or duplicated copy", r, copies.Load())
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, resumed.ID)
	if got.Attempt != 2 || got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.AttemptHistory) != 2 {
		t.Fatal("resume churned attempts", got.Attempt)
	}
}

func TestRefusedResumeDoesNotMaterializeCheckpoint(t *testing.T) {
	q, clock := fixture(t)
	w := recoveryWorker(t)
	w.Clock = clock
	w.Machine.HostID = "refuse-host"
	w.Client = Client{URL: "http://queue.invalid", Token: "test", HTTP: &http.Client{Transport: handlerTestTransport((API{Queue: q, Token: "test"}).Handler())}}
	s := spec("experiment")
	s.Run.Command = []string{"true"}
	submit(t, q, s)
	first := check(t, q, w.Machine, nil).Job
	writeSave(t, attemptCheckpoint(*first), "7")
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance("refuse-host", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	finishInterrupted(t, q, first)
	if _, err := q.ChangeHostMaintenance("refuse-host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	j := check(t, q, w.Machine, nil).Job
	if j == nil || j.Attempt != 2 || j.MaintenanceResume == nil {
		t.Fatal("missing maintenance resume")
	}
	if _, err := q.ChangeHostMaintenance("refuse-host", HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = j.Session
	var copies atomic.Int32
	w.checkpointPrepare = func(j Job) (string, error) { copies.Add(1); return prepareCheckpoint(j) }
	p, err := startAfterFixtureInputs(t, w, store, *j)
	if p != nil {
		p.stop(clock.Now(), 0, true)
		t.Fatal("refused command started")
	}
	if err == nil || copies.Load() != 0 {
		t.Fatal("refused attempt materialized checkpoint", copies.Load(), err)
	}
	if _, err := os.Stat(attemptCheckpoint(*j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused checkpoint exists", err)
	}
	if _, err := os.Stat(filepath.Join(attemptCheckpoint(*first), "state.json")); err != nil {
		t.Fatal("refusal lost original checkpoint", err)
	}
}

func TestRefusedCheckpointCopyCleanupCustody(t *testing.T) {
	w := recoveryWorker(t)
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 2
	j.Session = w.Machine.Session
	source := filepath.Join(j.Checkpoint, "attempt-1")
	writeSave(t, source, "7")
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Job = &j
	store.state.Result = &Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, ExecutionEvidence: unstartedExecution(j.Attempt)}
	copied, release := make(chan struct{}), make(chan struct{})
	store.nasPrepare.timeout = time.Millisecond
	_, err = store.nasPrepare.run("copy", func() (string, error) {
		path, e := prepareCheckpoint(j)
		close(copied)
		<-release
		return path, e
	})
	<-copied
	if !errors.Is(err, errCheckpointIOPending) {
		t.Fatal("copy not pending", err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	if err := store.cleanupOrphan(w.Clock); !errors.Is(err, errWorkerReleasePending) {
		t.Fatal("released a checkpoint writer", err)
	}
	if err := w.cleanupRefusedCheckpoint(context.Background(), store); !errors.Is(err, errCheckpointIOPending) {
		t.Fatal("removed a live copy", err)
	}
	close(release)
	<-store.nasPrepare.pending.done
	// A newer attempt and the original save must never be cleanup candidates.
	newer := j
	newer.Attempt = 3
	writeSave(t, attemptCheckpoint(newer), "9")
	removed := 0
	failRemoval := true
	w.checkpointRemove = func(path string) error {
		if path != attemptCheckpoint(j) {
			t.Error("wrong cleanup path", path)
		}
		if failRemoval {
			return errors.New("fake storage unavailable")
		}
		removed++
		return os.RemoveAll(path)
	}
	store.nasCleanup.timeout = time.Second
	if err := w.cleanupRefusedCheckpoint(context.Background(), store); err == nil {
		t.Fatal("cleanup failure was swallowed")
	}
	if _, err := os.Stat(attemptCheckpoint(j)); err != nil {
		t.Fatal("failed cleanup lost custody", err)
	}
	failRemoval = false
	if err := w.cleanupRefusedCheckpoint(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatal("copy not reclaimed", removed)
	}
	if _, err := os.Stat(attemptCheckpoint(j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused copy remains", err)
	}
	for _, keep := range []string{source, attemptCheckpoint(newer)} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatal("removed another attempt", err)
		}
	}
	for _, e := range []*ExecutionEvidence{nil, {Attempt: 2, Main: executionMayHaveRun, Sidecars: executionNotStarted}, {Attempt: 2, Main: executionNotStarted, Sidecars: executionMayHaveRun}, {Attempt: 1, Main: executionNotStarted, Sidecars: executionNotStarted}} {
		store.state.Result.ExecutionEvidence = e
		if err := w.cleanupRefusedCheckpoint(context.Background(), store); err != nil {
			t.Fatal(err)
		}
	}
	if removed != 1 {
		t.Fatal("unknown or executed checkpoint was reclaimed")
	}
}
