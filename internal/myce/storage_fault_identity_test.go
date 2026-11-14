package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestStorageFaultConfiguredSubdirectoryUsesObservedBacking(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.ServiceConfig.NAS = "/share/team"
	w.CheckpointRoot, w.JobFileRoot = "/share/team/checkpoints", "/share/team/inputs"
	w.rootMonitor = nil
	w.MountRoots = func(context.Context) ([]string, error) { return []string{"/share"}, nil }
	if missing := w.sampleStorageRoots(context.Background(), w.CheckpointRoot); len(missing) != 0 {
		t.Fatal("healthy configured subdirectory rejected", missing)
	}
	for _, role := range []string{"nas", "checkpoint", "job-files"} {
		if r := w.storageRoots()[role]; r.backing != "/share" {
			t.Fatal("configured directory was treated as observed backing", role, r)
		}
	}
	m.storageRoots = w.storageRoots()
	m.mounts["checkpoint"].path = w.CheckpointRoot
	m.mounts["job-files"].path = w.JobFileRoot
	w.observeStorageError(&os.PathError{Op: "read", Path: w.JobFileRoot + "/source", Err: syscall.EIO})
	state := m.mounts["checkpoint"].backingFault()
	if state == nil || !state.fault.Active || state != m.mounts["job-files"].backingFault() {
		t.Fatal("subdirectory roles lost shared fault attribution")
	}
	// NAS metadata failure projects through the observed mount identity too.
	machine := Machine{HostHealth: &HostHealth{History: []HostHealthSample{{At: clock.Now(), Mounts: []HostMountHealth{{Root: "nas", Reason: "mount_probe_failed"}}}}}}
	applyHostMountHealth(w, &machine)
	input := &Job{Spec: Spec{Priority: "production", Inputs: map[string]string{"asset": "source"}}}
	if storageRootReason(input, &machine) == "" {
		t.Fatal("subdirectory NAS probe failure lost dependent role projection")
	}
}

func TestStorageFaultNestedMountLossKeepsRecoveryIdentity(t *testing.T) {
	w, m, clock := storageFaultFixture(t)
	w.JobFileRoot = "/share/nested/inputs"
	p := m.mounts["job-files"]
	p.path = w.JobFileRoot
	p.dataProbe = func(string, uint64, func()) error { return nil }
	mounted := true
	w.MountRoots = func(context.Context) ([]string, error) {
		if mounted {
			return []string{"/share", "/share/nested"}, nil
		}
		return []string{"/share"}, nil
	}
	w.sampleStorageRoots(context.Background(), w.CheckpointRoot)
	m.readStorageRoots = w.storageRoots
	w.observeStorageError(&os.PathError{Op: "read", Path: w.JobFileRoot + "/source", Err: syscall.EIO})
	state := p.backingFault()
	if state == nil || !state.fault.Active {
		t.Fatal("fixture did not observe nested data fault")
	}
	mounted = false // Parent metadata succeeds after the nested mount disappears.
	w.Machine.UnavailableRoots = w.sampleStorageRoots(context.Background(), w.CheckpointRoot)
	if !slices.ContainsFunc(w.Machine.UnavailableRoots, func(r UnavailableRoot) bool { return r.Root == "job-files" }) {
		t.Fatal("surviving parent mount hid missing nested mount")
	}
	clock.Advance(time.Second)
	if err := m.sample(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.backingFault() != state || !state.fault.Active {
		t.Fatal("inventory refresh orphaned the nested backing's active latch")
	}
	for range hostHealthConfirmation {
		await(t, func() bool { return len(p.pending) == 1 })
		result := <-p.pending
		result.dataProof = false // Even fresh successful metadata is insufficient.
		p.pending <- result
		clock.Advance(time.Second)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	mounted = true
	w.Machine.UnavailableRoots = w.sampleStorageRoots(context.Background(), w.CheckpointRoot)
	input := &Job{Spec: Spec{Priority: "production", Inputs: map[string]string{"asset": "source"}}}
	if !state.fault.Active || state.successes != 0 || w.storageRootReason(input) == "" {
		t.Fatal("mount return/metadata cleared the data-fault hold")
	}
	for range hostHealthConfirmation {
		await(t, func() bool { return len(p.pending) == 1 })
		clock.Advance(time.Second)
		if err := m.sample(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if state.fault.Active || w.storageRootReason(input) != "" {
		t.Fatal("three fresh data proofs did not recover the learned backing")
	}
	for _, lane := range m.mounts {
		await(t, func() bool { return len(lane.pending) == 1 })
	}
}

func TestStorageFaultQueueCheckpointReplyPublishesIdentity(t *testing.T) {
	w := recoveryWorker(t)
	w.Clock = &fakeClock{now: time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)}
	clock := w.Clock.(*fakeClock)
	nas := t.TempDir()
	w.HostHealthSource = func(context.Context) (HostHealthReading, error) {
		return HostHealthReading{Load: 1, Pressure: 1, WakeSeconds: .001}, nil
	}
	w.MountRoots = func(context.Context) ([]string, error) { return []string{nas}, nil }
	foreign, reopen := workerForeignConsumers, workerStorageReopen
	workerForeignConsumers = nil
	fault := &os.PathError{Op: "read", Path: filepath.Join(nas, "job", "previous", "save"), Err: syscall.EIO}
	var prepared atomic.Bool
	// The fault starts at checkpoint preparation, not at a racing background
	// health probe that could fence the attempt before it reaches that seam.
	workerStorageReopen = func(string, uint64, func()) error {
		if !prepared.Load() {
			return nil
		}
		return fault
	}
	t.Cleanup(func() { workerForeignConsumers, workerStorageReopen = foreign, reopen })
	w.checkpointPrepare = func(Job) (string, error) { prepared.Store(true); return "", fault }
	j := recoveryJob(t, []string{"true"})
	j.Checkpoint = filepath.Join(nas, "job")
	ctx, cancel := context.WithCancel(context.Background())
	sent := false
	reports := make(chan Machine, 1)
	dependent := &Job{Spec: Spec{Priority: "experiment"}}
	w.Client.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		out := httptest.NewRecorder()
		switch r.URL.Path {
		case "/api/workers/launch":
			_ = json.NewEncoder(out).Encode(map[string]any{"ok": true, "start_gate": startGateProtocol})
		case "/api/workers/checkin":
			var c Checkin
			if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
				return nil, err
			}
			reply := Reply{CheckpointRoot: nas, HostHygieneProtocol: hostHygieneProtocol, HostMountProgressProtocol: hostMountProgressProtocol, HostProbeProtocol: hostProbeProtocol}
			if !sent {
				j.Session, j.Machine = c.Machine.Session, c.Machine.ID
				reply.Job, sent = &j, true
			}
			// The attempt-local CheckpointUnavailable flag can disappear when
			// a later start retry is refused by the latched mount fault and its
			// actor completes. Await the root identity/fence this test guards,
			// which must remain visible after that completion too.
			if prepared.Load() && strings.Contains(storageRootReason(dependent, &c.Machine), "checkpoint unavailable") {
				select {
				case reports <- c.Machine:
				default:
				}
				reply.Attempts = []AttemptDirective{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: true}}
				reply.Leave = true
				cancel()
			}
			_ = json.NewEncoder(out).Encode(attemptReply(c, reply))
		default:
			if strings.HasSuffix(r.URL.Path, "/events") {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			out.WriteString(`{}`)
		}
		return out.Result(), nil
	})}
	done := make(chan struct{})
	var runErr error
	go func() { runErr = w.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			select {
			case <-done:
				return true
			default:
				return false
			}
		})
		if runErr != nil {
			t.Error(runErr)
		}
	})
	var reported Machine
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		select {
		case reported = <-reports:
			return true
		default:
		}
		return false
	})
	if !strings.Contains(storageRootReason(dependent, &reported), "checkpoint unavailable") {
		t.Fatal("first queue-provided checkpoint root escaped owned fault observation", reported.UnavailableRoots)
	}
}
