package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Exercise a real command exit, durable worker receipt, queue completion,
// finding and placement with fake scheduling/probe time and no network storage.
func TestCheckpointFailureCommandAttribution(t *testing.T) {
	for _, tc := range []struct {
		name             string
		stalled, grouped bool
	}{{name: "healthy"}, {name: "stalled", stalled: true}, {name: "native-member-stalled", stalled: true, grouped: true}} {
		t.Run(tc.name, func(t *testing.T) {
			stalled := tc.stalled
			q, clock := fixture(t)
			a, b := machine("storage-a", "fake"), machine("storage-b", "fake")
			a.HostMemoryMeasured, b.HostMemoryMeasured = true, true
			a.FreeHostMemoryGB, b.FreeHostMemoryGB = 64, 64
			a.HostAvailableMemoryGB, b.HostAvailableMemoryGB = 64, 64
			check(t, q, a, nil)
			check(t, q, b, nil)
			gate := filepath.Join(t.TempDir(), "exit")
			ready := filepath.Join(t.TempDir(), "ready")
			s := spec("production")
			s.Resumable = true
			if tc.grouped {
				s.Run.MemoryGB = .1
				s.Needs.HostMemoryGB = 1
			}
			s.Run.Command = []string{"sh", "-c", `: > "$2"; while [ ! -e "$1" ]; do sleep 0.01; done; exit 1`, "sh", gate, ready}
			j := submit(t, q, s)
			assigned := check(t, q, a, nil).Job
			if assigned == nil {
				t.Fatal("missing assignment")
			}
			api := httptest.NewServer(API{q, "test"}.Handler())
			defer api.Close()
			root := t.TempDir()
			health := &hostHealthMonitor{clock: clock, mounts: map[string]*hostMountProbe{}}
			w := Worker{StateDir: newTestStateRoot(t), CheckpointRoot: root, Clock: clock, Machine: a,
				Client: Client{URL: api.URL, Token: "test"}, storageHealth: health, memoryCeilings: fakeCeilings(t),
				HostMemorySource:  func(context.Context) (float64, error) { return 64, nil },
				checkpointPrepare: func(Job) (string, error) { return root, nil }}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = assigned.Session
			p, err := startAfterFixtureInputs(t, w, store, *assigned)
			if err != nil {
				t.Fatal(err)
			}
			defer p.stop(clock.Now(), 0, true)
			// A native group returns its supervisor before main's launch gate.
			// Inject the storage fault only once authored code is running.
			await(t, func() bool {
				_, err := os.Stat(ready)
				if err == nil {
					return true
				}
				select {
				case f := <-p.done:
					t.Fatalf("command exited before ready: %+v", f)
				default:
				}
				return false
			})
			clock.Advance(time.Second)
			health.mu.Lock()
			mount := HostMountHealth{Root: "checkpoint", LatencySeconds: .001}
			if stalled {
				mount.Reason = "mount_probe_stalled"
				mount.OperationAgeSeconds = 120
			}
			health.health.History = []HostHealthSample{{At: clock.Now(), Mounts: []HostMountHealth{mount}}}
			health.mu.Unlock()
			if err := os.WriteFile(gate, nil, 0600); err != nil {
				t.Fatal(err)
			}
			finished := <-p.done
			if finished.err != nil {
				t.Fatal(finished.err)
			}
			// Replay the persisted result, after storage has recovered and the
			// old host-health ring has expired. Attribution must survive both.
			health.mu.Lock()
			health.health.History = nil
			health.mu.Unlock()
			raw, err := os.ReadFile(filepath.Join(store.dir, store.state.Identity+".result.json"))
			if err != nil {
				t.Fatal(err)
			}
			var r Result
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatal(err)
			}
			if r.Success || r.Interrupted != stalled {
				t.Fatalf("wrong worker outcome: %+v", r)
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantState, wantOwner := "failed", findingOwnerJob
			if stalled {
				wantState, wantOwner = "queued", findingOwnerMycelium
			}
			if got.State != wantState || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Finding == nil || got.Finding.Owner != wantOwner {
				t.Fatalf("wrong queue attribution: state=%s losses=%d retries=%d finding=%+v", got.State, got.Losses, got.InfrastructureRetries, got.Finding)
			}
			if stalled {
				if !strings.Contains(got.Finding.What, "mount_probe_stalled") || !strings.Contains(got.Finding.What, "120.000s") || !strings.Contains(got.Finding.What, clock.Now().UTC().Format(time.RFC3339Nano)) {
					t.Fatal("finding lost storage evidence", got.Finding)
				}
				if h := got.AttemptHistory; len(h) != 1 || !checkpointStorageInterruption(h[0].ExecutionCause) || !h[0].Interrupted {
					t.Fatal("history lost storage interruption", h)
				}
				if err := q.Complete(r); !errors.Is(err, ErrStale) {
					t.Fatal("stale storage result accepted", err)
				}
				if next := check(t, q, a, nil).Job; next != nil {
					t.Fatal("retried on failed host while alternative exists", next)
				}
				if next := check(t, q, b, nil).Job; next == nil || next.ID != j.ID || next.Attempt != 2 {
					t.Fatal("not assigned to another host", next)
				}
			} else if got.Finding.Code != "command_exit" {
				t.Fatal("healthy exit lost job ownership", got.Finding)
			}
		})
	}
}

func TestCheckpointFailureEvidenceWindow(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 14, 19, 17, 0, 0, time.UTC)}
	root := t.TempDir()
	started := clock.Now()
	clock.Advance(2 * time.Minute)
	exited := clock.Now()
	stall := HostMountHealth{Root: "checkpoint", Reason: "mount_probe_stalled", OperationAgeSeconds: 120}
	for _, tc := range []struct {
		name    string
		history []HostHealthSample
		path    string
		want    bool
	}{
		{"stall", []HostHealthSample{{At: exited, Mounts: []HostMountHealth{stall}}}, root, true},
		{"failure", []HostHealthSample{{At: exited, Mounts: []HostMountHealth{{Root: "checkpoint", Reason: "mount_probe_failed"}}}}, root, true},
		{"old", []HostHealthSample{{At: started.Add(-time.Second), Mounts: []HostMountHealth{stall}}}, root, false},
		{"expired-during-command", []HostHealthSample{{At: started, Mounts: []HostMountHealth{stall}}}, root, false},
		{"future", []HostHealthSample{{At: exited.Add(time.Second), Mounts: []HostMountHealth{stall}}}, root, false},
		{"recovered", []HostHealthSample{{At: started, Mounts: []HostMountHealth{stall}}, {At: exited, Mounts: []HostMountHealth{{Root: "checkpoint"}}}}, root, false},
		{"advisory", []HostHealthSample{{At: exited, Mounts: []HostMountHealth{{Root: "checkpoint", Reason: "mount_probe_slow"}}}}, root, false},
		{"other-root", []HostHealthSample{{At: exited, Mounts: []HostMountHealth{{Root: "nas", Reason: "mount_probe_stalled"}}}}, root, false},
		{"local-checkpoint", []HostHealthSample{{At: exited, Mounts: []HostMountHealth{stall}}}, t.TempDir(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := Worker{CheckpointRoot: root, storageHealth: &hostHealthMonitor{clock: clock, health: HostHealth{History: tc.history}}}
			if got := w.checkpointFailure(tc.path, started, exited); (got != nil) != tc.want {
				t.Fatal("incorrect evidence window", got)
			}
		})
	}
	// A blocked sampler is itself sufficient; reading it must not spawn I/O.
	p := &hostMountProbe{started: started, pending: make(chan hostMountResult, 1)}
	w := Worker{CheckpointRoot: root, storageHealth: &hostHealthMonitor{clock: clock, mounts: map[string]*hostMountProbe{"checkpoint": p}}}
	if got := w.checkpointFailure(root, started, exited); got == nil || got.OperationAgeSeconds != 120 {
		t.Fatal("pending checkpoint operation lost", got)
	}
	p.progressed.Store(&exited)
	if got := w.checkpointFailure(root, started, exited); got != nil {
		t.Fatal("progressing operation classified as stalled", got)
	}
}

func TestCheckpointFailureErrnoAndOutcomeFences(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 14, 19, 19, 0, 0, time.UTC)}
	root := t.TempDir()
	m := &hostHealthMonitor{clock: clock, mounts: map[string]*hostMountProbe{}, storageRoots: map[string]storageRoot{"checkpoint": {path: root, backing: "network"}}}
	m.storageFault("checkpoint", &os.PathError{Op: "rename", Path: filepath.Join(root, "progress.json"), Err: syscall.EIO}, clock.Now(), false)
	w := Worker{CheckpointRoot: root, storageHealth: m}
	e := w.checkpointFailure(root, clock.Now(), clock.Now())
	if e == nil || e.Operation != "rename" || e.Errno != int(syscall.EIO) {
		t.Fatal("lost observed syscall evidence", e)
	}
	// Probe EBADF is logged by the fixed-file probe but is deliberately not a
	// backing-wide data fault. Keep that operation/errno with exit attribution.
	p := &hostMountProbe{root: "checkpoint", clock: clock, pending: make(chan hostMountResult, 1)}
	p.pending <- hostMountResult{latency: time.Millisecond, err: &os.PathError{Op: "close", Path: filepath.Join(root, "private-probe"), Err: syscall.EBADF}}
	mount := p.sample(clock.Now(), nil)
	probeWorker := Worker{CheckpointRoot: root, storageHealth: &hostHealthMonitor{clock: clock, mounts: map[string]*hostMountProbe{"checkpoint": p}, health: HostHealth{History: []HostHealthSample{{At: clock.Now(), Mounts: []HostMountHealth{mount}}}}}}
	if got := probeWorker.checkpointFailure(root, clock.Now(), clock.Now()); got == nil || got.Operation != "close" || got.Errno != int(syscall.EBADF) {
		t.Fatal("lost probe operation/errno", got)
	}
	for _, tc := range []struct {
		name   string
		result Result
		cause  attemptCause
	}{
		{"success", Result{Success: true}, attemptCause{memberOutcome: memberOutcome{Exit: &memberExit{Status: 0}}}},
		{"oom", Result{}, attemptCause{OutOfMemory: true}},
		{"cancelled", Result{}, attemptCause{Cancelled: true}},
		{"stale", Result{}, attemptCause{Stale: true}},
		{"disk", Result{ExecutionCause: &RuntimeCause{Operation: "command", Class: diskLimitExceeded}}, attemptCause{}},
		{"wall-time", Result{ExecutionCause: wallTimeCause()}, attemptCause{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cause.CheckpointStorage = e
			if tc.cause.Exit == nil {
				tc.cause.Exit = &memberExit{Status: 1}
			}
			if r := classifyAttemptOutcome(tc.result, tc.cause); checkpointStorageInterruption(r.ExecutionCause) || r.Interrupted {
				t.Fatal("storage overrode stronger outcome", r)
			}
		})
	}
	c := safeRuntimeCause(&RuntimeCause{Operation: "checkpoint", Class: "storage_interrupted", CheckpointStorage: e})
	if c == nil || !strings.Contains(c.String(), "operation=rename errno=5") {
		t.Fatal("sanitizer discarded storage evidence", c)
	}
	e.Operation = "private-host/path/token"
	c = safeRuntimeCause(&RuntimeCause{Operation: "checkpoint", Class: "storage_interrupted", CheckpointStorage: e})
	raw, _ := json.Marshal(c)
	if strings.Contains(string(raw), "private") || c.CheckpointStorage.Errno != 0 {
		t.Fatal("private storage diagnostic leaked", string(raw))
	}
}

func TestCheckpointFailureDoesNotExhaustRetryBudget(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		t.Run(priority, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("storage-worker", "fake")
			j := submit(t, q, spec(priority))
			for attempt := 1; attempt <= 4; attempt++ {
				a := check(t, q, m, nil).Job
				if a == nil || a.Attempt != attempt {
					t.Fatal("retry not assigned", a)
				}
				if err := q.Started(result(a, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				r := result(a, false)
				r.Interrupted = true
				r.ExecutionCause = &RuntimeCause{Operation: "checkpoint", Class: "storage_interrupted", CheckpointStorage: &CheckpointStorageEvidence{At: clock.Now(), Reason: "mount_probe_stalled", OperationAgeSeconds: 120}}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 {
					t.Fatal("storage interruption spent retry budget", got)
				}
			}
		})
	}
}
