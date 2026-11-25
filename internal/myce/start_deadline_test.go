package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProductionStartDeadlineQualifiedRouteDiagnostic(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	decoy := cpuMachine("a-incompatible", "a-incompatible", "cpu", 8, 32)
	decoy.CapabilityProfile.OS = "darwin"
	check(t, q, decoy, nil)
	if err := setMaintenance(t, q, decoy.ID, "drain"); err != nil {
		t.Fatal(err)
	}
	// The only qualified route lacks RAM; the drained host cannot execute
	// this extension and must not supply its startup failure explanation.
	m.HostAvailableMemoryGB = .05
	check(t, q, m, nil)
	s.Priority = "production"
	// This diagnostic tests caller-owned expiry. Generated startup windows
	// now wait through real capacity shortages under #1011.
	deadline := clock.Now().Add(ProductionStartTimeout)
	s.StartBy = &deadline
	j := submit(t, q, s)
	var logs testLogBuffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	clock.Advance(ProductionStartTimeout)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	failed := job(t, q, j.ID)
	want := m.ID + ": RAM needs 0.100 GiB, available 0.050 GiB"
	if failed.State != "failed" || failed.Attempt != 0 || !strings.Contains(failed.Reason, want) || strings.Contains(failed.Reason, decoy.ID) {
		t.Errorf("wrong startup diagnostic: %+v", failed)
	}
	if got := statusJobValue(failed); got.Reason != "production start deadline exceeded" {
		t.Errorf("public reason leaked internal diagnostics: %q", got.Reason)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := logs.String(); strings.Count(got, "event=production_start_failed") != 1 || !strings.Contains(got, "job="+j.ID) || !strings.Contains(got, "attempt=0") || !strings.Contains(got, want) || strings.Contains(got, decoy.ID) {
		t.Errorf("missing or repeated deadline evidence: %s", got)
	}
}

func TestProductionStartDeadlineBlockers(t *testing.T) {
	for _, blocker := range []string{"experiments"} {
		t.Run(blocker, func(t *testing.T) {
			q, c := fixture(t)
			m := machine("b70", "xpu")
			m.TotalMemoryGB = 8
			check(t, q, m, nil)
			var experiment *Job
			if blocker == "experiments" {
				if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
					t.Fatal(err)
				}
				submit(t, q, spec("experiment"))
				experiment = check(t, q, m, nil).Job
			} else if err := setMaintenance(t, q, "b70", "disable"); err != nil {
				t.Fatal(err)
			}
			s := spec("production")
			deadline := c.Now().Add(6 * time.Second)
			s.StartBy = &deadline
			j := submit(t, q, s)
			if experiment != nil {
				check(t, q, m, experiment)
			}
			waiting := job(t, q, j.ID)
			want := "waiting for GPU batch slot held by " + experiment.ID
			if blocker == "disabled" {
				want = "host b70 disabled by operator"
			}
			if !strings.Contains(waiting.Reason, want) || !strings.Contains(waiting.Reason, "6s left to start") {
				t.Fatalf("waiting: %s", waiting.Reason)
			}
			c.Advance(6 * time.Second)
			failed := job(t, q, j.ID)
			if failed.State != "failed" || !strings.Contains(failed.Reason, want) {
				t.Fatalf("failed: %+v", failed)
			}
			if experiment != nil {
				requireState(t, q, experiment.ID, "stopping")
				if len(q.s.HostReleases) == 0 {
					t.Fatal("release custody cleared")
				}
				// Room arriving at the deadline cannot resurrect production.
				finish(t, q, experiment, false)
				if reply := check(t, q, m, nil); reply.Job != nil && reply.Job.ID == j.ID {
					t.Fatal("zombie start")
				}
			}
		})
	}
}

func TestCheckinDefersStartupReasonsButEnforcesDeadline(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "cpu")
	check(t, q, m, nil)
	s := spec("production")
	s.AllowedMachines = []string{"absent"}
	deadline := clock.Now().Add(10 * time.Second)
	s.StartBy = &deadline
	j := submit(t, q, s)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	stored := func() Job {
		q.mu.RLock()
		defer q.mu.RUnlock()
		return *q.s.Jobs[j.ID]
	}
	if reason := stored().Reason; !strings.Contains(reason, "10s left to start") {
		t.Fatalf("initial compact reason %q", reason)
	}
	clock.Advance(time.Second)
	check(t, q, m, nil)
	if reason := stored().Reason; !strings.Contains(reason, "10s left to start") {
		t.Fatalf("check-in rebuilt compact startup advice: %q", reason)
	}
	if detail, err := q.jobSnapshot(j.ID); err != nil || !strings.Contains(detail.Reason, "9s left to start") {
		t.Fatalf("detailed status did not project the current startup budget: %+v %v", detail, err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if reason := stored().Reason; !strings.Contains(reason, "9s left to start") {
		t.Fatalf("tick did not refresh compact startup advice: %q", reason)
	}
	clock.Advance(9 * time.Second)
	check(t, q, m, nil)
	if got := stored(); got.State != "failed" || got.Attempt != 0 || got.FinishedAt == nil || !got.FinishedAt.Equal(deadline) {
		t.Fatalf("check-in failed to enforce the exact startup deadline: %+v", got)
	}
}

func TestProductionStartDeadlinePermit(t *testing.T) {
	for _, timely := range []bool{true, false} {
		t.Run(fmt.Sprintf("timely=%t", timely), func(t *testing.T) {
			q, c := fixture(t)
			s := spec("production")
			s.WallTime = "10m"
			j := submit(t, q, s)
			assigned := check(t, q, machine("b70", "xpu"), nil).Job
			if assigned == nil {
				t.Fatal("not assigned")
			}
			custody := NativeCustodyReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Machine: assigned.Machine, Session: assigned.Session, ProcessID: "original-process", CustodyID: "original-group", Phase: "reserved"}
			if err := q.NativeCustody(custody); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			var permit map[string]any
			client := Client{URL: server.URL, Token: "test"}
			if timely {
				if err := client.Call(context.Background(), "POST", "/api/workers/started", result(assigned, true), &permit); err != nil {
					t.Fatalf("in-time permit: %v", err)
				}
				if permit["start_gate"] != startGateProtocol {
					t.Fatalf("permit: %+v", permit)
				}
			}
			before := job(t, q, j.ID)
			c.Advance(ProductionStartTimeout)
			err := client.Call(context.Background(), "POST", "/api/workers/started", result(assigned, true), &permit)
			if timely {
				// The reply might have been lost: acknowledge the original durable
				// start, without minting a new start, budget or process identity.
				got := job(t, q, j.ID)
				starts := 0
				for _, e := range got.LedgerEntries {
					if e.Kind == "started" {
						starts++
					}
				}
				if err != nil || got.State != "running" || got.StartedAt == nil || !got.StartedAt.Equal(*before.StartedAt) || starts != 1 || len(q.s.ProductionStarts) != 1 || got.Starts != 1 || permit["wall_time_remaining_seconds"] != (10*time.Minute-ProductionStartTimeout).Seconds() {
					t.Fatal("replay changed original authorization", got, permit, err)
				}
				custody.ProcessID, custody.CustodyID = "second-process", "second-group"
				if err := q.NativeCustody(custody); !errors.Is(err, ErrStale) {
					t.Fatal("replay authorized a duplicate process", err)
				}
			} else {
				var refusal *HTTPError
				if !errors.As(err, &refusal) || refusal.Code != "request_stale" {
					t.Fatal("late first start did not retain its typed refusal", err)
				}
				requireState(t, q, j.ID, "failed")
			}
		})
	}
}

func TestProductionStartDeadlineWorkerPreparationCannotLaunchLate(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	s.Run.Command = []string{"sh", "-c", "echo started > {work}/started"}
	j := submit(t, q, s)
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	prepared := t.TempDir()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}, checkpointPrepare: func(Job) (string, error) { c.Advance(ProductionStartTimeout); return prepared, nil }}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.MemorySource = func(context.Context) (float64, error) { return 8, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	p, err := startAfterFixtureInputs(t, w, store, *assigned)
	// Custody reservation now refuses the expired assignment before a native
	// supervisor exists, rather than returning a process for the later hand-off.
	if p != nil || !errors.Is(err, errWorkerStartAuthorization) || !strings.Contains(err.Error(), "request_stale") {
		t.Fatalf("late launch was not refused before execution: process=%v err=%v", p, err)
	}
	if _, err := os.Stat(filepath.Join(w.StateDir, "job-files", assigned.ID, "1", "started")); !os.IsNotExist(err) {
		t.Fatalf("command ran: %v", err)
	}
	if got := job(t, q, j.ID); got.StartedAt != nil {
		t.Fatal("expired reservation retained start")
	}

	requireState(t, q, j.ID, "failed")
	finish(t, q, assigned, false)
}

func TestProductionStartDeadlineStatusPage(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("production"))
	for _, expired := range []bool{false, true} {
		if expired {
			c.Advance(ProductionStartTimeout)
		}
		snapshot, err := q.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		var html bytes.Buffer
		if err := statusPage.Execute(&html, snapshot); err != nil {
			t.Fatal(err)
		}
		want := "150s left to start"
		if expired {
			want = "production start deadline exceeded"
		}
		if !strings.Contains(html.String(), want) {
			t.Fatalf("missing %s", want)
		}
	}
}

func TestProductionStartDeadlinePastDoesNotPreempt(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("b70", "xpu")
	experiment := check(t, q, m, nil).Job
	s := spec("production")
	now := c.Now()
	s.StartBy = &now
	j := submit(t, q, s)
	requireState(t, q, j.ID, "failed")
	if reply := check(t, q, m, experiment); reply.Stop {
		t.Fatal("past-deadline job stopped experiment")
	}
}

func TestProductionStartDeadlineRequiresSuccessfulExec(t *testing.T) {
	q, c := fixture(t)
	command := filepath.Join(t.TempDir(), "bad-interpreter")
	if err := os.WriteFile(command, []byte("#!/myce-does-not-exist\n"), 0700); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{command}
	submit(t, q, s)
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	p, err := startAfterFixtureInputs(t, w, store, *assigned)
	if p != nil {
		<-p.done
	}
	if err == nil {
		t.Fatal("supervisor startup was mistaken for command exec")
	}
}

func TestProductionStartDeadlineRoomAtBoundary(t *testing.T) {
	q, c := fixture(t)
	m := machine("b70", "xpu")
	submit(t, q, spec("experiment"))
	experiment := check(t, q, m, nil).Job
	s := spec("production")
	deadline := c.Now().Add(6 * time.Second)
	s.StartBy = &deadline
	j := submit(t, q, s)
	check(t, q, m, experiment)
	c.Advance(6 * time.Second)
	finish(t, q, experiment, false) // First queue mutation at the boundary makes room.
	if reply := check(t, q, m, nil); reply.Job != nil && reply.Job.ID == j.ID {
		t.Fatal("deadline-boundary assignment")
	}
	requireState(t, q, j.ID, "failed")
}

func TestProductionStartDeadlineWorkerSendsRejectedResultCleanup(t *testing.T) {
	w := recoveryWorker(t)

	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	j := recoveryJob(t, []string{"echo", "done"})
	j.Spec.Priority = "production"
	deadline := w.Clock.Now().Add(-time.Second)
	j.Spec.StartBy = &deadline
	j.Machine, j.Session = w.Machine.ID, "old-session"
	r := result(&j, true)
	store.state.Session = j.Session
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	child, err := newAttemptStore(store, j)
	if err != nil {
		t.Fatal(err)
	}
	child.state.Result = &r
	if err = child.save(); err != nil {
		t.Fatal(err)
	}
	store.close()
	ctx, cancel := context.WithCancel(context.Background())
	// Failure teardown must release handlers before joining the gracefully
	// stopping worker, and keep the server alive until that join completes.
	teardown := make(chan struct{})
	entered := make(chan struct{}, 1)
	blockCompletion := os.Getenv("MYCE_TEST_BLOCK_DEADLINE_COMPLETION") == "1"
	completed := make(chan Result, 1)
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api/workers/checkin":
			var c Checkin
			json.NewDecoder(req.Body).Decode(&c)
			c, err := normalizeAttemptCheckin(c)
			if err != nil {
				t.Error(err)
				return
			}
			reply := Reply{AttemptProtocol: keyedAttemptProtocol, CheckpointRoot: filepath.Dir(j.Checkpoint)}
			if len(c.Attempts) == 0 {
				cancel()
			} else {
				if len(c.Attempts) != 1 {
					t.Error("expected one held attempt")
					return
				}
				a := c.Attempts[0]
				reply.Attempts = []AttemptDirective{{Execution: a.Execution, Session: a.Session, Stop: true, Kill: true}}
			}
			json.NewEncoder(rw).Encode(reply)
		case "/api/workers/complete":
			var got Result
			json.NewDecoder(req.Body).Decode(&got)
			if blockCompletion {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-teardown
			}
			select {
			case completed <- got:
			default:
				t.Error("unexpected duplicate cleanup completion")
			}
			rw.Write([]byte(`{}`))
		default:
			rw.Write([]byte(`{}`))
		}
	}))
	w.Client.URL = server.URL
	done := make(chan struct{})
	var runErr error
	t.Cleanup(func() {
		cancel()
		close(teardown)
		<-done
		server.Close()
		if blockCompletion {
			fmt.Println("blocked-completion teardown joined")
		}
	})
	go func() {
		defer close(done)
		runErr = w.Run(ctx)
	}()
	if blockCompletion {
		select {
		case <-entered:
			t.Fatal("intentional failure with completion handler blocked")
		case <-done:
			t.Fatalf("worker exited before blocked completion: %v", runErr)
		case <-time.After(testWaitTimeout):
			t.Fatal("completion handler was not reached")
		}
	}
	select {
	case <-done:
		err = runErr
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("worker stalled")
	}
	select {
	case got := <-completed:
		if !got.ReleaseOnly || got.ID != j.ID || got.Session != j.Session {
			t.Fatalf("cleanup acknowledgement: %+v", got)
		}
	default:
		t.Fatal("verified cleanup was discarded with the stale result")
	}
}

// #991: a fatal assertion must not wait forever in Server.Close while the
// worker retries a blocked completion. Use the real fixture in a subprocess so
// its intentional assertion failure can be checked without failing this test.
func TestProductionStartDeadlineBlockedCompletionTeardown(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestProductionStartDeadlineWorkerSendsRejectedResultCleanup$", "-test.v")
	command.Env = append(os.Environ(), "MYCE_TEST_BLOCK_DEADLINE_COMPLETION=1")
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 ||
		!bytes.Contains(output, []byte("intentional failure with completion handler blocked")) ||
		!bytes.Contains(output, []byte("blocked-completion teardown joined")) {
		t.Fatalf("blocked-handler failure did not join teardown: %v\n%s", err, output)
	}
}

func TestProductionStartDeadlineExecErrorPreservesUnknownRelease(t *testing.T) {
	q, c := fixture(t)
	dir := t.TempDir()
	command := filepath.Join(dir, "bad-interpreter")
	if err := os.WriteFile(command, []byte("#!/myce-does-not-exist\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ps"), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{command}
	submit(t, q, s)
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.MemorySource = func(context.Context) (float64, error) { return 8, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	_, err = startAfterFixtureInputs(t, w, store, *assigned)
	if err == nil || !strings.Contains(err.Error(), "command release unconfirmed") {
		t.Fatalf("unknown group cleanup treated as released: %v", err)
	}
}

func TestProductionStartDeadlineReleaseOnlyClearsPendingStatus(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("production"))
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	c.Advance(ProductionStartTimeout)
	requireState(t, q, assigned.ID, "failed")
	r := result(assigned, false)
	r.ReleaseOnly = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, assigned.ID)
	if !got.ReleaseConfirmed || strings.Contains(got.Reason, "release pending") {
		t.Fatalf("settled cleanup still pending: %+v", got)
	}
}
