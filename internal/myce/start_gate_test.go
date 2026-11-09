package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProductionStartGateRecoveryWindow(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, outage := range []time.Duration{10 * time.Second, 151 * time.Second} {
			t.Run(outage.String(), func(t *testing.T) {
				q, c := fixture(t)
				s := spec("production")
				deadline := c.Now().Add(30 * time.Second)
				s.StartBy = &deadline
				submit(t, q, s)
				a := check(t, q, machine("b70", "xpu"), nil).Job
				if e := q.Started(result(a, true)); e != nil {
					t.Fatal(e)
				}
				c.Advance(10 * time.Minute) // A long run must not consume the recovery window.
				lostAt := c.Now()
				r := result(a, false)
				r.Interrupted = true
				if e := q.Complete(r); e != nil {
					t.Fatal(e)
				}
				got := job(t, q, a.ID)
				if got.State != "queued" || !got.Spec.StartBy.Equal(lostAt.Add(30*time.Second)) {
					t.Fatalf("no fresh original-length window: %+v", got)
				}
				c.Advance(outage)
				b := check(t, q, machine("b70", "xpu"), nil).Job
				if outage > 30*time.Second {
					got = job(t, q, a.ID)
					if b != nil || got.State != "failed" || !strings.Contains(got.Reason, "production start deadline exceeded") {
						t.Fatalf("long outage: %+v", got)
					}
				} else {
					if b == nil || b.Attempt <= a.Attempt {
						t.Fatal("brief loss did not resume")
					}
					if e := q.Started(result(b, true)); e != nil {
						t.Fatal(e)
					}
					// A second loss gets the original 30s window, not elapsed time since submission.
					c.Advance(time.Minute)
					lostAgain := c.Now()
					r = result(b, false)
					r.Interrupted = true
					if e := q.Complete(r); e != nil {
						t.Fatal(e)
					}
					if got = job(t, q, a.ID); !got.Spec.StartBy.Equal(lostAgain.Add(30 * time.Second)) {
						t.Fatalf("window grew: %+v", got)
					}
				}
			})
		}

	})

	t.Run("ProductionStartGateDetectedLoss", func(t *testing.T) {
		for _, started := range []bool{false, true} {
			t.Run(map[bool]string{false: "unstarted", true: "started"}[started], func(t *testing.T) {
				q, c := fixture(t)
				if e := q.SetLossTimeouts(5*time.Second, 20*time.Second, 60*time.Second); e != nil {
					t.Fatal(e)
				}
				s := spec("production")
				s.AllowedMachines = []string{"b70", "retry"}
				j := submit(t, q, s)
				m := machine("b70", "xpu")
				a := check(t, q, m, nil).Job
				check(t, q, machine("witness", "fake"), nil)
				if started {
					if e := q.Started(result(a, true)); e != nil {
						t.Fatal(e)
					}
				}
				c.Advance(5 * time.Second)
				check(t, q, machine("witness", "fake"), nil)
				got := job(t, q, j.ID)
				if got.State != "queued" || got.Losses != 1 {
					t.Fatalf("loss not detected: %+v", got)
				}
				want := *j.Spec.StartBy
				if started {
					want = c.Now().Add(ProductionStartTimeout)
				}
				if !got.Spec.StartBy.Equal(want) {
					t.Fatalf("loss window: %s, want %s", got.Spec.StartBy, want)
				}
				c.Advance(time.Second)
				// Loss leaves the original GPU slot in custody in both arms.
				resumed := check(t, q, machine("retry", "xpu"), nil).Job
				if resumed == nil || resumed.Attempt != a.Attempt+1 {
					t.Fatal("lost worker did not resume")
				}
				if e := q.Started(result(resumed, true)); e != nil {
					t.Fatal(e)
				}
			})
		}

	})

	t.Run("ProductionStartGatePlannedStopDoesNotRenewOnLoss", func(t *testing.T) {
		for _, path := range []string{"quiet", "restart", "completion"} {
			t.Run(path, func(t *testing.T) {
				q, c := fixture(t)
				if err := q.SetLossTimeouts(5*time.Second, 20*time.Second, 60*time.Second); err != nil {
					t.Fatal(err)
				}
				s := spec("production")
				s.AllowedMachines = []string{"b70"}
				j := submit(t, q, s)
				m := machine("b70", "xpu")
				a := check(t, q, m, nil).Job
				check(t, q, machine("witness", "fake"), nil)
				if err := q.Started(result(a, true)); err != nil {
					t.Fatal(err)
				}
				if err := q.Leave(m.ID, true); err != nil {
					t.Fatal(err)
				}
				requireState(t, q, j.ID, "stopping")
				c.Advance(5 * time.Second)
				switch path {
				case "quiet":
					check(t, q, machine("witness", "fake"), nil)
				case "restart":
					m.Session = "new-session"
					if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: a.Session}); err != nil {
						t.Fatal(err)
					}
				case "completion":
					r := result(a, false)
					r.Interrupted = true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				}
				got := job(t, q, j.ID)
				if !got.Spec.StartBy.Equal(*j.Spec.StartBy) {
					t.Fatalf("planned stop renewed deadline: %s, want %s", got.Spec.StartBy, j.Spec.StartBy)
				}
			})
		}

	})

	t.Run("PreemptedNonresumableRestartDoesNotChargeLoss", func(t *testing.T) {
		q, _ := fixture(t)
		s := spec("experiment")
		s.Resumable = false
		j := submit(t, q, s)
		m := machine("one", "fake")
		old := check(t, q, m, nil).Job
		q.s.Jobs[j.ID].Losses = 2
		submit(t, q, spec("production"))
		m.Session = "new-session"
		_, e := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: old.Session})
		if e != nil {
			t.Fatal(e)
		}
		got := job(t, q, j.ID)
		if got.State != "queued" || got.Losses != 2 {
			t.Fatal("preemption consumed infrastructure retry", got)
		}
		if e = q.Complete(result(old, true)); e != ErrStale {
			t.Fatal("stale preempted result accepted", e)
		}

	})
}

func TestProductionStartGateExecutionAndReplay(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, c := fixture(t)
		s := spec("production")
		s.Resumable = false
		s.Run.Command = []string{"/bin/sh", "-c", "echo ran >> {work}/executions"}
		submit(t, q, s)
		a := check(t, q, machine("b70", "xpu", "test-host"), nil).Job
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		handler := API{Queue: q, Token: "test"}.Handler()
		server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/workers/start-permit" {
				t.Error("extra preparation round trip")
			}
			handler.ServeHTTP(rw, r)
			if r.URL.Path == "/api/workers/complete" {
				cancel()
			}
		}))
		defer server.Close()
		w := recoveryWorker(t)
		w.Machine = machine("b70", "xpu", "test-host")
		w.Machine.Device = "0"
		w.nativeGPUDeviceProbe = syntheticNativeGPUDeviceProbe(MachineDevice{ID: "0", Platform: "xpu", Selector: "0"})
		w.InventoryProbe = benchmarkTestInventory
		w.CPUIsolationProbe = func(context.Context) error { return nil }
		w.Client = Client{URL: server.URL, Token: "test"}
		store, e := w.openStore()
		if e != nil {
			t.Fatal(e)
		}
		store.state.Session = a.Session
		p, e := startAfterFixtureInputs(t, w, store, *a)
		if e != nil {
			t.Fatal(e)
		}
		select {
		case f := <-p.done:
			if !f.result.Success || f.err != nil {
				t.Fatalf("finish: %+v", f)
			}
		case <-time.After(testWaitTimeout):
			t.Fatal("start stalled")
		}
		// Completion is still offline in the durable spool; the queue knows the start.
		c.Advance(ProductionStartTimeout)
		requireState(t, q, a.ID, "running")
		store.close()
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		select {
		case e = <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(testWaitTimeout):
			t.Fatal("replay stalled")
		}
		requireState(t, q, a.ID, "succeeded")
		data, e := os.ReadFile(filepath.Join(p.job.Spec.Run.Workdir, "executions"))
		if e != nil || string(data) != "ran\n" {
			t.Fatalf("execution count: %q, %v", data, e)
		}

	})

	t.Run("ProductionStartDeadlineWorkerPreparationCannotLaunchLate", func(t *testing.T) {
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

	})

	t.Run("ProductionStartDeadlineRequiresSuccessfulExec", func(t *testing.T) {
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

	})

	t.Run("ProductionStartDeadlineDockerLateDispatch", func(t *testing.T) {
		q, _ := fixture(t)
		w, store, clock, root := dockerFixture(t)
		q.clock = clock
		s := dockerJob().Spec
		s.Priority = "production"
		s.Resumable = false
		submit(t, q, s)
		assigned := check(t, q, w.Machine, nil).Job
		server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		defer server.Close()
		w.Client = Client{URL: server.URL, Token: "test"}
		clock.advance(ProductionStartTimeout)
		p, err := startAfterFixtureInputs(t, w, store, *assigned)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-p.done:
		case <-time.After(testWaitTimeout):
			p.stop(clock.Now(), 0, true)
			t.Fatal("Docker cleanup did not settle")
		}
		if _, err := os.Stat(filepath.Join(root, "running")); !os.IsNotExist(err) {
			t.Fatalf("late Docker start: %v", err)
		}
		if got := job(t, q, assigned.ID); got.State != "failed" {
			t.Fatalf("job: %+v", got)
		}

	})

	t.Run("ProductionStartDeadlineExecErrorPreservesUnknownRelease", func(t *testing.T) {
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

	})

	t.Run("ProductionStartGateUnreachableDoesNotExecute", func(t *testing.T) {
		q, c := fixture(t)
		s := spec("production")
		s.Resumable = false
		s.Run.Command = []string{"/bin/sh", "-c", "echo ran > {work}/ran"}
		submit(t, q, s)
		a := check(t, q, machine("b70", "xpu"), nil).Job
		server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		server.Close()
		w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
		store, e := w.openStore()
		if e != nil {
			t.Fatal(e)
		}
		defer store.close()
		store.state.Session = a.Session
		retries := 0
		w.Client.Pause = func(context.Context, time.Duration) error { retries++; return errors.New("unexpected outage retry") }
		p, e := startAfterFixtureInputs(t, w, store, *a)
		// The durable custody gate precedes supervisor creation and Started.
		if p != nil || !errors.Is(e, errWorkerStartAuthorization) || !errors.Is(e, errWorkerControlUnreachable) || retries != 0 {
			t.Fatalf("unreachable queue did not refuse execution immediately: process=%v err=%v", p, e)
		}

		if _, e = os.Stat(filepath.Join(w.StateDir, "job-files", a.ID, "1", "ran")); !os.IsNotExist(e) {
			t.Fatalf("payload ran or marker inaccessible: %v", e)
		}
		c.Advance(ProductionStartTimeout)
		requireState(t, q, a.ID, "failed")

	})

	t.Run("ProductionStartGateWorkerRestartReconciles", func(t *testing.T) {
		q, _ := fixture(t)
		s := spec("production")
		s.Run.Command = []string{"/bin/sh", "-c", "echo ran >> {work}/ran"}
		submit(t, q, s)
		a := check(t, q, machine("b70", "xpu"), nil).Job
		if e := q.Started(result(a, true)); e != nil {
			t.Fatal(e)
		}
		w := recoveryWorker(t)
		w.Machine = machine("b70", "xpu")
		w.InventoryProbe = benchmarkTestInventory
		w.CPUIsolationProbe = func(context.Context) error { return nil }
		w.Clock = RealClock{}
		store, e := w.openStore()
		if e != nil {
			t.Fatal(e)
		}
		store.state.Session = a.Session
		store.state.Job = a
		if e = store.save(); e != nil {
			t.Fatal(e)
		}
		store.close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var reconciled Result
		server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/workers/complete" {
				if !decode(rw, r, &reconciled) {
					return
				}
				cancel()
				respond(rw, map[string]bool{"ok": true}, nil)
				return
			}
			if r.URL.Path == "/api/workers/checkin" {
				respond(rw, Reply{AttemptProtocol: keyedAttemptProtocol, CheckpointRoot: q.root}, nil)
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			respond(rw, map[string]bool{}, nil)
		}))
		defer server.Close()
		w.Client = Client{URL: server.URL, Token: "test"}
		done := make(chan error, 1)
		go func() { done <- w.Run(ctx) }()
		select {
		case e = <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(testWaitTimeout):
			t.Fatal("restart stalled")
		}
		if !reconciled.Interrupted || reconciled.ID != a.ID || reconciled.Attempt != a.Attempt {
			t.Fatalf("not reconciled: %+v", reconciled)
		}
		if _, e = os.Stat(filepath.Join(w.StateDir, "job-files", a.ID, "1", "ran")); !os.IsNotExist(e) {
			t.Fatalf("re-executed: %v", e)
		}

	})

	t.Run("ProductionStartGateStorageFailureDoesNotExecute", func(t *testing.T) {
		q, c := fixture(t)
		s := spec("production")
		s.Resumable = false
		s.Run.Command = []string{"/bin/sh", "-c", "echo ran > {work}/ran"}
		submit(t, q, s)
		a := check(t, q, machine("b70", "xpu"), nil).Job
		if _, e := q.db.Exec("PRAGMA query_only=ON"); e != nil {
			t.Fatal(e)
		}
		server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		defer server.Close()
		w := Worker{StateDir: newTestStateRoot(t), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
		store, e := w.openStore()
		if e != nil {
			t.Fatal(e)
		}
		defer store.close()
		store.state.Session = a.Session
		p, e := startAfterFixtureInputs(t, w, store, *a)
		// The durable custody gate precedes supervisor creation and Started.
		if p != nil || !errors.Is(e, errWorkerStartAuthorization) {
			t.Fatalf("storage refusal did not refuse before execution: process=%v err=%v", p, e)
		}

		if _, e = os.Stat(filepath.Join(w.StateDir, "job-files", a.ID, "1", "ran")); !os.IsNotExist(e) {
			t.Fatalf("payload ran: %v", e)
		}
		if _, e = q.db.Exec("PRAGMA query_only=OFF"); e != nil {
			t.Fatal(e)
		}
		if got := job(t, q, a.ID); got.StartedAt != nil {
			t.Fatal("failed write retained start")
		}

	})

	t.Run("WorkerRechecksMemoryBeforeCommand", func(t *testing.T) {
		q, clock := fixture(t)
		j := submit(t, q, spec("production"))
		j.Attempt = 1
		w := Worker{Clock: clock, StateDir: newTestStateRoot(t), Machine: machine("memory", "fake"), MemorySource: func(context.Context) (float64, error) { return 2, nil }}
		s, e := w.openStore()
		if e != nil {
			t.Fatal(e)
		}
		defer s.close()
		await(t, func() bool {
			_, e = w.start(s, *j)
			return !errors.Is(e, errWorkerCheckpoint) || !strings.Contains(e.Error(), "input copy pending")
		})
		if !errors.Is(e, errWorkerMemory) {
			t.Fatalf("memory recheck: %v", e)
		}
		if s.state.Job != nil {
			t.Fatal("insufficient-memory command entered spool")
		}

	})
}
