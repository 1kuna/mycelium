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
	if failed.State != "failed" || failed.Attempt != 0 || !strings.Contains(failed.Reason, "waiting for RAM envelope: "+m.ID) || strings.Contains(failed.Reason, decoy.ID) {
		t.Errorf("wrong startup diagnostic: %+v", failed)
	}
	if got := statusJobValue(failed); got.Reason != "production start deadline exceeded" {
		t.Errorf("public reason leaked internal diagnostics: %q", got.Reason)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := logs.String(); strings.Count(got, "event=production_start_failed") != 1 || !strings.Contains(got, "job="+j.ID) || !strings.Contains(got, "attempt=0") || !strings.Contains(got, "waiting for RAM envelope: "+m.ID) || strings.Contains(got, decoy.ID) {
		t.Errorf("missing or repeated deadline evidence: %s", got)
	}
}

func TestProductionStartDeadlineDefault(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	s.Needs.Platform = Platform{Required: []string{"xpu"}}
	j := submit(t, q, s)
	c.Advance(149 * time.Second)
	waiting := job(t, q, j.ID)
	if waiting.State != "queued" || !strings.Contains(waiting.Reason, "with Intel (XPU)") || !strings.Contains(waiting.Reason, "1s left to start") {
		t.Fatalf("waiting: %+v", waiting)
	}
	c.Advance(time.Second)
	failed := job(t, q, j.ID)
	if failed.State != "failed" || !strings.Contains(failed.Reason, "production start deadline exceeded") || !strings.Contains(failed.Reason, "with Intel (XPU)") {
		t.Fatalf("expired: %+v", failed)
	}
	if check(t, q, machine("b70", "xpu"), nil).Job != nil {
		t.Fatal("expired production was assigned")
	}
}

func TestProductionStartDeadlineActualStart(t *testing.T) {
	for _, boundary := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "at"}[boundary], func(t *testing.T) {
			q, c := fixture(t)
			m := machine("b70", "xpu")
			j := submit(t, q, spec("production"))
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("not assigned")
			}
			c.Advance(ProductionStartTimeout - time.Second)
			if boundary {
				c.Advance(time.Second)
			}
			err := q.Started(result(assigned, true))
			if boundary {
				if err == nil {
					t.Fatal("late start accepted")
				}
				failed := job(t, q, j.ID)
				if failed.State != "failed" || !strings.Contains(failed.Reason, "command start on b70") {
					t.Fatalf("failed: %+v", failed)
				}
				if reply := check(t, q, m, assigned); !reply.Stop || !reply.Kill {
					t.Fatalf("no stop: %+v", reply)
				}
				next := submit(t, q, spec("experiment"))
				if check(t, q, m, nil).Job != nil {
					t.Fatal("unreleased expired attempt reused")
				}
				finish(t, q, assigned, true)
				requireState(t, q, j.ID, "failed")
				if check(t, q, m, nil).Job == nil {
					t.Fatalf("released host not reused for %s", next.ID)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				c.Advance(2 * time.Second)
				requireState(t, q, j.ID, "running")
				if got := job(t, q, j.ID); strings.Contains(got.Reason, "left to start") {
					t.Fatalf("started job still waiting: %s", got.Reason)
				}
				finish(t, q, assigned, true)
				requireState(t, q, j.ID, "succeeded")
			}
		})
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

func TestProductionStartDeadlineRestart(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("production"))
	deadline := *j.Spec.StartBy
	c.Advance(149 * time.Second)
	path := strings.TrimSuffix(q.lock.Name(), ".lock")
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, q.root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := job(t, reopened, j.ID); got.State != "queued" || !got.Spec.StartBy.Equal(deadline) {
		t.Fatalf("reopened: %+v", got)
	}
	c.Advance(time.Second)
	requireState(t, reopened, j.ID, "failed")
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

func TestProductionStartDeadlineRetryAndGiveUp(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("production"))
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatal(err)
	}
	c.Advance(ProductionStartTimeout)
	r := result(assigned, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	requeued := job(t, q, j.ID)
	if requeued.State != "queued" || !requeued.Spec.StartBy.Equal(c.Now().Add(ProductionStartTimeout)) {
		t.Fatalf("started attempt did not get a recovery window: %+v", requeued)
	}
	s := spec("production")
	giveUp := c.Now().Add(time.Second)
	s.GiveUpAt = &giveUp
	earlier := submit(t, q, s)
	c.Advance(2 * time.Second)
	if got := job(t, q, earlier.ID); got.State != "failed" || got.Reason != "give-up time reached" {
		t.Fatalf("give-up: %+v", got)
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

func TestProductionStartDeadlineDisabledFleetFailsPromptly(t *testing.T) {
	for _, alternate := range []string{"", "cuda", "xpu"} {
		t.Run("alternate-"+alternate, func(t *testing.T) {
			q, _ := fixture(t)
			check(t, q, machine("b70", "xpu"), nil)
			if alternate != "" {
				check(t, q, machine("other", alternate), nil)
			}
			if err := setMaintenance(t, q, "b70", "disable"); err != nil {
				t.Fatal(err)
			}
			s := spec("production")
			s.Needs.Platform = Platform{Required: []string{"xpu"}}
			j := submit(t, q, s)
			got := job(t, q, j.ID)
			if alternate == "xpu" {
				if got.State != "queued" {
					t.Fatalf("enabled alternate rejected: %+v", got)
				}
			} else if got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") || got.Attempt != 0 {
				t.Fatalf("disabled-only fleet did not fail immediately: %+v", got)
			}
		})
	}
}

func TestProductionStartDeadlineManualDrainWaitsForReturn(t *testing.T) {
	for _, mode := range []string{"idle", "capacity", "capacity-before-drain", "departed"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			capacity := strings.HasPrefix(mode, "capacity")
			if capacity {
				m.HostAvailableMemoryGB = 1
			}
			check(t, q, m, nil)
			s := spec("production")
			s.Needs.HostMemoryGB = 4
			var j *Job
			drain := HostMaintenanceRequest{Action: "drain"}
			if mode == "capacity-before-drain" {
				j = submit(t, q, s)
				if !job(t, q, j.ID).StartCapacityWaiting {
					t.Fatal("fixture was not waiting for capacity")
				}
				// An owner-tagged rollout can drain open work. The ordinary
				// operator drain's production_work_open refusal still applies.
				drain.DrainOwner = "maintenance-test"
			}
			if _, err := q.ChangeHostMaintenance(m.ID, drain); err != nil {
				t.Fatal(err)
			}
			if j == nil {
				j = submit(t, q, s)
			}
			if mode == "departed" {
				if err := q.Leave(m.ID, false); err != nil {
					t.Fatal(err)
				}
				if reply := check(t, q, m, nil); !reply.Leave || !qcMachineGone(t, q, m.ID) {
					t.Fatal("worker did not leave during maintenance", reply)
				}
			}
			clock.Advance(2 * ProductionStartTimeout)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			waiting := job(t, q, j.ID)
			if waiting.State != "queued" || waiting.Attempt != 0 || !waiting.StartCapacityWaiting || !strings.Contains(waiting.Reason, "waiting for drained host worker to return") || !strings.Contains(waiting.Reason, "paused for capacity") {
				t.Fatalf("manual drain discarded work or its budget: %+v", waiting)
			}
			if detail := JobReasonDetail(waiting); !strings.Contains(detail, "waiting for drained host worker to return") {
				t.Fatal("consumer status hid the drain wait", detail)
			}
			if offered := check(t, q, m, nil).Job; offered != nil {
				t.Fatal("drained host accepted work", offered.ID)
			}
			if mode == "departed" {
				// The rollout verifies the replacement worker before undrain.
				m.Session = "returned-worker"
				check(t, q, m, nil)
			}
			if err := setMaintenance(t, q, m.ID, "resume"); err != nil {
				t.Fatal(err)
			}
			if capacity {
				if got := job(t, q, j.ID); got.State != "queued" || !got.StartCapacityWaiting {
					t.Fatal("undrain lost the remaining RAM wait", got.State, got.Reason)
				}
			}
			m.HostAvailableMemoryGB = 64
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != j.ID || assigned.Spec.StartBy.Sub(clock.Now()) != ProductionStartTimeout {
				t.Fatal("return did not restore the original job and budget", assigned)
			}
			if err := q.Started(result(assigned, true)); err != nil {
				t.Fatal(err)
			}
			finish(t, q, assigned, true)
			requireState(t, q, j.ID, "succeeded")
		})
	}
}

func TestProductionStartDeadlineDisabledDepartedHostFailsPromptly(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	if err := setMaintenance(t, q, m.ID, "disable"); err != nil {
		t.Fatal(err)
	}
	if err := q.Leave(m.ID, false); err != nil {
		t.Fatal(err)
	}
	if reply := check(t, q, m, nil); !reply.Leave || !qcMachineGone(t, q, m.ID) {
		t.Fatal("disabled worker did not leave", reply)
	}
	j := submit(t, q, spec("production"))
	if got := job(t, q, j.ID); got.State != "failed" || got.Attempt != 0 || !strings.Contains(got.Reason, "no eligible host is enabled") {
		t.Fatal("removed disabled host did not fail fast", got.State, got.Reason)
	}
}

func TestProductionStartDeadlineDrainAbsoluteCutoffs(t *testing.T) {
	for _, kind := range []string{"start_by", "give_up_at"} {
		t.Run(kind, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			check(t, q, m, nil)
			if err := setMaintenance(t, q, m.ID, "drain"); err != nil {
				t.Fatal(err)
			}
			s := spec("production")
			cutoff := clock.Now().Add(time.Minute)
			if kind == "start_by" {
				s.StartBy = &cutoff
			} else {
				s.GiveUpAt = &cutoff
			}
			j := submit(t, q, s)
			requireState(t, q, j.ID, "queued")
			clock.Advance(time.Minute)
			requireState(t, q, j.ID, "failed")
		})
	}
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
			reply := Reply{CheckpointRoot: filepath.Dir(j.Checkpoint)}
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
			json.NewEncoder(rw).Encode(attemptReply(c, reply))
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
