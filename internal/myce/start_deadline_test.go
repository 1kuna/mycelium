package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProductionStartDeadlineDefault(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	s.Needs.Platform = Platform{Required: []string{"xpu"}}
	j := submit(t, q, s)
	c.Advance(149 * time.Second)
	waiting := job(t, q, j.ID)
	if waiting.State != "queued" || !strings.Contains(waiting.Reason, "needs xpu") || !strings.Contains(waiting.Reason, "1s left to start") {
		t.Fatalf("waiting: %+v", waiting)
	}
	c.Advance(time.Second)
	failed := job(t, q, j.ID)
	if failed.State != "failed" || !strings.Contains(failed.Reason, "production start deadline exceeded") || !strings.Contains(failed.Reason, "needs xpu") {
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
	for _, blocker := range []string{"experiments", "Docker"} {
		t.Run(blocker, func(t *testing.T) {
			q, c := fixture(t)
			m := machine("b70", "xpu")
			m.TotalMemoryGB = 8
			check(t, q, m, nil)
			var experiment *Job
			if blocker == "experiments" || blocker == "Docker" {
				if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
					t.Fatal(err)
				}
				s := spec("experiment")
				if blocker == "Docker" {
					s.Run.Docker = true
					s.Run.Command = []string{"docker", "run", "image@sha256:" + strings.Repeat("a", 64)}
				}
				submit(t, q, s)
				experiment = check(t, q, m, nil).Job
			} else if err := q.DisableHost("b70", "disabled by operator", false); err != nil {
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
			want := "stopping 1 attempts"
			if blocker == "Docker" {
				want = "Docker cleanup"
			}
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
	q, c := fixture(t)
	j := submit(t, q, spec("production"))
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	var permit map[string]any
	client := Client{URL: server.URL, Token: "test"}
	if err := client.Call(context.Background(), "POST", "/api/workers/start-permit", result(assigned, true), &permit); err != nil {
		t.Fatalf("in-time permit: %v", err)
	}
	if permit["remaining_seconds"] != float64(150) {
		t.Fatalf("permit: %+v", permit)
	}
	c.Advance(ProductionStartTimeout)
	err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/workers/start-permit", result(assigned, true), &permit)
	if err == nil {
		t.Fatal("late permit granted")
	}
	requireState(t, q, j.ID, "failed")
}

func TestProductionStartDeadlineWorkerPreparationCannotLaunchLate(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	marker := filepath.Join(t.TempDir(), "started")
	s.Run.Command = []string{"sh", "-c", "echo started > " + marker}
	j := submit(t, q, s)
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: t.TempDir(), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}, checkpointPrepare: func(Job) (string, error) { c.Advance(ProductionStartTimeout); return t.TempDir(), nil }}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	p, err := w.start(store, *assigned)
	if err == nil || p != nil {
		t.Fatalf("late launch: %v %v", p, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("command ran: %v", err)
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
	failed := job(t, q, j.ID)
	if failed.State != "failed" || !failed.Spec.StartBy.Equal(*j.Spec.StartBy) {
		t.Fatalf("retry renewed deadline: %+v", failed)
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
	w := Worker{StateDir: t.TempDir(), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	p, err := w.start(store, *assigned)
	if p != nil {
		<-p.done
	}
	if err == nil {
		t.Fatal("supervisor startup was mistaken for command exec")
	}
}

func TestProductionStartDeadlineDockerLateDispatch(t *testing.T) {
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
	p, err := w.start(store, *assigned)
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
}

func TestProductionStartDeadlineUnconfirmedHost(t *testing.T) {
	q, c := fixture(t)
	m := machine("b70", "xpu")
	m.DockerCleanup = "cleanup unconfirmed"
	check(t, q, m, nil)
	j := submit(t, q, spec("production"))
	if reply := check(t, q, m, nil); reply.Job != nil {
		t.Fatal("unconfirmed Docker host admitted production")
	}
	waiting := job(t, q, j.ID)
	if !strings.Contains(waiting.Reason, "Docker cleanup") {
		t.Fatal(waiting.Reason)
	}
	c.Advance(ProductionStartTimeout)
	requireState(t, q, j.ID, "failed")
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
			if err := q.DisableHost("b70", "maintenance", true); err != nil {
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

func TestProductionStartDeadlineOwnDockerPreparationPermit(t *testing.T) {
	q, _ := fixture(t)
	m := machine("b70", "xpu")
	s := dockerJob().Spec
	s.Priority = "production"
	submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("not assigned")
	}
	m.DockerCleanup = "creating"
	check(t, q, m, assigned)
	if _, err := q.PermitStart(result(assigned, true)); err != nil {
		t.Fatalf("own preparation rejected: %v", err)
	}
	sibling := machine("sibling", "xpu")
	sibling.HostID = "b70"
	sibling.DockerCleanup = "cleanup unconfirmed"
	check(t, q, sibling, nil)
	if _, err := q.PermitStart(result(assigned, true)); err == nil {
		t.Fatal("unknown sibling cleanup ignored")
	}
}

func TestProductionStartDeadlineWorkerSendsRejectedResultCleanup(t *testing.T) {
	w := recoveryWorker(t)
	w.HostStateDir = t.TempDir()
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
	store.state.Session, store.state.Job, store.state.Result = j.Session, &j, &r
	if err = store.save(); err != nil {
		t.Fatal(err)
	}
	store.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan Result, 1)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api/workers/checkin":
			var c Checkin
			json.NewDecoder(req.Body).Decode(&c)
			if c.Running == nil {
				cancel()
			}
			json.NewEncoder(rw).Encode(Reply{Stop: c.Running != nil, Kill: c.Running != nil, CheckpointRoot: filepath.Dir(j.Checkpoint)})
		case "/api/workers/complete":
			var got Result
			json.NewDecoder(req.Body).Decode(&got)
			completed <- got
			rw.Write([]byte(`{}`))
		default:
			rw.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	w.Client.URL = server.URL
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case err = <-done:
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
	s.Run.Command = []string{command}
	submit(t, q, s)
	assigned := check(t, q, machine("b70", "xpu"), nil).Job
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{StateDir: t.TempDir(), Machine: machine("b70", "xpu"), Clock: c, Client: Client{URL: server.URL, Token: "test"}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	_, err = w.start(store, *assigned)
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
