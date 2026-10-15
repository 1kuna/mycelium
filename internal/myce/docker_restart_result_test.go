package myce

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Freeze the live adapter before driving terminal observations synchronously.
func restartResultFixture(t *testing.T) (Worker, *workerStore, *dockerAttempt, *dockerTestClock, string) {
	t.Helper()
	w, s, c, root := dockerFixture(t)
	p, err := startDockerFixture(w, s, dockerGroupJob(t))
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.docker.close()
	return w, s, p.docker, c, root
}
func restartRead(t *testing.T, s *workerStore) dockerExecution {
	t.Helper()
	var state dockerExecution
	b, e := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &state); e != nil {
		t.Fatal(e)
	}
	return state
}
func restartTick(t *testing.T, d *dockerAttempt) bool {
	t.Helper()
	done, e := d.reconcile()
	if e != nil {
		t.Fatal(e)
	}
	return done
}
func restartMainExit(t *testing.T, d *dockerAttempt, root string) {
	t.Helper()
	if e := os.Remove(filepath.Join(root, "running")); e != nil {
		t.Fatal(e)
	}
	restartTick(t, d)
	if !d.state.MainFinished {
		t.Fatal("main result not observed")
	}
}
func restartOnce(t *testing.T, w Worker, s *workerStore) {
	t.Helper()
	p, e := w.restoreDocker(s)
	if e != nil {
		t.Fatal(e)
	}
	if p != nil {
		p.docker.close()
	}
}
func restartQueue(t *testing.T, s *workerStore, w Worker) *Queue {
	t.Helper()
	q, _ := fixture(t)
	j := *s.state.Job
	j.State = "running"
	j.Machine = w.Machine.ID
	j.Session = s.state.Session
	q.s.Jobs[j.ID] = &j
	return q
}
func restartReleasedResult(t *testing.T, w Worker, s *workerStore) Result {
	t.Helper()
	s.state.Result = nil
	p, e := w.restoreDocker(s)
	if e != nil || p != nil || s.state.Result == nil {
		t.Fatalf("released recovery: %v %v", p, e)
	}
	return *s.state.Result
}

// Cases 1, 4 and 7: disk reload, repeated recovery, original deadline, private
// evidence and the release-before-result / result-before-ack crash boundaries.
func TestDockerRestartCompletedMain(t *testing.T) {
	w, s, d, c, root := restartResultFixture(t)
	restartMainExit(t, d, root)
	deadline := *d.state.StopAt
	for _, step := range []time.Duration{StopGrace / 2, StopGrace/2 - time.Nanosecond} {
		c.advance(step)
		restartOnce(t, w, s)
		st := restartRead(t, s)
		if st.Released || st.Forced || !st.StopAt.Equal(deadline) {
			t.Fatal("early release/kill or renewed grace")
		}
		b, _ := os.ReadFile(filepath.Join(root, "calls"))
		if strings.Count(string(b), "kill --signal=TERM sidecar-") != 2 || strings.Contains(string(b), "kill --signal=KILL") {
			t.Fatal("stop protocol changed")
		}
	}
	c.advance(time.Nanosecond)
	restartOnce(t, w, s) // KILL at the boundary; a settled scan may also finish.
	p, e := w.restoreDocker(s)
	if e != nil {
		t.Fatal(e)
	}
	if p != nil {
		defer p.docker.close()
		select {
		case f := <-p.done:
			if f.err != nil || !f.result.Success || f.result.Interrupted {
				t.Fatalf("resumed result: %+v", f)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cleanup did not complete")
		}
	}
	var saved Result
	data, e := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".result.json"))
	if e != nil || json.Unmarshal(data, &saved) != nil || !saved.Success || saved.Interrupted {
		t.Fatalf("saved resumed result: %+v %v", saved, e)
	}
	// Simulate the durable-release/before-result-write crash window.
	if e := os.Remove(filepath.Join(s.dir, s.state.Identity+".result.json")); e != nil {
		t.Fatal(e)
	}
	st := restartRead(t, s)
	if !st.Released || !st.EvidenceSaved || !st.NetworkRemoved {
		t.Fatalf("incomplete release: %+v", st)
	}
	r := restartReleasedResult(t, w, s) // No result file: release-only crash window.
	if !r.Success || r.Interrupted || r.Forced {
		t.Fatalf("main result lost: %+v", r)
	}
	for id, m := range st.Containers {
		want := 0
		if id != "id1" {
			want = 137
		}
		if m.ExitCode == nil || *m.ExitCode != want || m.StderrTail != "payload stderr\n" {
			t.Fatalf("missing evidence for %s: %+v", id, m)
		}
	}
	if err := saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
		t.Fatal(err)
	}
	s.state.Result = nil
	if err := s.recoverResult(w.Machine.ID); err != nil {
		t.Fatal(err)
	}
	if *s.state.Result != r {
		t.Fatal("result replay changed")
	}
	q := restartQueue(t, s, w)
	for i := 0; i < 2; i++ {
		if e := q.Complete(r); e != nil {
			t.Fatal(e)
		}
	}
	got := job(t, q, r.ID)
	if got.State != "succeeded" || got.Attempt != 1 || got.Losses != 0 {
		t.Fatalf("queue: %+v", got)
	}
	if err := acknowledgeDocker(s, w.Machine.ID, c.Now()); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker-ack.json"))
	c.advance(time.Hour)
	restartOnce(t, w, s)
	if err := acknowledgeDocker(s, w.Machine.ID, c.Now()); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker-ack.json"))
	if string(first) != string(second) {
		t.Fatal("ack retention boundary renewed")
	}
}

// Case 2 includes zero first observed while recovery cleans up the old main.
func TestDockerRestartUnfinishedMainStaysLoss(t *testing.T) {
	w, s, _, _, root := restartResultFixture(t)
	if err := os.Remove(filepath.Join(root, "running")); err != nil {
		t.Fatal(err)
	}
	restartOnce(t, w, s)
	restartOnce(t, w, s)
	r := restartReleasedResult(t, w, s)
	if r.Success || !r.Interrupted {
		t.Fatalf("promoted cleanup exit zero: %+v", r)
	}
	q := restartQueue(t, s, w)
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	got := job(t, q, r.ID)
	if got.State != "queued" || got.Losses != 1 {
		t.Fatalf("not one loss: %+v", got)
	}
	m := w.Machine
	m.HostAvailableMemoryGB = 32
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB = 32
	reply := check(t, q, m, nil)
	if reply.Job == nil || reply.Job.Attempt != 2 {
		t.Fatalf("retry: %+v", reply)
	}
}

// Case 3: durable payload failures retain precedence over recovery interruption.
func TestDockerRestartMainFailures(t *testing.T) {
	for _, kind := range []string{"nonzero", "memory", "oom", "containment", "operator"} {
		t.Run(kind, func(t *testing.T) {
			w, s, d, c, root := restartResultFixture(t)
			if kind == "nonzero" || kind == "oom" {
				if e := os.WriteFile(filepath.Join(root, "mode"), []byte("main-"+kind), 0600); e != nil {
					t.Fatal(e)
				}
			}
			restartMainExit(t, d, root)
			switch kind {
			case "memory":
				d.state.MemoryFailure = "Docker memory ceiling not enforced"
			case "containment":
				d.state.Failure = "Docker containment violation"
			case "operator":
				d.state.OperatorConfirmed = true
			}
			if e := d.saveLocked(); e != nil {
				t.Fatal(e)
			}
			c.advance(StopGrace)
			restartOnce(t, w, s)
			restartOnce(t, w, s)
			r := restartReleasedResult(t, w, s)
			if r.Success || (kind != "operator" && r.Interrupted) {
				t.Fatalf("failure changed: %+v", r)
			}
		})
	}
}

// Case 5: each failed proof retains custody; retries never renew grace.
func TestDockerRestartProofFailures(t *testing.T) {
	for _, kind := range []string{"main-save", "evidence-save", "logs", "inspect", "network", "release-save", "pending-start"} {
		t.Run(kind, func(t *testing.T) {
			w, s, d, c, root := restartResultFixture(t)
			if kind == "main-save" {
				d.persist = func(dockerExecution) error { return errors.New("disk unavailable") }
				if e := os.Remove(filepath.Join(root, "running")); e != nil {
					t.Fatal(e)
				}
				if done, e := d.reconcile(); done || e == nil {
					t.Fatal("failed main write released")
				}
				st := restartRead(t, s)
				if st.MainFinished {
					t.Fatal("failed main write persisted")
				}
				b, _ := os.ReadFile(filepath.Join(root, "calls"))
				if strings.Contains(string(b), "--signal=TERM") {
					t.Fatal("TERM before main persistence")
				}
				restartOnce(t, w, s)
				restartOnce(t, w, s)
				r := restartReleasedResult(t, w, s)
				if r.Success || !r.Interrupted {
					t.Fatal("unknown main promoted")
				}
				return
			}
			restartMainExit(t, d, root)
			deadline := *d.state.StopAt
			switch kind {
			case "inspect":
				_ = os.WriteFile(filepath.Join(root, "mode"), []byte("down"), 0600)
			case "network":
				_ = os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-network-rm"), 0600)
			case "logs":
				_ = os.WriteFile(filepath.Join(root, "mode"), []byte("logs-error"), 0600)
			case "evidence-save":
				_ = os.Mkdir(filepath.Join(s.dir, s.state.Identity+".docker-evidence.json.tmp"), 0700)
			case "release-save":
				d.persist = func(st dockerExecution) error {
					if st.Released {
						return errors.New("release write failed")
					}
					return saveWorkerJSON(s.dir, st.Identity+".docker.json", st)
				}
			case "pending-start":
				d.state.Pending = true
				d.state.Operation = "start"
				d.state.PendingContainer = "missing"
				if e := d.saveLocked(); e != nil {
					t.Fatal(e)
				}
			}
			c.advance(StopGrace)
			for i := 0; i < 2; i++ {
				done, _ := d.reconcile()
				if done || d.state.Released {
					t.Fatal("failed proof released")
				}
			}
			if !d.state.StopAt.Equal(deadline) || restartRead(t, s).Released {
				t.Fatal("deadline/release changed")
			}
			if kind == "pending-start" {
				return
			} // No acceptance witness: deliberately cannot release.
			_ = os.WriteFile(filepath.Join(root, "mode"), nil, 0600)
			if kind == "evidence-save" {
				if e := os.Remove(filepath.Join(s.dir, s.state.Identity+".docker-evidence.json.tmp")); e != nil {
					t.Fatal(e)
				}
			}
			d.persist = nil
			restartOnce(t, w, s)
			restartOnce(t, w, s)
			r := restartReleasedResult(t, w, s)
			if !r.Success || r.Interrupted {
				t.Fatalf("retry lost outcome: %+v", r)
			}
		})
	}
}

// Case 6: disk adoption binds all four fields and opaque custody identity.
func TestDockerRestartRejectsMismatchedResults(t *testing.T) {
	for _, field := range []string{"identity", "job", "attempt", "machine", "session"} {
		t.Run(field, func(t *testing.T) {
			w, s, d, _, _ := restartResultFixture(t)
			if field == "identity" {
				d.state.Identity = "foreign"
			} else {
				switch field {
				case "job":
					d.state.Owner.ID = "foreign"
				case "attempt":
					d.state.Owner.Attempt++
				case "machine":
					d.state.Owner.Machine = "foreign"
				case "session":
					d.state.Owner.Session = "foreign"
				}
			}
			if e := saveWorkerJSON(s.dir, s.state.Identity+".docker.json", d.state); e != nil {
				t.Fatal(e)
			}
			if p, e := w.restoreDocker(s); e == nil {
				if p != nil {
					p.docker.close()
				}
				t.Fatal("foreign custody adopted")
			}
			if field != "identity" {
				if e := saveWorkerJSON(s.dir, s.state.Identity+".result.json", d.state.Owner); e != nil {
					t.Fatal(e)
				}
				if e := s.recoverResult(w.Machine.ID); e == nil {
					t.Fatal("foreign result adopted")
				}
				s.state.Result = d.state.Owner
				if e := s.recoverResult(w.Machine.ID); e == nil {
					t.Fatal("foreign state result adopted")
				}
			}
		})
	}
}

func TestDockerRestartLateSuccessOnlySettlesExactCustody(t *testing.T) {
	q, _ := fixture(t)
	a := machine("old", "fake")
	check(t, q, a, nil)
	j := submit(t, q, dockerJob().Spec)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("not assigned")
	}
	old := result(running, true)
	if e := q.update(func() error {
		q.retainRelease(q.s.Jobs[j.ID])
		next := q.s.Jobs[j.ID]
		next.Attempt++
		next.Machine = "replacement"
		next.Session = "replacement-session"
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	for _, field := range []string{"job", "attempt", "machine", "session"} {
		bad := old
		switch field {
		case "job":
			bad.ID = "unknown"
		case "attempt":
			bad.Attempt += 2
		case "machine":
			bad.Machine = "wrong"
		case "session":
			bad.Session = "wrong"
		}
		if e := q.Complete(bad); !errors.Is(e, ErrStale) {
			t.Fatalf("%s not rejected: %v", field, e)
		}
		if len(q.s.HostReleases) != 1 {
			t.Fatal("wrong result settled old custody")
		}
	}
	if e := q.Complete(old); e != nil {
		t.Fatal(e)
	}
	got := job(t, q, j.ID)
	if len(q.s.HostReleases) != 0 || got.State != "running" || got.Attempt != 2 || got.Losses != 0 {
		t.Fatalf("old success completed replacement: %+v", got)
	}
	if e := q.Complete(old); !errors.Is(e, ErrStale) {
		t.Fatalf("obsolete result adopted: %v", e)
	}
}

func TestDockerRestartGracefulSidecarEvidence(t *testing.T) {
	w, s, d, _, root := restartResultFixture(t)
	if e := os.WriteFile(filepath.Join(root, "mode"), []byte("term"), 0600); e != nil {
		t.Fatal(e)
	}
	restartMainExit(t, d, root)
	restartOnce(t, w, s)
	r := restartReleasedResult(t, w, s)
	if !r.Success || r.Interrupted {
		t.Fatalf("sidecar TERM became loss: %+v", r)
	}
	for id, m := range restartRead(t, s).Containers {
		if id == "id1" {
			continue
		}
		if m.ExitCode == nil || *m.ExitCode != 143 || m.TermOutcome != "sent" || m.StderrTail != "payload stderr\n" {
			t.Fatalf("TERM evidence: %+v", m)
		}
	}
}
