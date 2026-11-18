package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func deadCustodianFixture(t *testing.T, j *Job) (*workerStore, custodianSnapshot) {
	t.Helper()
	dir, id := custodianRecoveryFixture(t, j)
	cmd := exec.Command("sh", "-c", "read gate")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	birth, err := processBirth(cmd.Process.Pid)
	_ = input.Close()
	_ = cmd.Wait()
	if err != nil || birth == "" {
		t.Fatal("helper birth", err)
	}
	state := workerState{Job: j, Session: j.Session, Identity: ID()}
	req := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: Machine{ID: j.Machine}, State: state}
	v := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: cmd.Process.Pid, Birth: birth, State: state}
	for kind, value := range map[string]any{"request": req, "snapshot": v} {
		if err := saveWorkerJSON(custodianDir(dir), custodianName(id, kind), value); err != nil {
			t.Fatal(err)
		}
	}
	state.Custodian = id
	return &workerStore{dir: dir, state: state}, v
}

func TestCustodianInitializedPrelaunchRecovery(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	j := check(t, q, machine("worker", "fake"), nil).Job
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	other := submit(t, q, spec("experiment"))
	check(t, q, machine("other", "fake"), nil)
	store, _ := deadCustodianFixture(t, j)
	w := Worker{Clock: clock, custodianAbsent: func(context.Context, string) error { return nil }}
	// This is the same recovery entry used on worker restart. No offline
	// resolver or mutation of the saved parent assignment is needed.
	p, err := w.recoverAttempt(store)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	select {
	case f := <-p.done:
		r := f.result
		if r.ReleasePending || r.Error != errCustodianStartup.Error() || r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.ExecutionEvidence.Sidecars != executionNotStarted || r.ExecutionCause == nil || !r.ExecutionCause.PrelaunchRefused {
			t.Fatalf("initialized prelaunch death lost exact release evidence: %+v", r)
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, j.ID)
		if got.State != "cancelled" || !got.ReleaseConfirmed || got.CommandStarted != nil && *got.CommandStarted {
			t.Fatal("cancelled assignment did not release", got)
		}
		requireState(t, q, other.ID, "running")
	case <-time.After(testWaitTimeout):
		t.Fatal("initialized prelaunch death did not complete")
	}
}

func TestCustodianInitializedRecoveryRetainsUncertainCustody(t *testing.T) {
	for _, kind := range []string{"live", "lock", "started", "snapshot-custody", "parent-custody", "wrong-session", "wrong-request", "state", "gate", "work", "intent", "manager", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
			s, v := deadCustodianFixture(t, &j)
			dir, id := custodianDir(s.dir), s.state.Custodian
			w := Worker{Clock: &workerFakeClock{now: time.Unix(100, 0)}, custodianAbsent: func(context.Context, string) error { return nil }}
			switch kind {
			case "live":
				v.PID = os.Getpid()
				v.Birth, _ = processBirth(v.PID)
			case "lock":
				unlock, err := commandLock(dir, "custodian-"+id+".lock", false)
				if err != nil {
					t.Fatal(err)
				}
				defer unlock()
			case "started":
				v.Started = true
			case "snapshot-custody":
				v.State.LaunchPending = true
			case "parent-custody":
				s.state.Preparation = "pending-builder"
			case "wrong-session":
				v.State.Session = "other"
			case "wrong-request":
				if err := saveWorkerJSON(dir, custodianName(id, "request"), custodianRequest{ID: ID()}); err != nil {
					t.Fatal(err)
				}
			case "state", "gate":
				if err := os.WriteFile(filepath.Join(dir, custodianName(id, kind)), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "work":
				if err := os.MkdirAll(attemptStorageRoot(j, s.dir), 0700); err != nil {
					t.Fatal(err)
				}
			case "intent":
				if err := os.WriteFile(filepath.Join(s.dir, "attempt-"+s.state.Identity+".json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "manager":
				w.custodianAbsent = func(context.Context, string) error { return errors.New("manager still owns scope") }
			}
			if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), v); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, custodianName(id, "snapshot"))
			if kind == "corrupt" {
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.ReadFile(path)
			if err := w.recoverUnstartedCustodian(s); err == nil {
				t.Fatal("uncertain custody released")
			}
			after, _ := os.ReadFile(path)
			if string(before) != string(after) {
				t.Fatal("uncertain snapshot changed")
			}
		})
	}
}
