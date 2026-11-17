package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func custodianRecoveryFixture(t *testing.T, j *Job) (string, string) {
	t.Helper()
	dir, id := t.TempDir(), ID()
	if err := os.Mkdir(custodianDir(dir), 0700); err != nil {
		t.Fatal(err)
	}
	req := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: Machine{ID: j.Machine}, State: workerState{Job: j, Session: j.Session}}
	if err := saveWorkerJSON(custodianDir(dir), custodianName(id, "request"), req); err != nil {
		t.Fatal(err)
	}
	state := req.State
	state.Custodian = id
	if err := saveWorkerJSON(dir, "state-attempt-fixture.json", state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(custodianDir(dir), "custodian-"+id+".log"), []byte("2026/11/17 12:34:56 "+legacyCustodianRefusal+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return dir, id
}

// Recover the actual old missing-snapshot boundary, then use normal adoption
// and queue completion. Prior progress survives; accepted cancellation finishes.
func TestCustodianStartupRecovery(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "resumable", true: "cancelled"}[cancelled], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			submitted := submit(t, q, spec("experiment"))
			// Model progress retained from a prior attempt, not this unstarted launch.
			q.s.Jobs[submitted.ID].Progress = 0.5
			j := check(t, q, m, nil).Job
			if j == nil {
				t.Fatal("missing assignment")
			}
			progress, checkpoint := j.Progress, j.Checkpoint
			if cancelled {
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			}
			dir, id := custodianRecoveryFixture(t, j)
			calls := 0
			if err := reconcileCustodianStartup(t.Context(), dir, id, func(context.Context, string, string) error { calls++; return nil }); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("missing launch fence")
			}
			s := &workerStore{dir: dir, state: workerState{Custodian: id, Job: j, Session: j.Session}}
			p, err := (Worker{Clock: clock}).adoptCustodian(s)
			if err != nil {
				t.Fatal(err)
			}
			defer p.custodian.detach()
			select {
			case f := <-p.done:
				r := f.result
				if r.ReleasePending || r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.ExecutionEvidence.Sidecars != executionNotStarted || r.ExecutionCause == nil || !r.ExecutionCause.PrelaunchRefused {
					t.Fatalf("invalid no-execution result: %+v", r)
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				if cancelled {
					if got.State != "cancelled" || !got.ReleaseConfirmed {
						t.Fatalf("cancellation retained: %+v", got)
					}
				} else {
					if got.State != "queued" || got.Progress != progress || got.Checkpoint != checkpoint || got.Losses != 0 {
						t.Fatalf("progress lost or refusal charged: %+v", got)
					}
					if len(got.Copies) == 0 || !got.Copies[len(got.Copies)-1].ReleaseConfirmed {
						t.Fatal("old copy retains custody")
					}
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("recovered result not adopted")
			}
		})
	}
}

func TestCustodianStartupRecoveryRetainsUncertainty(t *testing.T) {
	for _, kind := range []string{"missing-log", "substring", "extra-log", "bad-time", "snapshot", "corrupt-snapshot", "dangling-snapshot", "staging", "state-mismatch", "duplicate", "request-custody", "worker-custody", "writer", "manager", "worker-lock", "owner-lock", "expired"} {
		t.Run(kind, func(t *testing.T) {
			j := &Job{ID: ID(), Attempt: 3, Machine: "worker", Session: "session"}
			dir, id := custodianRecoveryFixture(t, j)
			custody := custodianDir(dir)
			log := filepath.Join(custody, "custodian-"+id+".log")
			statePath := filepath.Join(dir, "state-attempt-fixture.json")
			ctx := t.Context()
			fence := func(context.Context, string, string) error { return nil }
			switch kind {
			case "missing-log":
				if err := os.Remove(log); err != nil {
					t.Fatal(err)
				}
			case "substring":
				if err := os.WriteFile(log, []byte(legacyCustodianRefusal), 0600); err != nil {
					t.Fatal(err)
				}
			case "extra-log":
				f, err := os.OpenFile(log, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteString("another invocation\n")
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "bad-time":
				if err := os.WriteFile(log, []byte("2026/99/99 12:34:56 "+legacyCustodianRefusal+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "snapshot", "corrupt-snapshot":
				if err := os.WriteFile(filepath.Join(custody, custodianName(id, "snapshot")), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "dangling-snapshot":
				if err := os.Symlink("missing", filepath.Join(custody, custodianName(id, "snapshot"))); err != nil {
					t.Fatal(err)
				}
			case "staging":
				if err := os.MkdirAll(attemptStorageRoot(*j, dir), 0700); err != nil {
					t.Fatal(err)
				}
			case "state-mismatch", "worker-custody", "writer":
				var s workerState
				b, err := os.ReadFile(statePath)
				if err != nil {
					t.Fatal(err)
				}
				if err := decodeWorkerState(b, &s); err != nil {
					t.Fatal(err)
				}
				if kind == "state-mismatch" {
					s.Job.Attempt++
				} else if kind == "writer" {
					s.InputWriterPending = true
				} else {
					s.Cgroup = "unreleased"
				}
				if err := saveWorkerJSON(dir, filepath.Base(statePath), s); err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				b, err := os.ReadFile(statePath)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "state-attempt-duplicate.json"), b, 0600); err != nil {
					t.Fatal(err)
				}
			case "request-custody":
				var r custodianRequest
				if err := readCustodianJSON(custody, custodianName(id, "request"), &r); err != nil {
					t.Fatal(err)
				}
				r.State.Preparation = "builder"
				if err := saveWorkerJSON(custody, custodianName(id, "request"), r); err != nil {
					t.Fatal(err)
				}
			case "manager":
				fence = func(context.Context, string, string) error { return errors.New("manager unknown") }
			case "worker-lock", "owner-lock":
				root, name := dir, "worker.lock"
				if kind == "owner-lock" {
					root, name = custody, "custodian-"+id+".lock"
				}
				unlock, err := commandLock(root, name, false)
				if err != nil {
					t.Fatal(err)
				}
				defer unlock()
			case "expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := reconcileCustodianStartup(ctx, dir, id, fence); err == nil {
				t.Fatal("uncertainty released")
			}
			if kind != "snapshot" && kind != "corrupt-snapshot" && kind != "dangling-snapshot" {
				if _, err := os.Lstat(filepath.Join(custody, custodianName(id, "snapshot"))); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refusal wrote release snapshot", err)
				}
			}
		})
	}
}
