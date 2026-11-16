package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCustodianRejectsUnprovedIdentity(t *testing.T) {
	j := Job{ID: ID(), Attempt: 2, Machine: "worker", Session: "session"}
	id, dir := ID(), t.TempDir()
	birth, err := processBirth(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	base := custodianSnapshot{Protocol: custodianProtocol, ID: id, PID: os.Getpid(), Birth: birth, State: workerState{Job: &j, Session: j.Session}}
	for _, kind := range []string{"reused-pid", "wrong-attempt", "wrong-session", "wrong-manager", "wrong-result"} {
		t.Run(kind, func(t *testing.T) {
			v := base
			jcopy := j
			v.State.Job = &jcopy
			switch kind {
			case "reused-pid":
				v.Birth = "another-process-birth"
			case "wrong-attempt":
				jcopy.Attempt++
			case "wrong-session":
				v.State.Session = "other"
			case "wrong-result":
				v.Finished = true
				v.State.Result = &Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt + 1}}
			}
			if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), v); err != nil {
				t.Fatal(err)
			}
			g := &attemptCustodian{dir: dir, id: id}
			if err := g.read(j); err == nil || g.observed {
				t.Fatal("unproved custody adopted", kind)
			}
		})
	}
	// A finished exact result needs no live PID, so restart replay remains valid.
	base.Finished = true
	base.State.Result = &Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), base); err != nil {
		t.Fatal(err)
	}
	g := &attemptCustodian{dir: dir, id: id}
	if err := g.read(j); err != nil {
		t.Fatal(err)
	}
}

func TestCustodianStopDeadlineSurvivesAdoption(t *testing.T) {
	dir := t.TempDir()
	id := ID()
	j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
	r := Result{Machine: j.Machine, Session: j.Session, Execution: runExecution(&j)}
	snapshot := custodianSnapshot{Protocol: custodianProtocol, ID: id, Finished: true, State: workerState{Job: &j, Session: j.Session, Result: &r}}
	custody := custodianDir(dir)
	if err := os.Mkdir(custody, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(custody, custodianName(id, "snapshot"), snapshot); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Second)
	c := custodianControl{ID: id, Execution: runExecution(&j), Session: j.Session, StopAt: &deadline, UserCancel: true}
	if err := saveWorkerJSON(custody, custodianName(id, "control"), c); err != nil {
		t.Fatal(err)
	}
	s := &workerStore{dir: dir, state: workerState{Job: &j, Session: j.Session, Custodian: id}}
	p, err := (Worker{}).adoptCustodian(s)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	p.custodian.stopLocked(p, now, time.Minute, false)
	var saved custodianControl
	if err := readCustodianJSON(custody, custodianName(id, "control"), &saved); err != nil {
		t.Fatal(err)
	}
	if !saved.StopAt.Equal(deadline) || !saved.UserCancel {
		t.Fatal("adoption renewed deadline or lost cancel provenance", saved)
	}
}

func TestCustodianFinalGateRequiresExactRequest(t *testing.T) {
	dir := t.TempDir()
	g := &attemptCustodian{dir: dir, id: ID()}
	j := Job{ID: ID(), Attempt: 2, Machine: "worker", Session: "session"}
	launch := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: "2"}
	calls := 0
	w := Worker{batchAdmission: func(context.Context, HostLaunch) error { calls++; return errors.New("aggregate host memory capacity") }}
	// A stale receipt cannot authorize a later final gate; the aggregate ledger
	// evaluates the new nonce and returns the existing refusal to the custodian.
	old := custodianGateReceipt{Nonce: "old"}
	if err := saveWorkerJSON(dir, custodianName(g.id, "gate-receipt"), old); err != nil {
		t.Fatal(err)
	}
	request := custodianGate{Nonce: "new", Launch: launch}
	request.Launch.Session = "other"
	if err := saveWorkerJSON(dir, custodianName(g.id, "gate"), request); err != nil {
		t.Fatal(err)
	}
	w.serveCustodianGate(context.Background(), g, j)
	if calls != 0 {
		t.Fatal("wrong execution reached admission")
	}
	request.Launch = launch
	if err := saveWorkerJSON(dir, custodianName(g.id, "gate"), request); err != nil {
		t.Fatal(err)
	}
	w.serveCustodianGate(context.Background(), g, j)
	w.serveCustodianGate(context.Background(), g, j)
	var receipt custodianGateReceipt
	if err := readCustodianJSON(dir, custodianName(g.id, "gate-receipt"), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Nonce != "new" || receipt.Error != "aggregate host memory capacity" || calls != 1 {
		t.Fatal("gate bypassed aggregate admission or replayed", receipt, calls)
	}
}

func TestCustodianAccountingIncludesWorkerAndDetachedSiblings(t *testing.T) {
	dir := t.TempDir()
	worker, jobs := filepath.Join(dir, "worker.service"), filepath.Join(dir, "jobs.slice")
	for root, pid := range map[string]string{worker: "41", jobs: "42"} {
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "memory.stat"), []byte("anon 1073741824\nkernel 0\nshmem 0\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "cgroup.procs"), []byte(pid), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := &memoryAttempt{fs: osMemoryFS{}, path: filepath.Join(jobs, "scope", "attempt"), accountingRoots: []string{worker, jobs}}
	if gb, ok := a.accountingResidentGB(); !ok || gb != 2 {
		t.Fatal("detached sibling RAM omitted", gb, ok)
	}
	ids := map[int]bool{}
	if err := a.accountingProcessIDs(ids); err != nil {
		t.Fatal(err)
	}
	if !ids[41] || !ids[42] {
		t.Fatal("worker engines or detached jobs misclassified as outside processes", ids)
	}
	if err := os.Remove(filepath.Join(worker, "memory.stat")); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.accountingResidentGB(); ok {
		t.Fatal("incomplete sibling attribution reported complete")
	}
}

func TestCustodianSharedHostKeepsAggregateCPUQuota(t *testing.T) {
	w := Worker{Machine: Machine{SharedHost: &SharedHostProfile{}}}
	if runtime.GOOS == "linux" && w.detachedAttemptSupported() {
		t.Fatal("detached jobs split worker/engine aggregate CPU cap")
	}
	if runtime.GOOS == "darwin" && !w.detachedAttemptSupported() {
		t.Fatal("cooperative Darwin policy unnecessarily drained")
	}
}
