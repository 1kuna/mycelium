package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// An initial snapshot is not a release proof. Recover only when the exact
// helper has exited, its manager cannot dispatch it, and no preparation or
// launch was entered. Both launchers durably save helper state/intent before
// creating any payload process; the final admission gate also persists first.
// Missing/corrupt/permission-denied evidence retains custody, never retries it.
func (w Worker) recoverUnstartedCustodian(parent *workerStore) error {
	id, dir := parent.state.Custodian, custodianDir(parent.dir)
	unlock, err := commandLock(dir, "custodian-"+id+".lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	var v custodianSnapshot
	if err := readCustodianJSON(dir, custodianName(id, "snapshot"), &v); err != nil {
		return err
	}
	j := parent.state.Job
	if v.Protocol != custodianProtocol || v.ID != id || v.Started || v.Finished || v.PID <= 0 || v.Birth == "" || v.State.Job == nil || jobKey(*v.State.Job) != jobKey(*j) || v.State.Job.Machine != j.Machine || v.State.Session != j.Session || !custodianUnstarted(v.State) || !custodianUnstarted(parent.state) {
		return errors.New("initialized custodian has uncertain execution custody")
	}
	birth, err := processBirth(v.PID)
	if err != nil && !errors.Is(err, os.ErrNotExist) || err == nil && (birth == "" || birth == v.Birth) {
		return errors.New("initialized custodian death unconfirmed")
	}
	var req custodianRequest
	if err := readCustodianJSON(dir, custodianName(id, "request"), &req); err != nil {
		return err
	}
	if req.Protocol != custodianProtocol || req.ID != id || req.Machine.ID != j.Machine || req.State.Job == nil || jobKey(*req.State.Job) != jobKey(*j) || req.State.Job.Machine != j.Machine || req.State.Session != j.Session || req.State.Custodian != "" || !custodianUnstarted(req.State) || !validCustodianID(req.State.Identity) || req.State.Identity != v.State.Identity || req.State.Identity != parent.state.Identity {
		return errors.New("initialized custodian request identity unconfirmed")
	}
	for _, path := range []string{
		filepath.Join(dir, custodianName(id, "state")),
		filepath.Join(dir, custodianName(id, "gate")),
		filepath.Join(dir, custodianName(id, "gate-receipt")),
		filepath.Join(parent.dir, "attempt-"+req.State.Identity+".json"),
		filepath.Join(parent.dir, fmt.Sprintf("launch-%s-%d.outcome.json", j.ID, j.Attempt)),
		attemptStorageRoot(*j, parent.dir),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			return errors.New("initialized custodian has possible preparation or launch evidence")
		}
	}
	absent := w.custodianAbsent
	if absent == nil {
		absent = custodianManagerAbsent
	}
	if err := absent(w.launchContext(), id); err != nil {
		return err
	}
	if err := w.launchContext().Err(); err != nil {
		return err
	}
	// Commit the refusal facts and result in one atomic snapshot. A separate
	// launch marker could survive a failed snapshot write and block recovery.
	facts := memberOutcome{Infrastructure: []infrastructureFact{{Kind: launchRefused, At: w.Clock.Now(), Predicate: "launch_authorization", LaunchRefusal: custodianStartupFailed, PrelaunchRefused: true}}}
	cause := &recordedStartError{cause: errCustodianStartup, facts: facts}
	r := startFailureOutcome(Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Error: errCustodianStartup.Error()}, &executionEvidenceError{cause, unstartedExecution(j.Attempt)})
	timing := newLaunchRecorder(w.Clock, v.State.Job.LaunchTimings)
	timing.finish("")
	r.LaunchTimings = timing.snapshot()
	v.Finished, v.At, v.State = true, w.Clock.Now(), req.State
	v.State.Result = &r
	return saveWorkerJSON(dir, custodianName(id, "snapshot"), v)
}
