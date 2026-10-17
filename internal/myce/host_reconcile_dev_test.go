package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHostReconcileDevQueueOutageCompletesBeforeAdmission(t *testing.T) {
	root := t.TempDir()
	hostDir := filepath.Join(root, "host")
	database := filepath.Join(root, "queue.db")
	checkpoints := filepath.Join(root, "checkpoints")
	d, err := StartDev(DevConfig{HostStateDir: hostDir, HostID: "dev-host", Database: database, Checkpoints: checkpoints, Address: "127.0.0.1:0", Token: "test", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			d.Close()
		}
	}()
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"/bin/sh", "-c", "echo started; sleep 1; echo completed"}
	var submitted Job
	if err = d.Client().Call(context.Background(), "POST", "/api/jobs", s, &submitted); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { logs, e := d.Queue.Logs(submitted.ID, 0); return e == nil && len(logs) > 0 })
	// Stop the real HTTP queue while the real short command is still executing.
	d.stopQueue()
	if err = d.server.Close(); err != nil {
		t.Fatal(err)
	}
	d.wg.Wait()
	spool := filepath.Join(root, "workers", "fake-1")
	await(t, func() bool {
		paths, _ := filepath.Glob(filepath.Join(spool, "*.result.json"))
		for _, path := range paths {
			b, e := os.ReadFile(path)
			var result Result
			if e == nil && json.Unmarshal(b, &result) == nil && result.ID == submitted.ID && result.Success {
				return true
			}
		}
		return false
	})
	d.cancel()
	// A disconnected worker retains the result until upload. Closing local intake
	// lets it stop without inventing a queue acknowledgement or dropping custody.
	if _, err = changeHostIntent(hostDir, RealClock{}, hostIntentChange{Desired: "closed", Reason: "planned queue outage"}); err != nil {
		t.Fatal(err)
	}
	d.workerWG.Wait()
	if err = d.Queue.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	// Workers are stopped. Fake GPU/service inventory is the only fake boundary.
	report, err := (HostControl{Dir: hostDir, Clock: RealClock{}, Service: &fakeHostService{}}).Disable(context.Background(), "planned queue outage", 0)
	if err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	before, err := ReadHostIntent(hostDir)
	if err != nil || before.HostSettlement() != "pending" || len(before.Pending) != 1 {
		t.Fatal(before, err)
	}
	restarted, err := Open(database, checkpoints, RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err = restarted.ConfigureLocalHost(hostDir, d.URL); err != nil {
		t.Fatal(err)
	}
	// Submission is an admission transaction: it must reconcile the completed
	// attempt and accepted close before it can consider any new placement.
	next, err := restarted.Submit(spec("experiment"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := restarted.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var completed *Job
	for _, j := range snapshot.Jobs {
		if j.ID == submitted.ID {
			copy := j
			completed = copy
		}
	}
	if completed == nil || completed.State != "succeeded" || completed.Attempt != 1 || completed.Losses != 0 {
		t.Fatal("offline completion was lost or rerun", completed)
	}
	current, err := ReadHostIntent(hostDir)
	if err != nil || current.HostSettlement() != "confirmed" || len(current.Pending) != 0 {
		t.Fatal(current, err)
	}
	m := machine("fake-1", "fake")
	m.HostID = "dev-host"
	if reply := check(t, restarted, m, nil); reply.Job != nil || !reply.Leave {
		t.Fatal("closed host admitted", next, reply)
	}
}
