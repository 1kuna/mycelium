package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The dev composition supplies its real HTTP API, queue and command runner.
// Inventory, scheduling time, checkpoint availability and the service manager
// are fake. The legacy leg speaks the supported singular wire form and executes
// through Worker.start.
func TestM4DevSplitConvertConcurrentRollback(t *testing.T) {
	src, c, binary, manager, run := m4InstallFixture(t)
	root := t.TempDir()
	clock := &workerFakeClock{now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)}
	d, err := startDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: c.Checkpoints, Token: "synthetic-local-test", Workers: 1, HostID: "dev-controller", MemoryGB: 8, Interval: 10 * time.Millisecond}, func(q *Queue) {
		q.clock = clock
		fakeCheckpointAvailability(t, q, true)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	cancelAttempts := func() {
		snap, _ := d.Queue.Snapshot()
		for _, j := range snap.Jobs {
			_ = d.Queue.Cancel(j.ID)
		}
		clock.Advance(StopGrace)
	}
	defer cancelAttempts()
	client := d.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for i := range src {
		src[i].Queue = d.URL
		data, _ := json.Marshal(src[i])
		if err := os.WriteFile(src[i].Config, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	c, err = PlanMachineInstall(c.Config, []string{src[0].Config, src[1].Config}, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacy := func(name string) Machine {
		m := machine(name, "fake", "host")
		m.HostMemoryBudgetGB = c.HostMemoryBudgetGB
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 128
		return m
	}
	checkAPI := func(m Machine) Reply {
		t.Helper()
		var reply Reply
		if err := client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: m}, &reply); err != nil {
			t.Fatal(err)
		}
		return reply
	}
	for _, s := range src {
		checkAPI(legacy(s.Name))
	}
	spark := machine("spark", "fake", "spark")
	checkAPI(spark)
	submitAPI := func(s Spec) *Job {
		t.Helper()
		var j Job
		if err := client.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		return &j
	}
	legacyIntents := map[string]Spec{}
	runLegacy := func(m Machine, spool string) *Job {
		t.Helper()
		s := spec("experiment")
		s.SubmissionKey = "legacy-" + m.ID
		s.Resumable = false
		s.Needs.HostMemoryGB = 1
		s.AllowedMachines = []string{m.ID}
		s.Run.Command = []string{"sh", "-c", "printf legacy-ok"}
		legacyIntents[m.ID] = s
		j := submitAPI(s)
		var reply Reply
		// Storage availability is injected; assignment needs no filesystem retry.
		reply = checkAPI(m)
		if reply.Job == nil || reply.Job.ID != j.ID {
			d.Queue.mu.Lock()
			reason, nasAvailable := d.Queue.s.Jobs[j.ID].Reason, d.Queue.nasAvailable
			d.Queue.mu.Unlock()
			t.Fatalf("legacy assignment missing: reply=%+v waiting=%q nas_available=%t", reply, reason, nasAvailable)
		}
		assigned := *reply.Job
		w := Worker{StateDir: spool, Machine: m, Client: client, Clock: RealClock{}, CheckpointRoot: c.Checkpoints, HostMemorySource: func(context.Context) (float64, error) { return 128, nil }}
		w.initMemoryCeilings()
		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer store.close()
		store.state.Session = m.Session
		if !filepath.IsAbs(assigned.Checkpoint) {
			assigned.Checkpoint = filepath.Join(c.Checkpoints, assigned.Checkpoint)
		}
		p, err := w.start(store, assigned)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case done := <-p.done:
			if done.err != nil || !done.result.Success {
				t.Fatal("legacy command", done)
			}
			if err := w.replay(ctx, store); err != nil {
				t.Fatal(err)
			}
			if err := client.Call(ctx, "POST", "/api/workers/complete", done.result, nil); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			p.stop(w.Clock.Now(), 0, true)
			<-p.done
			t.Fatal(ctx.Err())
		}
		store.state.Job, store.state.Result = nil, nil
		if err := store.save(); err != nil {
			t.Fatal(err)
		}
		return job(t, d.Queue, j.ID)
	}
	oldJob := runLegacy(legacy(src[0].Name), src[0].State)
	for _, host := range []string{"host", "spark"} {
		if _, err := d.Queue.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
			t.Fatal(err)
		}
	}
	mapping := []MachineRename{{src[0].Name, "host", "0"}, {src[1].Name, "host", "1"}, {"spark", "spark", "0"}}
	if _, err := d.Queue.ConvertMachines(mapping, d.Queue.models, false); err != nil {
		t.Fatal(err)
	}
	// Queue-first interval: the old Spark session reconnects before its upgrade.
	checkAPI(spark)
	if err := client.Call(ctx, "POST", "/api/workers/spark/leave", map[string]bool{"now": true}, nil); err != nil {
		t.Fatal(err)
	}
	checkAPI(spark) // Final source check-in records its settled leave.
	sparkCtx, stopSpark := context.WithCancel(ctx)
	sparkDone := make(chan error, 1)
	sw := Worker{Clock: RealClock{}, StateDir: filepath.Join(root, "spark-machine"), Machine: machine("spark", "fake", "spark"), Client: client, ModelConfig: d.Queue.models, Interval: 10 * time.Millisecond, InventoryProbe: func(context.Context) ([]MachineDevice, error) {
		return []MachineDevice{{ID: "0", Platform: "fake", Identity: "GPU-spark", Selector: "GPU-spark", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}, nil
	}}
	sw.Machine.Device = "GPU-spark"
	go func() { sparkDone <- sw.Run(sparkCtx) }()
	sparkStopped := false
	defer func() {
		if !sparkStopped {
			cancelAttempts()
			stopSpark()
			select {
			case <-sparkDone:
			case <-time.After(45 * time.Second):
				t.Error("Spark worker cleanup did not finish")
			}
		}
	}()
	await(t, func() bool {
		snap, _ := d.Queue.Snapshot()
		for _, m := range snap.Machines {
			if m.ID == "spark" && len(m.Devices) == 1 && !m.Gone {
				return true
			}
		}
		return false
	})
	if _, err := d.Queue.ChangeHostMaintenance("spark", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	ss := spec("experiment")
	ss.Resumable = false
	ss.AllowedMachines = []string{"spark"}
	ss.Run.Command = []string{"sh", "-c", "printf spark-ok"}
	sj := submitAPI(ss)
	await(t, func() bool { return job(t, d.Queue, sj.ID).State == "succeeded" })
	if _, err := d.Queue.ChangeHostMaintenance("spark", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	if replay := submitAPI(legacyIntents[src[0].Name]); replay.ID != oldJob.ID {
		t.Fatal("conversion changed keyed replay")
	}
	if err := ConvertWorkerInstall(ctx, c, binary, false, run); err != nil {
		t.Fatal(err)
	}
	saved, err := ReadServiceConfig(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	devices := []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake0", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}, {ID: "1", Platform: "fake", Identity: "fake1", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	machineWorker := Worker{Clock: RealClock{}, StateDir: saved.State, Machine: legacy("host"), Client: client, ModelConfig: d.Queue.models, Interval: 10 * time.Millisecond, CheckpointRoot: c.Checkpoints, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return append([]MachineDevice(nil), devices...), nil }, CPUIsolationProbe: func(context.Context) error { return nil }, HostMemorySource: func(context.Context) (float64, error) { return 128, nil }}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- machineWorker.Run(workerCtx) }()
	stopped := false
	defer func() {
		if !stopped {
			cancelAttempts()
			stopWorker()
			select {
			case <-workerDone:
			case <-time.After(45 * time.Second):
				t.Error("machine worker cleanup did not finish")
			}
		}
	}()
	await(t, func() bool {
		snap, _ := d.Queue.Snapshot()
		for _, m := range snap.Machines {
			if m.ID == "host" && len(m.Devices) == 2 {
				return true
			}
		}
		return false
	})
	if _, err := d.Queue.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	jobs := []*Job{}
	for _, device := range []string{"0", "1"} {
		s := spec("experiment")
		s.Needs.HostMemoryGB = 1
		s.AllowedMachines = []string{"host"}
		s.Needs.GPU = m3Spec("experiment", device, 8, 1).Needs.GPU
		s.Run.Command = []string{"sh", "-c", `while [ ! -f "$MYCE_CHECKPOINT_DIR/finish" ]; do sleep .02; done`}
		jobs = append(jobs, submitAPI(s))
	}
	await(t, func() bool {
		return job(t, d.Queue, jobs[0].ID).StartedAt != nil && job(t, d.Queue, jobs[1].ID).StartedAt != nil
	})
	if job(t, d.Queue, jobs[0].ID).Machine != "host" || job(t, d.Queue, jobs[1].ID).Machine != "host" {
		t.Fatal("split placement survived conversion")
	}
	for _, j := range jobs {
		active := job(t, d.Queue, j.ID)
		path := filepath.Join(c.Checkpoints, j.ID, "attempt-"+fmt.Sprint(active.Attempt), "finish")
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool {
		return job(t, d.Queue, jobs[0].ID).State == "succeeded" && job(t, d.Queue, jobs[1].ID).State == "succeeded"
	})
	if _, err := d.Queue.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	// Rollback's service stop drives precisely this worker, preserving neighbors.
	rollbackRunner := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "systemctl" && args[1] == "stop" && args[len(args)-1] == filepath.Base(c.Service) && !stopped {
			stopWorker()
			if err := <-workerDone; err != nil {
				t.Fatal(err)
			}
			stopped = true
		}
		return run(ctx, name, args...)
	}
	if err := RollbackMachineInstall(ctx, c.Config, false, rollbackRunner); err != nil {
		t.Fatal(err)
	}
	if manager.states[filepath.Base(c.Service)].active {
		t.Fatal("machine unit survived rollback")
	}
	stopSpark()
	if err := <-sparkDone; err != nil {
		t.Fatal(err)
	}
	sparkStopped = true
	if err := d.Queue.RollbackMachines(false); err != nil {
		t.Fatal(err)
	}
	// Restore only the recorded legacy workers, then run a real command again.
	for _, s := range src {
		checkAPI(legacy(s.Name))
	}
	checkAPI(spark)
	if _, err := d.Queue.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	final := runLegacy(legacy(src[1].Name), src[1].State)
	if job(t, d.Queue, final.ID).State != "succeeded" {
		t.Fatal("legacy did not work after rollback")
	}
}
