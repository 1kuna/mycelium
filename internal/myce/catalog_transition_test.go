package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCatalogTransitionCheckin(t *testing.T) {
	q, clock := fixture(t)
	oldCatalog := ModelConfig{Models: []Model{{Name: "previous", Engine: "fake", Path: "fake", Platforms: []string{"fake"}, MemoryGB: 1}}}
	nextCatalog := ModelConfig{Models: []Model{{Name: "current", Engine: "fake", Path: "fake", Platforms: []string{"fake"}, MemoryGB: 1}}}
	if err := q.ConfigureModels(oldCatalog); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	m.ModelConfigHash = modelConfigHash(oldCatalog)
	check(t, q, m, nil)
	if err := q.ConfigureModels(nextCatalog); err != nil {
		t.Fatal(err)
	}
	beat := Checkin{Machine: m, period: time.Second, Recovering: true}
	if _, err := q.Checkin(beat); err == nil {
		t.Fatal("enabled worker bypassed the catalog fence during recovery")
	}
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(LossAfter + time.Second)
	j := submit(t, q, spec("experiment"))
	unknown := beat
	unknown.Machine.ID = "unknown"
	unknown.Machine.HostID = hostID(&m)
	busy := beat
	busy.ModelReports = []ModelReport{{Name: "previous", State: "ready"}}
	missingHash := beat
	missingHash.Machine.ModelConfigHash = ""
	for _, invalid := range []Checkin{unknown, busy, missingHash} {
		if _, err := q.Checkin(invalid); err == nil {
			t.Fatal("ambiguous or busy stale-catalog worker accepted")
		}
	}
	r, err := q.Checkin(beat)
	if err != nil {
		t.Fatal(err)
	}
	assertCatalogFencedReply(t, r)
	if _, err = q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "resume"}); err == nil {
		t.Fatal("resumed a stale-catalog worker")
	}
	requireState(t, q, j.ID, "queued")
}

func assertCatalogFencedReply(t *testing.T, r Reply) {
	t.Helper()
	if !r.AdmissionBlocked || r.Job != nil || len(r.Attempts)+len(r.ModelPlan)+len(r.EnvironmentPlan)+len(r.CopyPlan)+len(r.ExtensionQualificationPlan) != 0 {
		t.Fatalf("catalog-stale worker received work or preparation: %+v", r)
	}
}

func TestCatalogTransitionDev(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("installer runs under an ordinary test account")
	}
	ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
	defer cancel()
	root := t.TempDir()
	writeCatalog := func(name string) (string, ModelConfig) {
		t.Helper()
		path := filepath.Join(root, name+".yaml")
		data := fmt.Sprintf("models:\n  - name: %s\n    engine: fake\n    path: fake\n    platforms: [fake]\n    memory_gb: 1\n", name)
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		catalog, err := ReadModels(path)
		if err != nil {
			t.Fatal(err)
		}
		return path, catalog
	}
	oldPath, oldCatalog := writeCatalog("previous")
	newPath, newCatalog := writeCatalog("current")
	const token = "catalog-test"
	d, err := startTestDev(t, DevConfig{
		Address: "127.0.0.1:0", Database: filepath.Join(root, "queue", "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"),
		Workers: 1, MemoryGB: 8, Interval: 50 * time.Millisecond, Token: token, ModelConfig: oldCatalog,
	}, func(q *Queue) {
		clock := q.clock.(*devClock)
		clock.mu.Lock()
		defer clock.mu.Unlock()
		clock.base = &fakeClock{now: clock.nowLocked()}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := d.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	client := d.Client()
	drain := func(host string) {
		t.Helper()
		if _, err := d.Queue.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain"}); err != nil {
			t.Fatal(err)
		}
	}
	drain("dev-host")
	heartbeat := func(m Machine, previous string) (Reply, error) {
		var reply Reply
		err := client.Call(ctx, "POST", "/api/workers/checkin", Checkin{
			Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol, Fresh: previous != "", PreviousSession: previous,
		}, &reply)
		return reply, err
	}
	type worker struct {
		config  ServiceConfig
		machine Machine
		manager func(context.Context, string, ...string) ([]byte, error)
	}
	var workers []*worker
	_, publisher, _ := signedTestRelease(t)
	candidate := filepath.Join(root, "candidate")
	if err := os.WriteFile(candidate, []byte("new worker image"), 0700); err != nil {
		t.Fatal(err)
	}
	slot := []byte("installed worker image")
	hash, err := FileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		c, source := preserveWorkerFixture(t)
		c.Name, c.Queue, c.ModelCatalog = fmt.Sprintf("managed-%d", i), d.URL, oldPath
		c.HostID = c.Name
		if err := os.WriteFile(source, slot, 0700); err != nil {
			t.Fatal(err)
		}
		if err := Install(ctx, c, source, token, fakeInstallManager(c)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(c.State, "fleet-publisher.pub"), publisher, 0600); err != nil {
			t.Fatal(err)
		}
		m := machine(c.Name, "fake", c.HostID)
		m.ModelURL = "http://127.0.0.1:1"
		m.ModelConfigHash = modelConfigHash(oldCatalog)
		m.Fleet = fleetTestProof(d.Queue.clock, m.Session, 100+i, false)
		m.Fleet.SHA256 = fleetBytesHash(slot)
		w := &worker{config: c, machine: m}
		w.manager = func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name != "systemctl" || len(args) < 2 {
				t.Fatalf("unexpected manager command: %s %v", name, args)
			}
			text := strings.Join(args, " ")
			if strings.Contains(text, "myce-host-reconcile.") {
				if args[1] == "is-active" {
					return nil, installTestExit(4)
				}
				return nil, nil
			}
			switch {
			case strings.Contains(text, "MainPID"):
				return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", w.machine.Fleet.PID)), nil
			case strings.Contains(text, "UnitFileState"):
				return []byte("enabled\n"), nil
			case len(args) == 3 && args[1] == "show":
				return installUnitObservation(c), nil
			case args[1] == "stop":
				t.Fatal("installer required a manual stop")
			case args[1] == "restart":
				installed, err := ReadServiceConfig(c.Config)
				if err != nil {
					return nil, err
				}
				catalog, err := ReadModels(installed.ModelCatalog)
				if err != nil {
					return nil, err
				}
				previous := w.machine.Session
				w.machine.Session += "-reinstalled"
				w.machine.ModelConfigHash = modelConfigHash(catalog)
				w.machine.Fleet = fleetTestProof(d.Queue.clock, w.machine.Session, w.machine.Fleet.PID+100, true)
				w.machine.Fleet.SHA256 = hash
				_, err = heartbeat(w.machine, previous)
				return nil, err
			}
			return nil, nil
		}
		if _, err := heartbeat(m, ""); err != nil {
			t.Fatal(err)
		}
		drain(c.HostID)
		workers = append(workers, w)
	}
	if err := d.Queue.ConfigureModels(newCatalog); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Resumable = false
	j := submit(t, d.Queue, s)
	for i, w := range workers {
		if err := client.Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": (LossAfter + time.Second).Seconds()}, nil); err != nil {
			t.Fatal(err)
		}
		for _, peer := range workers[i:] {
			reply, err := heartbeat(peer.machine, "")
			if err != nil {
				t.Fatal(err)
			}
			assertCatalogFencedReply(t, reply)
		}
		next := w.config
		next.ModelCatalog = newPath
		if err := Install(ctx, next, candidate, token, w.manager); err != nil {
			t.Fatal(err)
		}
		status, err := d.Queue.HostMaintenanceStatus(next.HostID)
		if err != nil || len(status.Workers) != 1 || !status.Workers[0].Fresh ||
			status.Workers[0].Machine.ModelConfigHash != modelConfigHash(newCatalog) {
			t.Fatalf("reinstalled worker did not report the active catalog: %+v %v", status, err)
		}
		receipt, err := os.ReadFile(filepath.Join(next.State, "slot-reconciliation.json"))
		var proof slotReconciliation
		if err != nil || json.Unmarshal(receipt, &proof) != nil || proof.Slot.SHA256 != fleetBytesHash(slot) || proof.Running.PID == w.machine.Fleet.PID {
			t.Fatalf("active service did not reconcile its previous slot: %v", err)
		}
		requireState(t, d.Queue, j.ID, "queued")
	}
	last := workers[len(workers)-1]
	if _, err := d.Queue.ChangeHostMaintenance(last.config.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	reply, err := heartbeat(last.machine, "")
	if err != nil || len(reply.Attempts) != 1 || reply.Attempts[0].Job == nil || reply.Attempts[0].Job.ID != j.ID {
		t.Fatalf("matching catalog did not regain normal admission: %+v %v", reply, err)
	}
	select {
	case err := <-d.Errors():
		t.Fatalf("old dev worker exited: %v", err)
	default:
	}
}
