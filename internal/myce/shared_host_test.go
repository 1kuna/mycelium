package myce

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"
)

func sharedHostFixture(t *testing.T, q *Queue) Machine {
	t.Helper()
	m := cpuMachine("interactive", "interactive", "cpu", 8, 12)
	m.CapabilityProfile.SystemMemoryBytes = 24 << 30
	m.HostMemoryBudgetGB, m.CPUBudgetCores = 8, 2
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 24, UsedGB: 14, MyceliumGB: 0}
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: 4}}); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSharedHostAdmissionUsesHistoryLiveReserveAndAcknowledgement(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	// A quieter live reading must not erase the observed external peak: 24-14-4=6.
	m.MemoryObservation.UsedGB = 8
	m.HostAvailableMemoryGB = 12
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	large := submit(t, q, cpuSpec(1, 6.5))
	small := submit(t, q, cpuSpec(1, 2))
	reply := check(t, q, m, nil)
	if reply.Job != nil || reply.SharedHost == nil {
		t.Fatal("unacknowledged profile admitted work", reply)
	}
	m.SharedHost = cloneSharedHost(reply.SharedHost)
	reply = check(t, q, m, nil)
	got := reply.Job
	if got == nil || got.ID != small.ID {
		t.Fatalf("fitting light work not admitted: %+v", reply)
	}
	requireState(t, q, large.ID, "queued")
	if err := q.Started(result(got, false)); err != nil {
		t.Fatal(err)
	}
	// New pressure withholds further work, but phase one does not stop a Started job.
	clock.Advance(time.Second)
	m.MemoryObservation.UsedGB = 21
	m.HostAvailableMemoryGB = 0
	reply = check(t, q, m, got)
	for _, d := range reply.Attempts {
		if d.Stop || d.Kill {
			t.Fatal("phase one unexpectedly stopped work")
		}
	}
	if reply.Stop || reply.Kill {
		t.Fatal("phase one unexpectedly stopped work")
	}
	finish(t, q, got, true)
	requireState(t, q, large.ID, "queued")
	// Missing live evidence is unknown, even with retained history.
	m.MemoryObservation = nil
	check(t, q, m, nil)
	status, err := q.HostMaintenanceStatus(m.ID)
	if err != nil || status.Workers[0].Machine.SharedHostReason == "" || status.Workers[0].Machine.FreeHostMemoryGB != 0 {
		t.Fatal(status, err)
	}
}

func TestSharedHostHistoryExpiresAndReserveIsNotDoubleCounted(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	// Existing 4-GiB reserve is already removed: use min(12, raw 16 - 4), not 12-4.
	m.MemoryObservation.UsedGB = 8
	check(t, q, m, nil)
	q.mu.Lock()
	q.refreshHostMemory()
	got := q.s.Machines[m.ID].FreeHostMemoryGB
	// History, rather than live RAM, limits this to 6 GiB.
	if got != 6 {
		t.Errorf("headroom=%g want 6", got)
	}
	// Isolate a representative quieter history to prove the live reserve is not
	// deducted again from already-reserved availability.
	q.s.ExternalMemory = nil
	q.recordExternalMemory(&m)
	budget, _ := hostMemoryAllowance(20)
	policy := q.s.HostMaintenance[m.ID]
	policy.MemoryBudget = &budget
	q.s.HostMaintenance[m.ID] = policy
	q.s.Machines[m.ID].HostMemoryBudgetGB = 20
	q.refreshHostMemory()
	if free := q.s.Machines[m.ID].FreeHostMemoryGB; free != 12 {
		t.Errorf("reserve double counted: %g want 12", free)
	}
	clock.Advance(8 * 24 * time.Hour)
	q.refreshHostMemory()
	if q.s.Machines[m.ID].FreeHostMemoryGB != 0 || q.s.Machines[m.ID].SharedHostReason == "" {
		t.Error("expired history treated as zero external use")
	}
	q.mu.Unlock()
}

func TestSharedHostPolicyPersistsAndExclusionsRemainIndependent(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	m := sharedHostFixture(t, q)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, m, nil)
	status, err := q.HostMaintenanceStatus(m.ID)
	if err != nil || status.Policy.SharedHost == nil || !status.Policy.LastResort || status.Policy.Mode != "drained" {
		t.Fatal(status, err)
	}
	gpu := m
	gpu.GPU = "metal"
	q.mu.Lock()
	blocked := q.componentExcluded(&gpu, "metal", "")
	q.mu.Unlock()
	if !blocked {
		t.Fatal("profile did not use GPU exclusion path")
	}
	changeExclusion(t, q, "host=interactive;kind=gpu", "add")
	if _, err = q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{}}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	blocked = q.componentExcluded(&gpu, "metal", "")
	q.mu.Unlock()
	if !blocked {
		t.Fatal("removing profile cleared independent GPU exclusion")
	}
	status, err = q.HostMaintenanceStatus(m.ID)
	if err != nil || status.Policy.Mode != "drained" || !status.Policy.LastResort {
		t.Fatal("profile removal changed maintenance/placement", status, err)
	}
}

func TestSharedHostRejectsUnsafePolicyAndLaunches(t *testing.T) {
	q, _ := fixture(t)
	m := sharedHostFixture(t, q)
	for _, r := range []HostMaintenanceRequest{
		{Action: "budget", CPUBudgetCores: new(int)},
		{Action: "placement", LastResort: new(bool)},
		{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: math.NaN()}},
	} {
		if _, err := q.ChangeHostMaintenance(m.ID, r); err == nil {
			t.Error("accepted unsafe policy", r)
		}
	}
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	native := Job{Spec: cpuSpec(1, 1)}
	if err := normalizeResources(&native.Spec); err != nil {
		t.Fatal(err)
	}
	if !sharedHostJobFits(&native, &m) {
		t.Fatal("native CPU command refused")
	}
	native.Spec.Run.Docker = true
	if sharedHostJobFits(&native, &m) {
		t.Fatal("Docker descendants cannot inherit native priority")
	}
	// Worker checks raw availability after the protected reserve, not only its default reserve.
	w := Worker{Machine: m, HostMemoryReserveGB: 1, HostMemorySource: func(context.Context) (float64, error) { return 4, nil }}
	m.SharedHost.InteractiveReserveGB = 4
	free, err := w.sharedHostLiveMemory(context.Background())
	if err != nil || free != .25 {
		t.Fatal("worker reserve counted incorrectly", free, err)
	}
}

func TestSharedHostDevCommandPriority(t *testing.T) {
	oldRSS := workerMemoryRSS
	workerMemoryRSS = func(context.Context) (float64, error) { return 0, nil }
	t.Cleanup(func() { workerMemoryRSS = oldRSS })
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "dev-fixture", GPU: "cpu", Workers: 1, Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := d.Client()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := "/api/hosts/dev-host/maintenance"
	var status HostMaintenanceStatus
	await(t, func() bool {
		if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
			t.Fatal(err)
		}
		return len(status.Workers) == 1 && status.Workers[0].Fresh && status.ExternalMemory != nil
	})
	change := func(r HostMaintenanceRequest) {
		t.Helper()
		if err := c.Call(ctx, "POST", path, r, &status); err != nil {
			t.Fatal(err)
		}
	}
	change(HostMaintenanceRequest{Action: "drain"})
	await(t, func() bool {
		if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
			t.Fatal(err)
		}
		return status.Quiescent
	})
	cores, ram := 2, 2.0
	change(HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cores, MemoryBudgetGB: &ram})
	change(HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: .125}})
	change(HostMaintenanceRequest{Action: "resume"})
	s := cpuSpec(1, .01)
	s.Run.Command = []string{"sh", "-c", `n=$(ps -o nice= -p $$); test "$n" -ge 10 && test "${MYCE_TOKEN+x}" != x && printf shared-host-ok`}
	var j Job
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		if err := c.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &j); err != nil {
			t.Fatal(err)
		}
		if j.State == "failed" {
			t.Fatalf("native command failed: %+v", j)
		}
		return j.State == "succeeded"
	})
	if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
		t.Fatal(err)
	}
	if status.Workers[0].Machine.SharedHost == nil || !status.Policy.LastResort {
		t.Fatal("profile not acknowledged", status)
	}
}

func TestSharedHostPrefersOrdinaryAndFallsBackImmediately(t *testing.T) {
	q, _ := fixture(t)
	shared := sharedHostFixture(t, q)
	shared.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, shared, nil)
	if _, err := q.ChangeHostMaintenance(shared.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	ordinary := cpuMachine("ordinary", "ordinary", "cpu", 2, 16)
	check(t, q, ordinary, nil)
	first := submit(t, q, cpuSpec(2, 1))
	if r := check(t, q, shared, nil); r.Job != nil {
		t.Fatal("shared host took ordinary fitting work")
	}
	holder := check(t, q, ordinary, nil).Job
	if holder == nil || holder.ID != first.ID {
		t.Fatal("ordinary host not selected", holder)
	}
	if err := q.Started(result(holder, false)); err != nil {
		t.Fatal(err)
	}
	second := submit(t, q, cpuSpec(1, 1))
	fallback := check(t, q, shared, nil).Job
	if fallback == nil || fallback.ID != second.ID {
		t.Fatal("shared host did not take immediate fitting fallback", fallback)
	}
}
