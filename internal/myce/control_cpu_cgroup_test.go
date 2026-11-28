package myce

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeControlCPUCounter(t *testing.T, path string, usecs uint64) {
	t.Helper()
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "cpu.stat"), fmt.Appendf(nil, "usage_usec %d\n", usecs), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestControlCPUReserveConstrainsCheckinAdmission(t *testing.T) {
	t.Parallel()
	q, clock := fixture(t)
	local := cpuMachine("cpu-local", "queue-host", "cpu", 4, 16)
	remote := cpuMachine("cpu-remote", "other-host", "cpu", 4, 16)
	q.diskHost = local.HostID
	root := t.TempDir()
	queue := filepath.Join(root, "app.slice", "myce-queue.service")
	worker := filepath.Join(root, "myce.slice", "myce-worker.service")
	attempt := filepath.Join(root, "myce.slice", "myce-jobs.slice", custodianUnit("00000000000000000000000000000001"))
	// Parent counters include their payload. Only the isolated worker
	// leaves and the queue's own group belong in control overhead.
	writeControlCPUCounter(t, worker, 10_000_000)
	writeControlCPUCounter(t, attempt, 10_000_000)
	writeControlCPUCounter(t, filepath.Join(attempt, "payload"), 10_000_000)
	leaves := []string{queue, filepath.Join(worker, "worker"), filepath.Join(attempt, "worker")}
	for _, path := range leaves {
		writeControlCPUCounter(t, path, 0)
	}
	q.controlCPUSampler = &controlCPUSampler{read: func() (map[string]controlCPUCounter, error) {
		if !q.mu.TryLock() {
			t.Fatal("CPU cgroup discovery ran under the queue lock")
		}
		q.mu.Unlock()
		return readControlCPUCounters(root, queue)
	}}
	check(t, q, local, nil)
	check(t, q, remote, nil)
	allowance := observationAllowance(q.s.Machines[local.ID])
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(controlCPUWindow)
	for i, usecs := range []uint64{4_000_000, 6_000_000, 2_500_000} {
		writeControlCPUCounter(t, leaves[i], usecs)
	}
	writeControlCPUCounter(t, filepath.Join(attempt, "payload"), 40_000_000)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	status, err := q.statusSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]*Machine)
	for _, m := range status.Machines {
		got[m.ID] = m
	}
	if m := got[local.ID]; m == nil || m.ControlPlaneCPUCores != 1.25 || m.EffectiveCPUCores != 2 {
		t.Fatalf("status did not report measured local capacity: %+v", m)
	}
	if m := got[remote.ID]; m == nil || m.ControlPlaneCPUCores != 0 || m.EffectiveCPUCores != 4 {
		t.Fatalf("queue overhead changed another host's allowance: %+v", m)
	}
	if got := observationAllowance(q.s.Machines[local.ID]); got != allowance {
		t.Fatal("control CPU observation changed the historical allowance identity")
	}
	s := cpuSpec(3, 1)
	s.AllowedMachines = []string{local.ID}
	job := submit(t, q, s)
	if r := check(t, q, local, nil); r.Job != nil {
		t.Fatalf("check-in assigned %s over the measured allowance", r.Job.ID)
	}
	// With no further control CPU consumption, the same request becomes
	// eligible. Qualification and the submission stayed intact.
	clock.Advance(controlCPUWindow)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	assigned := check(t, q, local, nil).Job
	if assigned == nil || assigned.ID != job.ID {
		t.Fatalf("check-in did not admit %s when measured CPU fell: %+v", job.ID, assigned)
	}

	clock.Advance(controlCPUWindow)
	writeControlCPUCounter(t, queue, 20_000_000)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	ledger := q.s.Machines[local.ID]
	if ledger.CPUInUseCores != 3 || ledger.EffectiveCPUCores != 2 {
		t.Fatalf("subsequent measured control use did not explain the excess: %d charged, %d admitted", ledger.CPUInUseCores, ledger.EffectiveCPUCores)
	}
	next := submit(t, q, cpuSpec(1, 1))
	if got := check(t, q, local, assigned); got.Job != nil {
		t.Fatalf("over-budget ledger admitted another job: %s", got.Job.ID)
	}
	h := cpuWait(next, map[string]*Machine{local.ID: ledger})
	want := fmt.Sprintf("2 admitted (3 charged), %.2f measured control-plane cores reduce the configured allowance of 4", ledger.ControlPlaneCPUCores)
	if h == nil || !strings.Contains(h.Detail, want) {
		t.Fatalf("CPU hold did not explain the changed allowance: %+v", h)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if why := q.startPlacementPolicyRejection(q.s.Jobs[job.ID]); why != "" {
		t.Fatalf("a later measurement revoked an existing reservation: %s", why)
	}
}
