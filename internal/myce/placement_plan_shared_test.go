package myce

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// A unified CUDA host whose one GPU draws from the host pool (#538). The 40 GiB
// device budget (56 GiB less its 16 GiB reserve) holds a 17 GiB and a 13 GiB job, but not a 25 GiB third beside them.
func sharedUnifiedMachine() Machine {
	m := cpuMachine("unified", "unified-host", "cuda", 16, 101)
	m.CapabilityProfile.Arch, m.CapabilityProfile.MemoryLayout = "arm64", "unified"
	m.CapabilityProfile.SystemMemoryBytes = 128 << 30
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128}
	m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", Identity: "synthetic-unified", Selector: "0", MemoryMode: "unified", ReserveMemoryGB: 16, TotalMemoryGB: 56, BudgetGB: 40, FreeMemoryGB: 40, UnifiedMemory: true, MemoryMeasured: true}}
	return m
}

// The worker's card budget check, without unified CUDA host sampling.
func sharedWorker(m Machine, clock Clock) Worker {
	m.Devices = slices.Clone(m.Devices)
	m.GPU, m.Devices[0].Platform = "fake", "fake"
	return Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 101, nil }}
}

func sharedSpec(priority string, gb float64) Spec {
	s := spec(priority)
	s.Needs.MemoryGB, s.Needs.HostMemoryGB = gb, 1
	return s
}

// Two experiments share the GPU; returns the 17 GiB and 13 GiB attempts.
func sharedPair(t *testing.T, q *Queue, m Machine) (*Job, *Job) {
	t.Helper()
	first := submit(t, q, sharedSpec("experiment", 17))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID {
		t.Fatal("first job not assigned", job(t, q, first.ID).Reason)
	}
	second := submit(t, q, sharedSpec("experiment", 13))
	b := assignedJob(machineCheck(t, q, m, a))
	if b == nil || b.ID != second.ID || !slices.Equal(a.AllocatedDevices, []string{"0"}) || !slices.Equal(b.AllocatedDevices, []string{"0"}) {
		t.Fatal("13 GiB job did not share the GPU with the 17 GiB job", b, job(t, q, second.ID).Reason)
	}
	for _, j := range []*Job{a, b} {
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
	}
	return a, b
}

func TestSharedDeviceRunsTwoJobsThatFit(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	a, b := sharedPair(t, q, m)
	machineCheck(t, q, m, a, b)
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
	// The worker admits the second attempt on the same card within its budget.
	w := sharedWorker(m, clock)
	if err := localAttemptAdmission(w, []Job{*job(t, q, a.ID)}, *job(t, q, b.ID), nil); err != nil {
		t.Fatal("worker refused a co-tenant that fits:", err)
	}
	over := *job(t, q, b.ID)
	over.Spec.Needs.MemoryGB = 25
	if err := localAttemptAdmission(w, []Job{*job(t, q, a.ID)}, over, nil); err == nil || !strings.Contains(err.Error(), "device_budget") {
		t.Fatal("worker admitted co-tenants beyond the card budget:", err)
	}
}

func TestSharedDeviceEqualTierWaits(t *testing.T) {
	q, _ := fixture(t)
	m := sharedUnifiedMachine()
	a, b := sharedPair(t, q, m)
	waiting := submit(t, q, sharedSpec("experiment", 17))
	if got := assignedJob(machineCheck(t, q, m, a, b)); got != nil {
		t.Fatal("equal-tier job overfilled the shared GPU", got.ID)
	}
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
	requireState(t, q, waiting.ID, "queued")
	if reason := job(t, q, waiting.ID).Reason; !strings.Contains(reason, "device 0 shared with 2 attempts") {
		t.Fatal("waiting reason does not name the shared device:", reason)
	}
}

func TestSharedDeviceProductionStopsOneCoTenant(t *testing.T) {
	q, _ := fixture(t)
	m := sharedUnifiedMachine()
	a, b := sharedPair(t, q, m)
	demand := submit(t, q, sharedSpec("production", 25))
	if got := assignedJob(machineCheck(t, q, m, a, b)); got != nil {
		t.Fatal("production started before its victim released", got.ID)
	}
	// 10 GiB free: only stopping the 17 GiB job frees enough.
	if got := job(t, q, a.ID); got.State != "stopping" {
		t.Fatalf("17 GiB co-tenant state=%s; production waits: %s", got.State, job(t, q, demand.ID).Reason)
	}
	requireState(t, q, b.ID, "running")
	if got := assignedJob(machineCheck(t, q, m, a, b)); got != nil {
		t.Fatal("production started while its victim was still stopping", got.ID)
	}
	requireState(t, q, b.ID, "running")
	finishInterrupted(t, q, a)
	got := assignedJob(machineCheck(t, q, m, b))
	if got == nil || got.ID != demand.ID || !slices.Equal(got.AllocatedDevices, []string{"0"}) {
		t.Fatal("production did not start beside the remaining co-tenant after exact release", got, job(t, q, demand.ID).Reason)
	}
	requireState(t, q, b.ID, "running")
}

func TestSharedDeviceModelServerKeepsDeviceExclusive(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	server := sharedSpec("experiment", 17)
	server.Needs.Model = "served"
	first := submit(t, q, server)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID {
		t.Fatal("model-server job not assigned", job(t, q, first.ID).Reason)
	}
	small := submit(t, q, sharedSpec("experiment", 13))
	if got := assignedJob(machineCheck(t, q, m, a)); got != nil {
		t.Fatal("batch job joined a model-server job's GPU", got.ID)
	}
	requireState(t, q, small.ID, "queued")
	w := sharedWorker(m, clock)
	probe := *job(t, q, small.ID)
	probe.AllocatedDevices = []string{"0"}
	if err := localAttemptAdmission(w, []Job{*job(t, q, a.ID)}, probe, nil); err == nil || !strings.Contains(err.Error(), "local_device_reservation") {
		t.Fatal("worker admitted a co-tenant beside a model-server job:", err)
	}
}

func TestSharedDeviceRestartKeepsCustody(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	a, b := sharedPair(t, q, m)
	machineCheck(t, q, m, a, b)
	q = memoryFactsRestart(t, q, clock)
	fakeCheckpointAvailability(t, q, true)
	third := submit(t, q, sharedSpec("experiment", 17))
	if got := assignedJob(machineCheck(t, q, m, a, b)); got != nil {
		t.Fatal("queue restart lost a co-tenant's charge", got.ID)
	}
	requireState(t, q, third.ID, "queued")
	// A lost worker that returns re-adopts both attempts on the shared card.
	clock.Advance(61 * time.Second)
	check(t, q, machine("healthy", "fake"), nil)
	requireState(t, q, a.ID, "queued")
	requireState(t, q, b.ID, "queued")
	for _, directive := range machineCheck(t, q, m, a, b).Attempts {
		if directive.Stop || directive.Hold {
			t.Fatal("co-tenant prevented exact recovery", directive)
		}
	}
	for _, prior := range []*Job{a, b} {
		got := job(t, q, prior.ID)
		if got.State != "running" || got.Attempt != prior.Attempt || !slices.Equal(got.AllocatedDevices, []string{"0"}) {
			t.Fatal("shared custody was not recovered", got)
		}
	}
	requireState(t, q, third.ID, "queued")
}

func TestSharedDeviceFillsIdleCardsFirst(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	var running []*Job
	for i := 0; i < 3; i++ {
		s := spec("experiment")
		s.Needs.MemoryGB = 3
		queued := submit(t, q, s)
		got := assignedJob(machineCheck(t, q, m, running...))
		if got == nil || got.ID != queued.ID {
			t.Fatal("3 GiB job not admitted on an 8 GiB card", i, job(t, q, queued.ID).Reason)
		}
		running = append(running, got)
	}
	if !slices.Equal(running[0].AllocatedDevices, []string{"0"}) || !slices.Equal(running[1].AllocatedDevices, []string{"1"}) {
		t.Fatal("second job shared a busy card while another card was idle", running[0].AllocatedDevices, running[1].AllocatedDevices)
	}
	if len(running[2].AllocatedDevices) != 1 {
		t.Fatal("third job did not share one discrete card", running[2].AllocatedDevices)
	}
}
