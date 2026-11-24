package myce

import (
	"context"
	"errors"
	"fmt"
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
	s.Name = fmt.Sprintf("batch-%g-gib", gb)
	return s
}

// Two experiments share the GPU; returns the 17 GiB and 13 GiB attempts.
func sharedPair(t *testing.T, q *Queue, m Machine) (*Job, *Job) {
	t.Helper()
	first := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 17))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID {
		t.Fatal("first job not assigned", job(t, q, first.ID).Reason)
	}
	second := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 13))
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
	m := sharedCUDA40GiBCard()
	a, b := sharedPair(t, q, m)
	machineCheck(t, q, m, a, b)
	requireState(t, q, a.ID, "running")
	requireState(t, q, b.ID, "running")
	// The worker admits the second attempt on the same card within its budget.
	running := []Job{*job(t, q, a.ID)}
	next := *job(t, q, b.ID)
	over := *job(t, q, b.ID)
	over.Spec.Needs.MemoryGB = 25
	pools := *over.Memory
	card := *pools.Card
	card.TypicalGB, card.ChargeGB = 25, 25
	pools.Card, over.Memory = &card, &pools
	for _, mode := range []string{"dedicated", "fixed"} {
		t.Run(mode, func(t *testing.T) {
			w := sharedWorker(m, clock)
			d := &w.Machine.Devices[0]
			d.MemoryMode = mode
			if mode == "fixed" {
				d.MemoryMeasured, d.FreeMemoryGB = false, 0
			}
			if err := localAttemptAdmission(w, running, next, nil); err != nil {
				t.Fatal("worker refused a co-tenant that fits:", err)
			}
			if err := localAttemptAdmission(w, running, over, nil); err == nil || admissionDetail(err, over).Subcheck != memoryWaitReason {
				t.Fatal("worker admitted co-tenants beyond the card budget:", err)
			}
		})
	}
}

func sharedDiscreteMachine(platform string) Machine {
	m := m3Machine()
	m.Devices = m.Devices[:1]
	m.GPU, m.Devices[0].Platform = platform, platform
	m.CapabilityProfile.Accelerators[0].Type = platform
	return m
}

func TestSharedDeviceXPUTierIsolation(t *testing.T) {
	q, clock := fixture(t)
	m := sharedDiscreteMachine("xpu")
	var tenants []*Job
	for _, gb := range []float64{3, 2} {
		queued := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", gb))
		run := assignedJob(machineCheck(t, q, m, tenants...))
		if run == nil || run.ID != queued.ID {
			t.Fatal("experiment did not pack on XPU", run, job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		tenants = append(tenants, run)
	}
	// All three would fit, but either lower-tier teardown can reset the engine.
	demand := submitMeasuredCardJob(t, q, m, sharedSpec("production", 1))
	reply := machineCheck(t, q, m, tenants...)
	if got := assignedJob(reply); got != nil {
		t.Fatal("production joined lower-tier XPU tenants", got.ID)
	}
	for _, run := range tenants {
		requireState(t, q, run.ID, "stopping")
		if !slices.ContainsFunc(reply.Attempts, func(a AttemptDirective) bool {
			return a.ID == run.ID && a.Attempt == run.Attempt && a.Stop && a.PriorityStop != nil
		}) {
			t.Fatal("missing exact tenant stop directive", run.ID, reply.Attempts)
		}
	}
	clock.Advance(q.gpuStopGrace() + time.Second)
	if got := assignedJob(machineCheck(t, q, m, tenants...)); got != nil {
		t.Fatal("elapsed stop grace admitted production without release", got.ID)
	}
	stale := result(tenants[0], false)
	stale.Attempt++
	stale.Interrupted = true
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatal("inexact tenant release was accepted", err)
	}
	finishInterrupted(t, q, tenants[0])
	if got := assignedJob(machineCheck(t, q, m, tenants[1])); got != nil {
		t.Fatal("production started before every lower-tier tenant released", got.ID)
	}
	finishInterrupted(t, q, tenants[1])
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.ID != demand.ID || !slices.Equal(got.AllocatedDevices, []string{"0"}) {
		t.Fatal("production did not start after exact XPU releases", got, job(t, q, demand.ID).Reason)
	}
	if err := q.Started(result(got, false)); err != nil {
		t.Fatal("production start refused after release", err)
	}
}

func TestSharedDeviceMetalHigherTierPacks(t *testing.T) {
	testSharedDeviceTierPacks(t, "metal", "production")
}

func TestSharedDeviceXPUModelLoadWaitsForTierRelease(t *testing.T) {
	q, clock, m := sharedXPUModelFixture(t)
	var tenants []*Job
	for _, gb := range []float64{3, 2} {
		queued := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", gb))
		run := assignedJob(machineCheck(t, q, m, tenants...))
		if run == nil || run.ID != queued.ID {
			t.Fatal("experiment did not pack", run, job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		tenants = append(tenants, run)
	}
	addModelCall(t, q, "production")
	for _, run := range tenants {
		requireState(t, q, run.ID, "stopping")
	}
	clock.Advance(q.gpuStopGrace() + time.Second)
	if reply := machineCheck(t, q, m, tenants...); len(reply.ModelPlan) != 0 {
		t.Fatal("model loaded before lower-tier batch release", reply.ModelPlan)
	}
	finishInterrupted(t, q, tenants[0])
	if reply := machineCheck(t, q, m, tenants[1]); len(reply.ModelPlan) != 0 {
		t.Fatal("model loaded before every lower-tier batch released", reply.ModelPlan)
	}
	finishInterrupted(t, q, tenants[1])
	reply := machineCheck(t, q, m)
	if len(reply.ModelPlan) != 1 || reply.ModelPlan[0].State != "loading" || reply.ModelPlan[0].Device != "0" {
		t.Fatal("model did not load after exact batch releases", reply.ModelPlan)
	}
}

func sharedXPUModelFixture(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock, m := modelFixture(t)
	inventory := sharedDiscreteMachine("xpu")
	m.GPU, m.Devices, m.CapabilityProfile = "xpu", inventory.Devices, inventory.CapabilityProfile
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	q.models.Models[0].MemoryGB, q.models.Models[0].Platforms = 1, []string{"xpu"}
	m.ModelConfigHash = modelConfigHash(q.models)
	machineCheck(t, q, m)
	return q, clock, m
}

func TestSharedDeviceXPUEnginePromotionWaitsForTierRelease(t *testing.T) {
	for _, state := range []string{"ready", "loading"} {
		t.Run(state, func(t *testing.T) {
			q, _, m := sharedXPUModelFixture(t)
			// Establish the batch before the pending call takes queue precedence.
			queued := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 3))
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID {
				t.Fatal("no experiment co-tenant", run, job(t, q, queued.ID).Reason)
			}
			addModelCall(t, q, "experiment")
			slot := q.s.ModelSlots[slotKey(m.ID, "test", "0")]
			if slot == nil {
				t.Fatal("no experiment engine")
			}
			if state == "ready" {
				_, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "0", Attempt: slot.Attempt, State: "ready"}}})
				if err != nil {
					t.Fatal(err)
				}
			}
			high := addModelCall(t, q, "production")
			requireState(t, q, run.ID, "stopping")
			if high.Machine != "" || slot.Priority == "production" {
				t.Fatal("existing engine promoted before batch release", high, slot)
			}
			finishInterrupted(t, q, run)
			if slot.Priority != "production" || state == "ready" && high.Machine != m.ID {
				t.Fatal("existing engine did not promote after exact release", high, slot)
			}
		})
	}
}

func TestSharedDeviceXPUProductionBatchWaitsForEngineRelease(t *testing.T) {
	q, _, m := sharedXPUModelFixture(t)
	addModelCall(t, q, "experiment")
	slot := q.s.ModelSlots[slotKey(m.ID, "test", "0")]
	if slot == nil {
		t.Fatal("no experiment engine")
	}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "0", Attempt: slot.Attempt, State: "ready"}}}); err != nil {
		t.Fatal(err)
	}
	high := submitMeasuredCardJob(t, q, m, sharedSpec("production", 1))
	if got := assignedJob(machineCheck(t, q, m)); got != nil || slot.PriorityStop == nil {
		t.Fatal("production did not stop and wait for lower-tier engine", got, slot)
	}
	for _, attempt := range []string{"stale-engine", slot.Attempt} {
		reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "0", Attempt: attempt, State: "unloaded", CallsMeasured: true}}})
		if err != nil {
			t.Fatal(err)
		}
		got := assignedJob(reply)
		if attempt == "stale-engine" && got != nil {
			t.Fatal("stale engine release admitted production", got)
		}
		if attempt != "stale-engine" && (got == nil || got.ID != high.ID) {
			t.Fatal("production did not start after exact engine release", got, job(t, q, high.ID).Reason)
		}
	}
}

func TestSharedDeviceXPUModelSkipsRetainedCleanup(t *testing.T) {
	q, _, m := sharedXPUModelFixture(t)
	low := addModelCall(t, q, "experiment")
	slot := q.s.ModelSlots[slotKey(m.ID, "test", "0")]
	if slot == nil {
		t.Fatal("no experiment engine")
	}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "0", Attempt: slot.Attempt, State: "ready"}}}); err != nil {
		t.Fatal(err)
	}
	if err := q.finishCall(low.ID); err != nil {
		t.Fatal(err)
	}
	queued := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 3))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != queued.ID {
		t.Fatal("no experiment co-tenant", run, job(t, q, queued.ID).Reason)
	}
	pending := result(run, true)
	pending.ReleasePending = true
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, run.ID, "succeeded")
	other := m
	other.ID, other.Session = "two", "session-two"
	machineCheck(t, q, other)
	warm := &ModelCall{Project: "alternate", Model: "test", Priority: "experiment"}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"alternate": {Weight: 1, AllowedMachines: []string{other.ID}}}); err != nil {
		t.Fatal(err)
	}
	if err := q.addCall(warm); err != nil {
		t.Fatal(err)
	}
	ready := q.s.ModelSlots[slotKey(other.ID, "test", "0")]
	if ready == nil {
		t.Fatal("no alternate engine")
	}
	if _, err := q.Checkin(Checkin{Machine: other, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "0", Attempt: ready.Attempt, State: "ready"}}}); err != nil {
		t.Fatal(err)
	}
	if err := q.finishCall(warm.ID); err != nil {
		t.Fatal(err)
	}
	high := addModelCall(t, q, "production")
	if high.Machine != other.ID {
		t.Fatal("retained cleanup hid available model target", high)
	}
	if copy := job(t, q, run.ID).Copies[0]; copy.ReleaseConfirmed || copy.ReleasedAt != nil {
		t.Fatal("placement manufactured cleanup evidence", copy)
	}
}

func testSharedDeviceTierPacks(t *testing.T, platform, priority string) {
	t.Helper()
	q, _ := fixture(t)
	m := sharedDiscreteMachine(platform)
	if platform == "metal" {
		m.CapabilityProfile.MemoryLayout = "unified"
		m.Devices[0].UnifiedMemory, m.Devices[0].MemoryMode = true, "unified"
	}
	first := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 3))
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != first.ID {
		t.Fatal("experiment not assigned", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	newcomer := submitMeasuredCardJob(t, q, m, sharedSpec(priority, 2))
	got := assignedJob(machineCheck(t, q, m, run))
	if got == nil || got.ID != newcomer.ID || !slices.Equal(got.AllocatedDevices, run.AllocatedDevices) {
		t.Fatal("fitting newcomer did not pack", got, job(t, q, newcomer.ID).Reason)
	}
	requireState(t, q, run.ID, "running")
	if priorityStop(job(t, q, run.ID)) != nil {
		t.Fatal("packing stopped the existing tenant")
	}
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
	small := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 13))
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
	m := sharedCUDA40GiBCard()
	a, b := sharedPair(t, q, m)
	machineCheck(t, q, m, a, b)
	q = memoryFactsRestart(t, q, clock)
	fakeCheckpointAvailability(t, q, true)
	third := submitMeasuredCardJob(t, q, m, sharedSpec("experiment", 17))
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

// A multi-card demand keeps its cards to itself, so it must stop every
// co-tenant on them, not only enough to free its per-card share (#538 review).
func TestSharedDeviceMultiCardDemandStopsEveryCoTenant(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	var running []*Job
	for _, gb := range []float64{3, 3, 2, 2} {
		s := spec("experiment")
		s.Needs.MemoryGB, s.Needs.HostMemoryGB = gb, 1
		queued := submitMeasuredCardJob(t, q, m, s)
		got := assignedJob(machineCheck(t, q, m, running...))
		if got == nil || got.ID != queued.ID {
			t.Fatal("experiment not admitted beside another on an 8 GiB card", gb, job(t, q, queued.ID).Reason)
		}
		running = append(running, got)
	}
	// Each card holds a 3 GiB and a 2 GiB experiment; stopping the 3 GiB one
	// alone would free the 6 GiB share a 12 GiB two-card job needs per card.
	s := bigSpec("production", 12)
	s.Name = "two-card-production"
	production := submit(t, q, s)
	machineCheck(t, q, m, running...)
	for _, r := range running {
		if got := job(t, q, r.ID); got.State != "stopping" {
			t.Fatal("two-card production left a co-tenant running on its cards", got.ID, got.State, got.AllocatedDevices, job(t, q, production.ID).Reason)
		}
	}
}

// Unified accelerator work with an estimate can share host RAM from its first run.
func TestSharedDeviceUnifiedFirstRunShares(t *testing.T) {
	q, _ := fixture(t)
	m := sharedUnifiedMachine()
	s := plainGPUSpec()
	s.Needs.HostMemoryGB = 4
	first := assignGPU(t, q, m, submit(t, q, s))
	if first.Memory == nil || first.Memory.Card != nil || first.Memory.RAM.ChargeGB != 4 {
		t.Fatal("unified first run did not use only the RAM pool")
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	small := submit(t, q, sharedSpec("experiment", 13))
	if got := assignedJob(machineCheck(t, q, m, first)); got == nil || got.ID != small.ID {
		t.Fatal("fitting work did not share the unified pool")
	}
}

func sharedCUDA40GiBCard() Machine {
	m := sharedUnifiedMachine()
	m.CapabilityProfile.MemoryLayout = "separate"
	m.Devices[0].UnifiedMemory, m.Devices[0].MemoryMode = false, "dedicated"
	return m
}

func submitMeasuredCardJob(t *testing.T, q *Queue, m Machine, spec Spec) *Job {
	t.Helper()
	card := m
	if len(m.Devices) > 0 {
		card = m.Devices[0].machine(m)
	}
	if card.UnifiedMemory || spec.Needs.cpuOnly() || spec.Needs.MemoryGB <= 0 {
		return submit(t, q, spec)
	}
	key := pooledEvidenceKey(&Job{Spec: spec}, &card)
	key.Pool = "card"
	if err := q.update(func() error {
		e := q.memoryEvidence.stage()
		family := key.familyKey()
		e.family[family] = memoryEvidenceSeries{Peaks: []float64{spec.Needs.MemoryGB}}
		e.ratios[memoryRatioKey{key.Platform, "card", "family", "", memoryRatioBucket(1)}] = []float64{1, 1, 1, 1, 1, 1, 1, 1, 1}
		q.memoryEvidence = e
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return submit(t, q, spec)
}

func growthCheck(t *testing.T, q *Queue, m Machine, runs []*Job, used []float64, age float64) Reply {
	t.Helper()
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	for i, j := range runs {
		r := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true}
		if i < len(used) {
			r.CurrentHostMemory = &CurrentHostMemory{DeviceGB: &used[i], DeviceAgeSeconds: age}
		}
		c.Attempts = append(c.Attempts, r)
	}
	r, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
