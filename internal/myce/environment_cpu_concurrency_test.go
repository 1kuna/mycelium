package myce

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// #727: exercise queue offers, launch authorization, command actors and the
// actual environment runtime together. Neither command may have to finish for
// the missing environment to start, and the builder must retain its own core.
func TestEnvironmentPreparesBesideRunningCPUCommand(t *testing.T) {
	q, _ := fixture(t)
	clock := &devClock{} // Inject the runtime clock so heartbeat cadence advances.
	q.clock = clock
	m := m3Machine()
	m.CapabilityProfile.LogicalCPUs = 3
	longSpec := cpuSpec(2, .1)
	commandRelease := filepath.Join(t.TempDir(), "command-release")
	longSpec.Run.Command = []string{"sh", "-c", `while [ ! -f "$1" ]; do sleep .05; done`, "fixture", commandRelease}
	long := submit(t, q, longSpec)
	r, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"prepare-fixture"}}, "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"new": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: newTestStateRoot(t), Client: Client{URL: testServerForEnvironments(t, q), Token: "test"}, Machine: m, Clock: clock, Interval: time.Millisecond,
		CPUIsolationProbe: func(context.Context) error { return nil },
		InventoryProbe:    func(context.Context) ([]MachineDevice, error) { return slices.Clone(m.Devices), nil },
		environmentRun: func(ctx context.Context, _ []string, _ string, _ []string, _, _ io.Writer) error {
			started <- struct{}{}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		_ = os.WriteFile(commandRelease, nil, 0600)
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	await(t, func() bool {
		j, _ := q.jobSnapshot(long.ID)
		return j.StartedAt != nil
	})
	waitingSpec := cpuSpec(1, .1)
	waitingSpec.Environment.Name = "new"
	waiting := submit(t, q, waitingSpec)
	select {
	case <-started:
	case <-time.After(testWaitTimeout):
		t.Fatal("environment offer never started beside the running CPU command")
	}
	await(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		for _, p := range q.s.PreparationPlans {
			if p.Demand == waiting.ID && p.Started {
				return q.s.Machines[m.ID].CPUInUseCores == 3
			}
		}
		return false
	})
	got, _ := q.jobSnapshot(long.ID)
	if got.State != "running" || got.Attempt != 1 || got.ReleaseConfirmed {
		t.Fatalf("preparation disturbed the existing CPU command: %+v", got)
	}
	got, _ = q.jobSnapshot(waiting.ID)
	if got.State != "queued" || got.Attempt != 0 {
		t.Fatalf("builder core was reused by a command: %+v", got)
	}
	close(release)
	await(t, func() bool {
		j, _ := q.jobSnapshot(waiting.ID)
		return j.State == "succeeded" && j.ReleaseConfirmed
	})
	got, _ = q.jobSnapshot(long.ID)
	if got.State != "running" || got.Attempt != 1 {
		t.Fatalf("prepared command waited for or stopped unrelated CPU work: %+v", got)
	}
}

func TestEnvironmentPreparationCannotBorrowUnrelatedCPUCore(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	m.EnvironmentBuilder = true
	m.CapabilityProfile.LogicalCPUs = 2
	long := submit(t, q, cpuSpec(2, .1))
	active := assignedJob(machineCheck(t, q, m))
	if active == nil || active.ID != long.ID {
		t.Fatal("missing CPU assignment", active)
	}
	// An offer can lose its free core before launch. Authorization must reject
	// it, rather than treating the unrelated command as the builder's reserve.
	r := binaryRecipe(t, "cpu")
	q.mu.Lock()
	l := q.issuePreparation(cpuMachineView(q.s.Machines[m.ID]), "environment", r.Key)
	q.mu.Unlock()
	if err := q.AuthorizeLaunch(l); err == nil {
		t.Fatal("builder borrowed a fully occupied CPU reservation")
	}
	if localEnvironmentPreparationFits(Worker{Machine: m}, []Job{*active}, nil, true) {
		t.Fatal("worker admitted a builder without a free core")
	}
}

func TestEnvironmentPreparationDoesNotDeferToLoadingPeer(t *testing.T) {
	q, _ := fixture(t)
	busy := m3Machine()
	busy.ID, busy.HostID, busy.Session = "a", "a", "session-a"
	busy.EnvironmentBuilder = true
	deviceWork := assignedJob(machineCheck(t, q, busy))
	if deviceWork != nil {
		t.Fatal("unexpected work", deviceWork)
	}
	deviceJob := submit(t, q, spec("experiment"))
	deviceWork = assignedJob(machineCheck(t, q, busy))
	if deviceWork == nil || deviceWork.ID != deviceJob.ID {
		t.Fatal("missing device command", deviceWork)
	}
	ready := m3Machine()
	ready.ID, ready.HostID, ready.Session = "z", "z", "session-z"
	ready.EnvironmentBuilder = true
	machineCheck(t, q, ready)
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"new": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, .1)
	s.Environment.Name = "new"
	demand := submit(t, q, s)
	q.models.Models = append(q.models.Models, Model{Name: "loading", MemoryGB: 1})
	q.s.ModelSlots[slotKey(busy.ID, "loading", "1")] = &ModelSlot{Machine: busy.ID, Session: busy.Session, Device: "1", Model: "loading", State: "loading", Priority: "experiment"}
	q.refreshHostCPU()
	if plan := q.environmentPlan(cpuMachineView(q.s.Machines[busy.ID])); len(plan) != 0 {
		t.Fatal("loading peer attracted an unstartable offer", plan)
	}
	plan := q.environmentPlan(cpuMachineView(q.s.Machines[ready.ID]))
	if len(plan) != 1 || plan[0].Launch.Demand != demand.ID {
		t.Fatal("eligible worker deferred to a peer unable to prepare", plan)
	}
	if err := q.AuthorizeLaunch(*plan[0].Launch); err != nil {
		t.Fatal("eligible worker could not start preparation", err)
	}
}

func TestEnvironmentOwnedCoreSurvivesExecutionReleaseProjection(t *testing.T) {
	q, m := releaseTestHost(t, 3, 64, []releaseTestVictim{
		{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false},
	})
	owner := q.s.Machines["worker-a"]
	q.s.PreparationPlans = map[string]HostLaunch{
		"build": {Machine: owner.ID, Session: owner.Session, Kind: "environment", Attempt: "build", Started: true, CPUCores: 1},
	}
	q.refreshHostMemory()
	demand := spec("production")
	demand.Needs = ResourceNeeds{CPUCores: 2}
	jobs, slots, ok := q.hostReleases(m, 0, 0, demand.Priority, demand)
	if !ok || !slices.Equal(releaseTestIDs(jobs, slots), []string{"job/a", "job/b"}) {
		t.Fatalf("owned builder core distorted release projection: feasible=%t victims=%v", ok, releaseTestIDs(jobs, slots))
	}
	projected := q.cpuAfterReleases(m, jobs, slots, 0)
	if projected.CPUInUseCores != 1 || !cpuAllocationFits(&projected, 2) {
		t.Fatalf("projection lost the separately owned builder core: %+v", projected)
	}
}

func TestEnvironmentAndCopyCoreAccountingIsOrderIndependent(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	q.s.Machines[m.ID] = &m
	q.s.PreparationPlans = map[string]HostLaunch{
		"build": {Machine: m.ID, Session: m.Session, Kind: "environment", Attempt: "build", Started: true, CPUCores: 1},
		"copy":  {Machine: m.ID, Session: m.Session, Kind: "copy", Attempt: "copy", Started: true},
	}
	// Repeated map walks expose the observed environment/copy order bug.
	for range 128 {
		if used := q.hostCPUInUse(&m, nil, nil, false); used != 2 {
			t.Fatalf("same custody changed core charge: got %d, want 2", used)
		}
	}
}

// #1486: a GPU job whose environment is missing on a busy GPU host. The build
// was offered only to the job admissible right now, and neither queue nor worker
// let a device-target build run beside a device command, so the environment was
// never prepared there and the job could never become admissible: CUDA-only
// work waited on "no builder has started" while any-accelerator work ran
// elsewhere. A plain build now starts beside the device command, for the job
// that cannot fit yet, through the real worker runtime.
func TestEnvironmentPreparesAheadBesideRunningDeviceCommand(t *testing.T) {
	q, _ := fixture(t)
	clock := &devClock{}
	q.clock = clock
	m := m3Machine()
	m.Devices = m.Devices[:1]
	longSpec := spec("experiment")
	longSpec.Needs.MemoryGB, longSpec.Needs.Platform = 6, Platform{Required: []string{"fake"}}
	commandRelease := filepath.Join(t.TempDir(), "command-release")
	longSpec.Run.Command = []string{"sh", "-c", `while [ ! -f "$1" ]; do sleep .05; done`, "fixture", commandRelease}
	long := submit(t, q, longSpec)
	r, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"prepare-fixture"}}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"new": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	w := Worker{EnvironmentLimit: 1 << 20, StateDir: newTestStateRoot(t), Client: Client{URL: testServerForEnvironments(t, q), Token: "test"}, Machine: m, Clock: clock, Interval: time.Millisecond,
		CPUIsolationProbe: func(context.Context) error { return nil },
		InventoryProbe:    func(context.Context) ([]MachineDevice, error) { return slices.Clone(m.Devices), nil },
		environmentRun: func(ctx context.Context, _ []string, _ string, _ []string, _, _ io.Writer) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		_ = os.WriteFile(commandRelease, nil, 0600)
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	await(t, func() bool {
		j, _ := q.jobSnapshot(long.ID)
		return j.StartedAt != nil
	})
	waitingSpec := spec("experiment")
	waitingSpec.Needs.MemoryGB, waitingSpec.Needs.Platform = 6, Platform{Required: []string{"fake"}}
	waitingSpec.Environment.Name = "new"
	waiting := submit(t, q, waitingSpec)
	select {
	case <-started:
	case <-time.After(testWaitTimeout):
		j, _ := q.jobSnapshot(waiting.ID)
		t.Fatalf("no build started for the GPU job while the card was busy: %q", j.Reason)
	}
	await(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		for _, p := range q.s.PreparationPlans {
			if p.Demand == waiting.ID && p.Started {
				return true
			}
		}
		return false
	})
	if got, _ := q.jobSnapshot(long.ID); got.State != "running" || got.Attempt != 1 {
		t.Fatalf("preparation disturbed the device command: %+v", got)
	}
	if got, _ := q.jobSnapshot(waiting.ID); got.State != "queued" || got.Attempt != 0 {
		t.Fatalf("job started before its card was free: %+v", got)
	}
}
