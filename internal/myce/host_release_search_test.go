package myce

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

type releaseTestVictim struct {
	id, priority string
	cores        int
	ram          float64
	slot         bool
}

// Install a physical host with one execution per worker, leaving the target
// worker's command/GPU slot free so only host-wide releases are needed.
func releaseTestHost(t *testing.T, cores int, ram float64, victims []releaseTestVictim) (*Queue, *Machine) {
	t.Helper()
	q, _ := fixture(t)
	target := cpuMachine("target", "shared", "fake", cores, ram)
	q.s.Machines[target.ID] = &target
	for _, v := range victims {
		m := cpuMachine("worker-"+v.id, "shared", "fake", cores, ram)
		q.s.Machines[m.ID] = &m
		if v.slot {
			q.models.Models = append(q.models.Models, Model{Name: v.id, MemoryGB: 1, HostMemoryGB: v.ram})
			s := &ModelSlot{Machine: m.ID, Session: m.Session, Model: v.id, State: "ready", Priority: v.priority, Project: "test"}
			q.s.ModelSlots[slotKey(m.ID, v.id)] = s
		} else {
			s := spec(v.priority)
			s.Needs = ResourceNeeds{CPUCores: v.cores, HostMemoryGB: v.ram}
			q.s.Jobs[v.id] = &Job{ID: v.id, Machine: m.ID, Session: m.Session, Spec: s, State: "running"}
		}
	}
	q.refreshHostMemory()
	return q, &target
}

func releaseTestIDs(jobs []*Job, slots []*ModelSlot) []string {
	ids := []string{}
	for _, j := range jobs {
		ids = append(ids, "job/"+j.ID)
	}
	for _, s := range slots {
		ids = append(ids, "slot/"+slotKey(s.Machine, s.Model))
	}
	slices.Sort(ids)
	return ids
}

func TestHostReleaseSingleVictimBeforeGreedy(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cores       int
		ram         float64
		demandCores int
		demandRAM   float64
		victims     []releaseTestVictim
		want        []string
	}{
		{"mixed cores and RAM swap", 4, 13, 2, 6, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "deadline", 1, 6, false}, {"c", "deadline", 2, 6, false},
		}, []string{"job/c"}},
		{"same tier cores swap", 4, 16, 2, 0, []releaseTestVictim{
			{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false}, {"c", "experiment", 2, 0, false},
		}, []string{"job/c"}},
		{"RAM only swap", 16, 4, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "experiment", 1, 1, false}, {"c", "experiment", 1, 2, false},
		}, []string{"job/c"}},
		{"single deadline job before two experiments", 16, 4, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "experiment", 1, 1, false}, {"c", "deadline", 1, 2, false},
		}, []string{"job/c"}},
		{"single deadline slot before two experiments", 16, 4, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "experiment", 1, 1, false}, {"c", "deadline", 1, 2, true},
		}, []string{"slot/worker-c/c"}},
		{"single victim tier before excess", 16, 6, 1, 2, []releaseTestVictim{
			{"a", "deadline", 1, 2, false}, {"b", "experiment", 1, 4, false},
		}, []string{"job/b"}},
		{"greedy two experiments without sufficient singleton", 16, 4, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "experiment", 1, 1, false}, {"c", "deadline", 1, 1, false}, {"d", "deadline", 1, 1, false},
		}, []string{"job/a", "job/b"}},
		{"least excess then IDs", 16, 8, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 4, false}, {"b", "experiment", 1, 2, false}, {"c", "experiment", 1, 2, false},
		}, []string{"job/b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, m := releaseTestHost(t, tc.cores, tc.ram, tc.victims)
			demand := &Job{ID: "demand", Spec: spec("production"), State: "queued"}
			demand.Spec.Needs = ResourceNeeds{CPUCores: tc.demandCores, HostMemoryGB: tc.demandRAM}
			p := q.roomPlan(demand, m, true)
			if p == nil {
				t.Fatal("no feasible release plan")
			}
			if got := releaseTestIDs(p.jobs, p.slots); !slices.Equal(got, tc.want) {
				t.Fatalf("victims = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestR1StartedEnvironmentDoesNotDuplicateBuild(t *testing.T) {
	q, clock := fixture(t)
	r := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tool": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	slow, fast := machine("slow", "fake"), machine("fast", "fake")
	for i, m := range []*Machine{&slow, &fast} {
		m.EnvironmentBuilder = true
		m.EnvironmentBuildSeconds = map[string]float64{r.Key: float64(120 - i*110)}
		check(t, q, *m, nil)
	}
	s := spec("experiment")
	s.Environment.Name = "tool"
	j := submit(t, q, s)
	plan := check(t, q, fast, nil).EnvironmentPlan
	if len(plan) != 1 || plan[0].Launch == nil || plan[0].Launch.Demand != j.ID {
		t.Fatal("faster worker did not receive the build")
	}
	launch := *plan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	fast.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
	clock.Advance(time.Second)
	if continued := check(t, q, fast, nil).EnvironmentPlan; len(continued) != 1 || continued[0].Launch.Attempt != launch.Attempt {
		t.Fatal("faster worker lost its started build")
	}
	if duplicate := check(t, q, slow, nil).EnvironmentPlan; len(duplicate) != 0 {
		t.Fatalf("slower worker duplicated the started build: %s", duplicate[0].Key)
	}
}

func TestR1StartedCopyDoesNotDuplicateModel(t *testing.T) {
	q, clock, copying := syncFixture(t)
	cold := machine("cold", "fake")
	check(t, q, cold, nil)
	if err := q.SyncModel(SyncRequest{Model: "a", To: copying.ID}); err != nil {
		t.Fatal(err)
	}
	plan := check(t, q, copying, nil).CopyPlan
	if len(plan) != 1 || plan[0].Launch == nil {
		t.Fatal("missing copy offer", plan)
	}
	launch := *plan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	copying.CopyReports = []CopyReport{{Model: "a", Attempt: launch.Attempt, State: "copying", Bytes: 20_000_000_000, Total: 40_000_000_000}}
	clock.Advance(time.Second)
	check(t, q, copying, nil)
	unrelated := &Job{Spec: spec("experiment")}
	unrelated.Spec.Needs.Model = "b"
	if q.workerIdle(q.s.Machines[copying.ID], unrelated) {
		t.Fatal("copy of a made the worker idle for unrelated model b")
	}
	s := spec("experiment")
	s.Needs.Model = "a"
	j := submit(t, q, s)
	if duplicate := check(t, q, cold, nil).Job; duplicate != nil {
		t.Fatalf("cold worker took %s and duplicated a started 40 GB model copy", duplicate.ID)
	}
	if got := check(t, q, copying, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("worker already copying the model did not receive its demand", got)
	}
}

func TestHostReleaseConfirmedStoppingJobHasNoCredit(t *testing.T) {
	for _, extra := range []int{0, 13} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			victims := []releaseTestVictim{{"a", "production", 1, 4, false}, {"b", "experiment", 1, 4, false}}
			for i := 0; i < extra; i++ {
				victims = append(victims, releaseTestVictim{fmt.Sprintf("c%02d", i), "experiment", 1, 0, false})
			}
			q, m := releaseTestHost(t, 32, 8, victims)
			stopping := q.s.Jobs["b"]
			stopping.State, stopping.ReleaseConfirmed = "stopping", true
			q.refreshHostMemory()
			if m.FreeHostMemoryGB != 4 {
				t.Fatal("release confirmation did not refund RAM", m.FreeHostMemoryGB)
			}
			demand := spec("production")
			demand.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 8}
			jobs, slots, ok := q.hostReleasesExcept(m, 0, 8, demand, nil, nil)
			if ok || len(jobs)+len(slots) != 0 {
				t.Fatalf("already-refunded release supplied phantom RAM: feasible=%v victims=%v", ok, releaseTestIDs(jobs, slots))
			}
			if plan := q.roomPlan(&Job{ID: "demand", Spec: demand, State: "queued"}, q.s.Machines[stopping.Machine], true); plan != nil {
				t.Fatalf("local release projection supplied phantom RAM: victims=%v", releaseTestIDs(plan.jobs, plan.slots))
			}
		})
	}
}

func TestHostReleaseEnvironmentBuildKeepsCPUReservation(t *testing.T) {
	q, m := releaseTestHost(t, 3, 64, []releaseTestVictim{
		{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false},
	})
	owner := q.s.Machines["worker-a"]
	q.s.PreparationPlans = map[string]HostLaunch{
		"build": {Machine: owner.ID, Session: owner.Session, Kind: "environment", Key: "tool", Attempt: "build", Started: true},
	}
	q.refreshHostMemory()
	demand := spec("production")
	demand.Needs = ResourceNeeds{CPUCores: 2}
	jobs, slots, ok := q.hostReleasesExcept(m, 0, 0, demand, nil, nil)
	if !ok || !slices.Equal(releaseTestIDs(jobs, slots), []string{"job/b"}) {
		t.Fatalf("build takes a's released core: feasible=%v victims=%v; want [job/b]", ok, releaseTestIDs(jobs, slots))
	}
	for _, j := range jobs {
		j.State = "succeeded"
	}
	q.refreshHostMemory()
	if !cpuAllocationFits(m, 2) {
		t.Fatal("selected releases do not fit the live CPU ledger")
	}
}

func TestHostReleaseBuildSharesJobAndSlotReservation(t *testing.T) {
	q, m := releaseTestHost(t, 3, 64, []releaseTestVictim{
		{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false},
	})
	owner := q.s.Machines["worker-a"]
	q.models.Models = append(q.models.Models, Model{Name: "model", MemoryGB: 1})
	slot := &ModelSlot{Machine: owner.ID, Session: owner.Session, Model: "model", State: "ready", Priority: "experiment", Project: "test"}
	q.s.ModelSlots[slotKey(owner.ID, slot.Model)] = slot
	q.s.PreparationPlans = map[string]HostLaunch{}
	q.s.PreparationPlans["build"] = HostLaunch{Machine: owner.ID, Session: owner.Session, Kind: "environment", Attempt: "build", Started: true}
	q.refreshHostMemory()
	demand := spec("production")
	demand.Needs = ResourceNeeds{CPUCores: 2}
	jobs, slots, ok := q.hostReleasesExcept(m, 0, 0, demand, nil, nil)
	if !ok {
		t.Fatal("shared reservation should have a feasible plan")
	}
	// The complete projection must retain the build's core even when both its
	// job and model residency have been selected for release.
	projected := q.cpuAfterReleases(m, []*Job{q.s.Jobs["a"]}, []*ModelSlot{slot}, 0)
	if projected.CPUInUseCores != 2 {
		t.Fatal("lost the build's inherited reservation", projected.CPUInUseCores)
	}
	for _, j := range jobs {
		j.State = "succeeded"
	}
	for _, s := range slots {
		delete(q.s.ModelSlots, slotKey(s.Machine, s.Model))
	}
	q.refreshHostMemory()
	if !cpuAllocationFits(m, demand.Needs.cores()) {
		t.Fatal("chosen releases do not make room for the demand")
	}

}

func TestHostReleaseLocalBuildStopsWithExecution(t *testing.T) {
	q, _ := releaseTestHost(t, 3, 64, []releaseTestVictim{
		{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false},
	})
	owner := q.s.Machines["worker-a"]
	buildSpec := spec("experiment")
	q.s.PreparationPlans = map[string]HostLaunch{}
	q.s.PreparationPlans["build"] = HostLaunch{Machine: owner.ID, Session: owner.Session, Kind: "environment", Key: "tool", Attempt: "build", Started: true, Spec: &buildSpec}
	q.refreshHostMemory()
	demand := &Job{ID: "demand", State: "queued", Spec: spec("production")}
	demand.Spec.Needs = ResourceNeeds{CPUCores: 2}
	plan := q.roomPlan(demand, owner, true)
	if plan == nil || !slices.Equal(releaseTestIDs(plan.jobs, plan.slots), []string{"job/a"}) {
		t.Fatal("local job/build stop did not free its core", plan)
	}
	projected := q.cpuAfterReleases(owner, plan.jobs, plan.slots, plan.preparationCores)
	if !cpuAllocationFits(&projected, 2) {
		t.Fatal("local release projection retained a stopped build")
	}
}
