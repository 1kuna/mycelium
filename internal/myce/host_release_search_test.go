package myce

import (
	"fmt"
	"slices"
	"strings"
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

func TestR1StartedEnvironmentDoesNotDuplicateBuild(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("M3HeterogeneousCopyPreparation", func(t *testing.T) {
		q, _ := fixture(t)
		config := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Name: "cuda-copy", Path: "weights", Engine: "fake", MemoryGB: 2, Platforms: []string{"cuda"}, SizeBytes: 1024, SHA256: strings.Repeat("a", 64)}}}
		if err := q.ConfigureModels(config); err != nil {
			t.Fatal(err)
		}
		m := m3Machine()
		m.Devices[1].Platform = "cuda"
		profile := *m.CapabilityProfile
		profile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 8 << 30}}
		m.Devices[1].CapabilityProfile = &profile
		m.ModelConfigHash = modelConfigHash(config)
		machineCheck(t, q, m)
		if err := q.SyncModel(SyncRequest{Model: "cuda-copy", To: "cuda"}); err != nil {
			t.Fatal(err)
		}
		r := machineCheck(t, q, m)
		if len(r.CopyPlan) != 1 || r.CopyPlan[0].Launch == nil || r.CopyPlan[0].Launch.Device != "1" {
			t.Fatal("second-platform copy was not offered once", r.CopyPlan)
		}
		s := m3Spec("experiment", "1", 1, 1)
		s.Needs.Platform = Platform{Required: []string{"cuda"}}
		queued := submit(t, q, s)
		a := assignedJob(machineCheck(t, q, m))
		if a == nil {
			t.Fatal("missing neighboring execution", job(t, q, queued.ID).Reason)
		}
		if got := machineCheck(t, q, m, a); len(got.CopyPlan) != 0 {
			t.Fatal("background copy ignored busy machine", got.CopyPlan)
		}

	})

	t.Run("R1StartedCopyDoesNotDuplicateModel", func(t *testing.T) {
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

	})
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
