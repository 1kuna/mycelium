package myce

import (
	"fmt"
	"math/rand"
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

func TestHostReleaseOptimalVictims(t *testing.T) {
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
		{"two experiments before deadline job", 16, 4, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "experiment", 1, 1, false}, {"c", "deadline", 1, 2, false},
		}, []string{"job/a", "job/b"}},
		{"two experiments before deadline slot", 16, 4, 1, 2, []releaseTestVictim{
			{"a", "experiment", 1, 1, false}, {"b", "experiment", 1, 1, false}, {"c", "deadline", 1, 2, true},
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

// The oracle enumerates every allowed subset without the planner's pruning.
// It computes capacity and rank independently, including pending (zero-stop)
// releases. Policy matching/eligibility and hostNeed are the existing contract.
func bruteForceHostReleases(q *Queue, m *Machine, demand Spec, releasedJobs map[*Job]bool, releasedSlots map[*ModelSlot]bool) ([]string, bool) {
	type candidate struct {
		id          string
		tier, stops int
		cores       int
		ram         float64
	}
	var candidates []candidate
	for _, j := range q.s.Jobs {
		owner := q.s.Machines[j.Machine]
		if releasedJobs[j] || j.ReleaseConfirmed || owner == nil || owner.Gone || hostID(owner) != hostID(m) || !q.batchPreemptible(j, demand) {
			continue
		}
		v := candidate{id: "job/" + j.ID, tier: q.priorityMatch(j.Spec).Index, ram: hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)}
		if j.State == "running" {
			v.stops = 1
		}
		v.cores = j.Spec.Needs.cores()
		candidates = append(candidates, v)
	}
	for _, s := range q.s.ModelSlots {
		owner := q.s.Machines[s.Machine]
		if releasedSlots[s] || !slotReservesMemory(s) || owner == nil || owner.Gone || hostID(owner) != hostID(m) || !q.slotReclaimableFor(s, demand) {
			continue
		}
		model, _ := q.model(s.Model)
		v := candidate{id: "slot/" + slotKey(s.Machine, s.Model), tier: q.priorityMatch(q.slotSpec(s)).Index, ram: hostNeed(owner, model.MemoryGB, model.HostMemoryGB)}
		if s.State != "draining" && s.State != "unloading" {
			v.stops = 1
		}
		if slotReservesMemory(s) {
			v.cores = 1
		}
		candidates = append(candidates, v)
	}
	slices.SortFunc(candidates, func(a, b candidate) int { return strings.Compare(a.id, b.id) })
	needRAM := 0.0
	if demand.Needs.HostMemoryGB > 0 {
		needRAM = hostNeed(m, demand.Needs.MemoryGB, demand.Needs.HostMemoryGB)
	}
	ramCapacity := m.HostAvailableMemoryGB
	if m.hostBudgetGB > 0 {
		ramCapacity = min(ramCapacity, m.hostBudgetGB)
	}
	var bestCounts []int
	var bestIDs []string
	bestStops, bestExcess := 0, 0.0
	for mask := 0; mask < 1<<len(candidates); mask++ {
		counts := make([]int, len(q.effectivePriorityPolicy().Tiers))
		ids := []string{}
		freedRAM, freedCores, stops := 0.0, 0, 0
		for i, v := range candidates {
			if mask&(1<<i) != 0 {
				counts[v.tier] += v.stops
				stops += v.stops
				freedRAM += v.ram
				freedCores += v.cores
				ids = append(ids, v.id)
			}
		}
		if m.FreeHostMemoryGB+freedRAM < needRAM || m.EffectiveCPUCores-max(0, m.CPUInUseCores-freedCores) < demand.Needs.cores() {
			continue
		}
		unneededRAM := max(0, freedRAM-max(0, needRAM-m.FreeHostMemoryGB))
		unneededCores := max(0, freedCores-max(0, m.CPUInUseCores+demand.Needs.cores()-m.EffectiveCPUCores))
		excess := unneededRAM/max(1, ramCapacity) + float64(unneededCores)/float64(max(1, m.EffectiveCPUCores))
		order := slices.Compare(counts, bestCounts)
		if bestCounts == nil || order < 0 || order == 0 && (stops < bestStops || stops == bestStops && (excess < bestExcess || excess == bestExcess && slices.Compare(ids, bestIDs) < 0)) {
			bestCounts, bestIDs, bestStops, bestExcess = counts, ids, stops, excess
		}
	}
	return bestIDs, bestCounts != nil
}

func TestHostReleaseSearchMatchesBruteForce(t *testing.T) {
	const seed, cases = 100, 350
	rng := rand.New(rand.NewSource(seed))
	feasible := 0
	confirmed := 0
	slotStates := map[string]int{}
	for n := 0; n < cases; n++ {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			count := 1 + rng.Intn(8)
			if n == cases-1 {
				count = 12 // Exercise the inclusive exact-search boundary too.
			}
			victims := make([]releaseTestVictim, count)
			for i := range victims {
				victims[i] = releaseTestVictim{fmt.Sprintf("v%02d", i), []string{"production", "deadline", "experiment"}[rng.Intn(3)], 1 + rng.Intn(4), float64(rng.Intn(5)), rng.Intn(3) == 0}
			}
			q, m := releaseTestHost(t, 1+rng.Intn(12), 100, victims)
			if n%2 == 0 {
				q.s.PriorityPolicy = &PriorityPolicyRecord{Policy: PriorityPolicy{Version: 1, Tiers: []PriorityTier{
					{"production", []string{"*/production"}}, {"boost", []string{"boost/*"}}, {"deadline", []string{"*/deadline"}}, {"other", []string{"*/*"}},
				}}}
			}
			releasedJobs, releasedSlots := map[*Job]bool{}, map[*ModelSlot]bool{}
			var mandatoryJobs []*Job
			var mandatorySlots []*ModelSlot
			reserved := 0.0
			for _, v := range victims {
				owner := q.s.Machines["worker-"+v.id]
				owner.UnifiedMemory = n%3 == 0
				owner.MemoryCeilingSlackGB = float64(n%2) / 8
				if s := q.s.ModelSlots[slotKey(owner.ID, v.id)]; s != nil {
					s.State = []string{"ready", "loading", "draining", "unloading", "unload_failed", "failed"}[rng.Intn(6)]
					s.RetryReserved = rng.Intn(2) == 0
					slotStates[s.State]++
					s.InFlight = rng.Intn(2)
					if rng.Intn(4) == 0 {
						s.Project = "boost"
					}
					if rng.Intn(8) == 0 {
						now := q.clock.Now()
						s.PreemptionCount, s.RunSince = 2, &now
					}
					if rng.Intn(8) == 0 {
						releasedSlots[s] = true
						mandatorySlots = append(mandatorySlots, s)
					}
					model, _ := q.model(s.Model)
					if slotReservesMemory(s) {
						reserved += hostNeed(owner, model.MemoryGB, model.HostMemoryGB)
					}
				} else {
					j := q.s.Jobs[v.id]
					j.Spec.Needs.MemoryGB = float64(rng.Intn(4))
					if rng.Intn(4) == 0 {
						j.Spec.Project = "boost"
					}
					if rng.Intn(4) == 0 {
						until := q.clock.Now().Add(time.Minute)
						j.State, j.StopAt = "stopping", &until
						j.ReleaseConfirmed = rng.Intn(2) == 0
						if j.ReleaseConfirmed {
							confirmed++
						}
					}
					if rng.Intn(8) == 0 {
						now := q.clock.Now()
						j.PreemptionCount, j.StartedAt = 2, &now
					}
					if rng.Intn(8) == 0 && !j.ReleaseConfirmed {
						releasedJobs[j] = true
						mandatoryJobs = append(mandatoryJobs, j)
					}
					if !j.ReleaseConfirmed {
						reserved += hostNeed(owner, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
					}
				}
			}
			for _, owner := range q.s.Machines {
				owner.HostAvailableMemoryGB = reserved + float64(n%4)
			}
			m.UnifiedMemory, m.MemoryCeilingSlackGB = n%3 == 0, float64(n%2)/8
			q.refreshHostMemory()
			projected := cpuAfterReleases(m, mandatoryJobs, mandatorySlots, 0)
			for _, j := range mandatoryJobs {
				projected.FreeHostMemoryGB += hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
			}
			for _, s := range mandatorySlots {
				model, _ := q.model(s.Model)
				if slotReservesMemory(s) {
					projected.FreeHostMemoryGB += hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
				}
			}
			demand := spec("production")
			demand.Needs = ResourceNeeds{CPUCores: 1 + rng.Intn(4), HostMemoryGB: float64(rng.Intn(8)), MemoryGB: float64(rng.Intn(4))}
			want, possible := bruteForceHostReleases(q, &projected, demand, releasedJobs, releasedSlots)
			if possible {
				feasible++
			}
			for repeat := 0; repeat < 3; repeat++ {
				jobs, slots, ok := q.hostReleasesExcept(&projected, demand.Needs.MemoryGB, demand.Needs.HostMemoryGB, demand, releasedJobs, releasedSlots)
				got := releaseTestIDs(jobs, slots)
				if ok != possible || ok && !slices.Equal(got, want) {
					t.Fatalf("seed=%d repeat=%d: feasible=%v victims=%v; oracle feasible=%v victims=%v", seed, repeat, ok, got, possible, want)
				}
				if ok {
					capacity := cpuAfterReleases(&projected, jobs, slots, 0)
					for _, j := range jobs {
						if j.ReleaseConfirmed {
							t.Fatal("selected zero-contribution job", j.ID)
						}
						capacity.FreeHostMemoryGB += hostNeed(q.s.Machines[j.Machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
					}
					for _, s := range slots {
						if !slotReservesMemory(s) {
							t.Fatal("selected zero-contribution slot", slotKey(s.Machine, s.Model))
						}
						model, _ := q.model(s.Model)
						capacity.FreeHostMemoryGB += hostNeed(q.s.Machines[s.Machine], model.MemoryGB, model.HostMemoryGB)
					}
					if !cpuAllocationFits(&capacity, demand.Needs.cores()) || demand.Needs.HostMemoryGB > 0 && !hostFits(&capacity, demand.Needs.MemoryGB, demand.Needs.HostMemoryGB) {
						t.Fatal("selected plan does not fit")
					}
				}
			}
		})
	}
	if confirmed == 0 || len(slotStates) != 6 {
		t.Fatal("missing release-confirmed or non-ready slot coverage", confirmed, slotStates)
	}
	t.Logf("seed=%d: %d hosts compared with brute force (%d feasible, %d confirmed releases, slots=%v), each planned three times", seed, cases, feasible, confirmed, slotStates)
}

func TestHostReleaseSearchCapAndGreedyFallback(t *testing.T) {
	for _, count := range []int{12, 13} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			victims := []releaseTestVictim{{"a", "experiment", 1, 0, false}, {"b", "experiment", 1, 0, false}, {"c", "experiment", 2, 0, false}}
			for i := len(victims); i < count; i++ {
				victims = append(victims, releaseTestVictim{fmt.Sprintf("d%02d", i), "experiment", 1, 0, false})
			}
			q, m := releaseTestHost(t, count+1, 64, victims)
			demand := spec("production")
			demand.Needs = ResourceNeeds{CPUCores: 2}
			jobs, slots, ok := q.hostReleasesExcept(m, 0, 0, demand, nil, nil)
			want := []string{"job/c"}
			if count > 12 {
				want = []string{"job/a", "job/b"} // Legacy greedy-plus-prune result.
			}
			if !ok || !slices.Equal(releaseTestIDs(jobs, slots), want) {
				t.Fatalf("%d candidates: feasible=%v victims=%v; want %v", count, ok, releaseTestIDs(jobs, slots), want)
			}
			capacity := cpuAfterReleases(m, jobs, slots, 0)
			if !cpuAllocationFits(&capacity, demand.Needs.cores()) {
				t.Fatal("fallback produced an infeasible plan")
			}
		})
	}
}
