package myce

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"
)

// Reviewer oracle (#100): feasibility and freed capacity come from the live ledger
// (refreshHostMemory/refreshHostCPU after actually removing the victims),
// not from the planner's arithmetic. Zero-contribution victims are found by
// single-victim removal against that ledger.
type ledgerVictim struct {
	id          string
	job         *Job
	slot        *ModelSlot
	tier, stops int
}

func releaseLedgerAfter(q *Queue, m *Machine, set []ledgerVictim) (freeRAM, inUseRAM float64, inUseCores, freeCores int, fitsCores func(int) bool) {
	saveJobs := map[*Job]string{}
	saveSlots := map[string]*ModelSlot{}
	saveReceipts := map[string]HostRelease{}
	for _, v := range set {
		if v.job != nil {
			for k, r := range q.s.HostReleases {
				if r.Job == v.job.ID && r.Machine == v.job.Machine && r.Session == v.job.Session && r.Attempt == v.job.Attempt {
					saveReceipts[k] = r
					delete(q.s.HostReleases, k)
				}
			}
			saveJobs[v.job] = v.job.State
			v.job.State = "succeeded"
		} else {
			k := slotKey(v.slot.Machine, v.slot.Model)
			saveSlots[k] = v.slot
			delete(q.s.ModelSlots, k)
		}
	}
	q.refreshHostMemory()
	freeRAM, inUseRAM, inUseCores, freeCores = m.FreeHostMemoryGB, m.HostMemoryInUseGB, m.CPUInUseCores, m.FreeCPUCores
	copy := *m
	fitsCores = func(c int) bool { return cpuAllocationFits(&copy, c) }
	for j, s := range saveJobs {
		j.State = s
	}
	for k, s := range saveSlots {
		q.s.ModelSlots[k] = s
	}
	for k, r := range saveReceipts {
		q.s.HostReleases[k] = r
	}
	q.refreshHostMemory()
	return
}

func releaseLedgerOracle(q *Queue, m *Machine, demand Spec, gpu, host float64) ([]string, bool, int) {
	var all []ledgerVictim
	for _, j := range q.s.Jobs {
		owner := q.s.Machines[j.Machine]
		if owner == nil || owner.Gone || hostID(owner) != hostID(m) || (j.State != "running" && j.State != "stopping") || !q.batchPreemptible(j, demand) {
			continue
		}
		v := ledgerVictim{id: "job/" + j.ID, job: j, tier: q.priorityMatch(j.Spec).Index}
		if j.State == "running" {
			v.stops = 1
		}
		all = append(all, v)
	}
	for _, s := range q.s.ModelSlots {
		owner := q.s.Machines[s.Machine]
		if owner == nil || owner.Gone || hostID(owner) != hostID(m) || !q.slotReclaimableFor(s, demand) {
			continue
		}
		v := ledgerVictim{id: "slot/" + slotKey(s.Machine, s.Model), slot: s, tier: q.priorityMatch(q.slotSpec(s)).Index}
		if s.State != "draining" && s.State != "unloading" {
			v.stops = 1
		}
		all = append(all, v)
	}
	_, baseRAM, baseCores, _, _ := releaseLedgerAfter(q, m, nil)
	var cands []ledgerVictim
	zero := 0
	for _, v := range all {
		_, r, c, _, _ := releaseLedgerAfter(q, m, []ledgerVictim{v})
		if r == baseRAM && c == baseCores {
			zero++
			continue
		}
		cands = append(cands, v)
	}
	slices.SortFunc(cands, func(a, b ledgerVictim) int { return strings.Compare(a.id, b.id) })
	baseFree := m.FreeHostMemoryGB
	need := 0.0
	if host > 0 {
		need = hostNeed(m, gpu, host)
	}
	ramDeficit := max(0, need-baseFree)
	coreDeficit := max(0, baseCores+demand.Needs.cores()-m.EffectiveCPUCores)
	ramCap := m.HostAvailableMemoryGB
	if m.hostBudgetGB > 0 {
		ramCap = min(ramCap, m.hostBudgetGB)
	}
	var bestIDs []string
	var bestCounts []int
	bestStops, bestExcess := 0, 0.0
	for mask := 0; mask < 1<<len(cands); mask++ {
		var set []ledgerVictim
		counts := make([]int, len(q.effectivePriorityPolicy().Tiers))
		stops := 0
		ids := []string{}
		for i, v := range cands {
			if mask&(1<<i) != 0 {
				set = append(set, v)
				counts[v.tier] += v.stops
				stops += v.stops
				ids = append(ids, v.id)
			}
		}
		free, inUse, cores, _, fits := releaseLedgerAfter(q, m, set)
		if host > 0 && free < need || !fits(demand.Needs.cores()) {
			continue
		}
		freedRAM := baseRAM - inUse
		freedCores := baseCores - cores
		excess := max(0, freedRAM-ramDeficit)/max(1, ramCap) + float64(max(0, freedCores-coreDeficit))/float64(max(1, m.EffectiveCPUCores))
		order := slices.Compare(counts, bestCounts)
		if bestCounts == nil || order < 0 || order == 0 && (stops < bestStops || stops == bestStops && (excess < bestExcess || excess == bestExcess && slices.Compare(ids, bestIDs) < 0)) {
			bestCounts, bestIDs, bestStops, bestExcess = counts, ids, stops, excess
		}
	}
	return bestIDs, bestCounts != nil, zero
}

func TestHostReleaseReviewerSeededProperties(t *testing.T) {
	const seed, cases = 20261020, 600
	rng := rand.New(rand.NewSource(seed))
	feasible, nonOptimal, confirmed, zeroTotal, ties := 0, 0, 0, 0, 0
	slotStates := map[string]int{}
	for n := 0; n < cases; n++ {
		count := 1 + rng.Intn(10)
		if n%25 == 0 {
			count = 12
		}
		prios := []string{"production", "deadline", "experiment"}
		tieMode := n%4 == 0 // identical victims to force ID tie-breaks
		victims := make([]releaseTestVictim, count)
		for i := range victims {
			if tieMode {
				victims[i] = releaseTestVictim{fmt.Sprintf("v%02d", i), "experiment", 2, 4, rng.Intn(4) == 0}
			} else {
				victims[i] = releaseTestVictim{fmt.Sprintf("v%02d", i), prios[rng.Intn(3)], 1 + rng.Intn(4), float64(rng.Intn(6)), rng.Intn(3) == 0}
			}
		}
		q, m := releaseTestHost(t, 2+rng.Intn(14), 64, victims)
		if n%2 == 0 {
			q.s.PriorityPolicy = &PriorityPolicyRecord{Policy: PriorityPolicy{Version: 1, Tiers: []PriorityTier{
				{"production", []string{"*/production"}}, {"boost", []string{"boost/*"}}, {"deadline", []string{"*/deadline"}}, {"other", []string{"*/*"}},
			}}}
		}
		unified := rng.Intn(3) == 0
		slack := float64(rng.Intn(3)) / 8
		for _, owner := range q.s.Machines {
			owner.UnifiedMemory = unified
			owner.MemoryCeilingSlackGB = slack
		}
		for _, v := range victims {
			owner := q.s.Machines["worker-"+v.id]
			if s := q.s.ModelSlots[slotKey(owner.ID, v.id)]; s != nil {
				s.State = []string{"ready", "loading", "draining", "unloading", "unload_failed", "failed"}[rng.Intn(6)]
				s.RetryReserved = rng.Intn(2) == 0
				slotStates[s.State]++
				s.InFlight = rng.Intn(2)
				if rng.Intn(4) == 0 {
					s.Project = "boost"
				}
				if !tieMode {
					s.Priority = prios[rng.Intn(3)]
				}
			} else {
				j := q.s.Jobs[v.id]
				j.Spec.Needs.MemoryGB = float64(rng.Intn(3))
				switch rng.Intn(5) {
				case 0:
					until := q.clock.Now().Add(time.Minute)
					j.State, j.StopAt = "stopping", &until
				case 1:
					until := q.clock.Now().Add(time.Minute)
					j.State, j.StopAt, j.ReleaseConfirmed = "stopping", &until, true
					confirmed++
				case 2:
					j.ReleaseConfirmed = rng.Intn(3) == 0 // running, output upload after release
					if j.ReleaseConfirmed {
						confirmed++
					}
				}
				if rng.Intn(8) == 0 {
					now := q.clock.Now()
					j.PreemptionCount, j.StartedAt = 2, &now
				}
			}
		}
		// Size host RAM so the current reservation fits (no debt) but is tight.
		q.refreshHostMemory()
		avail := m.HostMemoryInUseGB + float64(rng.Intn(4))
		for _, owner := range q.s.Machines {
			owner.HostAvailableMemoryGB = avail
		}
		q.refreshHostMemory()
		if m.CPUInUseCores > m.EffectiveCPUCores {
			for _, owner := range q.s.Machines {
				owner.CapabilityProfile.LogicalCPUs = m.CPUInUseCores
			}
			q.refreshHostMemory()
		}
		m = q.s.Machines["target"]
		demand := spec([]string{"production", "production", "deadline"}[rng.Intn(3)])
		demand.Needs = ResourceNeeds{CPUCores: 1 + rng.Intn(6), HostMemoryGB: float64(rng.Intn(10)), MemoryGB: float64(rng.Intn(3))}
		gpu, host := demand.Needs.MemoryGB, demand.Needs.HostMemoryGB
		want, wantOK, zero := releaseLedgerOracle(q, m, demand, gpu, host)
		zeroTotal += zero
		jobs, slots, ok := q.hostReleasesExcept(m, gpu, host, demand, nil, nil)
		got := releaseTestIDs(jobs, slots)
		if wantOK {
			feasible++
		}
		if tieMode {
			ties++
		}
		if ok != wantOK {
			t.Fatalf("case %d: feasible=%v oracle=%v", n, ok, wantOK)
		}
		if ok && !slices.Equal(got, want) {
			nonOptimal++ // Information only.
		}
		assertReleaseLedgerProperties(t, q, m, demand, jobs, slots, ok, nil, nil)
		for repeat := 0; repeat < 2; repeat++ {
			js, ss, again := q.hostReleasesExcept(m, gpu, host, demand, nil, nil)
			if again != ok || !slices.Equal(releaseTestIDs(js, ss), got) {
				t.Fatalf("case %d: nondeterministic plan", n)
			}
		}
	}
	t.Logf("non-optimal=%d/%d feasible (%.2f%%); information only", nonOptimal, feasible, 100*float64(nonOptimal)/float64(feasible))
	t.Logf("seed=%d cases=%d feasible=%d confirmed=%d zero-contribution-candidates=%d tieCases=%d slots=%v", seed, cases, feasible, confirmed, zeroTotal, ties, slotStates)
}

// Greedy fallback (13..16 victims): every plan fits by the ledger, contains no
// zero-contribution victim, and feasibility agrees with the oracle's verdict.
func TestHostReleaseLargeHostProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(777))
	bad := 0
	feasible := 0
	for n := 0; n < 120; n++ {
		count := 13 + rng.Intn(4)
		victims := make([]releaseTestVictim, count)
		for i := range victims {
			victims[i] = releaseTestVictim{fmt.Sprintf("v%02d", i), []string{"production", "deadline", "experiment"}[rng.Intn(3)], 1 + rng.Intn(3), float64(rng.Intn(5)), rng.Intn(3) == 0}
		}
		q, m := releaseTestHost(t, 4+rng.Intn(30), 64, victims)
		for _, v := range victims {
			if s := q.s.ModelSlots[slotKey("worker-"+v.id, v.id)]; s != nil {
				s.State = []string{"ready", "draining", "unloading", "failed"}[rng.Intn(4)]
				s.RetryReserved = rng.Intn(2) == 0
			} else if rng.Intn(3) == 0 {
				j := q.s.Jobs[v.id]
				until := q.clock.Now().Add(time.Minute)
				j.State, j.StopAt, j.ReleaseConfirmed = "stopping", &until, rng.Intn(2) == 0
			}
		}
		q.refreshHostMemory()
		avail := m.HostMemoryInUseGB + float64(rng.Intn(3))
		for _, o := range q.s.Machines {
			o.HostAvailableMemoryGB = avail
		}
		q.refreshHostMemory()
		if m.CPUInUseCores > m.EffectiveCPUCores {
			for _, o := range q.s.Machines {
				o.CapabilityProfile.LogicalCPUs = m.CPUInUseCores
			}
			q.refreshHostMemory()
		}
		demand := spec("production")
		demand.Needs = ResourceNeeds{CPUCores: 1 + rng.Intn(8), HostMemoryGB: float64(rng.Intn(12))}
		jobs, slots, ok := q.hostReleasesExcept(m, 0, demand.Needs.HostMemoryGB, demand, nil, nil)
		assertReleaseLedgerProperties(t, q, m, demand, jobs, slots, ok, nil, nil)
		for repeat := 0; repeat < 2; repeat++ {
			js, ss, again := q.hostReleasesExcept(m, 0, demand.Needs.HostMemoryGB, demand, nil, nil)
			if again != ok || !slices.Equal(releaseTestIDs(js, ss), releaseTestIDs(jobs, slots)) {
				t.Fatalf("case %d: nondeterministic plan", n)
			}
		}
		var set []ledgerVictim
		for _, j := range jobs {
			if j.ReleaseConfirmed {
				t.Errorf("case %d greedy chose release-confirmed %s", n, j.ID)
				bad++
			}
			set = append(set, ledgerVictim{job: j})
		}
		for _, s := range slots {
			if !slotReservesMemory(s) {
				t.Errorf("case %d greedy chose non-reserving slot %s", n, s.Model)
				bad++
			}
			set = append(set, ledgerVictim{slot: s})
		}
		// Oracle verdict on full eligible set via the ledger.
		var all []ledgerVictim
		for _, j := range q.s.Jobs {
			if (j.State == "running" || j.State == "stopping") && q.batchPreemptible(j, demand) {
				all = append(all, ledgerVictim{job: j})
			}
		}
		for _, s := range q.s.ModelSlots {
			if q.slotReclaimableFor(s, demand) {
				all = append(all, ledgerVictim{slot: s})
			}
		}
		need := 0.0
		if demand.Needs.HostMemoryGB > 0 {
			need = hostNeed(m, 0, demand.Needs.HostMemoryGB)
		}
		freeAll, _, _, _, fitsAll := releaseLedgerAfter(q, m, all)
		wantOK := freeAll >= need && fitsAll(demand.Needs.cores())
		if ok {
			feasible++
			free, _, _, _, fits := releaseLedgerAfter(q, m, set)
			if free < need || !fits(demand.Needs.cores()) {
				t.Errorf("case %d greedy plan does not fit ledger", n)
				bad++
			}
		}
		if ok != wantOK {
			t.Errorf("case %d greedy feasible=%v ledger=%v", n, ok, wantOK)
			bad++
		}
	}
	t.Logf("greedy cases=120 feasible=%d bad=%d", feasible, bad)
}

// Check by actually removing releases and refreshing the live ledger, as the
// reviewer does, rather than trusting planner or cpuAfterReleases arithmetic.
func assertReleaseLedgerProperties(t *testing.T, q *Queue, m *Machine, demand Spec, jobs []*Job, slots []*ModelSlot, ok bool, mandatoryJobs []*Job, mandatorySlots []*ModelSlot) {
	t.Helper()
	m = q.s.Machines[m.ID]
	var mandatory, chosen, all []ledgerVictim
	for _, j := range mandatoryJobs {
		mandatory = append(mandatory, ledgerVictim{job: j})
	}
	for _, s := range mandatorySlots {
		mandatory = append(mandatory, ledgerVictim{slot: s})
	}
	for _, j := range jobs {
		chosen = append(chosen, ledgerVictim{job: j})
	}
	for _, s := range slots {
		chosen = append(chosen, ledgerVictim{slot: s})
	}
	_, baseRAM, baseCores, _, _ := releaseLedgerAfter(q, m, mandatory)
	for _, v := range chosen {
		_, ram, cores, _, _ := releaseLedgerAfter(q, m, append(slices.Clone(mandatory), v))
		if ram == baseRAM && cores == baseCores {
			t.Fatalf("zero-contribution victim in %v", releaseTestIDs(jobs, slots))
		}
	}
	need := 0.0
	if demand.Needs.HostMemoryGB > 0 {
		need = hostNeed(m, demand.Needs.MemoryGB, demand.Needs.HostMemoryGB)
	}
	if ok {
		free, _, _, _, fits := releaseLedgerAfter(q, m, append(slices.Clone(mandatory), chosen...))
		if free < need || !fits(demand.Needs.cores()) {
			t.Fatalf("plan %v does not fit the live ledger", releaseTestIDs(jobs, slots))
		}
	}
	for _, j := range q.s.Jobs {
		owner := q.s.Machines[j.Machine]
		if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") && q.batchPreemptible(j, demand) && !slices.Contains(mandatoryJobs, j) {
			all = append(all, ledgerVictim{job: j})
		}
	}
	for _, s := range q.s.ModelSlots {
		owner := q.s.Machines[s.Machine]
		if owner != nil && !owner.Gone && hostID(owner) == hostID(m) && slotReservesMemory(s) && q.slotReclaimableFor(s, demand) && !slices.Contains(mandatorySlots, s) {
			all = append(all, ledgerVictim{slot: s})
		}
	}
	freeAll, _, _, _, fitsAll := releaseLedgerAfter(q, m, append(slices.Clone(mandatory), all...))
	if possible := freeAll >= need && fitsAll(demand.Needs.cores()); ok != possible {
		t.Fatalf("planner feasible=%v, live ledger feasible=%v", ok, possible)
	}
}
