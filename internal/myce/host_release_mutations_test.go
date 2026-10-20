package myce

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

// Round-6 reviewer seeded scenarios: N identical victims with exactly one victim differing in
// one field, optionally with started builds. Check soundness through the live ledger.
type releaseMutation struct {
	name string
	slot bool // base victims are slots instead of jobs
	f    func(q *Queue, odd string, others []string)
}

func releaseMutations() []releaseMutation {
	job := func(q *Queue, id string) *Job { return q.s.Jobs[id] }
	owner := func(q *Queue, id string) *Machine { return q.s.Machines["worker-"+id] }
	slot := func(q *Queue, id string) *ModelSlot { return q.s.ModelSlots[slotKey("worker-"+id, id)] }
	future := func(q *Queue) *time.Time { t := q.clock.Now().Add(time.Hour); return &t }
	past := func(q *Queue) *time.Time { t := q.clock.Now().Add(-time.Hour); return &t }
	return []releaseMutation{
		{"none", false, func(q *Queue, odd string, _ []string) {}},
		{"tier", false, func(q *Queue, odd string, _ []string) { job(q, odd).Spec.Priority = "deadline" }},
		{"cores", false, func(q *Queue, odd string, _ []string) { job(q, odd).Spec.Needs.CPUCores++ }},
		{"ram", false, func(q *Queue, odd string, _ []string) { job(q, odd).Spec.Needs.HostMemoryGB++ }},
		{"ram-small", false, func(q *Queue, odd string, _ []string) { job(q, odd).Spec.Needs.HostMemoryGB -= 0.5 }},
		{"unified-gpu", false, func(q *Queue, odd string, _ []string) {
			owner(q, odd).UnifiedMemory = true
			job(q, odd).Spec.Needs.MemoryGB = 1
		}},
		{"gpu-separate", false, func(q *Queue, odd string, _ []string) { job(q, odd).Spec.Needs.MemoryGB = 1 }},
		{"slack", false, func(q *Queue, odd string, _ []string) { owner(q, odd).MemoryCeilingSlackGB = 0.5 }},
		{"stopping", false, func(q *Queue, odd string, _ []string) { j := job(q, odd); j.State, j.StopAt = "stopping", future(q) }},
		{"stopping-expired", false, func(q *Queue, odd string, _ []string) { j := job(q, odd); j.State, j.StopAt = "stopping", past(q) }},
		{"confirmed", false, func(q *Queue, odd string, _ []string) { job(q, odd).ReleaseConfirmed = true }},
		{"stopping-confirmed", false, func(q *Queue, odd string, _ []string) {
			j := job(q, odd)
			j.State, j.StopAt, j.ReleaseConfirmed = "stopping", future(q), true
		}},
		{"protected", false, func(q *Queue, odd string, _ []string) {
			j := job(q, odd)
			now := q.clock.Now()
			j.PreemptionCount, j.StartedAt = 2, &now
		}},
		{"gone", false, func(q *Queue, odd string, _ []string) { owner(q, odd).Gone = true }},
		{"other-host", false, func(q *Queue, odd string, _ []string) { owner(q, odd).HostID = "elsewhere" }},
		{"resumable", false, func(q *Queue, odd string, _ []string) { job(q, odd).Spec.Resumable = true }},
		{"build-on-odd", false, func(q *Queue, odd string, _ []string) { releaseHandBackBuild(q, "worker-"+odd) }},
		{"build-on-other", false, func(q *Queue, odd string, others []string) { releaseHandBackBuild(q, "worker-"+others[0]) }},
		{"build-on-all", false, func(q *Queue, odd string, others []string) {
			releaseHandBackBuild(q, "worker-"+odd)
			for _, o := range others {
				releaseHandBackBuild(q, "worker-"+o)
			}
		}},
		{"build-on-odd-covered", false, func(q *Queue, odd string, _ []string) {
			releaseHandBackBuild(q, "worker-"+odd)
			s := spec("production")
			s.Needs = ResourceNeeds{CPUCores: 1}
			m := owner(q, odd)
			q.s.Jobs["cover-"+odd] = &Job{ID: "cover-" + odd, Machine: m.ID, Session: m.Session, Spec: s, State: "running"}
		}},
		{"build-on-odd-unstarted", false, func(q *Queue, odd string, _ []string) {
			releaseHandBackBuild(q, "worker-"+odd)
			p := q.s.PreparationPlans["build-worker-"+odd]
			p.Started = false
			q.s.PreparationPlans["build-worker-"+odd] = p
		}},
		{"share-worker-with-build", false, func(q *Queue, odd string, others []string) {
			// odd moves onto others[0]'s worker; that worker has a build.
			m := q.s.Machines["worker-"+others[0]]
			j := job(q, odd)
			j.Machine, j.Session = m.ID, m.Session
			releaseHandBackBuild(q, m.ID)
		}},
		{"share-worker-no-build", false, func(q *Queue, odd string, others []string) {
			m := q.s.Machines["worker-"+others[0]]
			j := job(q, odd)
			j.Machine, j.Session = m.ID, m.Session
		}},
		{"receipt-stale", false, func(q *Queue, odd string, _ []string) {
			j := job(q, odd)
			q.s.HostReleases = map[string]HostRelease{"r": {Host: "shared", Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt + 7, CPUCores: 1, HostMemoryGB: 1}}
		}},
		{"receipt-matching", false, func(q *Queue, odd string, _ []string) {
			j := job(q, odd)
			q.s.HostReleases = map[string]HostRelease{"r": {Host: "shared", Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt, CPUCores: j.Spec.Needs.cores(), HostMemoryGB: j.Spec.Needs.HostMemoryGB}}
		}},
		// Slot-based classes.
		{"slot-none", true, func(q *Queue, odd string, _ []string) {}},
		{"slot-tier", true, func(q *Queue, odd string, _ []string) { slot(q, odd).Priority = "deadline" }},
		{"slot-project-tier", true, func(q *Queue, odd string, _ []string) { slot(q, odd).Project = "boost" }},
		{"slot-ram", true, func(q *Queue, odd string, _ []string) {
			for i := range q.models.Models {
				if q.models.Models[i].Name == odd {
					q.models.Models[i].HostMemoryGB++
				}
			}
		}},
		{"slot-unified", true, func(q *Queue, odd string, _ []string) { owner(q, odd).UnifiedMemory = true }},
		{"slot-loading", true, func(q *Queue, odd string, _ []string) { slot(q, odd).State = "loading" }},
		{"slot-draining", true, func(q *Queue, odd string, _ []string) { slot(q, odd).State = "draining" }},
		{"slot-unloading", true, func(q *Queue, odd string, _ []string) { slot(q, odd).State = "unloading" }},
		{"slot-unload-failed", true, func(q *Queue, odd string, _ []string) { slot(q, odd).State = "unload_failed" }},
		{"slot-failed-retry", true, func(q *Queue, odd string, _ []string) { s := slot(q, odd); s.State, s.RetryReserved = "failed", true }},
		{"slot-inflight", true, func(q *Queue, odd string, _ []string) { slot(q, odd).InFlight = 1 }},
		{"slot-build", true, func(q *Queue, odd string, _ []string) { releaseHandBackBuild(q, "worker-"+odd) }},
		{"slot-protected", true, func(q *Queue, odd string, _ []string) {
			s := slot(q, odd)
			now := q.clock.Now()
			s.PreemptionCount, s.RunSince = 2, &now
		}},
	}
}

func TestHostReleaseNearIdenticalProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	cases, feasible, nonOptimal := 0, 0, 0
	perMut := map[string]int{}
	for _, mut := range releaseMutations() {
		for _, n := range []int{3, 5, 8, 12} {
			for _, cores := range []int{1, 2} {
				for _, policy := range []bool{false, true} {
					for rep := 0; rep < 3; rep++ {
						if n == 12 && (policy || rep == 1) {
							continue
						}
						victims := make([]releaseTestVictim, n)
						for i := range victims {
							victims[i] = releaseTestVictim{fmt.Sprintf("v%02d", i), "experiment", cores, 4, mut.slot}
						}
						oddIdx := rng.Intn(n)
						odd := victims[oddIdx].id
						var others []string
						for _, v := range victims {
							if v.id != odd {
								others = append(others, v.id)
							}
						}
						rng.Shuffle(len(others), func(i, j int) { others[i], others[j] = others[j], others[i] })
						q, m := releaseTestHost(t, 64, 1024, victims)
						if policy {
							q.s.PriorityPolicy = &PriorityPolicyRecord{Policy: PriorityPolicy{Version: 1, Tiers: []PriorityTier{
								{"production", []string{"*/production"}}, {"boost", []string{"boost/*"}}, {"deadline", []string{"*/deadline"}}, {"other", []string{"*/*"}},
							}}}
						}
						for _, j := range q.s.Jobs {
							j.Attempt = 1
						}
						mut.f(q, odd, others)
						// Random extra build on some base victim's worker.
						if rep == 2 && len(others) > 1 {
							releaseHandBackBuild(q, "worker-"+others[1])
						}
						q.refreshHostMemory()
						avail := m.HostMemoryInUseGB + float64(rng.Intn(3))
						for _, o := range q.s.Machines {
							o.HostAvailableMemoryGB = avail
							o.CapabilityProfile.LogicalCPUs = max(1, m.CPUInUseCores+rng.Intn(2))
						}
						q.refreshHostMemory()
						m = q.s.Machines["target"]
						nd := 14
						if n == 12 {
							nd = 3
						}
						for d := 0; d < nd; d++ {
							dc := []int{1, 2, 3, 5, 8, 13}[rng.Intn(6)]
							dr := []float64{0, 3, 4.5, 9, 17, 33}[rng.Intn(6)]
							prio := []string{"production", "deadline"}[rng.Intn(2)]
							demand := spec(prio)
							demand.Needs = ResourceNeeds{CPUCores: dc, HostMemoryGB: dr}
							want, wantOK := bruteForceHostReleases(q, m, demand, nil, nil)
							jobs, slots, ok := q.hostReleasesExcept(m, 0, dr, demand, nil, nil)
							got := releaseTestIDs(jobs, slots)
							cases++
							perMut[mut.name]++
							if wantOK {
								feasible++
							}
							if ok != wantOK {
								t.Fatalf("%s: planner feasible=%v oracle=%v", mut.name, ok, wantOK)
							}
							if ok && !slices.Equal(got, want) {
								nonOptimal++ // Information only.
							}
							assertReleaseLedgerProperties(t, q, m, demand, jobs, slots, ok, nil, nil)
							for repeat := 0; repeat < 2; repeat++ {
								js, ss, again := q.hostReleasesExcept(m, 0, dr, demand, nil, nil)
								if again != ok || !slices.Equal(got, releaseTestIDs(js, ss)) {
									t.Fatalf("%s: nondeterministic plan", mut.name)
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("seed=6: near-identical cases=%d feasible=%d non-optimal=%d (%.2f%% of feasible) mutations=%d; information only", cases, feasible, nonOptimal, 100*float64(nonOptimal)/float64(feasible), len(perMut))
}
