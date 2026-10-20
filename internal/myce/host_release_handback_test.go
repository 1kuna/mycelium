package myce

import (
	"slices"
	"testing"
	"time"
)

// Round-6 reviewer regressions: build hand-back variants, verified against the live ledger.
func releaseHandBackLedger(t *testing.T, q *Queue, target *Machine, jobs []*Job, slots []*ModelSlot, stopTargetBuilds bool, cores int) (bool, int) {
	t.Helper()
	saveJobs := map[*Job]string{}
	for _, j := range jobs {
		saveJobs[j] = j.State
		j.State = "succeeded"
	}
	saveSlots := map[string]*ModelSlot{}
	for _, s := range slots {
		k := slotKey(s.Machine, s.Model)
		saveSlots[k] = s
		delete(q.s.ModelSlots, k)
	}
	saveBuilds := map[string]HostLaunch{}
	if stopTargetBuilds {
		for k, l := range q.s.PreparationPlans {
			if l.Machine == target.ID && !q.preparationShared(target, &Job{Spec: Spec{}}, &l) {
				saveBuilds[k] = l
				delete(q.s.PreparationPlans, k)
			}
		}
	}
	q.refreshHostMemory()
	m := q.s.Machines[target.ID]
	fits, inUse := cpuAllocationFits(m, cores), m.CPUInUseCores
	for j, s := range saveJobs {
		j.State = s
	}
	for k, s := range saveSlots {
		q.s.ModelSlots[k] = s
	}
	for k, l := range saveBuilds {
		q.s.PreparationPlans[k] = l
	}
	q.refreshHostMemory()
	return fits, inUse
}

func releaseHandBackJob(q *Queue, id, worker, prio string, cores int, stopping bool) *Job {
	m := q.s.Machines[worker]
	s := spec(prio)
	s.Needs = ResourceNeeds{CPUCores: cores}
	j := &Job{ID: id, Machine: m.ID, Session: m.Session, Spec: s, State: "running", Attempt: 1}
	if stopping {
		until := q.clock.Now().Add(time.Hour)
		j.State, j.StopAt = "stopping", &until
	}
	q.s.Jobs[id] = j
	return j
}

func releaseHandBackWorker(q *Queue, id string, cores int) *Machine {
	m := cpuMachine(id, "shared", "fake", cores, 64)
	q.s.Machines[id] = &m
	return &m
}

func TestHostReleaseReviewerHandBack(t *testing.T) {
	type result struct {
		ok  bool
		ids []string
	}
	direct := func(t *testing.T, q *Queue, cores int) (result, bool, int) {
		q.refreshHostMemory()
		m := q.s.Machines["target"]
		d := spec("production")
		d.Needs = ResourceNeeds{CPUCores: cores}
		jobs, slots, ok := q.hostReleasesExcept(m, 0, 0, d, nil, nil)
		fits, used := releaseHandBackLedger(t, q, m, jobs, slots, false, cores)
		return result{ok, releaseTestIDs(jobs, slots)}, fits, used
	}
	t.Run("zero-contribution-stopping-on-build-worker", func(t *testing.T) {
		q, _ := releaseTestHost(t, 3, 64, nil)
		for _, w := range []string{"worker-a", "worker-b", "worker-c"} {
			releaseHandBackWorker(q, w, 3)
		}
		releaseHandBackJob(q, "a", "worker-a", "experiment", 1, true)
		releaseHandBackJob(q, "b", "worker-b", "experiment", 1, false)
		releaseHandBackJob(q, "c", "worker-c", "experiment", 1, false)
		releaseHandBackBuild(q, "worker-a")
		r, fits, used := direct(t, q, 1)
		t.Logf("planner %v fits=%v inUseAfter=%d (want [job/b], since stopping a frees nothing)", r, fits, used)
		if !slices.Equal(r.ids, []string{"job/b"}) {
			t.Errorf("zero-contribution victim included: %v", r.ids)
		}
	})
	t.Run("several-builds", func(t *testing.T) {
		q, _ := releaseTestHost(t, 5, 64, nil)
		for _, w := range []string{"a", "b", "c", "d", "e"} {
			releaseHandBackWorker(q, "worker-"+w, 5)
			releaseHandBackJob(q, w, "worker-"+w, "experiment", 1, false)
		}
		for _, w := range []string{"a", "b", "c"} {
			releaseHandBackBuild(q, "worker-"+w)
		}
		r, fits, used := direct(t, q, 2)
		t.Logf("planner %v fits=%v inUseAfter=%d", r, fits, used)
		if !r.ok || !fits || !slices.Equal(r.ids, []string{"job/d", "job/e"}) {
			t.Errorf("want [job/d job/e] fitting; got %v fits=%v", r, fits)
		}
	})
	t.Run("all-workers-building-infeasible", func(t *testing.T) {
		q, _ := releaseTestHost(t, 4, 64, nil)
		for _, w := range []string{"a", "b", "c", "d"} {
			releaseHandBackWorker(q, "worker-"+w, 4)
			releaseHandBackJob(q, w, "worker-"+w, "experiment", 1, false)
			releaseHandBackBuild(q, "worker-"+w)
		}
		r, fits, used := direct(t, q, 2)
		t.Logf("planner %v fits=%v inUseAfter=%d", r, fits, used)
		if r.ok {
			t.Errorf("stops cannot free cores, want infeasible; got %v (fits=%v)", r, fits)
		}
	})
	t.Run("two-jobs-per-build-worker", func(t *testing.T) {
		for _, tc := range []struct {
			cores int
			want  []string
			ok    bool
		}{{1, []string{"job/a1"}, true}, {2, []string{"job/a1", "job/b1"}, true}, {3, nil, false}} {
			q, _ := releaseTestHost(t, 4, 64, nil)
			for _, w := range []string{"a", "b"} {
				releaseHandBackWorker(q, "worker-"+w, 4)
				releaseHandBackJob(q, w+"1", "worker-"+w, "experiment", 1, false)
				releaseHandBackJob(q, w+"2", "worker-"+w, "experiment", 1, false)
				releaseHandBackBuild(q, "worker-"+w)
			}
			r, fits, used := direct(t, q, tc.cores)
			t.Logf("need %d: planner %v fits=%v inUseAfter=%d", tc.cores, r, fits, used)
			if r.ok != tc.ok || tc.ok && (!fits || !slices.Equal(r.ids, tc.want)) {
				t.Errorf("need %d want %v %v got %v fits=%v", tc.cores, tc.ok, tc.want, r, fits)
			}
		}
	})
	// roomPlan level: build on the winning worker.
	plan := func(t *testing.T, q *Queue, cores int, env string) (*placementPlan, bool, int) {
		q.refreshHostMemory()
		m := q.s.Machines["target"]
		d := &Job{ID: "demand", State: "queued", Spec: spec("production")}
		d.Spec.Needs = ResourceNeeds{CPUCores: cores}
		if env != "" {
			d.EnvironmentRecipes = map[string]EnvironmentRecipe{m.GPU: {Key: env}}
		}
		p := q.roomPlan(d, m, true)
		if p == nil {
			return nil, false, 0
		}
		// Ledger after: chosen releases plus stopped non-shared builds on target.
		saved := map[string]HostLaunch{}
		for k, l := range q.s.PreparationPlans {
			if l.Machine == m.ID && !q.preparationShared(m, d, &l) {
				saved[k] = l
				delete(q.s.PreparationPlans, k)
			}
		}
		fits, used := releaseHandBackLedger(t, q, m, p.jobs, p.slots, false, cores)
		for k, l := range saved {
			q.s.PreparationPlans[k] = l
		}
		return p, fits, used
	}
	buildOnTarget := func(q *Queue, key string) {
		m := q.s.Machines["target"]
		bs := spec("experiment")
		if q.s.PreparationPlans == nil {
			q.s.PreparationPlans = map[string]HostLaunch{}
		}
		q.s.PreparationPlans["tb-"+key] = HostLaunch{Machine: m.ID, Session: m.Session, Kind: "environment", Key: key, Attempt: "tb-" + key, Started: true, Spec: &bs}
	}
	t.Run("winner-build-with-own-job", func(t *testing.T) {
		q, _ := releaseTestHost(t, 3, 64, nil)
		for _, w := range []string{"x", "y"} {
			releaseHandBackWorker(q, "worker-"+w, 3)
			releaseHandBackJob(q, w, "worker-"+w, "experiment", 1, false)
		}
		releaseHandBackJob(q, "own", "target", "experiment", 1, false)
		buildOnTarget(q, "other")
		p, fits, used := plan(t, q, 2, "")
		if p == nil {
			t.Fatal("no plan")
		}
		t.Logf("plan %v prepCores=%d stops=%d fits=%v inUseAfter=%d", releaseTestIDs(p.jobs, p.slots), p.preparationCores, p.stops, fits, used)
		if !fits || !slices.Equal(releaseTestIDs(p.jobs, p.slots), []string{"job/own", "job/x"}) {
			t.Errorf("want [job/own job/x] fitting")
		}
	})
	t.Run("winner-build-only", func(t *testing.T) {
		q, _ := releaseTestHost(t, 3, 64, nil)
		for _, w := range []string{"x", "y"} {
			releaseHandBackWorker(q, "worker-"+w, 3)
			releaseHandBackJob(q, w, "worker-"+w, "experiment", 1, false)
		}
		buildOnTarget(q, "other")
		p, fits, used := plan(t, q, 2, "")
		if p == nil {
			t.Fatal("no plan")
		}
		t.Logf("plan %v prepCores=%d stops=%d fits=%v inUseAfter=%d", releaseTestIDs(p.jobs, p.slots), p.preparationCores, p.stops, fits, used)
		if !fits || !slices.Equal(releaseTestIDs(p.jobs, p.slots), []string{"job/x"}) {
			t.Errorf("want [job/x] fitting")
		}
	})
	t.Run("winner-build-and-victim-build", func(t *testing.T) {
		q, _ := releaseTestHost(t, 4, 64, nil)
		for _, w := range []string{"x", "y", "z"} {
			releaseHandBackWorker(q, "worker-"+w, 4)
			releaseHandBackJob(q, w, "worker-"+w, "experiment", 1, false)
		}
		releaseHandBackBuild(q, "worker-x")
		buildOnTarget(q, "other")
		// in use: target build 1 + x,y,z = 4. Stop target build -> 3. Need 2: one more,
		// not x (its build inherits the core) -> [job/y].
		p, fits, used := plan(t, q, 2, "")
		if p == nil {
			t.Fatal("no plan")
		}
		t.Logf("plan %v prepCores=%d fits=%v inUseAfter=%d", releaseTestIDs(p.jobs, p.slots), p.preparationCores, fits, used)
		if !fits || !slices.Equal(releaseTestIDs(p.jobs, p.slots), []string{"job/y"}) {
			t.Errorf("want [job/y] fitting")
		}
	})
}

func releaseHandBackBuild(q *Queue, worker string) {
	owner := q.s.Machines[worker]
	if q.s.PreparationPlans == nil {
		q.s.PreparationPlans = map[string]HostLaunch{}
	}
	q.s.PreparationPlans["build-"+worker] = HostLaunch{Machine: owner.ID, Session: owner.Session, Kind: "environment", Key: "k-" + worker, Attempt: "build-" + worker, Started: true}
}
