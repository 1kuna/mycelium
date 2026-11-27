package myce

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

func measuredCPUWorker(id string, cores int, ram float64) Machine {
	m := guardedCPUMachine(id, id, "cpu", cores, ram)
	m.Devices = []MachineDevice{}
	m.CPUExtensionFingerprint = "runtime-v1"
	m.HostMemoryBudgetGB = ram
	return m
}

func measuredCPUPull(t *testing.T, q *Queue, m Machine, active []*Job) *Job {
	t.Helper()
	c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
	for _, j := range active {
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true})
	}
	reply, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range reply.Attempts {
		if a.Job != nil && a.Job.StartedAt == nil {
			if err := q.Started(result(a.Job, true)); err != nil {
				t.Fatal(err)
			}
			return a.Job
		}
	}
	return nil
}

func replayMeasuredCPUBacklog(t *testing.T, measured bool) (time.Duration, [3]int) {
	t.Helper()
	q, clock := fixture(t)
	// The fastest host has two slots and is the most valuable host by
	// inventory. Each slower host can fit the whole small backlog at once.
	workers := []Machine{
		measuredCPUWorker("slow-a", 16, 24),
		measuredCPUWorker("slow-b", 16, 32),
		measuredCPUWorker("fast", 16, 64),
	}
	durations := []time.Duration{150 * time.Second, 210 * time.Second, 30 * time.Second}
	s := cpuSpec(1, 2)
	// Model a two-slot owner budget with a fit vector that leaves the fastest
	// host larger than the other two for the legacy least-valuable ordering.
	workers[2].CPUBudgetCores = 2
	for i, m := range workers {
		if measured {
			teachDuration(t, q, clock, m, s, durations[i])
		} else {
			check(t, q, m, nil)
		}
	}
	for _, m := range workers {
		check(t, q, m, nil)
	}
	const pieces = 6
	for i := range pieces {
		s.SubmissionKey = fmt.Sprintf("piece-%d", i)
		submit(t, q, s)
	}
	type running struct {
		job *Job
		end time.Time
	}
	active := make([][]running, len(workers))
	var placements [3]int
	started, finished := 0, 0
	start := clock.Now()
	for finished < pieces && clock.Now().Sub(start) < 2*durations[1] {
		for i := range workers {
			var reports []*Job
			pending := active[i][:0]
			for _, attempt := range active[i] {
				if clock.Now().Before(attempt.end) {
					pending = append(pending, attempt)
					reports = append(reports, attempt.job)
				} else {
					finish(t, q, attempt.job, true)
					finished++
				}
			}
			active[i] = pending
			if j := measuredCPUPull(t, q, workers[i], reports); j != nil {
				placements[i]++
				started++
				active[i] = append(active[i], running{j, clock.Now().Add(durations[i])})
			}
		}
		if finished < pieces {
			clock.Advance(5 * time.Second)
		}
	}
	if started != pieces || finished != pieces {
		t.Fatalf("started %d and finished %d of %d pieces; placements %v", started, finished, pieces, placements)
	}
	return clock.Now().Sub(start), placements
}

func TestCompletionMeasuredCPUBacklog(t *testing.T) {
	legacy, legacyPlacements := replayMeasuredCPUBacklog(t, false)
	measured, measuredPlacements := replayMeasuredCPUBacklog(t, true)
	t.Logf("legacy %s %v, measured %s %v (5x slower, 7x slower, fast)", legacy, legacyPlacements, measured, measuredPlacements)
	if legacyPlacements != [3]int{3, 2, 1} || legacy != 215*time.Second {
		t.Fatal("unmeasured work changed least-valuable placement", legacy, legacyPlacements)
	}
	if measured >= legacy || measuredPlacements != [3]int{1, 0, 5} {
		t.Fatal("measured placement did not improve the backlog", measured, measuredPlacements)
	}
}

func TestCompletionSlowCPUFinishesBeforeFastRelease(t *testing.T) {
	q, clock := fixture(t)
	slow, fast := measuredCPUWorker("slow", 16, 24), measuredCPUWorker("fast", 1, 64)
	short, long := cpuSpec(1, 2), cpuSpec(1, 2)
	long.Name = "long command"
	teachDuration(t, q, clock, slow, short, 15*time.Second)
	teachDuration(t, q, clock, fast, short, 10*time.Second)
	teachDuration(t, q, clock, fast, long, 60*time.Second)
	check(t, q, slow, nil)
	long.AllowedMachines = []string{fast.ID}
	_ = startObservation(t, q, fast, long)
	j := submit(t, q, short)
	run := measuredCPUPull(t, q, slow, nil)
	if run == nil || run.ID != j.ID {
		t.Fatal("slow host should take work it can finish before the fast slot is free", run)
	}
}

func TestCompletionCPUWaitsOnlyForFutureMeasuredRelease(t *testing.T) {
	q, clock := fixture(t)
	slow, fast := measuredCPUWorker("slow", 16, 24), measuredCPUWorker("fast", 1, 64)
	s := cpuSpec(1, 2)
	teachDuration(t, q, clock, slow, s, 70*time.Second)
	teachDuration(t, q, clock, fast, s, 10*time.Second)
	check(t, q, slow, nil)
	pinned := s
	pinned.AllowedMachines = []string{fast.ID}
	holder := startObservation(t, q, fast, pinned)
	check(t, q, fast, &holder)
	j := submit(t, q, s)
	if got := measuredCPUPull(t, q, slow, nil); got != nil {
		t.Fatal("slow worker took a piece that would finish sooner after the fast release", got.ID)
	}
	clock.Advance(10 * time.Second)
	check(t, q, fast, &holder)
	if got := measuredCPUPull(t, q, slow, nil); got == nil || got.ID != j.ID {
		t.Fatal("missed release estimate continued to withhold work", got)
	}
}

// Run the check-in assignment planner with the same measured live work and
// different retained history sizes. Setup and persistence are outside timing.
func BenchmarkCompletionAssignment(b *testing.B) {
	for _, waiting := range []int{3, 150, 500} {
		for _, retained := range []int{0, 12000} {
			b.Run(fmt.Sprintf("waiting=%d/history=%d", waiting, retained), func(b *testing.B) {
				q, clock := fixture(b)
				s := cpuSpec(1, 2)
				var workers []Machine
				for i, seconds := range []float64{150, 210, 30} {
					m := measuredCPUWorker(fmt.Sprintf("worker-%d", i), 16, float64(24+8*i))
					if i == 2 {
						m.CPUBudgetCores = 1
					}
					workers = append(workers, m)
					check(b, q, m, nil)
					pinned := s
					pinned.AllowedMachines = []string{m.ID}
					submit(b, q, pinned)
					j := check(b, q, m, nil).Job
					if j == nil {
						b.Fatal("calibration was not assigned")
					}
					if err := q.Started(result(j, true)); err != nil {
						b.Fatal(err)
					}
					clock.Advance(time.Duration(seconds) * time.Second)
					if err := q.Complete(result(j, true)); err != nil {
						b.Fatal(err)
					}
				}
				for _, m := range workers {
					check(b, q, m, nil)
				}
				pinned := s
				fast := workers[2]
				pinned.AllowedMachines = []string{fast.ID}
				submit(b, q, pinned)
				holder := check(b, q, fast, nil).Job
				if holder == nil {
					b.Fatal("holder was not assigned")
				}
				if err := q.Started(result(holder, true)); err != nil {
					b.Fatal(err)
				}
				template := submit(b, q, s)
				drainAdmissionWait(b, q)
				q.mu.Lock()
				for i := 1; i < waiting; i++ {
					id := fmt.Sprintf("queued-%d", i)
					j := cloneJob(template)
					j.ID = id
					q.s.Jobs[id] = &j
				}
				for i := range retained {
					id := fmt.Sprintf("retained-%d", i)
					q.s.WorkObservations[id] = WorkObservation{Key: ObservationKey{Work: id}, LastSeconds: 3, Samples: 1}
				}
				q.mu.Unlock()
				if err := q.update(func() error { return nil }); err != nil {
					b.Fatal(err)
				}
				drainAdmissionWait(b, q)
				q.mu.Lock()
				defer q.mu.Unlock()
				q.observationLive = q.records.live
				defer func() { q.observationLive = nil }()
				want := 2
				if waiting == 3 {
					want = 0
				}
				scan := q.placementScan()
				if plan := q.assignmentPlan(scan); len(plan) != want {
					b.Fatalf("wanted %d immediate assignments, got %d from %d live jobs and %d targets", want, len(plan), len(q.liveJobs()), len(scan.machines))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					q.assignmentPlan(q.placementScan())
				}
			})
		}
	}
}

func completionFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, true)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target.Platform = "cpu"
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	warm := cpuMachine("warm", "warm-host", "cpu", 8, 32)
	warm.CapabilityProfile.OS, warm.CapabilityProfile.Arch = v.Target.OS, v.Target.Arch
	warm.ExtensionFingerprint = "cpu-v1"
	warm.EnvironmentBuilder = true
	cold := cloneMachine(&warm)
	cold.ID, cold.HostID, cold.Session = "cold", "cold-host", "cold-session"
	s := Spec{Name: "count", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	for _, m := range []*Machine{&warm, &cold} {
		r := newExtensionReport(e, m, s.workType())
		r.Setup, r.Qualified, r.CheckID = true, true, "check-"+m.ID
		m.ExtensionReports = []ExtensionReport{r}
		recipe, ok := extensionRecipe(e, m)
		if !ok {
			t.Fatal("missing environment")
		}
		m.EnvironmentBuildSeconds = map[string]float64{recipe.Key: 10}
		if m.ID == "warm" {
			m.Environments = []string{recipe.Key}
		}
		check(t, q, *m, nil)
	}
	return q, clock, warm, cold, s
}

func seedCompletion(t *testing.T, q *Queue, j *Job, m *Machine, seconds float64) {
	t.Helper()
	copy := *extensionJobs(j, m, true)[0]
	copy.Machine, copy.Session = m.ID, m.Session
	key, why := q.workObservationKey(&copy)
	if key == nil {
		t.Fatal(why)
	}
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, LastSeconds: seconds, Samples: 2, RecordedAt: q.clock.Now()}
}

// Placement learns from its own accepted work (MY-31): the target whose last
// accepted run of this kind of work was shorter wins, and status says why.
func TestCompletionLastDurationPlacementAndStatus(t *testing.T) {
	q, clock, warm, cold, s := completionFixture(t)
	j := submit(t, q, s)
	wm, cm := q.s.Machines[warm.ID], q.s.Machines[cold.ID]
	seedCompletion(t, q, q.s.Jobs[j.ID], wm, 10)
	seedCompletion(t, q, q.s.Jobs[j.ID], cm, 1)
	clock.Advance(time.Second)
	if better, known := q.completionBetter(q.s.Jobs[j.ID], cm, wm); !known || !better {
		t.Fatal("shorter last accepted duration did not win", better, known)
	}
	if got := check(t, q, warm, nil).Job; got != nil {
		t.Fatal("slower target stole work")
	}
	// Existing preparation finishes; the next pull can admit it.
	cold.Environments = warm.Environments
	run := check(t, q, cold, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("chosen worker did not receive work", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	finish(t, q, run, true)
	got := job(t, q, j.ID)
	if got.Timing == nil || !strings.Contains(got.Timing.PlacementReason, "last accepted duration 1.00s on this target") {
		t.Fatal("missing chosen reason", got.Timing)
	}
	var html bytes.Buffer
	snap, err := q.publicSnapshot()
	if err != nil || statusPage.Execute(&html, snap) != nil || !strings.Contains(html.String(), "last accepted duration 1.00s on this target") {
		t.Fatal("HTML hid reason", err)
	}
}

func TestCompletionExactEvidenceAndReleaseCosts(t *testing.T) {
	q, _, warm, cold, s := completionFixture(t)
	j := submit(t, q, s)
	live := q.s.Jobs[j.ID]
	wm, cm := q.s.Machines[warm.ID], q.s.Machines[cold.ID]
	seedCompletion(t, q, live, wm, 10)
	seedCompletion(t, q, live, cm, 1)
	live.Spec.Resumable, live.Attempt = true, 1
	if _, ok := q.estimateCompletion(live, wm); ok {
		t.Fatal("first queued retry reused full-work duration")
	}
	live.Spec.Resumable, live.Attempt = false, 0
	// Invalidated and forbidden records never rank placement.
	for id, p := range q.s.WorkObservations {
		p.StaleReason = "driver or backend fingerprint changed"
		q.s.WorkObservations[id] = p
	}
	if _, ok := q.completionBetter(live, cm, wm); ok {
		t.Fatal("stale durations ranked")
	}
	seedCompletion(t, q, live, wm, 10)
	seedCompletion(t, q, live, cm, 1)
	for id, p := range q.s.WorkObservations {
		if p.Key.Host == cm.HostID {
			p.Forbidden = true
			q.s.WorkObservations[id] = p
		}
	}
	if _, ok := q.estimateCompletion(live, cm); ok {
		t.Fatal("OOM record reused")
	}
	seedCompletion(t, q, live, cm, 1)
	// A duration cannot override existing useful-victim disruption ordering.
	if q.completionPlanBetter(live, &placementPlan{machine: cm, stops: 1}, &placementPlan{machine: wm}) {
		t.Fatal("speed authorized extra victim")
	}
	if q.completionPlanBetter(live, &placementPlan{machine: cm, stops: 1, cost: 1}, &placementPlan{machine: wm, stops: 1}) {
		t.Fatal("speed overrode lost work")
	}
	model, _, m := modelFixture(t)
	if _, ok := model.estimateCompletion(callJob(&ModelCall{Model: "test"}), &m); ok {
		t.Fatal("unqualified model invented a duration")
	}
}
