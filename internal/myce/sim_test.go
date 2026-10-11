package myce

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func simTestWork(streams ...SimStream) SimWorkload {
	zero := 0.0
	return SimWorkload{Jitter: &zero, Streams: streams}
}
func simTestFleet(n int) SimFleet {
	f := SimFleet{}
	for i := 0; i < n; i++ {
		f.Machines = append(f.Machines, SimMachine{Name: string(rune('a' + i)), Platform: "fake", MemoryGB: 8, HostGB: 32, Speed: 1})
	}
	return f
}
func simTestStream(name, priority string, start, duration float64, count int) SimStream {
	return SimStream{Name: name, Project: name, Priority: priority, Platforms: []string{"fake"}, MemoryGB: 8, Duration: duration, Every: 24, Start: start / 3600, Count: count}
}
func runSimTest(t *testing.T, f SimFleet, w SimWorkload, seconds float64) SimReport {
	t.Helper()
	r, e := Simulate(f, w, SimOptions{Days: seconds / 86400, Seed: 1})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func nearSim(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-7 {
		t.Fatalf("got %.12f want %.12f", got, want)
	}
}
func TestSimProductionPreemption(t *testing.T) {
	exp := simTestStream("exp", "experiment", 0, 100, 2)
	exp.Checkpoint = 5
	prod := simTestStream("prod", "production", 10, 5, 1)
	r := runSimTest(t, simTestFleet(2), simTestWork(exp, prod), 120)
	nearSim(t, r.Production.Max, 1)
	if r.Preemptions != 1 || r.Completed != 3 || r.LostSeconds != 0 {
		t.Fatalf("%+v", r)
	}
	nearSim(t, r.Projects["exp"].GPUHours, 200.0/3600)
}
func TestSimNonResumableLostWork(t *testing.T) {
	r := runSimTest(t, simTestFleet(1), simTestWork(simTestStream("exp", "experiment", 0, 100, 1), simTestStream("prod", "production", 10, 5, 1)), 120)
	nearSim(t, r.Production.Max, 0)
	nearSim(t, r.LostSeconds, 10)
	if r.Preemptions != 1 || r.Completed != 2 {
		t.Fatalf("%+v", r)
	}
}
func TestSimLeaveMidJob(t *testing.T) {
	f := simTestFleet(2)
	f.Machines[0].Windows = []SimWindow{{Days: []int{0}, Start: 0, End: 10.0 / 3600}}
	f.Machines[1].Windows = []SimWindow{{Days: []int{0}, Start: 10.0 / 3600, End: 24}}
	s := simTestStream("exp", "experiment", 0, 20, 1)
	s.Checkpoint = 5
	r := runSimTest(t, f, simTestWork(s), 30)
	if r.Completed != 1 {
		t.Fatalf("%+v", r)
	}
	nearSim(t, r.Projects["exp"].ExperimentFinishSeconds, 20)
	nearSim(t, r.Machines["a"].BusyHours, 10.0/3600)
	nearSim(t, r.Machines["b"].BusyHours, 10.0/3600)
}
func TestSimFairWeights(t *testing.T) {
	a := simTestStream("a", "experiment", 0, 10, 100)
	b := simTestStream("b", "experiment", 0, 10, 100)
	w := simTestWork(a, b)
	w.Projects = map[string]ProjectPolicy{"a": {Weight: 2}, "b": {Weight: 1}}
	r := runSimTest(t, simTestFleet(1), w, 300)
	if r.Projects["a"].ExperimentCompleted != 20 || r.Projects["b"].ExperimentCompleted != 10 {
		t.Fatalf("%+v %+v", r.Projects["a"], r.Projects["b"])
	}
	nearSim(t, r.Projects["a"].Share, 200.0/3)
}
func TestSimIdleWaitingFit(t *testing.T) {
	clock := &simClock{time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	q := &Queue{simulation: true, clock: clock, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	m := &Machine{ID: "a", GPU: "fake", FreeMemoryGB: 8, TotalMemoryGB: 8}
	q.s.Machines[m.ID] = m
	s := simTestStream("a", "experiment", 0, 10, 1)
	q.s.Jobs["a"] = &Job{ID: "a", State: "queued", Spec: simSpec(&simTask{stream: s}, clock.Now())}
	idleHours := 0.0
	if simIdleWaiting(q, m) {
		idleHours += 7200.0 / 3600
	}
	nearSim(t, idleHours, 2)
	m.FreeMemoryGB = 7
	if simIdleWaiting(q, m) {
		t.Fatal("incompatible work counted as fitting")
	}
	m.FreeMemoryGB = 8
	q.s.Policies = map[string]ProjectPolicy{"a": {Weight: 1, MaxConcurrentExperiments: 1}}
	q.s.Jobs["running"] = &Job{State: "running", Machine: "other", Spec: q.s.Jobs["a"].Spec}
	if simIdleWaiting(q, m) {
		t.Fatal("capped work counted as runnable")
	}
}
func TestSimImpossibleAndDeadline(t *testing.T) {
	s := simTestStream("deadline", "deadline", 0, 20, 1)
	s.Deadline = 10
	s.MemoryGB = 9
	r := runSimTest(t, simTestFleet(1), simTestWork(s), 30)
	if len(r.NeverPlaceable) != 1 || r.DeadlineMisses != 1 || r.Unfinished != 1 {
		t.Fatalf("%+v", r)
	}
}
func TestSimSplitBatch(t *testing.T) {
	s := simTestStream("batch", "experiment", 0, 10, 1)
	s.Batch = 4
	w := simTestWork(s)
	f := simTestFleet(2)
	one := runSimTest(t, f, w, 60)
	split, e := Simulate(f, w, SimOptions{Days: 60.0 / 86400, Seed: 1, Split: true})
	if e != nil {
		t.Fatal(e)
	}
	nearSim(t, one.BatchMeanSeconds, 40)
	nearSim(t, split.BatchMeanSeconds, 20)
}
func TestSimModelCallLoadReuse(t *testing.T) {
	s := simTestStream("call", "production", 0, 10, 2)
	s.Call = true
	s.Model = "small"
	w := simTestWork(s)
	w.Models = []SimModel{{Name: "small", MemoryGB: 4, Platforms: []string{"fake"}, LoadSeconds: 5}}
	r := runSimTest(t, simTestFleet(1), w, 40)
	if r.ModelLoads != 1 || r.ModelReloads != 0 || r.Completed != 2 {
		t.Fatalf("%+v", r)
	}
	nearSim(t, r.LoadingSeconds, 5)
	nearSim(t, r.Production.Median, 5)
	nearSim(t, r.Production.Max, 15)
}
func TestSimDeterministic(t *testing.T) {
	f := simTestFleet(2)
	a := simTestStream("a", "experiment", 0, 10, 8)
	b := simTestStream("b", "production", 10, 2, 2)
	w := SimWorkload{Streams: []SimStream{a, b}}
	var previous []byte
	for i := 0; i < 8; i++ {
		r := runSimTest(t, f, w, 100)
		data, _ := json.Marshal(r)
		if previous != nil && !reflect.DeepEqual(data, previous) {
			t.Fatalf("nondeterministic\n%s\n%s", previous, data)
		}
		previous = data
	}
}

func TestSimSuddenLossCheckpoint(t *testing.T) {
	f := simTestFleet(3)
	f.Machines[0].Departure = "loss"
	f.Machines[0].Windows = []SimWindow{{Days: []int{0}, Start: 0, End: 13.0 / 3600}}
	s := simTestStream("exp", "experiment", 0, 30, 1)
	s.Checkpoint = 5
	r := runSimTest(t, f, simTestWork(s), 110)
	if r.Completed != 1 {
		t.Fatalf("%+v", r)
	}
	nearSim(t, r.InfrastructureLostSeconds, 3)
	nearSim(t, r.Projects["exp"].ExperimentFinishSeconds, 93) // loss at 13 + real 60s timeout + remaining 20
}
