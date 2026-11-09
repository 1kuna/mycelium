package myce

import (
	"math"
	"testing"
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

// A check-in can admit a model after its command releases memory. Virtual
// workers must observe that load in the same instant, before jumping time.
