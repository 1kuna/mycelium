package myce

import (
	"testing"
	"time"
)

func repeatedFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	a := cpuMachine("a-slow", "slow-host", "cpu", 2, 32)
	a.ExtensionFingerprint = "runtime-v1"
	b := cloneMachine(&a)
	b.ID, b.HostID, b.Session = "z-fast", "fast-host", "fast-session"
	check(t, q, a, nil)
	check(t, q, b, nil)
	return q, clock, a, b, cpuSpec(2, .1)
}

func cleanRepeatedResult(j *Job) Result {
	r := result(j, true)
	zero := 0.0
	r.MemoryPeak = &AttemptMemoryPeak{Pressure: &MemoryPressureEvidence{Reason: "eligible"}}
	r.Resources = &AttemptResources{CPU: AttemptCPUTime{Source: "cgroup_v2", UsageSeconds: &zero, ThrottledSeconds: &zero, Pressure: &IOPressure{Some: &PressureStall{}}}, IO: AttemptIO{Pressure: &IOPressure{Some: &PressureStall{}}}}
	return r
}

func teachRepeated(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, duration time.Duration) Job {
	t.Helper()
	s.AllowedMachines = []string{m.ID}
	j := startObservation(t, q, m, s)
	clock.Advance(duration)
	if err := q.Complete(cleanRepeatedResult(&j)); err != nil {
		t.Fatal(err)
	}
	return j
}

// Laptop proof: real HTTP API and supervised shell commands, two synthetic
// inventories, measured real command durations and clean counter fixtures. The
// command simulates different service speeds on one laptop; native throughput
// and real sensor completeness still require separate acceptance.
