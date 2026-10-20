package myce

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Exercise the complete check-in (including maintainModels -> candidate ->
// batchResourcesFit), rather than timing only one host-release search. These
// demands must stay queued, so every iteration sees the same 16-victim host.
func BenchmarkR1HostReleaseCheckin(b *testing.B) {
	for _, scenario := range []string{"infeasible-cores", "GPU-blocked"} {
		b.Run(scenario, func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
			q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { q.Close() })
			q.checkpointProbe = func() bool { return true }
			q.nasAvailable = true
			target := cpuMachine("target", "shared", "fake", 32, 128)
			target.LastSeen = clock.Now()
			q.s.Machines[target.ID] = &target
			for i := 0; i < 16; i++ {
				id := fmt.Sprintf("victim-%02d", i)
				m := cpuMachine(id, "shared", "fake", 32, 128)
				m.LastSeen = clock.Now()
				q.s.Machines[m.ID] = &m
				s := spec("experiment")
				s.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: 8, Platform: Platform{Required: []string{"cpu"}}}
				q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, StartedAt: &clock.now}
			}
			for i := 0; i < 20; i++ {
				id := fmt.Sprintf("demand-%02d", i)
				s := spec("production")
				s.Needs = ResourceNeeds{CPUCores: 33, HostMemoryGB: 64, MemoryGB: 1, Platform: Platform{Any: true}}
				if scenario == "GPU-blocked" {
					s.Needs.CPUCores, s.Needs.MemoryGB = 2, 16 // Host resources fit; the 8 GiB GPU cannot.
				}
				q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
			}
			checkin := func() {
				reply, err := q.Checkin(Checkin{Machine: target})
				if err != nil || reply.Job != nil {
					b.Fatal("blocked check-in admitted work", err, reply.Job)
				}
			}
			checkin()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				checkin()
			}
			b.StopTimer()
			for i := 0; i < 16; i++ {
				if j := q.s.Jobs[fmt.Sprintf("victim-%02d", i)]; j.State != "running" || j.ReleaseConfirmed {
					b.Fatal("blocked demand changed the victim set", j.ID, j.State)
				}
			}
		})
	}
}
