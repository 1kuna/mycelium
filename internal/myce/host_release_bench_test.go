package myce

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Complete check-ins from the round-5 review performance gate: twenty
// blocked production demands competing with one physical host of victims.
func BenchmarkR1ReleaseCheckin(b *testing.B) {
	type victimShape struct {
		priority string
		cores    int
		ram      float64
	}
	uniform := func(n, cores int, ram float64) []victimShape {
		out := make([]victimShape, n)
		for i := range out {
			out[i] = victimShape{"experiment", cores, ram}
		}
		return out
	}
	mixed := func() []victimShape {
		out := []victimShape{}
		for i := 0; i < 12; i++ {
			p := []string{"experiment", "deadline"}[i%2]
			out = append(out, victimShape{p, 1 + (i*7)%4, float64(2 + (i*5)%11)})
		}
		return out
	}
	type scenario struct {
		name                 string
		hostCores            int
		hostRAM              float64
		victims              []victimShape
		demandCores          int
		demandRAM, demandGPU float64
	}
	mixedCores, mixedRAM := 0, 0.0
	for _, v := range mixed() {
		mixedCores += v.cores
		mixedRAM += v.ram
	}
	for _, sc := range []scenario{
		{"16v-core-infeasible", 32, 128, uniform(16, 2, 8), 33, 64, 1},
		{"16v-gpu-blocked", 32, 128, uniform(16, 2, 8), 2, 16, 16},
		{"12v-gpu-blocked-need10", 24, 96, uniform(12, 2, 8), 20, 80, 16},
		{"12v-gpu-blocked-need2", 24, 96, uniform(12, 2, 8), 4, 16, 16},
		{"12v-gpu-blocked-mixed-need10", mixedCores, mixedRAM, mixed(), mixedCores - 6, mixedRAM - 16, 16},
		{"12v-core-infeasible", 24, 96, uniform(12, 2, 8), 25, 64, 1},
	} {
		b.Run(sc.name, func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
			q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { q.Close() })
			q.checkpointProbe = func() bool { return true }
			q.nasAvailable = true
			target := cpuMachine("target", "shared", "fake", sc.hostCores, sc.hostRAM)
			target.LastSeen = clock.Now()
			q.s.Machines[target.ID] = &target
			for i, v := range sc.victims {
				id := fmt.Sprintf("victim-%02d", i)
				m := cpuMachine(id, "shared", "fake", sc.hostCores, sc.hostRAM)
				m.LastSeen = clock.Now()
				q.s.Machines[m.ID] = &m
				s := spec(v.priority)
				s.Needs = ResourceNeeds{CPUCores: v.cores, HostMemoryGB: v.ram, Platform: Platform{Required: []string{"cpu"}}}
				q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, StartedAt: &clock.now}
			}
			for i := 0; i < 20; i++ {
				id := fmt.Sprintf("demand-%02d", i)
				s := spec("production")
				s.Needs = ResourceNeeds{CPUCores: sc.demandCores, HostMemoryGB: sc.demandRAM, MemoryGB: sc.demandGPU, Platform: Platform{Any: true}}
				q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
			}
			checkin := func() {
				reply, err := q.Checkin(Checkin{Machine: target})
				if err != nil || reply.Job != nil {
					b.Fatal("blocked check-in admitted work", err, reply.Job)
				}
			}
			checkin()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				checkin()
			}
			b.StopTimer()
			for i := range sc.victims {
				if j := q.s.Jobs[fmt.Sprintf("victim-%02d", i)]; j.State != "running" || j.ReleaseConfirmed {
					b.Fatal("blocked demand changed the victim set", j.ID, j.State)
				}
			}
		})
	}
}

// Equal running/stopping victims exercise the exact-search cap and ID ties.
func BenchmarkR1TiedReleaseCheckin(b *testing.B) {
	for _, need := range []int{6} {
		for _, stopping := range []bool{false, true} {
			b.Run(fmt.Sprintf("stopping=%v-need%d", stopping, need), func(b *testing.B) {
				clock := &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
				q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { q.Close() })
				q.checkpointProbe = func() bool { return true }
				q.nasAvailable = true
				target := cpuMachine("target", "shared", "fake", 12, 48)
				target.LastSeen = clock.Now()
				q.s.Machines[target.ID] = &target
				until := clock.now.Add(time.Hour)
				for i := 0; i < 12; i++ {
					id := fmt.Sprintf("victim-%02d", i)
					m := cpuMachine(id, "shared", "fake", 12, 48)
					m.LastSeen = clock.Now()
					q.s.Machines[m.ID] = &m
					s := spec("experiment")
					s.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 4, Platform: Platform{Required: []string{"cpu"}}}
					j := &Job{ID: id, Spec: s, State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, StartedAt: &clock.now}
					if stopping {
						j.State, j.StopAt = "stopping", &until
					}
					q.s.Jobs[id] = j
				}
				for i := 0; i < 20; i++ {
					id := fmt.Sprintf("demand-%02d", i)
					s := spec("production")
					s.Needs = ResourceNeeds{CPUCores: need, HostMemoryGB: float64(4 * need), MemoryGB: 16, Platform: Platform{Any: true}}
					q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
				}
				checkin := func() {
					reply, err := q.Checkin(Checkin{Machine: target})
					if err != nil || reply.Job != nil {
						b.Fatal("blocked check-in admitted work", err, reply.Job)
					}
				}
				checkin()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					checkin()
				}
			})
		}
	}
}
