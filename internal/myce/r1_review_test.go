package myce

import (
	"fmt"
	"testing"
	"time"
)

func TestR1HostReleaseSkipsUnneededDimensions(t *testing.T) {
	for _, cores := range []int{16, 2} {
		for _, zeroSlot := range []bool{false, true} {
			t.Run(fmt.Sprintf("cores=%d/zeroRAMSlot=%v", cores, zeroSlot), func(t *testing.T) {
				q, clock := fixture(t)
				a, b, c := cpuMachine("w1", "shared", "fake", cores, 12), cpuMachine("w2", "shared", "fake", cores, 12), cpuMachine("w3", "shared", "fake", cores, 12)
				check(t, q, a, nil)
				check(t, q, b, nil)
				check(t, q, c, nil)
				var low *Job
				if zeroSlot {
					model := Model{Name: "zero", Engine: "fake", Path: "weights", Platforms: []string{"fake"}, MemoryGB: 1}
					if err := q.ConfigureModels(ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Minute}); err != nil {
						t.Fatal(err)
					}
					q.s.Machines[a.ID].ModelURL = "http://worker"
					q.s.Machines[a.ID].ModelConfigHash = modelConfigHash(q.models)
					q.s.ModelSlots[slotKey(a.ID, model.Name)] = &ModelSlot{LastUsed: clock.Now(), Machine: a.ID, Session: a.Session, Model: model.Name, State: "ready", Priority: "experiment", Project: "test"}
					q.refreshHostMemory()
				} else {
					s := spec("experiment")
					s.Needs.HostMemoryGB = minimumHostMemoryGB
					s.AllowedMachines = []string{a.ID}
					low = submit(t, q, s)
					if run := check(t, q, a, nil).Job; run == nil || run.ID != low.ID {
						t.Fatal("missing zero-RAM victim")
					}
				}
				s := spec("deadline")
				s.Needs.HostMemoryGB = 6
				s.AllowedMachines = []string{b.ID}
				ram := submit(t, q, s)
				if run := check(t, q, b, nil).Job; run == nil || run.ID != ram.ID {
					t.Fatal("missing RAM victim")
				}
				s = spec("production")
				s.Needs.HostMemoryGB = 8
				s.AllowedMachines = []string{c.ID}
				submit(t, q, s)
				if job(t, q, ram.ID).State != "stopping" {
					t.Fatal("RAM victim was not stopped")
				}
				if zeroSlot {
					if slot := q.s.ModelSlots[slotKey(a.ID, "zero")]; slot.State != "ready" {
						t.Fatal("stopped zero-RAM slot after RAM release covers cores", slot.State)
					}
				} else if job(t, q, low.ID).State != "running" {
					t.Fatal("stopped zero-RAM job after RAM release covers cores")
				}
			})
		}
	}
}
