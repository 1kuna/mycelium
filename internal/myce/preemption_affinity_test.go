package myce

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestCPUPreemptionPrefersIdleHost(t *testing.T) {
	q, clock := fixture(t)
	a := m3Machine()
	a.ID, a.HostID = "host-a", "host-a"
	a.GPU, a.CapabilityProfile.Accelerators[0].Type = "xpu", "xpu"
	a.Devices[0].Platform, a.Devices[1].Platform = "xpu", "xpu"
	low := spec("experiment")
	low.Needs.CPUCores, low.Needs.HostMemoryGB = 8, 1
	victim := submit(t, q, low)
	run := assignedJob(machineCheck(t, q, a))
	if run == nil || run.ID != victim.ID {
		t.Fatal("missing XPU attempt", run, job(t, q, victim.ID).Reason)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	b := cpuMachine("host-b", "host-b", "cpu", 8, 32)
	b.Devices = []MachineDevice{}
	machineCheck(t, q, b)
	clock.Advance(time.Second)
	s := cpuSpec(8, 1)
	s.Priority = "production"
	demand := submit(t, q, s)
	if got := job(t, q, victim.ID); got.State != "running" || priorityStop(got) != nil {
		t.Fatal("preempted XPU work despite fitting idle host", got.State, priorityStop(got))
	}
	if next := assignedJob(machineCheck(t, q, b)); next == nil || next.ID != demand.ID {
		t.Fatal("idle host did not receive production CPU demand", next)
	}
}

func TestCPUPreemptionKeepsSelectedHostAfterRelease(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			q, clock := fixture(t)
			a := m3Machine()
			a.ID, a.HostID = "host-a", "host-a"
			a.GPU, a.CapabilityProfile.Accelerators[0].Type = "xpu", "xpu"
			a.Devices[0].Platform, a.Devices[1].Platform = "xpu", "xpu"
			low := spec("experiment")
			low.Needs.CPUCores, low.Needs.HostMemoryGB = 8, 1
			victim := submit(t, q, low)
			run := assignedJob(machineCheck(t, q, a))
			if run == nil || run.ID != victim.ID {
				t.Fatal("missing XPU attempt", run, job(t, q, victim.ID).Reason)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			s := cpuSpec(8, 1)
			s.Priority = "production"
			demand := submit(t, q, s)
			if got := job(t, q, victim.ID); got.State != "stopping" || priorityStop(got) == nil || priorityStop(got).Demand != demand.ID {
				t.Fatal("required host preemption missing", got.State, priorityStop(got))
			}
			if restart {
				var path string
				if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
					t.Fatal(err)
				}
				root := q.root
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				q, err = Open(path, root, clock)
				if err != nil {
					t.Fatal(err)
				}
				defer q.Close()
				fakeCheckpointAvailability(t, q, true)
				machineCheck(t, q, a, run)
			}

			clock.Advance(time.Second)
			b := cpuMachine("host-b", "host-b", "cpu", 8, 32)
			b.Devices = []MachineDevice{}
			if next := assignedJob(machineCheck(t, q, b)); next != nil && next.ID == demand.ID {
				t.Fatal("demand escaped to another host before victim release")
			}
			if next := assignedJob(machineCheck(t, q, a, run)); next != nil && next.ID == demand.ID {
				t.Fatal("demand started before exact release")
			}
			finishInterrupted(t, q, run)
			if next := assignedJob(machineCheck(t, q, b)); next != nil && next.ID == demand.ID {
				t.Fatal("demand escaped to another host after victim release")
			}
			next := assignedJob(machineCheck(t, q, a))
			if next == nil || next.ID != demand.ID || next.Machine != a.ID {
				t.Fatal("demand did not use the host it preempted", next)
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGPUPreemptionKeepsSelectedCards(t *testing.T) {
	for _, cards := range []int{1, 2} {
		t.Run(string(rune('0'+cards)), func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			if cards == 2 {
				third := m.Devices[1]
				third.ID, third.Selector, third.Identity = "2", "2", "fake-2"
				m.Devices = append(m.Devices, third)
			}
			low := sharedSpec("experiment", float64(cards)*8)
			victim := submit(t, q, low)
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != victim.ID {
				t.Fatal("missing victim", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			blockerSpec := sharedSpec("production", 8)
			blockerSpec.Needs.GPU = &GPUSelection{Devices: []string{m.Devices[cards].ID}}
			blocker := submit(t, q, blockerSpec)
			busy := assignedJob(machineCheck(t, q, m, run))
			if busy == nil || busy.ID != blocker.ID {
				t.Fatal("missing sibling blocker", busy)
			}
			if err := q.Started(result(busy, false)); err != nil {
				t.Fatal(err)
			}
			demand := submit(t, q, sharedSpec("production", float64(cards)*8))
			if stop := priorityStop(job(t, q, victim.ID)); stop == nil || stop.Demand != demand.ID {
				t.Fatal("missing priority stop", stop)
			}
			clock.Advance(time.Second)
			finish(t, q, busy, true)
			if next := assignedJob(machineCheck(t, q, m, run)); next != nil && next.ID == demand.ID {
				t.Fatal("demand escaped onto freed sibling cards")
			}
			finishInterrupted(t, q, run)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil || next.ID != demand.ID || !slices.Equal(next.AllocatedDevices, run.AllocatedDevices) {
				t.Fatal("demand did not use reclaimed cards", next)
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
