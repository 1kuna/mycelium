package myce

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMachineWedgedKillFallsBackBeforeStartBy(t *testing.T) {
	for _, sameMachine := range []bool{true, false} {
		name := "other-machine"
		if sameMachine {
			name = "same-machine"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			aMachine, bMachine := m3Machine(), m3Machine()
			device := "1"
			if !sameMachine {
				aMachine.Devices = aMachine.Devices[:1]
				bMachine.ID, bMachine.HostID = "neighbor", "neighbor"
				bMachine.Devices = bMachine.Devices[:1]
				device = "0"
			} else {
				bMachine = aMachine
			}
			submit(t, q, m3Spec("experiment", "0", 8, 1))
			a := assignedJob(machineCheck(t, q, aMachine))
			s := m3Spec("experiment", device, 8, 1)
			s.AllowedMachines = []string{bMachine.ID}
			submit(t, q, s)
			var b *Job
			if sameMachine {
				b = assignedJob(machineCheck(t, q, bMachine, a))
			} else {
				b = assignedJob(machineCheck(t, q, bMachine))
			}
			if a == nil || b == nil {
				t.Fatal("missing experiment assignments")
			}
			heartbeat := func(extra ...*Job) Reply {
				if sameMachine {
					return machineCheck(t, q, aMachine, append([]*Job{a}, extra...)...)
				}
				machineCheck(t, q, aMachine, a)
				return machineCheck(t, q, bMachine, extra...)
			}
			heartbeat(b)
			if err := q.Cancel(a.ID); err != nil {
				t.Fatal(err)
			}
			stopAt := *job(t, q, a.ID).StopAt
			clock.Advance(stopAt.Sub(clock.Now()))
			prod := submit(t, q, spec("production"))
			heartbeat(b)
			if got := job(t, q, b.ID); got.State != "running" || got.PreemptionCount != 0 {
				t.Fatal("quick kill no longer spares neighbor", got.State)
			}
			clock.Advance(q.preemptionGrace())
			if got := assignedJob(heartbeat(b)); got != nil {
				t.Fatal("admitted before exact cleanup", got.ID)
			}
			if got := job(t, q, b.ID); got.State != "stopping" || got.PreemptionCount != 1 {
				t.Fatalf("wedged kill retained credit at bound: neighbor=%s preemptions=%d", got.State, got.PreemptionCount)
			}
			finishInterrupted(t, q, b)
			next := assignedJob(heartbeat())
			if next == nil || next.ID != prod.ID || next.Machine != bMachine.ID || len(next.AllocatedDevices) != 1 || next.AllocatedDevices[0] != device {
				t.Fatal("production did not use fallback device", next)
			}
			if !clock.Now().Before(*next.Spec.StartBy) {
				t.Fatal("production placed after StartBy")
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
			// Keep reporting the wedged attempt every 10s past the entire start
			// window. Heartbeats cannot renew its planning credit or prove release.
			for i := 1; i <= 200; i++ {
				clock.Advance(time.Second)
				if i%10 == 0 {
					heartbeat(next)
				} else if !sameMachine {
					// Production's independent worker must satisfy its shorter
					// five-second loss timeout while the wedged worker reports at 10s.
					machineCheck(t, q, bMachine, next)
				}
			}
			if got := job(t, q, prod.ID); got.State != "running" || got.StartedAt == nil {
				t.Fatal("production failed despite fallback", got.State, got.Reason)
			}
			if got := job(t, q, a.ID); got.State != "stopping" || got.ReleaseConfirmed {
				t.Fatal("wedged custody was released by time", got.State)
			}
		})
	}
}

func TestMachineRecoveryPlansElsewhere(t *testing.T) {
	q, clock := fixture(t)
	a, b := m3Machine(), m3Machine()
	a.ID, a.HostID, a.Devices = "a", "a", a.Devices[:1]
	b.ID, b.HostID, b.Devices = "b", "b", b.Devices[:1]
	s := m3Spec("experiment", "0", 8, 1)
	s.AllowedMachines = []string{b.ID}
	submit(t, q, s)
	victim := assignedJob(machineCheck(t, q, b))
	machineCheck(t, q, b, victim)
	recoveringCheck(t, q, a)
	prod := submit(t, q, spec("production"))
	if got := assignedJob(recoveringCheck(t, q, a)); got != nil {
		t.Fatal("recovery admitted work", got.ID)
	}
	machineCheck(t, q, b, victim)
	if got := job(t, q, victim.ID); got.State != "stopping" || got.PreemptionCount != 1 {
		t.Fatalf("recovering free device hid fallback: neighbor=%s preemptions=%d", got.State, got.PreemptionCount)
	}
	finishInterrupted(t, q, victim)
	next := assignedJob(machineCheck(t, q, b))
	if next == nil || next.ID != prod.ID || !clock.Now().Before(*next.Spec.StartBy) {
		t.Fatal("production did not place on machine B before StartBy", next)
	}
}

func TestMachineRecoveryStatusAndClear(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	since := clock.Now()
	assertStatus := func(recovering, attention bool) {
		t.Helper()
		s, err := q.Snapshot()
		if err != nil || len(s.Machines) != 1 {
			t.Fatal("snapshot", err)
		}
		data, err := json.Marshal(s.Machines[0])
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			Recovering bool       `json:"recovering"`
			Since      *time.Time `json:"recovering_since"`
			Attention  bool       `json:"recovery_needs_attention"`
		}
		if err = json.Unmarshal(data, &state); err != nil {
			t.Fatal(err)
		}
		if state.Recovering != recovering || state.Attention != attention || recovering && (state.Since == nil || !state.Since.Equal(since)) || !recovering && state.Since != nil {
			t.Fatalf("recovery status missing or wrong: %+v", state)
		}
		words := HyphaAvailability(s.Machines[0])
		if strings.Contains(words, "recovering") != recovering || strings.Contains(words, "needs attention") != attention {
			t.Fatal("status hides recovery", words)
		}
	}
	if r := recoveringCheck(t, q, m); r.AdmissionBlocked {
		t.Fatal("recovery must not hold saved-result replay")
	}
	assertStatus(true, false)
	for i := 0; i < 12; i++ {
		clock.Advance(10 * time.Second)
		recoveringCheck(t, q, m)
	}
	assertStatus(true, false)
	clock.Advance(time.Second)
	recoveringCheck(t, q, m)
	assertStatus(true, true)
	machineCheck(t, q, m)
	assertStatus(false, false)
}

func TestMachineCompletionPlanningCreditBound(t *testing.T) {
	for _, afterBound := range []bool{false, true} {
		name := "within-bound"
		if afterBound {
			name = "after-bound"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			submit(t, q, m3Spec("experiment", "0", 8, 1))
			a := assignedJob(machineCheck(t, q, m))
			submit(t, q, m3Spec("experiment", "1", 8, 1))
			b := assignedJob(machineCheck(t, q, m, a))
			machineCheck(t, q, m, a, b)
			if err := q.Cancel(a.ID); err != nil {
				t.Fatal(err)
			}
			bound := job(t, q, a.ID).StopAt.Add(q.preemptionGrace())
			clock.Advance(bound.Sub(clock.Now()) - time.Second)
			finishInterrupted(t, q, a)
			r, ok := q.s.BatchReleases[m.ID+"/0"]
			if !ok || !r.Until.Equal(bound) {
				t.Fatalf("completion credit not bounded by StopAt + grace: got=%v want=%v", r.Until, bound)
			}
			if afterBound {
				clock.Advance(time.Second)
			}
			submit(t, q, spec("production"))
			got := job(t, q, b.ID)
			if (got.State == "stopping") != afterBound {
				t.Fatalf("completion credit expiry: afterBound=%t neighbor=%s", afterBound, got.State)
			}
		})
	}
}
