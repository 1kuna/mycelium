package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Use the real inventory-loss/requeue path, including exact cleanup, rather
// than seeding retry history. Unstarted faults retain only their unused budget.
func deviceFaultStartBudgetFixture(t *testing.T, q *Queue, clock *fakeClock, s Spec, started bool) (Machine, *Job) {
	t.Helper()
	m := m3Machine()
	j := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != j.ID {
		t.Fatal("missing first attempt", a)
	}
	if started {
		if err := q.Started(result(a, true)); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(time.Minute)
	m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "device_fault_xe", 0
	_, err := q.Checkin(Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol,
		Attempts: []AttemptReport{{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: started}}})
	if err != nil {
		t.Fatal(err)
	}
	release := result(a, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || got.InfrastructureRetries != 1 || got.Losses != 1 ||
		len(got.AttemptHistory) != 1 || got.AttemptHistory[0].DeviceFault == nil || !got.AttemptHistory[0].DeviceFault.At.Equal(clock.Now()) {
		t.Fatal("fault did not create exactly one typed infrastructure retry", got.State, got.InfrastructureRetries, got.Losses, got.AttemptHistory)
	}
	return m, got
}

func TestDeviceFaultStartBudgetRecovery(t *testing.T) {
	for _, mode := range []string{"operator-clear", "health-proven", "unstarted", "capacity-after-clear", "unstarted-capacity-after-clear"} {
		t.Run(mode, func(t *testing.T) {
			s := spec("production")
			s.StartWindow, s.SubmissionKey = "600s", "fault-recovery"
			capacityAfterClear := strings.Contains(mode, "capacity-after-clear")
			if capacityAfterClear {
				s.Needs.HostMemoryGB = 4
			}
			q, clock := fixture(t)
			m, j := deviceFaultStartBudgetFixture(t, q, clock, s, !strings.HasPrefix(mode, "unstarted"))
			remaining := 10 * time.Minute
			if strings.HasPrefix(mode, "unstarted") {
				remaining -= time.Minute
			}
			if j.DeviceFaultStartWait == nil || j.DeviceFaultStartWait.Remaining != remaining || !j.Spec.StartBy.Equal(clock.Now().Add(remaining)) {
				t.Fatal("loss renewal or pause lost the remaining budget", j.Spec.StartBy, j.DeviceFaultStartWait)
			}
			if capacityAfterClear {
				m.HostAvailableMemoryGB = 0
				machineCheck(t, q, m)
			}
			// Twice the declared window: this failed with a production start
			// deadline before device-fault waiting paused the budget.
			for range 20 {
				clock.Advance(time.Minute)
				if next := assignedJob(machineCheck(t, q, m)); next != nil {
					t.Fatal("fault retry consumed the last healthy card", next)
				}
			}
			q = reopenCancellationQueue(t, q, clock)
			machineCheck(t, q, m)
			got := job(t, q, j.ID)
			if got.State != "queued" || got.DeviceFaultStartWait == nil || got.DeviceFaultStartWait.Remaining != remaining ||
				got.Blocker == nil || got.Blocker.Code != deviceFaultRetryHold || !strings.Contains(got.Blocker.Evidence, "paused for device recovery") ||
				got.InfrastructureRetries != 1 || got.Losses != 1 || submit(t, q, s).ID != j.ID {
				t.Fatal("held recovery expired, changed budget/identity, charged a retry, or hid its blocker", got.State, got.Reason, got.Blocker)
			}
			m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "", 8
			wantDevice := "0"
			if mode == "operator-clear" {
				// A recovered but busy card still preserves healthy capacity;
				// the retry can promptly use its otherwise last-healthy sibling.
				m.Devices[0].FreeMemoryGB, wantDevice = 0, "1"
			}
			if mode == "health-proven" {
				m.Devices[0].Recovery = &DeviceRecovery{FaultEvent: strings.Repeat("a", 64), HealthySince: clock.Now().Add(-time.Minute), ClearedAt: clock.Now(), ObservationSeconds: 60, Probe: "vendor health"}
			}
			if capacityAfterClear {
				// Real incident control: after a clear the job remained queued
				// with 600s paused for RAM/reservation capacity, not expired.
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 0
				if next := assignedJob(machineCheck(t, q, m)); next != nil {
					t.Fatal("clear bypassed host RAM admission", next)
				}
				clock.Advance(20 * time.Minute)
				machineCheck(t, q, m)
				q = reopenCancellationQueue(t, q, clock)
				machineCheck(t, q, m)
				got := job(t, q, j.ID)
				paused := fmt.Sprintf("%.0fs left to start (paused for capacity)", remaining.Seconds())
				if got.State != "queued" || got.DeviceFaultStartWait != nil || !strings.Contains(got.Reason, paused) ||
					got.StartCapacityBudget == nil || *got.StartCapacityBudget != remaining || got.StartWindow != 10*time.Minute ||
					got.InfrastructureRetries != 1 || got.Losses != 1 {
					t.Fatal("post-clear capacity wait expired or charged the job", got.State, got.Reason)
				}
				m.HostAvailableMemoryGB = 32
			}
			next := assignedJob(machineCheck(t, q, m))
			if next == nil || next.ID != j.ID || next.Attempt != j.Attempt+1 || next.DeviceFaultStartWait != nil ||
				!next.Spec.StartBy.Equal(clock.Now().Add(remaining)) || next.StartWindow != 10*time.Minute ||
				len(next.AllocatedDevices) != 1 || next.AllocatedDevices[0] != wantDevice {
				t.Fatal("cleared recovery did not promptly resume its remaining budget", next)
			}
			// Both choices are eligible after a fresh two-card clear. The
			// operator request alone never authorizes a still-quarantined card.
			for _, id := range []string{"0", "1"} {
				if q.deviceFaultRetryBlocked(next, allocationMachine(q.s.Machines[m.ID], []string{id})) {
					t.Fatal("clear retained the last-healthy guard", id)
				}
			}
			if err := q.Started(result(next, true)); err != nil {
				t.Fatal(err)
			}
			if mode == "unstarted-capacity-after-clear" {
				// The saved 540s belongs to startup, not the original window:
				// a later loss after Started must still renew the full 600s.
				clock.Advance(10 * time.Second)
				m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "device_fault_xe", 0
				machineCheck(t, q, m, next)
				got := job(t, q, j.ID)
				if got.State != "queued" || got.StartWindow != 10*time.Minute || got.StartCapacityBudget != nil ||
					got.DeviceFaultStartWait == nil || got.DeviceFaultStartWait.Remaining != 10*time.Minute || !got.Spec.StartBy.Equal(clock.Now().Add(10*time.Minute)) {
					t.Fatal("started loss renewed from partial startup budget", got.State, got.Spec.StartBy, got.DeviceFaultStartWait)
				}
				release := result(next, false)
				release.ReleaseOnly = true
				if err := q.Complete(release); err != nil {
					t.Fatal(err)
				}
				m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "", 8
				next = assignedJob(machineCheck(t, q, m))
				if next == nil || next.ID != j.ID || !next.Spec.StartBy.Equal(clock.Now().Add(10*time.Minute)) {
					t.Fatal("second clear did not preserve the renewed original window", next)
				}
				if err := q.Started(result(next, true)); err != nil {
					t.Fatal(err)
				}
			}
			finish(t, q, next, true)
			requireState(t, q, j.ID, "succeeded")
		})
	}
}

func TestDeviceFaultStartBudgetRecoveryBound(t *testing.T) {
	s := spec("production")
	s.StartWindow = "600s"
	q, clock := fixture(t)
	m, j := deviceFaultStartBudgetFixture(t, q, clock, s, true)
	deadline := clock.Now().Add(MaxProductionStartWindow)
	clock.Advance(MaxProductionStartWindow - time.Second)
	machineCheck(t, q, m)
	if got := job(t, q, j.ID); got.State != "queued" || got.Blocker == nil || got.Blocker.Code != deviceFaultRetryHold {
		t.Fatal("recovery wait expired before its bound", got.State, got.Reason)
	}
	q = reopenCancellationQueue(t, q, clock)
	machineCheck(t, q, m)
	clock.Advance(time.Second)
	machineCheck(t, q, m)
	got := job(t, q, j.ID)
	if got.State != "failed" || got.FinishedAt == nil || !got.FinishedAt.Equal(deadline) || got.DiagnosticCode != deviceFaultRecoveryTimeout ||
		got.ExecutionCause == nil || got.ExecutionCause.Predicate != deviceFaultRecoveryTimeout || got.InfrastructureRetries != 1 || got.Losses != 1 {
		t.Fatal("permanent quarantine did not terminate with an uncharged typed timeout", got.State, got.Reason, got.ExecutionCause)
	}
	m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "", 8
	if next := assignedJob(machineCheck(t, q, m)); next != nil {
		t.Fatal("late clear resurrected expired recovery", next)
	}
}

func TestDeviceFaultStartBudgetLateClear(t *testing.T) {
	for _, cutoffKind := range []string{"recovery_bound", "start_by", "give_up_at"} {
		t.Run(cutoffKind, func(t *testing.T) {
			q, clock := fixture(t)
			s := spec("production")
			s.StartWindow = "600s"
			cutoff := clock.Now().Add(2 * time.Minute)
			if cutoffKind == "start_by" {
				s.StartBy = &cutoff
			} else if cutoffKind == "give_up_at" {
				s.GiveUpAt = &cutoff
			}
			m, j := deviceFaultStartBudgetFixture(t, q, clock, s, true)
			if cutoffKind == "recovery_bound" {
				clock.Advance(MaxProductionStartWindow - time.Second)
				machineCheck(t, q, m)
				if got := job(t, q, j.ID); got.State != "queued" || got.DeviceFaultStartWait == nil {
					t.Fatal("hold expired before its bound", got.State, got.Reason)
				}
				clock.Advance(2 * time.Second)
			} else {
				clock.Advance(MaxProductionStartWindow + time.Second)
			}
			// No maintenance occurs between crossing the bound and this first
			// event, which installs cleared inventory before running maintenance.
			m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "", 8
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("late clearance bypassed the active hold's bound", next)
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.FinishedAt == nil || !got.FinishedAt.Equal(clock.Now()) || got.InfrastructureRetries != 1 || got.Losses != 1 {
				t.Fatal("late-clear expiry missing or charged another retry", got.State, got.Reason)
			}
			if cutoffKind == "recovery_bound" {
				if got.DiagnosticCode != deviceFaultRecoveryTimeout || got.ExecutionCause == nil || got.ExecutionCause.Predicate != deviceFaultRecoveryTimeout {
					t.Fatal("late clear lost the typed recovery timeout", got.DiagnosticCode, got.ExecutionCause)
				}
			} else if got.DiagnosticCode == deviceFaultRecoveryTimeout || cutoffKind == "start_by" && !got.Spec.StartBy.Equal(cutoff) ||
				cutoffKind == "give_up_at" && got.Reason != "give-up time reached" {
				t.Fatal("late clear overrode the earlier absolute cutoff", got.Reason, got.Spec.StartBy)
			}
		})
	}
}

func TestDeviceFaultStartBudgetAbsoluteCutoffs(t *testing.T) {
	for _, kind := range []string{"start_by", "give_up_at"} {
		t.Run(kind, func(t *testing.T) {
			s := spec("production")
			s.StartWindow = "600s"
			q, clock := fixture(t)
			cutoff := clock.Now().Add(2 * time.Minute)
			if kind == "start_by" {
				s.StartBy = &cutoff
			} else {
				s.GiveUpAt = &cutoff
			}
			m, j := deviceFaultStartBudgetFixture(t, q, clock, s, true)
			if kind == "start_by" && !j.Spec.StartBy.Equal(cutoff) {
				t.Fatal("loss renewed caller-owned StartBy", j.Spec.StartBy, cutoff)
			}
			clock.Advance(cutoff.Sub(clock.Now()))
			machineCheck(t, q, m)
			got := job(t, q, j.ID)
			if got.State != "failed" || got.FinishedAt == nil || !got.FinishedAt.Equal(cutoff) || got.DiagnosticCode == deviceFaultRecoveryTimeout {
				t.Fatal("device recovery pause overrode the earlier caller cutoff", got.State, got.Reason)
			}
		})
	}
}
