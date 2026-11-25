package myce

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestMemoryAdmissionRefusalLoop(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	m.HostMemoryBudgetGB = 96
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	s := sharedSpec("experiment", 24)
	s.Needs.HostMemoryGB = 12
	j := submit(t, q, s)
	var offers []int
	for i := range 25 {
		// The historical queue reading varied while the native gate kept
		// refusing the full 36 GiB check after helpers had started.
		m.HostAvailableMemoryGB = 85 + float64(i%2)
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 121 - 4 - m.HostAvailableMemoryGB, ReserveGB: 4}
		if a := assignedJob(machineCheck(t, q, m)); a != nil {
			if a.ID != j.ID {
				t.Fatal("unexpected offer", a.ID)
			}
			offers = append(offers, i)
			// The authorization can arrive before the final native gate.
			if err := q.Started(result(a, true)); err != nil {
				t.Fatal(err)
			}
			r := capacityResult(a, false)
			r.ExecutionCause.Admission = admissionDetail(checkMemoryPool(36, 93.435, 4, 59.906, true), *a)
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
		}
		clock.Advance(26 * time.Second)
	}
	// The 26-second poll only sees the first check-in at or after expiry.
	want := []int{0, 3, 8, 18}
	if !slices.Equal(offers, want) {
		t.Fatalf("launch-refusal offers at check-ins %v, want %v", offers, want)
	}
}

func TestMemoryPressureRetryLoop(t *testing.T) {
	q, clock := fixture(t)
	m := sharedUnifiedMachine()
	m.HostMemoryBudgetGB = 96
	m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
	s := sharedSpec("experiment", 35)
	s.Needs.HostMemoryGB = 25
	j := submit(t, q, s)
	var stops []int
	for i := range 9 {
		m.HostAvailableMemoryGB = 90
		m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 27, ReserveGB: 4}
		a := assignedJob(machineCheck(t, q, m))
		if a != nil {
			if a.ID != j.ID || a.Memory.RAM.ChargeGB != 60 {
				t.Fatalf("unexpected offer %s with charge %g", a.ID, a.Memory.RAM.ChargeGB)
			}
			if err := q.Started(result(a, true)); err != nil {
				t.Fatal(err)
			}
		}
		clock.Advance(time.Second)
		if a != nil {
			// The historic readings preceding the stops are unavailable.
			// With 40 GiB physical free, a 54 GiB drop trips the
			// next-sample forecast without falling below the reserve.
			m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 36, 81
			machineCheck(t, q, m, a)
			requireState(t, q, j.ID, "stopping")
			stops = append(stops, i)
			release := result(a, false)
			release.ReleaseOnly = true
			if err := q.Complete(release); err != nil {
				t.Fatal(err)
			}
		}
		clock.Advance(6*time.Minute - time.Second)
	}
	want := []int{0, 1, 4, 5, 8}
	if !slices.Equal(stops, want) {
		t.Fatalf("pressure stops at check-ins %v, want %v", stops, want)
	}
}

func TestMemoryAdmissionHelperDoesNotDiscountFullDemand(t *testing.T) {
	for _, tc := range []struct {
		name       string
		afterProbe float64
	}{
		{"freed-before-probe", 0},
		{"freed-and-reacquired-around-probe", 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, clock := fixture(t)
			helperGB := 8.0
			w := Worker{Machine: unifiedCUDAAdmissionMachine(), Clock: clock,
				hostRawMemorySource: func(context.Context) (float64, error) {
					helperGB = 0
					room := 34 - helperGB
					helperGB = tc.afterProbe
					return room, nil
				},
			}
			j := Job{Memory: &MemoryPools{RAM: memoryPoolEstimate{ChargeGB: 36, TypicalGB: 36}, KeepGB: 4}}
			err := w.checkJobRAMWithReservations(t.Context(), j, 0, 0)
			d := admissionDetail(err, j)
			if !errors.Is(err, errWorkerMemory) || d == nil || d.Required != 36 || !strings.Contains(err.Error(), "30.000 GiB net room") || helperGB != tc.afterProbe {
				t.Fatalf("worker must charge the full 36 GiB against 30 GiB at the probe: %v (%+v)", err, d)
			}
		})
	}
}

func TestMemoryAdmissionBackoffDeadlineAndReset(t *testing.T) {
	for _, mode := range []string{"same-host", "other-host"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 8, 64)
			s := cpuSpec(1, 36)
			j := submit(t, q, s)
			offer := assignedJob(machineCheck(t, q, m))
			for i, delay := range []time.Duration{1, 2, 4, 8, 15, 15} {
				if offer == nil || offer.ID != j.ID || offer.Attempt != i+1 {
					t.Fatalf("missing offer for retry %d: %v (%s)", i+1, offer, job(t, q, j.ID).Reason)
				}
				if err := q.Started(result(offer, true)); err != nil {
					t.Fatal(err)
				}
				r := capacityResult(offer, false)
				r.ExecutionCause.Admission = admissionDetail(checkMemoryPool(36, 93.435, 4, 59.906, true), *offer)
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				d := delay * time.Minute
				got, err := q.jobSnapshot(j.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.Hold == nil || got.Hold.Kind != memoryWaitReason || got.Hold.Scope != hostID(&m) ||
					got.Hold.Expires == nil || !got.Hold.Expires.Equal(clock.Now().Add(d)) || !strings.Contains(got.Hold.Detail, "29.529 GiB net room") {
					t.Fatalf("refusal %d did not expose its retry time and comparison: %+v", i+1, got.Hold)
				}
				if i == 0 {
					q = reopenCancellationQueue(t, q, clock)
				}
				clock.Advance(d - time.Second)
				// A healthy (or delayed) report cannot lift the time fence,
				// even with a stale, exact-looking start for the refused attempt.
				alias := m
				alias.ID, alias.Session, alias.HostAvailableMemoryGB = "sibling", "sibling", 96
				if i == 0 {
					if next := assignedJob(machineCheck(t, q, alias)); next != nil {
						t.Fatal("another worker on the same host bypassed backoff", next)
					}
					alias.Leaving = true // Only the original route polls at expiry.
					machineCheck(t, q, alias)
				}
				if next := assignedJob(machineCheck(t, q, m, offer)); next != nil {
					t.Fatal("delayed heartbeat bypassed backoff", next)
				}
				if i == 5 && mode == "other-host" {
					other := cpuMachine("other", "other", "cpu", 8, 64)
					offer = assignedJob(machineCheck(t, q, other))
					if offer == nil || offer.ID != j.ID {
						t.Fatal("backoff blocked another host", offer)
					}
					machineCheck(t, q, other, offer)
					break
				}
				clock.Advance(time.Second)
				// The identical capacity reading is sufficient after expiry
				// if ordinary placement and exact release also allow it.
				offer = assignedJob(machineCheck(t, q, m))
			}
			if mode == "same-host" {
				if offer == nil {
					t.Fatal("no retry at the capped deadline")
				}
				machineCheck(t, q, m, offer)
			}
			if got := job(t, q, j.ID); len(got.LaunchCapacityWait) != 0 {
				t.Fatal("exact command start did not reset launch backoff", got.LaunchCapacityWait)
			}
		})
	}
}

func TestMemoryAdmissionRefusalDoesNotReclaim(t *testing.T) {
	for _, delta := range []float64{.5, 8} {
		q, clock := fixture(t)
		m := cpuMachine("worker", "host", "cpu", 8, 32)
		m.Devices = []MachineDevice{}
		low := submit(t, q, cpuSpec(1, 8))
		victim := assignedJob(machineCheck(t, q, m))
		s := cpuSpec(1, 4)
		s.Priority = "production"
		demand := submit(t, q, s)
		a := assignedJob(machineCheck(t, q, m, victim))
		if a == nil || a.ID != demand.ID {
			t.Fatal("both jobs should fit ordinary admission", a)
		}
		if err := q.Complete(capacityResult(a, false)); err != nil {
			t.Fatal(err)
		}
		requireState(t, q, low.ID, "running")
		clock.Advance(time.Second)
		m.HostAvailableMemoryGB += delta
		if next := assignedJob(machineCheck(t, q, m, victim)); next != nil {
			t.Fatal("capacity fluctuation bypassed backoff", next)
		}
		requireState(t, q, low.ID, "running")
		clock.Advance(time.Minute - time.Second)
		if next := assignedJob(machineCheck(t, q, m, victim)); next == nil || next.ID != demand.ID {
			t.Fatal("ordinary placement did not resume at expiry", next)
		}
		requireState(t, q, low.ID, "running")
	}
}

func TestMemoryPressureRetryEvidenceAndResume(t *testing.T) {
	for _, mode := range []string{"copied-generation", "advanced-then-unknown", "expired-window", "alternate-host"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := sharedUnifiedMachine()
			m.HostMemoryBudgetGB = 96
			m.Capabilities = append(m.Capabilities, pooledMemoryCapability)
			s := sharedSpec("experiment", 35)
			s.Needs.HostMemoryGB = 25
			j := submit(t, q, s)
			healthy := func() {
				m.HostAvailableMemoryGB = 90
				m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 27, ReserveGB: 4}
			}
			oldRevision := j.PauseRevision
			for i := range 2 {
				healthy()
				a := assignedJob(machineCheck(t, q, m))
				if a == nil || a.Attempt != i+1 {
					t.Fatal("one retry should follow the first stop", a)
				}
				if err := q.Started(result(a, true)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				a.Progress = 7
				if mode == "advanced-then-unknown" && i == 1 {
					a.Progress = 8
				}
				machineCheck(t, q, m, a)
				clock.Advance(time.Second)
				a.Progress = -1
				m.HostAvailableMemoryGB, m.MemoryObservation.UsedGB = 36, 81
				machineCheck(t, q, m, a)
				requireState(t, q, j.ID, "stopping")
				r := result(a, false)
				r.ReleaseOnly = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					wait := 6 * time.Minute
					if mode == "expired-window" {
						wait = 10 * time.Minute
					}
					clock.Advance(wait)
				}
			}
			healthy()
			if mode == "expired-window" {
				if next := assignedJob(machineCheck(t, q, m)); next == nil || next.Attempt != 3 {
					t.Fatal("stops outside the rolling window still held the host", next)
				}
				return
			}
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("third same-host attempt bypassed pressure hold", next)
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Hold == nil || got.Hold.Kind != memoryPressureRetryHold || got.Hold.Expires == nil ||
				!got.Hold.Expires.Equal(clock.Now().Add(15*time.Minute)) || !strings.Contains(got.Hold.Detail, "36.000 GiB room") ||
				!strings.Contains(got.Hold.Detail, "54.000 GiB projected next sample") {
				t.Fatalf("pressure hold lost the trigger comparison or expiry: %+v", got.Hold)
			}
			stop := got.AttemptHistory[len(got.AttemptHistory)-1].PriorityStop
			if stop == nil || stop.PoolPressure == nil || stop.PoolPressure.FreeGB != 40 || stop.PoolPressure.KeepGB != 4 {
				t.Fatalf("exact release did not retain original stop evidence: %+v", stop)
			}
			q = reopenCancellationQueue(t, q, clock)
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("queue restart cleared pressure hold", next)
			}
			if mode == "alternate-host" {
				other := m
				other.ID, other.HostID, other.Session = "alternate", "alternate", "alternate"
				if next := assignedJob(machineCheck(t, q, other)); next == nil || next.Attempt != 3 {
					t.Fatal("pressure hold blocked an alternate host after release", next)
				}
				return
			}
			if mode == "advanced-then-unknown" {
				clock.Advance(15*time.Minute - time.Second)
				if next := assignedJob(machineCheck(t, q, m)); next != nil {
					t.Fatal("pressure time fence ended early", next)
				}
				clock.Advance(time.Second)
			} else {
				if _, err := q.ResumeJob(j.ID, oldRevision); err == nil {
					t.Fatal("old resume request cleared the new hold")
				}
				if _, err := q.ResumeJob(j.ID, got.PauseRevision); err != nil {
					t.Fatal(err)
				}
			}
			if next := assignedJob(machineCheck(t, q, m)); next == nil || next.Attempt != 3 {
				t.Fatal("pressure hold did not allow an ordinary retry", next)
			}
		})
	}
}
