package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The retained incident has the worker comparison but not its matching queue
// snapshot. Replay both numbers without attributing the difference to a sensor,
// transport delay or changing reservations that the evidence cannot distinguish.
func TestAssignChurnPrelaunchGrowthBoundary(t *testing.T) {
	for _, pair := range [][2]float64{{17.106637954711914, 17.005722045898438}, {16.86988639831543, 16.814495086669922}} {
		q, clock := fixture(t)
		m := cpuMachine("worker", "host", "cpu", 16, 32)
		m.Devices = []MachineDevice{}
		holder := submit(t, q, cpuSpec(4, pair[0]-4))
		running := assignedJob(machineCheck(t, q, m))
		if running == nil || running.ID != holder.ID {
			t.Fatal("missing holder")
		}
		next := submit(t, q, cpuSpec(1, 4))
		m.HostAvailableMemoryGB = pair[0]
		report := AttemptReport{Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Session: running.Session, Started: true}
		r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}})
		if err != nil || assignedJob(r) == nil || assignedJob(r).ID != next.ID {
			t.Fatalf("boundary assignment missing: %v", err)
		}
		w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return pair[0], nil }}
		if err := localAttemptAdmission(w, []Job{*running}, *assignedJob(r), nil); err != nil {
			t.Fatalf("identical inputs disagree (own charge must occur once): %v", err)
		}
		clock.Advance(2 * time.Second)
		w.HostMemorySource = func(context.Context) (float64, error) { return pair[1], nil }
		err = localAttemptAdmission(w, []Job{*running}, *assignedJob(r), nil)
		var refusal *admissionError
		if !errors.As(err, &refusal) || refusal.detail.Subcheck != "live_host_growth" || refusal.detail.Required != pair[0] || *refusal.detail.Available != pair[1] {
			t.Fatalf("did not replay retained comparison: %v", err)
		}
	}
}

func assignChurnWaveFixture(t *testing.T, ram float64) (*Queue, *workerFakeClock, Machine, Spec) {
	t.Helper()
	q, clock, m, wave := fanOutFixture(t, 1)
	m.Devices = []MachineDevice{}
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	check(t, q, m, nil)
	wave.Priority, wave.SizeHint.HostMemoryGB = "production", ram
	return q, clock, m, wave
}

func TestAssignChurnPendingProductionWave(t *testing.T) {
	q, clock, m, wave := assignChurnWaveFixture(t, 32)
	h := cpuSpec(2, 16)
	h.Priority = "production"
	holder := submit(t, q, h)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != holder.ID {
		t.Fatal("missing production holder", running)
	}
	demand := submit(t, q, wave)
	clock.Advance(time.Second)
	lower := submit(t, q, cpuSpec(1, 4))
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil {
		t.Fatalf("started %s while pending wave %s will need its RAM (consumer %s)", got.ID, demand.ID, lower.ID)
	}
	finish(t, q, running, true)
	got := assignedJob(machineCheck(t, q, m))
	if got == nil || got.Batch == nil || got.Batch.Parent != demand.ID {
		t.Fatal("production did not start promptly after capacity release", got)
	}
	if job(t, q, lower.ID).PreemptionCount != 0 {
		t.Fatal("consumer paid an avoidable preemption")
	}
}

func TestAssignChurnProductionAfterRunningWave(t *testing.T) {
	q, clock, m, wave := assignChurnWaveFixture(t, 16)
	parent := submit(t, q, wave)
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.Batch == nil || running.Batch.Parent != parent.ID {
		t.Fatal("missing running wave", running)
	}
	production := cpuSpec(1, 32)
	production.Priority = "production"
	demand := submit(t, q, production)
	clock.Advance(time.Second)
	lower := submit(t, q, cpuSpec(1, 4))
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil {
		t.Fatal("running wave's eventual release was hidden from production admission", got.ID)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != lower.ID {
		t.Fatal("withdrawn demand still withheld capacity", got)
	}
}

func TestAssignChurnProductionWaveUsesFreeTarget(t *testing.T) {
	for _, mode := range []string{"serial"} {
		t.Run(mode, func(t *testing.T) {
			q, _, m, wave := assignChurnWaveFixture(t, 32)
			peer := cloneMachine(&m)
			peer.ID, peer.HostID, peer.Session = "free-worker", "free-host", "free-session"
			e := registeredExtension(t, q, wave.Extension)
			report := newExtensionReport(e, &peer, wave.workType())
			report.Setup, report.Qualified, report.CheckID = true, true, "peer-check"
			peer.ExtensionReports = []ExtensionReport{report}
			check(t, q, peer, nil)
			h := cpuSpec(2, 16)
			h.Priority, h.AllowedMachines = "production", []string{m.ID}
			submit(t, q, h)
			running := assignedJob(machineCheck(t, q, m))
			if running == nil {
				t.Fatal("missing occupied target")
			}
			demand := submit(t, q, wave)
			x := cpuSpec(1, 4)
			x.AllowedMachines = []string{m.ID}
			lower := submit(t, q, x)
			if got := assignedJob(machineCheck(t, q, m, running)); got == nil || got.ID != lower.ID {
				t.Fatal("consumer withheld despite an immediately usable production target", got)
			}
			if got := assignedJob(machineCheck(t, q, peer)); got == nil || got.Batch == nil || got.Batch.Parent != demand.ID {
				t.Fatal("production did not use free target", got)
			}
		})
	}
}

func TestAssignChurnExistingConsumerStillYields(t *testing.T) {
	q, _, m, wave := assignChurnWaveFixture(t, 32)
	h := cpuSpec(2, 16)
	h.Priority = "production"
	submit(t, q, h)
	holder := assignedJob(machineCheck(t, q, m))
	submit(t, q, cpuSpec(1, 4))
	consumer := assignedJob(machineCheck(t, q, m, holder))
	if holder == nil || consumer == nil {
		t.Fatal("missing initial executions")
	}
	demand := submit(t, q, wave)
	finish(t, q, holder, true)
	stopped := job(t, q, consumer.ID)
	if stopped.State != "stopping" || priorityStop(stopped) == nil || priorityStop(stopped).Demand != demand.ID {
		t.Fatal("production did not preempt existing lower-tier work", stopped.State)
	}
	finishInterrupted(t, q, consumer)
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.Batch == nil || got.Batch.Parent != demand.ID {
		t.Fatal("production did not start after exact release", got)
	}
}
