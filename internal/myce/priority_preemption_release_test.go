package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A release-only receipt precedes asynchronous output publication. The job
// remains open for its final result, but no longer owns compute capacity.
func TestPriorityPreemptionReleasedPublication(t *testing.T) {
	for _, devices := range []bool{false, true} {
		t.Run("devices="+strconv.FormatBool(devices), func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			m := machine("worker", "fake")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
			if devices {
				m = m3Machine()
				m.Devices = m.Devices[:1]
			}
			pull := func() *Job {
				if devices {
					return assignedJob(machineCheck(t, q, m))
				}
				return check(t, q, m, nil).Job
			}
			s := spec("experiment")
			s.Resumable = false
			s.Needs.HostMemoryGB = 4
			input := filepath.Join(t.TempDir(), "input")
			if err := os.WriteFile(input, []byte("input"), 0600); err != nil {
				t.Fatal(err)
			}
			inputRef, err := IdentifyFile(input)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(q.files.blob(inputRef), []byte("input"), 0600); err != nil {
				t.Fatal(err)
			}
			s.Files = FileManifest{"input": inputRef}
			s.Outputs = map[string]string{"out": "out"}
			victim := submit(t, q, s)
			run := pull()
			if run == nil || run.ID != victim.ID || !run.storedFileResults() {
				t.Fatal("missing stored-file experiment", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			released := result(run, false)
			released.ReleaseOnly = true
			if err := q.Complete(released); err != nil {
				t.Fatal(err)
			}
			pending := job(t, q, victim.ID)
			if pending.State != "running" || !pending.ReleaseConfirmed || len(pending.Copies) != 1 || !pending.Copies[0].ReleaseConfirmed || len(pending.OutputRefs) != 0 {
				t.Fatal("release accepted a result or lost exact release", pending)
			}
			if !strings.Contains(pending.Reason, "execution released; awaiting final result") {
				t.Error("missing publication diagnostic", pending.Reason)
			}
			owner := q.s.Machines[m.ID]
			if owner.HostMemoryInUseGB != 0 || owner.CPUInUseCores != 0 {
				t.Error("released execution still reserves host resources", owner.HostMemoryInUseGB, owner.CPUInUseCores)
			}
			for _, target := range q.machineTargets(owner) {
				if target.GPU == "cpu" {
					continue
				}
				if !q.workerBatchIdle(target) || q.reclaimableGPUMemory(target, "production") != target.FreeMemoryGB {
					t.Error("released execution reserves a slot or supplies phantom GPU memory", target.Device)
				}
				for _, usage := range q.deviceUsage(target) {
					if usage.Job == victim.ID {
						t.Error("released execution still charged compute usage", usage)
					}
				}
			}
			demand := spec("production")
			demand.Needs.HostMemoryGB = 4
			production := submit(t, q, demand)
			if got := job(t, q, production.ID); strings.Contains(got.Reason, "batch slot allocated") {
				t.Error("released execution still reported as a slot blocker", got.Reason)
			}
			next := pull()
			if next == nil || next.ID != production.ID {
				t.Fatalf("production not admitted on first pull after release: %+v", next)
			}
			if err := q.Started(result(next, false)); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, victim.ID); got.State != "running" || got.PreemptionCount != 0 || got.Cancellation != nil {
				t.Fatal("pending publication was interrupted", got)
			}
			if err := q.Started(result(run, false)); !errors.Is(err, ErrStale) {
				t.Error("released attempt can start again", err)
			}
			if err := q.AuthorizeLaunch(HostLaunch{Machine: run.Machine, Session: run.Session, Kind: "batch", Key: run.ID, Attempt: strconv.Itoa(run.Attempt)}); !errors.Is(err, ErrStale) {
				t.Error("released attempt can launch again", err)
			}
			// Publication and exact final-result replay remain valid while the
			// replacement runs. No unverified result was accepted by release.
			output := filepath.Join(t.TempDir(), "out")
			if err := os.WriteFile(output, []byte("retained output"), 0600); err != nil {
				t.Fatal(err)
			}
			ref, err := IdentifyFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(q.files.blob(ref), []byte("retained output"), 0600); err != nil {
				t.Fatal(err)
			}
			final := result(run, true)
			final.OutputRefs = FileManifest{"out": ref}
			if err := q.Complete(final); err != nil {
				t.Fatal("delayed publication rejected", err)
			}
			if got := job(t, q, run.ID); got.State != "succeeded" || got.OutputRefs["out"] != ref {
				t.Fatal("delayed output lost", got)
			}
			requireState(t, q, next.ID, "running")
		})
	}
}

func TestPriorityPreemptionReleasedFanOut(t *testing.T) {
	q, clock, m, demand := fanOutFixture(t, 2)
	s := cpuSpec(1, 1)
	s.Priority = "experiment"
	victim := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != victim.ID {
		t.Fatal("no experiment", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	released := result(run, false)
	released.ReleaseOnly = true
	if err := q.Complete(released); err != nil {
		t.Fatal(err)
	}
	demand.Priority = "production"
	parent := submit(t, q, demand)
	next := check(t, q, m, nil).Job
	if next == nil || next.Batch == nil || next.Batch.Parent != parent.ID {
		t.Fatalf("production fan-out failed to materialize on free capacity: %+v", next)
	}
	if err := q.Started(result(next, false)); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, victim.ID, "running")
}

func TestPriorityPreemptionAfterReleasedAttempt(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices = m.Devices[:1]
	victim := submit(t, q, spec("experiment"))
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != victim.ID {
		t.Fatal("no experiment", first)
	}
	// A completed interrupted attempt releases custody, then retries under
	// the same job. Its new unstarted offer legitimately reserves the slot.
	finishInterrupted(t, q, first)
	clock.Advance(time.Second)
	second := assignedJob(machineCheck(t, q, m))
	if second == nil || second.ID != victim.ID || second.Attempt != first.Attempt+1 {
		t.Fatal("no retry", second)
	}
	if second.ReleaseConfirmed || !second.Copies[0].ReleaseConfirmed {
		t.Fatal("retry did not distinguish current and historical custody", second)
	}
	oldRelease := result(first, false)
	oldRelease.ReleaseOnly = true
	if err := q.Complete(oldRelease); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, victim.ID); got.ReleaseConfirmed || got.Attempt != second.Attempt {
		t.Fatal("old release discharged the new reservation", got)
	}
	production := submit(t, q, spec("production"))
	got := job(t, q, victim.ID)
	if got.State != "stopping" || priorityStop(got) == nil || priorityStop(got).Demand != production.ID {
		t.Fatal("released previous attempt hid slot-holding retry from preemption", got)
	}
	if next := assignedJob(machineCheck(t, q, m, second)); next != nil {
		t.Fatal("replacement admitted before exact cleanup", next)
	}
	ack := result(second, false)
	ack.ReleaseOnly = true
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	if next := assignedJob(machineCheck(t, q, m)); next == nil || next.ID != production.ID {
		t.Fatal("production not admitted after preempted reservation released", next)
	}
}
