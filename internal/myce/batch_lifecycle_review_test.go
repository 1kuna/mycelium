package myce

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBatchLifecycleRetryHoldUsesAllocatedTarget(t *testing.T) {
	for _, target := range []string{"cpu", "1", "0,1"} {
		t.Run(target, func(t *testing.T) {
			q, _ := fixture(t)
			m := m3Machine()
			m.CPUExtensionFingerprint = "cpu-runtime"
			m.Devices[0].Fingerprint, m.Devices[1].Fingerprint = "runtime-0", "runtime-1"
			m.Devices[0].ExtensionFingerprint, m.Devices[1].ExtensionFingerprint = "extension-0", "extension-1"
			s := cpuSpec(1, 1)
			if target != "cpu" {
				s = spec("experiment")
				s.Needs.GPU = &GPUSelection{Devices: strings.Split(target, ",")}
				if target == "0,1" {
					s.Needs.MemoryGB = 12
				}
			}
			j := submit(t, q, s)
			a := assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != j.ID {
				t.Fatal("missing allocated attempt", a)
			}
			r := result(a, false)
			r.Interrupted = true
			r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if target == "0,1" {
				m.Devices[0].FreeMemoryGB = 7 // Reorders the same group in placement.
			}
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("unchanged projected target retried", next.ID, next.AllocatedDevices)
			}
		})
	}
}

func TestBatchLifecycleRetryEvidenceIgnoresUnrelatedPreparation(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	j := &Job{ID: "held", Machine: m.ID, Spec: spec("experiment"), ExecutionCause: &RuntimeCause{Operation: "launch", Class: environmentFailed}}
	j.Spec.Environment = Environment{Name: "needed"}
	m.Environments = []string{"needed", "unrelated"}
	m.EnvironmentReports = []EnvironmentReport{{Key: "needed", State: "ready"}, {Key: "unrelated", State: "building"}}
	q.s.Machines[m.ID] = &m
	q.holdUnproductiveInfrastructure(j)
	slices.Reverse(m.EnvironmentReports)
	slices.Reverse(m.Environments)
	m.EnvironmentReports[0].State = "ready"
	if !q.batchRetryBlocked(j, &m) {
		t.Fatal("report order/unrelated build released hold")
	}
	m.EnvironmentReports[1].State = "rebuilt"
	if q.batchRetryBlocked(j, &m) {
		t.Fatal("relevant preparation did not release hold")
	}
}

func TestBatchLifecycleRequalificationReleasesHold(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	e := fakeExtension(t)
	j := &Job{ID: "held", Machine: m.ID, Spec: spec("experiment"), Extensions: []Extension{e}, ExecutionCause: &RuntimeCause{Operation: "launch", Class: launchRefused}}
	j.Spec.Kind = "sample"
	r := newExtensionReport(e, &m, j.Spec.workType())
	r.Setup, r.Qualified, r.CheckID = true, false, "failed-check"
	m.ExtensionReports = []ExtensionReport{r}
	q.s.Machines[m.ID] = &m
	q.holdUnproductiveInfrastructure(j)
	if !q.batchRetryBlocked(j, &m) {
		t.Fatal("unchanged failed qualification released hold")
	}
	m.ExtensionReports[0].Qualified = true
	m.ExtensionReports[0].CheckID = "accepted-check"
	if q.batchRetryBlocked(j, &m) {
		t.Fatal("accepted requalification did not permit retry")
	}
}

func TestBatchLifecycleProductionBudgetDoesNotDisableOtherHolds(t *testing.T) {
	for _, priority := range []string{"experiment", "deadline"} {
		t.Run(priority, func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.SetProductionInfrastructureRetries(2); err != nil {
				t.Fatal(err)
			}
			s := spec(priority)
			if priority == "deadline" {
				deadline := clock.Now().Add(time.Hour)
				s.Deadline = &deadline
			}
			submit(t, q, s)
			m := machine("worker", "fake")
			a := check(t, q, m, nil).Job
			r := result(a, false)
			r.Interrupted = true
			r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if next := check(t, q, m, nil).Job; next != nil {
				t.Fatal("production budget disabled no-progress hold", next.ID)
			}
		})
	}
}

func TestBatchLifecycleRestoredNASGenerationIsNotNewProgress(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("production"))
	m := machine("worker", "fake")
	for attempt := range 2 {
		a := check(t, q, m, nil).Job
		if a == nil || a.ID != j.ID {
			t.Fatal("missing recovery attempt", a)
		}
		if err := q.Started(result(a, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		dir := filepath.Join(q.root, attemptCheckpoint(*a))
		if filepath.IsAbs(attemptCheckpoint(*a)) {
			dir = attemptCheckpoint(*a)
		}
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":7}}`), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: a.ID, Attempt: a.Attempt, Progress: 7}}); err != nil {
			t.Fatal(err)
		}
		if attempt == 1 {
			if grace := q.batchStopGrace(q.s.Jobs[j.ID]); grace != unmeasuredBatchGrace {
				t.Fatal("restoration manufactured a checkpoint cadence", grace)
			}
		}
		r := result(a, false)
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: launchRefused}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if attempt == 0 {
			q = reopenCancellationQueue(t, q, clock)
		}
	}
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("restored generation counted as fresh progress", next.ID)
	}
}

func TestBatchLifecycleFanOutRelativeWindowWithAbsoluteDeadline(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 2)
	s.Priority, s.StartWindow = "production", "60s"
	deadline := clock.Now().Add(time.Hour)
	s.Deadline = &deadline
	q.s.WorkObservations = nil // Unknown curves use one serial piece per wave.
	parent := submit(t, q, s)
	for range 2 {
		clock.Advance(90 * time.Second)
		wave := check(t, q, m, nil).Job
		if wave == nil || wave.Batch == nil || wave.Batch.Parent != parent.ID || !wave.Spec.StartBy.Equal(clock.Now().Add(time.Minute)) {
			t.Fatal("parent relative window expired valid wave", wave)
		}
		if err := q.Started(result(wave, false)); err != nil {
			t.Fatal(err)
		}
		finish(t, q, wave, true)
	}
	if got := job(t, q, parent.ID); got.State != "succeeded" {
		t.Fatal("fan-out failed", got.State, got.Reason)
	}
}

func TestBatchLifecycleMainExitPreservesGPUDrainDeadline(t *testing.T) {
	w := recoveryWorker(t)
	clock := &manualWorkerClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
	w.Clock = clock
	w.memoryCeilings = fakeCeilings(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = w.Machine.Session
	j := recoveryJob(t, []string{"sh", "-c", `trap 'exit 0' TERM; sh -c 'trap "" TERM; touch "$1"; while :; do sleep .01; done' fixture "{work}/child" & touch "{work}/main"; while :; do sleep .01; done`})
	j.Spec.Resumable = false
	var p *process
	await(t, func() bool {
		p, err = w.start(s, j)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { clock.Advance(time.Hour); p.enforceGrace(clock.Now()) }()
	nativeFile(t, p.job.Spec.Run.Workdir, "main")
	nativeFile(t, p.job.Spec.Run.Workdir, "child")
	p.stopDirective(clock.Now(), time.Minute, false, false)
	await(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.outcome.Exit != nil })
	clock.Advance(11 * time.Second)
	p.mu.Lock()
	deadline, killed := p.stopAt, p.killed
	p.mu.Unlock()
	if deadline == nil || deadline.Sub(clock.Now()) != 49*time.Second || killed {
		t.Fatal("main exit shortened queue drain deadline", deadline, killed)
	}
	select {
	case <-p.done:
		t.Fatal("checkpoint descendant lost its drain grace")
	default:
	}
	clock.Advance(49 * time.Second)
	p.enforceGrace(clock.Now())
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("drain deadline did not finish cleanup")
	}
}
