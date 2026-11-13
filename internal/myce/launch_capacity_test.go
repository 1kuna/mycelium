package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func capacityResult(j *Job, pending bool) Result {
	r := result(j, false)
	r.Interrupted, r.ReleasePending = true, pending
	r.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", Admission: admissionDetail(refuseAdmission("live_host_growth", "GiB", 4, 3, false), *j)}
	r.ExecutionEvidence = unstartedExecution(j.Attempt)
	r.ExecutionEvidence.Sidecars = executionMayHaveRun
	return r
}

// Older workers label the post-Started local refusal io_error. Positive
// no-main-exec evidence still requires an uncharged wait, not a lost attempt.
func TestLaunchCapacityLateLegacyRefusal(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 96
	j := submit(t, q, spec("production"))
	a := check(t, q, m, nil).Job
	if err := q.Started(result(a, true)); err != nil {
		t.Fatal(err)
	}
	r := capacityResult(a, true)
	r.ExecutionCause.Class = "io_error"
	// Unknown or possible main execution cannot acquire this compatibility
	// exemption merely by attaching a memory predicate to a generic I/O error.
	for _, evidence := range []*ExecutionEvidence{nil, {Attempt: a.Attempt, Main: executionMayHaveRun}} {
		unknown := r
		unknown.ExecutionEvidence = evidence
		if launchCapacityRefusal(unknown) {
			t.Fatal("unproven refusal exempted", evidence)
		}
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Copies[0].LossCharged || got.Copies[0].ReleaseConfirmed {
		t.Fatalf("legacy refusal lost budget or custody: %+v", got)
	}
	clock.Advance(time.Second)
	m.HostAvailableMemoryGB++
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("unreleased supervisor reused", next)
	}
	release := result(a, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	if next := check(t, q, m, nil).Job; next == nil || next.ID != j.ID || next.Attempt != 2 {
		t.Fatal("exact release did not permit capacity retry", next)
	}
}

// Real HTTP-ready native helpers reach the member gate on every attempt. Queue
// time is injected; neither a missing Started nor its presence proves main exec.
func TestLaunchCapacityNativeHelperRepeated(t *testing.T) {
	q, clock := fixture(t)
	s := nativeSpec(t)
	s.Priority, s.SubmissionKey = "production", "capacity-operation"
	s.Run.Env["NATIVE_TEST_BLOCK"] = "0"
	s.Resumable = false
	m := machine("worker", "fake", "host")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	j := submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	checkpoint := filepath.Join(t.TempDir(), "job")
	for attempt := 1; attempt <= 5; attempt++ {
		offer := check(t, q, m, nil).Job
		if offer == nil || offer.ID != j.ID || offer.Attempt != attempt {
			t.Fatalf("missing attempt %d: %+v", attempt, offer)
		}
		w := recoveryWorker(t)
		w.Clock, w.Machine, w.Client = clock, m, Client{URL: server.URL, Token: "test"}
		w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
		w.nativeListenerProbe = func(string, int) error { return nil }
		calls := 0
		w.batchAdmission = func(context.Context, HostLaunch) error {
			calls++
			// Alternate refusal before and after durable Started authorization.
			if attempt <= 4 && calls == 4+attempt%2 {
				return refuseAdmission("live_host_growth", "GiB", 4, 3, false)
			}
			return nil
		}
		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		store.state.Session = m.Session
		// The queue reply carries a relative checkpoint; the worker loop joins its root.
		offer.Checkpoint = checkpoint
		p, err := startAfterFixtureInputs(t, w, store, *offer)
		if err != nil || p == nil {
			store.close()
			t.Fatalf("native group: %v", err)
		}
		var r Result
		select {
		case f := <-p.done:
			if f.err != nil {
				t.Fatal(f.err)
			}
			r = f.result
		case <-time.After(testWaitTimeout):
			p.stop(clock.Now(), 0, true)
			t.Fatal("native attempt did not finish")
		}
		store.close()
		if attempt == 5 {
			if !r.Success {
				t.Fatalf("main did not start after capacity changed: %+v", r)
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			break
		}
		if started := job(t, q, j.ID).StartedAt != nil; started != (attempt%2 == 1) {
			t.Fatalf("Started race not exercised: attempt=%d started=%t", attempt, started)
		}
		c, e := r.ExecutionCause, r.ExecutionEvidence
		if !launchCapacityRefusal(r) || c.Admission == nil || c.Admission.Subcheck != "live_host_growth" || c.PrelaunchRefused || e == nil || e.Main != executionNotStarted || e.Sidecars != executionMayHaveRun {
			t.Fatalf("lost member refusal/custody: %+v cause=%+v evidence=%+v", r, c, e)
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		clock.Advance(ProductionStartTimeout + time.Second)
		if next := check(t, q, m, nil).Job; next != nil {
			t.Fatal("unchanged capacity re-offered", next)
		}
		got := job(t, q, j.ID)
		if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || submit(t, q, s).ID != j.ID {
			t.Fatalf("capacity consumed budget or identity: %+v", got)
		}
		if err := q.Complete(r); !errors.Is(err, ErrStale) {
			t.Fatal("old result not fenced", err)
		}
		m.HostAvailableMemoryGB++
	}
	got := job(t, q, j.ID)
	if got.State != "succeeded" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal(got)
	}
}

func TestLaunchCapacityExactReleaseAndCallerDeadlines(t *testing.T) {
	for _, deadline := range []string{"generated", "absolute", "relative", "cancel"} {
		t.Run(deadline, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake", "host")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			s := spec("production")
			if deadline == "absolute" {
				at := clock.Now().Add(time.Second)
				s.StartBy = &at
			}
			if deadline == "relative" {
				s.StartWindow = "1s"
			}
			j := submit(t, q, s)
			offer := check(t, q, m, nil).Job
			cutoff := *offer.Spec.StartBy
			clock.Advance(500 * time.Millisecond)
			if err := q.Started(result(offer, true)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(250 * time.Millisecond)
			r := capacityResult(offer, true)
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); !got.Spec.StartBy.Equal(cutoff) {
				t.Fatal("Started capacity refusal changed cutoff", got.Spec.StartBy, cutoff)
			}
			m.HostAvailableMemoryGB++
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("unreleased helper reused", got)
			}
			if got := check(t, q, machine("elsewhere", "fake"), nil).Job; got != nil {
				t.Fatal("unreleased helper overlapped on another host", got)
			}
			if deadline == "cancel" {
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(ProductionStartTimeout + time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if (deadline == "generated" || deadline == "relative") && got.State != "queued" {
				t.Fatal("relative deadline killed capacity wait", got)
			}
			if deadline == "absolute" && got.State != "failed" {
				t.Fatal("caller deadline ignored", got)
			}
			release := result(offer, false)
			release.ReleaseOnly = true
			if err := q.Complete(release); err != nil {
				t.Fatal(err)
			}
			if deadline == "generated" || deadline == "relative" {
				if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("exact release did not permit start", got)
				}
			} else if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("deadline/cancelled work assigned", got)
			}
		})
	}
}

func TestLaunchCapacityInitialWait(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake", "host")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 1
	check(t, q, m, nil)
	s := spec("production")
	s.Needs.HostMemoryGB = 4
	j := submit(t, q, s)
	clock.Advance(ProductionStartTimeout + time.Second)
	if got := job(t, q, j.ID); got.State != "queued" || got.Attempt != 0 {
		t.Fatal("capacity expired before its first offer", got)
	}
	m.HostAvailableMemoryGB = 16
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID || !got.Spec.StartBy.After(clock.Now()) {
		t.Fatal("capacity recovery did not get startup allowance", got)
	}
}

func TestLaunchCapacityOrdinaryPreemption(t *testing.T) {
	for _, tier := range []string{"experiment", "production"} {
		t.Run(tier, func(t *testing.T) {
			q, _ := fixture(t)
			m := cpuMachine("worker", "host", "fake", 8, 32)
			m.TotalMemoryGB = 8
			m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "card", Selector: "0", BudgetGB: 8, FreeMemoryGB: 8, CapabilityProfile: m.CapabilityProfile}}
			v := cpuSpec(1, 4)
			v.Priority = tier
			submit(t, q, v)
			victim := assignedJob(machineCheck(t, q, m))
			s := cpuSpec(1, 4)
			s.Priority = "production"
			j := submit(t, q, s)
			offer := assignedJob(machineCheck(t, q, m, victim))
			if offer == nil || offer.ID != j.ID {
				t.Fatal("missing co-tenant", offer)
			}
			if err := q.Complete(capacityResult(offer, false)); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.Losses != 0 || got.InfrastructureRetries != 0 || got.State != "queued" {
				t.Fatal("capacity contention spent demand budget", got.State, got.Losses, got.InfrastructureRetries)
			}
			if got := job(t, q, victim.ID); (got.State == "stopping") != (tier == "experiment") {
				t.Fatalf("wrong tier preemption: %s %+v", tier, got)
			}
			if got := assignedJob(machineCheck(t, q, m, victim)); got != nil {
				t.Fatal("started before release", got)
			}
			if err := q.Complete(result(victim, true)); err != nil {
				t.Fatal(err)
			}
			if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != j.ID {
				t.Fatal("did not start after real release", got)
			}
		})
	}
}

// Stored failure manifests take a separate verification transaction before
// complete(). The typed refusal must protect that path's generated cutoff too.
func TestLaunchCapacityDiagnosticDeadlineRace(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	path := filepath.Join(t.TempDir(), "diagnostic")
	if err := os.WriteFile(path, []byte("capacity diagnostic"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UploadJobInput(context.Background(), path, ref); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	healthy := machine("healthy", "cpu")
	check(t, q, healthy, nil)
	s := spec("production")
	s.Files, s.Diagnostics = FileManifest{"input": ref}, &DiagnosticSpec{}
	j := submit(t, q, s)
	offer := check(t, q, m, nil).Job
	r := capacityResult(offer, false)
	r.Diagnostics = &AttemptDiagnostics{Files: FileManifest{"summary.json": ref, "log-tail.txt": ref}}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	syncDir := q.files.syncDir
	q.files.syncDir = func(path string) error {
		close(entered)
		<-release
		return syncDir(path)
	}
	clock.Advance(ProductionStartTimeout + time.Second)
	done := make(chan error, 1)
	go func() { done <- q.Complete(r) }()
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("diagnostic verification did not reach barrier")
	}
	clock.Advance(time.Second)
	maintained := make(chan error, 1)
	go func() { maintained <- q.Tick() }()
	select {
	case err := <-maintained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("diagnostic I/O blocked queue maintenance")
	}
	for elapsed := time.Duration(0); elapsed <= q.machineLossTimeout(m.ID); elapsed += time.Second {
		clock.Advance(time.Second)
		check(t, q, healthy, nil)
	}
	if got := job(t, q, j.ID); got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 || !q.s.Machines[m.ID].Gone {
		t.Fatal("maintenance expired/charged known refusal during diagnostic I/O", got.State, got.Reason, got.Losses, got.InfrastructureRetries, q.s.Machines[m.ID].Gone)
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("valid diagnostic refusal rejected after generated cutoff", err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("diagnostic completion did not finish")
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || got.InfrastructureRetries != 0 || got.Losses != 0 {
		t.Fatal("diagnostic refusal expired or spent budget", got)
	}
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("diagnostic refusal bypassed unchanged hold", next)
	}
}

func TestLaunchCapacityQualificationWindow(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Workload = nil
	s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	s.RequireTarget = true
	s.AllowedMachines = []string{m.ID}
	s.Needs = ResourceNeeds{}
	s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir(), Timeout: "3m"}
	m.HostAvailableMemoryGB = .01
	machineCheck(t, q, m)
	j := submit(t, q, s)
	clock.Advance(MaxProductionStartWindow + time.Second)
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatal("qualification capacity wait expired", got)
	}
	m.HostAvailableMemoryGB = 24
	offer := assignedJob(machineCheck(t, q, m))
	if offer == nil || offer.ID != j.ID || offer.Spec.StartBy == nil || offer.Spec.StartBy.Sub(clock.Now()) != MaxProductionStartWindow {
		t.Fatalf("qualification lost generated startup allowance: offer=%+v state=%s reason=%s", offer, job(t, q, j.ID).State, job(t, q, j.ID).Reason)
	}
	if err := q.Complete(capacityResult(offer, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(MaxProductionStartWindow + time.Second)
	m.HostAvailableMemoryGB++
	offer = assignedJob(machineCheck(t, q, m))
	if offer == nil || offer.ID != j.ID || offer.Spec.StartBy.Sub(clock.Now()) != MaxProductionStartWindow {
		t.Fatal("qualification refusal lost startup allowance", offer)
	}
	if err := q.Started(result(offer, true)); err != nil {
		t.Fatal("qualification immediately expired", err)
	}
	finish(t, q, offer, true)
}

func TestLaunchCapacityInitialRouteUnavailable(t *testing.T) {
	for _, mode := range []string{"gone", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake", "host")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 1
			check(t, q, m, nil)
			s := spec("production")
			s.Needs.HostMemoryGB = 4
			j := submit(t, q, s)
			if !job(t, q, j.ID).StartCapacityWaiting {
				t.Fatal("initial contention was not recorded")
			}
			clock.Advance(ProductionStartTimeout + time.Second)
			if mode == "disabled" {
				if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
					t.Fatal(err)
				}
			} else if err := q.update(func() error {
				q.touchMachine(m.ID).Gone = true
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.State != "failed" || got.StartCapacityWaiting || !strings.Contains(got.Reason, "production") {
				t.Fatal("stale capacity flag suppressed unavailable diagnosis", got)
			}
		})
	}
}

// The same experiment survives ordinary priority preemptions interleaved with
// typed refusals after Started, including consecutive final refusals.
func TestLaunchCapacityExperimentPreemptionSequence(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "fake", 8, 32)
	m.TotalMemoryGB = 8
	m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "card", Selector: "0", BudgetGB: 8, FreeMemoryGB: 8, CapabilityProfile: m.CapabilityProfile}}
	s := cpuSpec(6, 10)
	s.SubmissionKey = "alternating-capacity"
	j := submit(t, q, s)
	for index, action := range []string{"preempt", "refuse", "preempt", "refuse_stop", "refuse"} {
		if backoff := job(t, q, j.ID).ProductionBackoff[hostID(&m)]; clock.Now().Before(backoff.Until) {
			// Repeated real preemptions retain their ordinary admission policy.
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("experiment bypassed production preemption backoff", next)
			}
			clock.Advance(backoff.Until.Sub(clock.Now()))
		}
		offer := assignedJob(machineCheck(t, q, m))
		if offer == nil || offer.ID != j.ID || offer.Attempt != index+1 {
			got := job(t, q, j.ID)
			t.Fatalf("missing experiment attempt %d: state=%s reason=%s", index+1, got.State, got.Reason)
		}
		if err := q.Started(result(offer, true)); err != nil {
			t.Fatal(err)
		}
		// Respect the existing protection after repeated real preemptions.
		clock.Advance(q.preemptionGrace() + time.Second)
		machineCheck(t, q, m, offer)
		if action == "preempt" {
			p := cpuSpec(4, 10)
			p.Priority = "production"
			production := submit(t, q, p)
			if got := job(t, q, j.ID); got.State != "stopping" {
				t.Fatal("ordinary production did not preempt experiment", got.State, got.Reason)
			}
			if next := assignedJob(machineCheck(t, q, m, offer)); next != nil {
				t.Fatal("production overlapped unreleased experiment", next)
			}
			finishInterrupted(t, q, offer)
			next := assignedJob(machineCheck(t, q, m))
			if next == nil || next.ID != production.ID {
				t.Fatal("production did not start after experiment release", next)
			}
			finish(t, q, next, true)
		} else {
			var production *Job
			if action == "refuse_stop" {
				p := cpuSpec(4, 10)
				p.Priority = "production"
				production = submit(t, q, p)
				if got := job(t, q, j.ID); got.State != "stopping" {
					t.Fatal("refusal did not race ordinary preemption", got.State, got.Reason)
				}
			}
			r := capacityResult(offer, true)
			if index == 4 {
				// Older typed workers have no component evidence field.
				r.ExecutionEvidence = nil
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("refused experiment overlapped helper custody", next)
			}
			release := result(offer, false)
			release.ReleaseOnly = true
			if err := q.Complete(release); err != nil {
				t.Fatal(err)
			}
			if production != nil {
				next := assignedJob(machineCheck(t, q, m))
				if next == nil || next.ID != production.ID {
					t.Fatal("production did not start after refused helper release", next)
				}
				finish(t, q, next, true)
			}
			if next := assignedJob(machineCheck(t, q, m)); next != nil {
				t.Fatal("unchanged typed refusal reoffered", next)
			}
			if err := q.Complete(r); !errors.Is(err, ErrStale) {
				t.Fatal("old experiment result not fenced", err)
			}
			m.HostAvailableMemoryGB++
		}
		got := job(t, q, j.ID)
		if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || submit(t, q, s).ID != j.ID {
			t.Fatal("preemption/capacity spent experiment budget or identity", got)
		}
	}
	offer := assignedJob(machineCheck(t, q, m))
	if offer == nil || offer.ID != j.ID {
		t.Fatal("experiment did not start after changed capacity", offer)
	}
	finish(t, q, offer, true)
}

func TestLaunchCapacityProjectionCacheIsolation(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 8, 32)
	m.Devices = []MachineDevice{}
	victim := submit(t, q, cpuSpec(1, 4))
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != victim.ID {
		t.Fatal("missing lower tier", running)
	}
	s := cpuSpec(1, 4)
	s.Priority = "production"
	if err := normalizeResources(&s); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	ordinary := &Job{ID: "ordinary", State: "queued", Spec: s}
	refused := cloneJob(ordinary)
	refused.ID, refused.Attempt, refused.Machine, refused.Session = "refused", 1, m.ID, m.Session
	q.recordLaunchCapacityWait(&refused, capacityResult(&refused, false))
	var target *Machine
	for _, candidate := range q.plannerMachines() {
		if candidate.GPU == "cpu" {
			target = candidate
			break
		}
	}
	if target == nil {
		t.Fatal("missing CPU target")
	}
	for _, order := range [][]*Job{{ordinary, &refused}, {&refused, ordinary}} {
		resources := map[batchResourceKey]batchResourceRoom{}
		for _, demand := range order {
			if !q.batchResourcesFitWithScan(demand, target, resources) {
				t.Fatal("cached projection lost eligible demand", demand.ID)
			}
			plan := resources[resourceKey(demand)].plan
			expected := q.batchResourceProjection(demand, target).plan
			if !reflect.DeepEqual(plan, expected) {
				t.Fatal("cached release plan differs from exact demand", demand.ID, plan, expected)
			}
			if demand.ID == refused.ID {
				if plan == nil || len(plan.jobs) != 1 || plan.jobs[0].ID != victim.ID {
					t.Fatal("refused demand lost measured lower-tier reclaim", plan)
				}
			} else if plan != nil && len(plan.jobs) > 0 {
				t.Fatal("ordinary demand inherited unnecessary stops", plan)
			}
		}
	}
}

func TestLaunchCapacityKnownRefusalRestart(t *testing.T) {
	for _, tier := range []string{"production", "experiment"} {
		t.Run(tier, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("worker", "fake")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			s := spec(tier)
			if tier == "production" {
				s.StartWindow = "1m"
			}
			j := submit(t, q, s)
			offer := check(t, q, m, nil).Job
			if err := q.Started(result(offer, true)); err != nil {
				t.Fatal(err)
			}
			r := capacityResult(offer, true)
			if err := q.update(func() error { q.retainLaunchCapacityRefusal(r); return nil }); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			previous := m.Session
			m.Session = "replacement"
			reply, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous, period: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.Losses != 0 || got.InfrastructureRetries != 0 || len(got.Copies) == 0 || !got.Copies[0].ReleaseConfirmed || got.Copies[0].Recoverable {
				t.Fatal("known refusal restart spent budget or lost exact fence", got.Losses, got.InfrastructureRetries, got.Copies)
			}
			next := reply.Job
			if next == nil || next.ID != j.ID || next.Attempt != 2 {
				t.Fatal("clean restart did not resume capacity demand", next)
			}
			if tier == "production" && (next.StartWindow != offer.StartWindow || !next.Spec.StartBy.Equal(clock.Now().Add(time.Minute))) {
				t.Fatal("fresh session did not restore relative startup budget")
			}
			finish(t, q, next, true)
		})
	}
}

func TestLaunchCapacityInitialLearnedMemory(t *testing.T) {
	for _, deadline := range []string{"generated", "absolute", "relative"} {
		t.Run(deadline, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			r := result(&first, true)
			host := 2.0
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			s.Priority, s.Needs.Memory = "production", "auto"
			if deadline == "absolute" {
				at := clock.Now().Add(time.Second)
				s.StartBy = &at
			}
			if deadline == "relative" {
				s.StartWindow = "1s"
			}
			m.HostAvailableMemoryGB = 1
			check(t, q, m, nil)
			j := submit(t, q, s)
			if next := check(t, q, m, nil).Job; next != nil {
				t.Fatal("learned need bypassed live RAM", next)
			}
			clock.Advance(ProductionStartTimeout + time.Second)
			got := job(t, q, j.ID)
			if deadline == "absolute" {
				if got.State != "failed" {
					t.Fatal("learned sizing suppressed caller cutoff", got.State)
				}
				return
			}
			if got.State != "queued" || !got.StartCapacityWaiting {
				t.Fatal("recipe default hid learned capacity wait", got.State, got.Reason)
			}
			m.HostAvailableMemoryGB = 8
			next := check(t, q, m, nil).Job
			if next == nil || next.ID != j.ID || next.Spec.Needs.HostMemoryGB <= 2 || !next.Spec.StartBy.After(clock.Now()) {
				t.Fatal("learned demand did not resume with startup window", next)
			}
			finish(t, q, next, true)
		})
	}
}

// A cold qualified job has no learned prior. Temporary exploration refusals
// must wait just like learned demand, including exact custody of an old helper.
func TestLaunchCapacityInitialColdMemory(t *testing.T) {
	for _, shortage := range []string{"physical", "release"} {
		for _, caller := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/caller=%v", shortage, caller), func(t *testing.T) {
				q, clock, m, s := observationFixture(t)
				var old *Job
				if shortage == "release" {
					other := cpuSpec(1, 4)
					other.Priority = "production"
					submit(t, q, other)
					old = check(t, q, m, nil).Job
					if old == nil {
						t.Fatal("missing retained owner")
					}
					r := result(old, true)
					r.ReleasePending = true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				} else {
					m.HostAvailableMemoryGB = 0
					check(t, q, m, nil)
				}
				s.Priority, s.Needs.Memory = "production", "auto"
				if caller {
					s.StartWindow = "1s"
				}
				j := submit(t, q, s)
				if next := check(t, q, m, nil).Job; next != nil {
					t.Fatal("cold demand bypassed shortage", next)
				}
				clock.Advance(ProductionStartTimeout + time.Second)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				if got.State != "queued" || !got.StartCapacityWaiting || got.InfrastructureRetries != 0 || got.Losses != 0 {
					t.Fatal("cold capacity wait expired or charged", got.State, got.Reason)
				}
				if old != nil {
					r := result(old, false)
					r.ReleaseOnly = true
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
				}
				m.HostAvailableMemoryGB = 32
				next := check(t, q, m, nil).Job
				if next == nil || next.ID != j.ID || !next.Spec.StartBy.After(clock.Now()) {
					t.Fatal("cold wait did not resume after release", next)
				}
				finish(t, q, next, true)
			})
		}
	}
}

func TestLaunchCapacityKnownRefusalQuarantine(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	j := submit(t, q, spec("production"))
	offer := assignedJob(machineCheck(t, q, m))
	if offer == nil || offer.ID != j.ID {
		t.Fatal("missing offer")
	}
	r := capacityResult(offer, true)
	// This is the persisted, identity-validated state while unlocked diagnostic
	// verification is in progress. A device check-in can race that verification.
	if err := q.update(func() error { q.recordLaunchCapacityWait(q.s.Jobs[j.ID], r); return nil }); err != nil {
		t.Fatal(err)
	}
	m.Devices[0].Reason, m.Devices[0].FreeMemoryGB = "device_fault_xe", 0
	machineCheck(t, q, m, offer)
	got := job(t, q, j.ID)
	if got.State != "running" || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal("quarantine charged known refusal", got.State, got.Losses, got.InfrastructureRetries)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal("quarantine fenced valid refusal completion", err)
	}
	if next := assignedJob(machineCheck(t, q, m)); next != nil {
		t.Fatal("reused unreleased helper on healthy card", next)
	}
	release := result(offer, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	got = job(t, q, j.ID)
	if got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatal("release charged known refusal", got.Losses, got.InfrastructureRetries)
	}
}

func TestLaunchCapacityBoundedExploration(t *testing.T) {
	for _, caller := range []bool{false, true} {
		t.Run(fmt.Sprint(caller), func(t *testing.T) {
			q, clock, linux, target, s := memoryExplorationFixture(t)
			memoryExplorationSeed(t, q, clock, linux, target, s, 3, foreignPeakAboveDarwinAllowance)
			s.Targets = []ExecutionTarget{machineTarget(&target)}
			s.Needs.HostMemoryGB = 1
			occupancy := s
			occupancy.Name = "independent-occupant"
			occupancy.Needs.Memory, occupancy.Needs.HostMemoryGB = "", 1
			busy := memoryExplorationRun(t, q, target, occupancy)
			if caller {
				s.StartWindow = "1s"
			}
			j := submit(t, q, s)
			// Inspect planner state under the same lock as evidence publication.
			q.mu.Lock()
			candidate := q.memoryCandidate(q.s.Jobs[j.ID], q.s.Machines[target.ID])
			q.mu.Unlock()
			if candidate.MemoryEnvelope == nil || candidate.MemoryEnvelope.Refusal != boundedMemoryCapacityWait {
				t.Fatal("fixture did not establish bounded capacity refusal", candidate.MemoryEnvelope)
			}
			if next := check(t, q, target, busy).Job; next != nil {
				t.Fatal("bounded probe bypassed occupied allowance", next)
			}
			clock.Advance(ProductionStartTimeout + time.Second)
			got := job(t, q, j.ID)
			if got.State != "queued" || !got.StartCapacityWaiting || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatal("bounded capacity wait expired or charged", got.State, got.Reason)
			}
			memoryExplorationComplete(t, q, clock, busy, 1, false, false)
			next := check(t, q, target, nil).Job
			if next == nil || next.ID != j.ID || (next.MemoryEnvelope == nil || next.MemoryEnvelope.Refusal != "") || !next.Spec.StartBy.After(clock.Now()) {
				if next == nil {
					t.Fatal("no bounded offer after release")
				}
				t.Fatalf("bounded offer: got=%s want=%s source=%+v cutoff=%v now=%v", next.ID, j.ID, next.MemoryEnvelope, next.Spec.StartBy, clock.Now())
			}
		})
	}
}

// Startup waiting resolves the generated-deadline backlog without rescanning
// retained attempts per job/target or retaining evidence across mutations.
func TestLaunchCapacityDeadlinePlanningCache(t *testing.T) {
	q, _ := largeRetainedHistoryFixture(t, 480)
	full := len(q.s.Jobs)
	generated := false
	for _, j := range q.s.Jobs {
		if j.State == "queued" && j.Spec.Priority == "production" {
			j.StartByExplicit = &generated
		}
	}
	scans, uncached := 0, 0
	q.observeMemoryHistory = func(jobs int, indexed bool) {
		if jobs >= full {
			scans++
			if !indexed {
				uncached++
			}
		}
	}
	for pass := range 2 {
		scans, uncached = 0, 0
		q.maintainStartDeadlines()
		if scans != 1 || uncached != 0 || q.memoryPlanning != nil {
			t.Fatalf("pass %d: scans=%d uncached=%d retained cache=%t", pass, scans, uncached, q.memoryPlanning != nil)
		}
	}
}
