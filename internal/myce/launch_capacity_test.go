package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
			if deadline == "generated" && got.State != "queued" {
				t.Fatal("generated deadline killed capacity wait", got)
			}
			if (deadline == "absolute" || deadline == "relative") && got.State != "failed" {
				t.Fatal("caller deadline ignored", got)
			}
			release := result(offer, false)
			release.ReleaseOnly = true
			if err := q.Complete(release); err != nil {
				t.Fatal(err)
			}
			if deadline == "generated" {
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
	s := spec("production")
	s.Files, s.Diagnostics = FileManifest{"input": ref}, &DiagnosticSpec{}
	j := submit(t, q, s)
	offer := check(t, q, m, nil).Job
	r := capacityResult(offer, false)
	r.Diagnostics = &AttemptDiagnostics{Files: FileManifest{"summary.json": ref, "log-tail.txt": ref}}
	clock.Advance(ProductionStartTimeout + time.Second)
	// No maintenance runs between expiry and receipt.
	if err := q.Complete(r); err != nil {
		t.Fatal("valid diagnostic refusal rejected after generated cutoff", err)
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
				q.s.Machines[m.ID].Gone = true
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
