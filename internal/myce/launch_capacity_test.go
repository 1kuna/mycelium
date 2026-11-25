package myce

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	if next := check(t, q, m, nil).Job; next != nil {
		t.Fatal("exact release bypassed retry time", next)
	}
	clock.Advance(time.Minute - time.Second)
	if next := check(t, q, m, nil).Job; next == nil || next.ID != j.ID || next.Attempt != 2 {
		t.Fatal("exact release and elapsed backoff did not permit capacity retry", next)
	}
}

// Real HTTP-ready native helpers reach the member gate on every attempt. Queue
// time is injected; neither a missing Started nor its presence proves main exec.

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
			if reply.Job != nil {
				t.Fatal("clean restart bypassed physical-host backoff", reply.Job)
			}
			clock.Advance(time.Minute - time.Second)
			next := check(t, q, m, nil).Job
			if next == nil || next.ID != j.ID || next.Attempt != 2 {
				t.Fatal("clean restart did not resume capacity demand after backoff", next)
			}
			if tier == "production" && (next.StartWindow != offer.StartWindow || !next.Spec.StartBy.Equal(clock.Now().Add(time.Minute))) {
				t.Fatal("fresh session did not restore relative startup budget")
			}
			finish(t, q, next, true)
		})
	}
}

// A cold qualified job has no learned prior. Temporary exploration refusals
// must wait just like learned demand, including exact custody of an old helper.

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

// Startup waiting resolves the generated-deadline backlog without rescanning
// retained attempts per job/target or retaining evidence across mutations.
