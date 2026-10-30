package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func reservationRequest(id, selector, job string) ReservationRequest {
	return ReservationRequest{ReservationOptions: ReservationOptions{ID: id, Selector: selector, Owner: "validation-agent", Reason: "device qualification", MaxLifetime: "2h0m0s"}, Job: job}
}

func reservationState(t *testing.T, q *Queue, id, state string) Reservation {
	t.Helper()
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	records, err := q.Reservations()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.ID == id {
			if r.State != state {
				t.Fatalf("reservation state: want %s, got %+v", state, r)
			}
			return r
		}
	}
	t.Fatal("missing reservation", id)
	return Reservation{}
}

func TestReservationDrainAdmissionAndRelease(t *testing.T) {
	for _, selector := range []string{"host=machine;devices=0", "host=machine"} {
		t.Run(selector, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			submit(t, q, exclusionSpec(m.ID, "0"))
			existing := assignedJob(machineCheck(t, q, m))
			if existing == nil {
				t.Fatal("missing existing work")
			}
			if err := q.Started(result(existing, false)); err != nil {
				t.Fatal(err)
			}
			bound := submit(t, q, exclusionSpec(m.ID, "0"))
			request := reservationRequest("check", selector, bound.ID)
			r, err := q.CreateReservation(request)
			if err != nil || r.State != "draining" || r.Text != "reserved for validation-agent: device qualification" || !r.CreatedAt.Equal(clock.Now()) {
				t.Fatal("acquisition did not drain", r, err)
			}
			waiting := submit(t, q, exclusionSpec(m.ID, "0"))
			for _, d := range machineCheck(t, q, m, existing).Attempts {
				if d.ID == existing.ID && (d.Stop || d.Kill) {
					t.Fatal("reservation stopped existing Started work", d)
				}
			}
			// Independent work on a sibling is withheld only by a host reservation.
			sibling := submit(t, q, exclusionSpec(m.ID, "1"))
			a := assignedJob(machineCheck(t, q, m, existing))
			if strings.Contains(selector, ";devices=") {
				if a == nil || a.ID != sibling.ID {
					t.Fatal("device reservation lost sibling capacity", a)
				}
				finish(t, q, a, true)
			} else if a != nil {
				t.Fatal("host reservation admitted sibling", a)
			}
			finish(t, q, existing, true)
			reservationState(t, q, r.ID, "ready")
			a = assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != bound.ID {
				t.Fatal("bound work not admitted", a)
			}
			if err := q.AuthorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: "1"}); err != nil {
				t.Fatal("bound launch refused", err)
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal("bound Started refused", err)
			}
			reservationState(t, q, r.ID, "in_use")
			completed := result(a, true)
			completed.ReleasePending = true
			if err := q.Complete(completed); err != nil {
				t.Fatal(err)
			}
			r = reservationState(t, q, r.ID, "waiting_for_release")
			if !r.CleanupPending {
				t.Fatal("terminal result manufactured cleanup", r)
			}
			machineCheck(t, q, m)
			requireState(t, q, waiting.ID, "queued")
			completed.ReleasePending, completed.ReleaseOnly, completed.Session = false, true, "stale-session"
			if err := q.Complete(completed); err != nil {
				t.Fatal(err)
			}
			reservationState(t, q, r.ID, "waiting_for_release")
			completed.Session = a.Session
			if err := q.Complete(completed); err != nil {
				t.Fatal(err)
			}
			r = reservationState(t, q, r.ID, "released")
			if r.CleanupPending || !strings.Contains(r.ReleaseReason, "physically released") {
				t.Fatal("release omitted cleanup evidence", r)
			}
			a = assignedJob(machineCheck(t, q, m))
			if a != nil && a.ID == sibling.ID {
				finish(t, q, a, true)
				a = assignedJob(machineCheck(t, q, m))
			}
			if a == nil || a.ID != waiting.ID {
				t.Fatal("waiting ordinary work not admitted", a)
			}
		})
	}
}

func TestReservationAcquisitionFencesCachedOfferAndPreservesOtherHolds(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	submit(t, q, exclusionSpec(m.ID, "0"))
	offer := assignedJob(machineCheck(t, q, m))
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	r, err := q.CreateReservation(reservationRequest("check", "host=machine;devices=0", bound.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(HostLaunch{Machine: offer.Machine, Session: offer.Session, Kind: "batch", Key: offer.ID, Attempt: "1"}); !errors.Is(err, ErrStale) {
		t.Fatal("cached offer passed launch", err)
	}
	if err := q.Started(result(offer, false)); !errors.Is(err, ErrStale) {
		t.Fatal("cached offer passed Started", err)
	}
	finishInterrupted(t, q, offer)
	if got := job(t, q, offer.ID); got.Losses != 0 || got.InfrastructureRetries != 0 || got.State != "queued" {
		t.Fatal("reservation withdrawal charged an infrastructure retry", got)
	}
	// Independent fences overlap and survive automatic, same-timestamp release.
	changeExclusion(t, q, "host=machine;kind=gpu", "add")
	drain, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "new-operator"})
	if err != nil {
		t.Fatal(err)
	}
	cancelAck(t, q, bound.ID)
	released := reservationState(t, q, r.ID, "released")
	if _, err := q.ReleaseReservation(r.ID, r.Revision, "stale release"); !errors.Is(err, ErrStale) {
		t.Fatal("stale revision accepted", err)
	}
	if !q.s.ComponentExclusions["host=machine;kind=gpu"].Active || q.s.HostMaintenance[m.HostID].DrainOwner != drain.Policy.DrainOwner || !q.maintenanceBlocked(m.HostID) {
		t.Fatal("reservation release cleared independent policy", released)
	}
}

func TestReservationRestartAndLifetimeExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	m := m3Machine()
	machineCheck(t, q, m)
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	request := reservationRequest("durable-check", "host=machine;devices=0", bound.ID)
	r, err := q.CreateReservation(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	reservationState(t, q, r.ID, "draining") // no fresh worker evidence yet
	if r2, err := q.CreateReservation(request); err != nil || !r2.ExpiresAt.Equal(r.ExpiresAt) || r2.Job != bound.ID {
		t.Fatal("caller replay lost binding or renewed lifetime", r2, err)
	}
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != bound.ID {
		t.Fatal("restarted bound work not admitted", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	// Keep the attempt live to isolate reservation expiry from loss detection.
	machineCheck(t, q, m, a)
	r = reservationState(t, q, r.ID, "in_use")
	if r.ReleasedAt != nil || !strings.Contains(r.Text, "reserved past max lifetime: bound job still running") || !r.CleanupPending || job(t, q, a.ID).State != "running" {
		t.Fatal("lifetime released live validation, hid its reason or cancelled work", r)
	}
	waiting := submit(t, q, exclusionSpec(m.ID, "0"))
	if next := assignedJob(machineCheck(t, q, m, a)); next != nil {
		t.Fatal("lifetime admitted unrelated work", next)
	}
	requireState(t, q, waiting.ID, "queued")
	request.Selector = r.Selector
	if replay, err := q.CreateReservation(request); err != nil || replay.ReleasedAt != nil || !replay.ExpiresAt.Equal(r.ExpiresAt) {
		t.Fatal("overdue replay changed reservation", replay, err)
	}
	done := result(a, true)
	done.ReleasePending = true
	if err := q.Complete(done); err != nil {
		t.Fatal(err)
	}
	r = reservationState(t, q, r.ID, "waiting_for_release")
	if r.ReleasedAt != nil || !r.CleanupPending || !strings.Contains(r.Text, "physical cleanup unconfirmed") {
		t.Fatal("lifetime released terminal validation before cleanup", r)
	}
	done.ReleasePending, done.ReleaseOnly = false, true
	if err := q.Complete(done); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "released")
	if next := assignedJob(machineCheck(t, q, m)); next == nil || next.ID != waiting.ID {
		t.Fatal("ordinary work did not follow exact cleanup", next)
	}
}

func TestReservationRetiredWorkerDoesNotBlockReplacement(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	// Conversion retains historical inventory; exact old custody is still
	// enumerated by hostObligations, but retired freshness is not required.
	q.s.Machines["retired-worker"] = &Machine{ID: "retired-worker", HostID: m.HostID, Gone: true,
		Replacement: &MachineReplacement{Machine: m.ID, Device: "0"}}
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	r, err := q.CreateReservation(reservationRequest("replacement-check", "host=machine;devices=0", bound.ID))
	if err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "ready")
	if a := assignedJob(machineCheck(t, q, m)); a == nil || a.ID != bound.ID {
		t.Fatal("historical worker blocked bound check on its live replacement", a)
	}
}

func TestReservationLifetimeOnlyReleasesLostBinding(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	r, err := q.CreateReservation(reservationRequest("lost-check", "host=machine;devices=0", bound.ID))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	r = reservationState(t, q, r.ID, "draining")
	if r.ReleasedAt != nil || !strings.Contains(r.Text, "bound job still queued") {
		t.Fatal("lifetime released queued validation", r)
	}
	// Model a lost binding with durable custody still present. The backstop
	// must consult the exact release ledger even without the retained job.
	release := HostRelease{Host: m.HostID, Job: bound.ID, Machine: m.ID, Session: m.Session, Attempt: 1, Device: "0"}
	key := releaseKey(release)
	if err := q.update(func() error {
		q.deleteJob(bound.ID)
		q.s.HostReleases = map[string]HostRelease{key: release}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r = reservationState(t, q, r.ID, "waiting_for_release")
	if !r.CleanupPending || !strings.Contains(r.Text, "physical cleanup unconfirmed") {
		t.Fatal("lost binding hid unresolved custody", r)
	}
	ack := Result{Machine: m.ID, Session: "stale-session", Execution: Execution{ID: bound.ID, Attempt: 1}, ReleaseOnly: true}
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, r.ID, "waiting_for_release")
	ack.Session = m.Session
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	r = reservationState(t, q, r.ID, "released")
	if r.CleanupPending || !strings.Contains(r.ReleaseReason, "maximum lifetime elapsed") {
		t.Fatal("lost binding backstop did not retain release evidence", r)
	}
}

func TestReservationWaitsForSupersededAttemptCleanup(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	if _, err := q.CreateReservation(reservationRequest("all-attempts", "host=machine;devices=0", bound.ID)); err != nil {
		t.Fatal(err)
	}
	// The current execution has succeeded and released, while an earlier
	// lost session still owes its own exact acknowledgement.
	if err := q.update(func() error {
		j := q.touchJob(bound.ID)
		j.State, j.Attempt, j.Machine, j.Session = "succeeded", 2, m.ID, m.Session
		j.ReleaseConfirmed = true
		j.Copies = []Copy{{Machine: m.ID, Session: "old-session", Attempt: 1}, {Machine: m.ID, Session: m.Session, Attempt: 2, ReleaseConfirmed: true}}
		release := HostRelease{Host: m.HostID, Job: j.ID, Machine: m.ID, Session: "old-session", Attempt: 1, Device: "0"}
		q.s.HostReleases = map[string]HostRelease{releaseKey(release): release}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	reservationState(t, q, "all-attempts", "waiting_for_release")
	ack := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: bound.ID, Attempt: 1}, ReleaseOnly: true}
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, "all-attempts", "waiting_for_release")
	ack.Session = "old-session"
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, "all-attempts", "released")
}

func TestReservationTerminalPreparationCustody(t *testing.T) {
	for _, phase := range []string{"offered", "started"} {
		t.Run(phase, func(t *testing.T) {
			q, clock := fixture(t)
			recipe := binaryRecipe(t, "fake")
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"validation": {"fake": recipe}}}); err != nil {
				t.Fatal(err)
			}
			m := m3Machine()
			m.EnvironmentBuilder = true
			machineCheck(t, q, m)
			s := exclusionSpec(m.ID, "0")
			s.Environment.Name = "validation"
			bound := submit(t, q, s)
			if _, err := q.CreateReservation(reservationRequest("preparing-check", "host=machine;devices=0", bound.ID)); err != nil {
				t.Fatal(err)
			}
			reply := machineCheck(t, q, m)
			if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch == nil {
				t.Fatal("missing bound preparation", reply.EnvironmentPlan)
			}
			launch := *reply.EnvironmentPlan[0].Launch
			if launch.Demand != bound.ID {
				t.Fatal("preparation not bound to validation", launch)
			}
			if phase == "started" {
				if err := q.AuthorizeLaunch(launch); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(2 * time.Hour)
			cancelAck(t, q, bound.ID)
			if phase == "offered" {
				reservationState(t, q, "preparing-check", "released")
				if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
					t.Fatal("terminal check's cached preparation passed launch", err)
				}
				return
			}
			r := reservationState(t, q, "preparing-check", "waiting_for_release")
			if !r.CleanupPending {
				t.Fatal("terminal job hid started preparation custody", r)
			}
			m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: "stale-attempt", State: "paused"}}
			machineCheck(t, q, m)
			reservationState(t, q, r.ID, "waiting_for_release")
			m.EnvironmentReports[0].Attempt = launch.Attempt
			machineCheck(t, q, m)
			reservationState(t, q, r.ID, "released")
		})
	}
}

func TestReservationBindingValidationAndPriority(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	for _, request := range []ReservationRequest{
		reservationRequest("missing", "host=machine", "unknown-job"),
		reservationRequest("wrong-device", "host=machine;devices=1", bound.ID),
		reservationRequest("fleet", "kind=gpu", bound.ID),
	} {
		if _, err := q.CreateReservation(request); err == nil {
			t.Fatal("unresolvable binding accepted", request)
		}
	}
	request := reservationRequest("check", "host=machine", bound.ID)
	request.MaxLifetime = "25h"
	if _, err := q.CreateReservation(request); err == nil {
		t.Fatal("unbounded reservation accepted")
	}
	request.MaxLifetime = "2h0m0s"
	if _, err := q.CreateReservation(request); err != nil {
		t.Fatal(err)
	}
	other := submit(t, q, exclusionSpec(m.ID, "1"))
	if _, err := q.CreateReservation(reservationRequest("overlap", "host=machine;devices=1", other.ID)); err == nil {
		t.Fatal("mutually blocking reservation accepted")
	}
	// A validation hold never grants production priority or bypasses takeover.
	if err := q.ConfigureHosts(map[string]HostPolicy{m.HostID: {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	production := exclusionSpec(m.ID, "1")
	production.Priority = "production"
	prod := submit(t, q, production)
	if a := assignedJob(machineCheck(t, q, m)); a == nil || a.ID != bound.ID {
		t.Fatal("unrelated production crossed reservation", a)
	}
	// Admission-only reservation does not itself stop either queued job.
	requireState(t, q, prod.ID, "queued")
}

func TestReservationDev(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "reservation-fixture", Workers: 1, Devices: 2, HostID: "fake-host", MemoryGB: 8, Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := d.Client()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	await(t, func() bool {
		var s Snapshot
		return c.Call(ctx, "GET", "/api/inspection", nil, &s) == nil && len(s.Machines) == 1 && len(s.Machines[0].Devices) == 2
	})
	// Hold admission with the existing exclusion while creating an ordinary
	// bound validation command, then let the reservation own admission.
	zero := uint64(0)
	var exclusion ComponentExclusion
	if err := c.Call(ctx, "POST", "/api/exclusions", ComponentExclusionRequest{Action: "add", Selector: "host=fake-host;devices=0", SetBy: "dev-operator", IfRevision: &zero}, &exclusion); err != nil {
		t.Fatal(err)
	}
	s := m3Spec("experiment", "0", 8, 1)
	s.Run.Command = []string{"sh", "-c", "test -z \"${MYCE_TOKEN+x}\" && printf validation-ok"}
	bound, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	var r Reservation
	if err := c.Call(ctx, "POST", "/api/reservations", reservationRequest("dev-check", "host=fake-host;devices=0", bound.ID), &r); err != nil {
		t.Fatal(err)
	}
	waiting, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Call(ctx, "POST", "/api/exclusions", ComponentExclusionRequest{Action: "remove", Selector: exclusion.Selector, SetBy: "dev-operator", IfRevision: &exclusion.Revision}, nil); err != nil {
		t.Fatal(err)
	}
	if j, err := c.Wait(ctx, bound.ID); err != nil || j.State != "succeeded" {
		t.Fatal(j, err)
	}
	if j, err := c.Wait(ctx, waiting.ID); err != nil || j.State != "succeeded" {
		t.Fatal(j, err)
	}
	var records []Reservation
	if err := c.Call(ctx, "GET", "/api/reservations", nil, &records); err != nil || len(records) != 1 || records[0].State != "released" {
		t.Fatal(records, err)
	}
}

func TestReservationAPIAuthorityAndConditionalRelease(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	srv := httptest.NewServer(API{Queue: q, Token: "fixture-only"}.Handler())
	defer srv.Close()
	c := Client{URL: srv.URL}
	request := reservationRequest("check", "host=machine;devices=0", bound.ID)
	if err := c.Call(t.Context(), "POST", "/api/reservations", request, nil); err == nil {
		t.Fatal("unauthenticated reservation accepted")
	}
	c.Token = "fixture-only"
	var r Reservation
	if err := c.Call(t.Context(), "POST", "/api/reservations", request, &r); err != nil {
		t.Fatal(err)
	}
	if err := c.Call(t.Context(), "POST", "/api/reservations/check/release", map[string]any{"if_revision": r.Revision, "reason": "operator verified operation abandoned"}, &r); err != nil || r.State != "released" {
		t.Fatal(r, err)
	}
	requireState(t, q, bound.ID, "queued")
}

func TestReservationCheckSubmissionAtomic(t *testing.T) {
	q, _, m, s := floorFreeFitFixture(t, 24)
	s.Workload = nil
	s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	s.RequireTarget, s.SubmissionKey = true, "reserved-check"
	s.AllowedMachines = []string{m.ID}
	s.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
	options := reservationRequest("atomic-check", "host="+m.HostID+";devices=0", "").ReservationOptions
	s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir(), Timeout: "10m", Reserve: &options}
	check := submit(t, q, s)
	r := reservationState(t, q, options.ID, "ready")
	if r.Job != check.ID {
		t.Fatal("reservation not bound to submitted check", r)
	}
	if replay := submit(t, q, s); replay.ID != check.ID || len(q.s.Reservations) != 1 {
		t.Fatal("replay created new job/hold", replay.ID)
	}
	waiting := submit(t, q, m3Spec("experiment", "0", 1, 1))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != check.ID {
		t.Fatal("ordinary work won atomic check reservation", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, waiting.ID, "queued")
	// Rejected reservation acquisition rolls back the job and its key together.
	s.SubmissionKey = "invalid-reservation"
	options.ID, options.Selector = "wrong-target", "host=absent"
	if _, err := q.Submit(s); err == nil {
		t.Fatal("unresolvable atomic reservation accepted")
	}
	if q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)] != nil || q.s.Reservations[options.ID].ID != "" {
		t.Fatal("failed atomic reservation left a job/key/hold")
	}
}

func TestReservationTerminalCleanupSurvivesRestart(t *testing.T) {
	for _, outcome := range []string{"succeeded", "failed", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
			db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
			q, err := Open(db, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			fakeCheckpointAvailability(t, q, true)
			m := m3Machine()
			machineCheck(t, q, m)
			bound := submit(t, q, exclusionSpec(m.ID, "0"))
			if _, err := q.CreateReservation(reservationRequest("check", "host=machine;devices=0", bound.ID)); err != nil {
				t.Fatal(err)
			}
			a := assignedJob(machineCheck(t, q, m))
			if a == nil {
				t.Fatal("missing bound assignment")
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			if outcome == "cancelled" {
				cancelAck(t, q, a.ID)
			}
			done := result(a, outcome == "succeeded")
			done.ReleasePending = true
			if err := q.Complete(done); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, a.ID, outcome)
			reservationState(t, q, "check", "waiting_for_release")
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err = Open(db, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			fakeCheckpointAvailability(t, q, true)
			reservationState(t, q, "check", "waiting_for_release")
			done.ReleasePending, done.ReleaseOnly = false, true
			if err := q.Complete(done); err != nil {
				t.Fatal(err)
			}
			reservationState(t, q, "check", "released")
			requireState(t, q, a.ID, outcome)
		})
	}
}

func TestReservationKeepsStartedQualification(t *testing.T) {
	q, w, _, _, _, plans := qualificationFixture(t)
	reconcileQualification(t, w, plans[0])
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	a := check(t, q, w.Machine, nil).Job
	if a == nil || automaticExtensionCheck(a) == nil {
		t.Fatal("missing automatic qualification attempt", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, spec("experiment"))
	if _, err := q.CreateReservation(reservationRequest("manual-validation", "host="+hostID(&w.Machine), bound.ID)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, a.ID); got.State != "running" || cancelled(got) || got.StopAt != nil {
		t.Fatal("reservation cancelled Started qualification", got.State, got.Reason)
	}
	if reply := check(t, q, w.Machine, a); reply.Stop || reply.Kill {
		t.Fatal("Started qualification received stop", reply)
	}
}

func TestReservationDoesNotDisplaceProduction(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	if err := q.ConfigureHosts(map[string]HostPolicy{m.HostID: {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	machineCheck(t, q, m)
	s := exclusionSpec(m.ID, "1")
	s.Priority = "production"
	prod := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != prod.ID {
		t.Fatal("production was not assigned")
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, exclusionSpec(m.ID, "0"))
	if _, err := q.CreateReservation(reservationRequest("qualification", "host=machine;devices=0", bound.ID)); err != nil {
		t.Fatal(err)
	}
	reservationState(t, q, "qualification", "draining")
	for _, directive := range machineCheck(t, q, m, a).Attempts {
		if directive.ID == a.ID && (directive.Stop || directive.Kill) {
			t.Fatal("validation displaced production")
		}
		if directive.ID == bound.ID {
			t.Fatal("validation bypassed production hold")
		}
	}
	finish(t, q, a, true)
	if next := assignedJob(machineCheck(t, q, m)); next == nil || next.ID != bound.ID {
		t.Fatal("bound check failed to follow production", next)
	}
}

func TestReservationRejectsWaitingProductionModelDemand(t *testing.T) {
	q, _, m := modelFixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&m): {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	demand := addModelCall(t, q, "production")
	if demand.ProductionHost != hostID(&m) || demand.Machine != "" {
		t.Fatal("fixture did not bind pending production model demand", demand.ProductionHost, demand.Machine)
	}
	bound := submit(t, q, spec("experiment"))
	if _, err := q.CreateReservation(reservationRequest("validation", "host="+hostID(&m), bound.ID)); err == nil || !strings.Contains(err.Error(), "production model call") {
		t.Fatal("reservation acquired a self-blocking production takeover target", err)
	}
	if len(q.s.Reservations) != 0 || !q.held(hostID(&m)) || demand.Error != "" || demand.StopAt != nil {
		t.Fatal("rejected reservation mutated production demand")
	}
}
