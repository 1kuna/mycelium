package myce

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func holdPolicy(now time.Time) PriorityPolicy {
	p := defaultPriorityPolicy()
	start, end := now.Add(time.Minute), now.Add(3*time.Minute)
	p.ScheduledHolds = []ScheduledHold{{ID: "weekly-owner", Enabled: true, Beneficiary: "beneficiary/*", Resource: HoldResource{Role: "accelerator"}, Reason: "owner configured demand", Timezone: "UTC", Windows: []HoldWindow{{StartAt: &start, EndAt: &end}}}}
	p.HostRoles = map[string][]string{"host-one": {"accelerator"}}
	return p
}

func holdStatus(t *testing.T, q *Queue, id string) ScheduledHoldStatus {
	t.Helper()
	s, err := q.statusSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range s.ScheduledHolds {
		if h.ID == id {
			return h
		}
	}
	t.Fatal("scheduled hold absent from status")
	return ScheduledHoldStatus{}
}

func TestScheduledHoldReplayDrainReleaseAndReopen(t *testing.T) {
	q, c := fixture(t)
	if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	one := machine("one", "fake", "host-one")
	check(t, q, one, nil)
	p := holdPolicy(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	old := spec("experiment")
	old.Project = "peer-one"
	old.Resumable = false
	prior := submit(t, q, old)
	running := check(t, q, one, nil).Job
	if running == nil || running.ID != prior.ID {
		t.Fatal("no pre-window work", running)
	}
	if err := q.Started(result(running, true)); err != nil {
		t.Fatal(err)
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "scheduled" {
		t.Fatal(h)
	}
	c.Advance(time.Minute)
	// Opening alone drains; it never invents a stop or priority promotion.
	if reply := check(t, q, one, running); reply.Stop {
		t.Fatal("window alone preempted Started work")
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "draining" || len(h.Blockers) == 0 {
		t.Fatal(h)
	}
	next := spec("experiment")
	next.Project = "peer-two"
	peer := submit(t, q, next)
	beneficiary := spec("experiment")
	beneficiary.Project = "beneficiary"
	beneficiary.AllowedMachines = []string{one.ID}
	owner := submit(t, q, beneficiary)
	q = reopenCancellationQueue(t, q, c)
	if h := holdStatus(t, q, "weekly-owner"); h.State != "draining" {
		t.Fatal("restart lost custody", h)
	}
	// Competing work remains usable on an unselected resource.
	two := machine("two", "fake", "host-two")
	if got := check(t, q, two, nil).Job; got == nil || got.ID != peer.ID {
		t.Fatal("unselected target held", got)
	}
	pending := result(running, true)
	pending.ReleasePending = true
	if err := q.Complete(pending); err != nil {
		t.Fatal(err)
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "draining" {
		t.Fatal("result mistaken for release", h)
	}
	if got := check(t, q, one, nil).Job; got != nil {
		t.Fatal("replacement before exact release", got)
	}
	release := result(running, false)
	release.ReleaseOnly = true
	if err := q.Complete(release); err != nil {
		t.Fatal(err)
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "exclusive_after_release" {
		t.Fatal("exact release not observed", h)
	}
	got := check(t, q, one, nil).Job
	if got == nil || got.ID != owner.ID {
		t.Fatal("beneficiary did not run", got)
	}
}

func TestScheduledHoldStaleOffersAndOverlap(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake", "host-one")
	check(t, q, m, nil)
	s := spec("experiment")
	s.Project = "beneficiary"
	j := submit(t, q, s)
	offered := check(t, q, m, nil).Job
	if offered == nil {
		t.Fatal("no offer")
	}
	p := holdPolicy(c.Now())
	first := p.ScheduledHolds[0]
	first.Windows[0].StartAt = timePointer(c.Now())
	p.ScheduledHolds = []ScheduledHold{first}
	// Conflicting overlapping rules intersect; neither beneficiary silently wins.
	other := first
	other.ID = "second-owner"
	other.Beneficiary = "other/*"
	p.ScheduledHolds = append(p.ScheduledHolds, other)
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	if err := q.Started(result(offered, true)); !errors.Is(err, ErrStale) {
		t.Fatal("stale offer crossed Started", err)
	}
	l := HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: j.ID, Attempt: "1"}
	if err := q.AuthorizeLaunch(l); !errors.Is(err, ErrStale) {
		t.Fatal("stale batch launch authorized", err)
	}
	// Removing one hold does not remove the other. Removing both adds no cooldown.
	p.ScheduledHolds = p.ScheduledHolds[1:]
	if _, err := q.SetPriorityPolicy(1, p); err != nil {
		t.Fatal(err)
	}
	if err := q.Started(result(offered, true)); !errors.Is(err, ErrStale) {
		t.Fatal("overlap removed by wrong owner", err)
	}
	p.ScheduledHolds = []ScheduledHold{}
	if _, err := q.SetPriorityPolicy(2, p); err != nil {
		t.Fatal(err)
	}
	if err := q.Started(result(offered, true)); err != nil {
		t.Fatal("removal added cooldown", err)
	}
}

func timePointer(t time.Time) *time.Time { return &t }

func TestScheduledHoldWindowsTimezoneDST(t *testing.T) {
	h := ScheduledHold{Enabled: true, Timezone: "America/New_York", Windows: []HoldWindow{{Weekdays: []int{0}, Start: "01:15", End: "01:45"}}}
	for _, tc := range []struct {
		instant string
		active  bool
	}{
		{"2026-11-01T05:14:59Z", false}, {"2026-11-01T05:15:00Z", true}, {"2026-11-01T05:45:00Z", false},
		{"2026-11-01T06:15:00Z", true}, {"2026-11-01T06:45:00Z", false}, {"2026-11-02T06:30:00Z", false},
	} {
		now, err := time.Parse(time.RFC3339, tc.instant)
		if err != nil {
			t.Fatal(err)
		}
		if got := h.active(now); got != tc.active {
			t.Fatalf("%s active=%t", tc.instant, got)
		}
	}
	h.Windows = []HoldWindow{{Weekdays: []int{0}, Start: "02:15", End: "03:30"}}
	for _, tc := range []struct {
		instant string
		active  bool
	}{{"2026-03-08T06:59:59Z", false}, {"2026-03-08T07:00:00Z", true}, {"2026-03-08T07:30:00Z", false}} {
		now, _ := time.Parse(time.RFC3339, tc.instant)
		if got := h.active(now); got != tc.active {
			t.Fatalf("spring %s active=%t", tc.instant, got)
		}
	}
	q, c := fixture(t)
	p := holdPolicy(c.Now())
	p.ScheduledHolds[0].Windows = []HoldWindow{{Weekdays: []int{int(c.Now().Weekday())}, Start: "12:01", End: "12:02"}}
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, c)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake", "host-one")
	c.Advance(time.Minute)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("weekly start boundary not held", got)
	}
	c.Advance(time.Minute)
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("weekly end boundary not released", got)
	}
}

func TestScheduledHoldPolicyCASPreservationAndValidation(t *testing.T) {
	q, c := fixture(t)
	p := holdPolicy(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	// Existing clients may omit newly introduced fields without erasing them.
	if _, err := q.SetPriorityPolicy(1, defaultPriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	if got := q.PriorityPolicy().Policy; !reflect.DeepEqual(got.ScheduledHolds, p.ScheduledHolds) || !reflect.DeepEqual(got.HostRoles, p.HostRoles) {
		t.Fatal("omitted local values erased", got)
	}
	if _, err := q.SetPriorityPolicy(1, p); !errors.Is(err, ErrPolicyRevision) {
		t.Fatal("stale owner overwrite", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ScheduledHold)
	}{
		{"unpadded start", func(h *ScheduledHold) { h.Windows = []HoldWindow{{Weekdays: []int{0}, Start: "1:00", End: "02:00"}} }},
		{"unpadded end", func(h *ScheduledHold) { h.Windows = []HoldWindow{{Weekdays: []int{0}, Start: "01:00", End: "2:00"}} }},
		{"overnight", func(h *ScheduledHold) { h.Windows = []HoldWindow{{Weekdays: []int{1}, Start: "23:00", End: "01:00"}} }},
		{"timezone", func(h *ScheduledHold) { h.Timezone = "invalid-zone" }},
		{"selector", func(h *ScheduledHold) { h.Beneficiary = "[/*" }},
		{"empty resource", func(h *ScheduledHold) { h.Resource = HoldResource{} }},
		{"mixed window", func(h *ScheduledHold) { h.Windows[0].Start = "12:00" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := cloneHoldPolicy(p)
			tc.mutate(&bad.ScheduledHolds[0])
			if _, err := q.SetPriorityPolicy(2, bad); err == nil {
				t.Fatal("invalid rule accepted")
			}
		})
	}
	// Exported collections are detached from the local durable policy.
	exported := q.PriorityPolicy()
	exported.Policy.HostRoles["host-one"][0] = "mutated"
	exported.Policy.ScheduledHolds[0].Windows[0].StartAt = timePointer(c.Now())
	if q.PriorityPolicy().Policy.HostRoles["host-one"][0] != "accelerator" {
		t.Fatal("caller mutated policy")
	}
}

func TestScheduledHoldPreparationAndIndependentHolds(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake", "host-one")
	check(t, q, m, nil)
	s := spec("experiment")
	j := submit(t, q, s)
	l := HostLaunch{Machine: m.ID, Session: m.Session, Kind: "environment", Key: "synthetic-env", Attempt: "stale-plan", Demand: j.ID, Spec: &s}
	if err := q.update(func() error { q.s.PreparationPlans = map[string]HostLaunch{l.Attempt: l}; return nil }); err != nil {
		t.Fatal(err)
	}
	p := holdPolicy(c.Now())
	p.ScheduledHolds[0].Windows[0].StartAt = timePointer(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	if err := q.AuthorizeLaunch(l); !errors.Is(err, ErrStale) {
		t.Fatal("stale preparation authorized", err)
	}
	changeHold(t, q, j.ID, 0, true, nil)
	p.ScheduledHolds = []ScheduledHold{}
	if _, err := q.SetPriorityPolicy(1, p); err != nil {
		t.Fatal(err)
	}
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("policy removal cleared job hold", got)
	}
	if _, err := q.ChangeHostMaintenance("host-one", HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	changeHold(t, q, j.ID, 1, false, nil)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("resume cleared maintenance", got)
	}
	if !q.maintenanceBlocked("host-one") {
		t.Fatal("maintenance erased")
	}
}

func TestScheduledHoldMeasuredSetupForecast(t *testing.T) {
	q, c := fixture(t)
	p := holdPolicy(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	j := &Job{Spec: spec("experiment"), State: "queued"}
	j.Spec.Environment = Environment{Name: "synthetic-env"}
	j.EnvironmentRecipes = map[string]EnvironmentRecipe{"fake": {Key: "env-key"}}
	m := machine("one", "fake", "host-one")
	m.EnvironmentBuildSeconds = map[string]float64{"env-key": 61}
	if reason := q.scheduledHoldReason(j, &m); !strings.Contains(reason, "measured setup") {
		t.Fatal("measured setup not reused", reason)
	}
	m.EnvironmentBuildSeconds = nil
	if reason := q.scheduledHoldReason(j, &m); reason != "" {
		t.Fatal("unknown setup invented hold", reason)
	}
}

func TestScheduledHoldBeneficiaryUsesExistingPreemption(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake", "host-one")
	submit(t, q, spec("experiment"))
	victim := check(t, q, m, nil).Job
	if victim == nil {
		t.Fatal("no victim")
	}
	if err := q.Started(result(victim, true)); err != nil {
		t.Fatal(err)
	}
	p := holdPolicy(c.Now())
	p.ScheduledHolds[0].Windows[0].StartAt = timePointer(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	s := spec("deadline")
	s.Project = "beneficiary"
	demand := submit(t, q, s)
	reply := check(t, q, m, victim)
	if !reply.Stop || reply.Job != nil {
		t.Fatal("deadline preemption not reused", reply)
	}
	if got := check(t, q, m, victim).Job; got != nil {
		t.Fatal("replacement before release", got)
	}
	finishInterrupted(t, q, victim)
	if got := check(t, q, m, nil).Job; got == nil || got.ID != demand.ID {
		t.Fatal("beneficiary not admitted after release", got)
	}
}

func TestScheduledHoldDeviceScopeAndReservationSurvivesExpiry(t *testing.T) {
	q, c := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	p := holdPolicy(c.Now())
	p.ScheduledHolds[0].Resource = HoldResource{Host: m.HostID, Device: "0"}
	p.ScheduledHolds[0].Windows[0].StartAt = timePointer(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	held := submit(t, q, exclusionSpec(m.ID, "0"))
	sibling := submit(t, q, exclusionSpec(m.ID, "1"))
	if run := assignedJob(machineCheck(t, q, m)); run == nil || run.ID != sibling.ID {
		t.Fatal("device hold withdrew sibling", run)
	}
	ownerSpec := exclusionSpec(m.ID, "0")
	ownerSpec.Project = "beneficiary"
	owner := submit(t, q, ownerSpec)
	request := reservationRequest("independent", "host=machine;devices=0", owner.ID)
	if _, err := q.CreateReservation(request); err != nil {
		t.Fatal(err)
	}
	c.Advance(3 * time.Minute)
	if run := assignedJob(machineCheck(t, q, m)); run != nil && run.ID == held.ID {
		t.Fatal("window expiry cleared reservation", run)
	}
	r := reservationState(t, q, request.ID, "ready")
	if r.ReleasedAt != nil {
		t.Fatal("independent reservation expired with calendar", r)
	}
}

func TestScheduledHoldSupersededDeviceReleaseStatus(t *testing.T) {
	q, c := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	peer := submit(t, q, exclusionSpec(m.ID, "1"))
	// A completed current attempt on device 1 cannot release its predecessor's
	// retained device 0 execution, even on the same physical machine.
	if err := q.update(func() error {
		j := q.touchJob(peer.ID)
		j.State, j.Attempt, j.Machine, j.Session = "succeeded", 2, m.ID, m.Session
		j.AllocatedDevices = []string{"1"}
		j.ReleaseConfirmed = true
		j.Copies = []Copy{{Machine: m.ID, Session: "old-session", Attempt: 1, AllocatedDevices: []string{"0"}}, {Machine: m.ID, Session: m.Session, Attempt: 2, AllocatedDevices: []string{"1"}, ReleaseConfirmed: true}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p := holdPolicy(c.Now())
	p.ScheduledHolds[0].Resource = HoldResource{Host: m.HostID, Device: "0"}
	p.ScheduledHolds[0].Windows[0].StartAt = timePointer(c.Now())
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "draining" {
		t.Fatal("current allocation hid old custody", h)
	}
	ack := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: peer.ID, Attempt: 1}, ReleaseOnly: true}
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "draining" {
		t.Fatal("wrong-session release cleared old custody", h)
	}
	ack.Session = "old-session"
	if err := q.Complete(ack); err != nil {
		t.Fatal(err)
	}
	if h := holdStatus(t, q, "weekly-owner"); h.State != "exclusive_after_release" {
		t.Fatal("exact prior release not observed", h)
	}
}

func TestScheduledHoldSetupForecastAcrossDST(t *testing.T) {
	for _, tc := range []struct{ now, start, end string }{
		{"2026-03-08T06:59:30Z", "02:15", "03:30"},
		{"2026-11-01T06:14:30Z", "01:15", "01:45"},
	} {
		t.Run(tc.now, func(t *testing.T) {
			q, c := fixture(t)
			c.now, _ = time.Parse(time.RFC3339, tc.now)
			p := holdPolicy(c.Now())
			p.ScheduledHolds[0].Timezone = "America/New_York"
			p.ScheduledHolds[0].Windows = []HoldWindow{{Weekdays: []int{0}, Start: tc.start, End: tc.end}}
			if _, err := q.SetPriorityPolicy(0, p); err != nil {
				t.Fatal(err)
			}
			j := &Job{Spec: spec("experiment"), State: "queued", EnvironmentRecipes: map[string]EnvironmentRecipe{"fake": {Key: "env-key"}}}
			j.Spec.Environment = Environment{Name: "synthetic-env"}
			m := machine("one", "fake", "host-one")
			m.EnvironmentBuildSeconds = map[string]float64{"env-key": 61}
			if reason := q.scheduledHoldReason(j, &m); !strings.Contains(reason, "measured setup") {
				t.Fatal("DST transition hid imminent window", reason)
			}
		})
	}
}
