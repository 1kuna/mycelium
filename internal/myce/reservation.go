package myce

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Bound lost bindings and never-ready admission fences, never execution custody.
const MaxReservationLifetime = 24 * time.Hour
const DefaultReservationLifetime = 2 * time.Hour

type ReservationOptions struct {
	ID          string `json:"id" yaml:"id"`
	Selector    string `json:"selector" yaml:"selector"`
	Owner       string `json:"owner" yaml:"owner"`
	Reason      string `json:"reason" yaml:"reason"`
	MaxLifetime string `json:"max_lifetime" yaml:"max_lifetime"`
}

func reservationRefusal(message string) error {
	return rejectFieldRule("reservation_invalid", errors.New(message))
}

type ReservationRequest struct {
	ReservationOptions
	Job string `json:"job"`
}

// ValidationReservationSelector keeps a single-card qualification from draining
// unrelated work when the caller names only its host. Whole-host validation is
// still available explicitly; CPU checks and ordinary validation jobs retain
// their authored selector. The queue resolves device names to stable identities.
func ValidationReservationSelector(selector string, spec Spec, wholeHost bool) (string, error) {
	p, devices, err := parseComponentSelector(selector)
	if err != nil {
		return "", err
	}
	if wholeHost {
		if !p.wholeHost() || len(devices) != 0 {
			return "", reservationRefusal("--whole-host requires a host-only selector")
		}
		return selector, nil
	}
	if !p.wholeHost() || len(devices) != 0 || spec.ExtensionCheck == nil || spec.Needs.cpuOnly() {
		return selector, nil
	}
	if spec.Needs.GPU == nil || len(spec.Needs.GPU.Devices) != 1 {
		return "", reservationRefusal("GPU qualification requires an explicit device selector; use --whole-host only for a check that needs the whole host")
	}
	return p.Selector + ";devices=" + spec.Needs.GPU.Devices[0], nil
}

type Reservation struct {
	ReservationOptions
	Job           string             `json:"job"`
	Target        ComponentExclusion `json:"target"`
	Revision      uint64             `json:"revision"`
	CreatedAt     time.Time          `json:"created_at"`
	ExpiresAt     time.Time          `json:"expires_at"`
	ReadyAt       *time.Time         `json:"ready_at,omitempty"`
	ExpiredAt     *time.Time         `json:"expired_at,omitempty"`
	ReleasedAt    *time.Time         `json:"released_at,omitempty"`
	ReleaseReason string             `json:"release_reason,omitempty"`
	// Projections, never evidence of physical release.
	State          string   `json:"state"`
	Pending        []string `json:"pending,omitempty"`
	CleanupPending bool     `json:"cleanup_pending,omitempty"`
	Text           string   `json:"text"`
}

func (p ComponentExclusion) matchesTarget(m *Machine) bool {
	if m == nil {
		return false
	}
	if len(m.groupMembers) > 0 {
		return slices.ContainsFunc(m.groupMembers, p.matchesTarget)
	}
	for _, d := range m.Devices {
		if d.ID == m.Device && d.Platform == m.GPU {
			return p.matchesComponent(hostID(m), d.Platform, d.Identity)
		}
	}
	return p.matchesComponent(hostID(m), m.GPU, "")
}

func (r Reservation) active() bool {
	return r.ReleasedAt == nil
}

// Must run in the submission/control transaction, before planning any placements.
func (q *Queue) acquireReservation(request ReservationRequest) (Reservation, error) {
	var out Reservation
	o := request.ReservationOptions
	if err := validateSubmissionKey(o.ID); err != nil {
		return out, reservationRefusal("reservation id must be a stable submission-key-style identifier")
	}
	if strings.TrimSpace(o.Owner) == "" || strings.TrimSpace(o.Reason) == "" {
		return out, reservationRefusal("reservation owner and reason are required")
	}
	if old, ok := q.s.Reservations[o.ID]; ok && old.ReservationOptions == o && old.Job == request.Job {
		return old, nil // Canonical replay also works with absent inventory.
	}
	lifetime, err := time.ParseDuration(o.MaxLifetime)
	if err != nil || lifetime <= 0 || lifetime > MaxReservationLifetime {
		return out, reservationRefusal("reservation max_lifetime must be positive and at most 24h")
	}
	p, err := q.resolveComponentSelector(o.Selector)
	if err != nil {
		return out, err
	}
	if p.Host == "" {
		return out, reservationRefusal("reservation requires one physical host or that host's devices")
	}
	o.Selector = p.Selector
	if old, ok := q.s.Reservations[o.ID]; ok {
		if old.ReservationOptions != o || old.Job != request.Job {
			return out, ErrStale
		}
		return old, nil // Retrying never renews or resurrects the hold.
	}
	j := q.retainedJob(request.Job)
	if j == nil || j.Spec.FanOut != nil || j.Spec.After != nil {
		return out, reservationRefusal("reservation requires an existing single job/check without dependencies")
	}
	for _, old := range q.s.Reservations {
		if old.active() && (old.Job == j.ID || reservationTargetsOverlap(old.Target, p)) {
			return out, reservationRefusal("job or target already has an active reservation")
		}
	}
	// Reuse permanent target/qualification/capacity validation, scoped to the
	// selected physical components. Temporary occupancy and maintenance can drain.
	view := &Queue{clock: q.clock, s: q.s, models: q.models}
	view.s.Machines = map[string]*Machine{}
	for _, m := range q.s.Machines {
		for _, target := range q.machineTargets(m) {
			if !p.matchesTarget(target) {
				continue
			}
			copy := *target
			copy.Devices = nil // qualifiedTarget sees only this selected view.
			view.s.Machines[targetKey(target)] = &copy
		}
	}
	if ok, reason := view.qualifiedTarget(j); !ok {
		return out, reservationRefusal("bound job cannot run on reservation target: " + reason)
	}
	if j.Machine != "" && !terminal(j.State) && !p.matchesTarget(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)) {
		return out, reservationRefusal("bound job is already assigned outside the reservation target")
	}
	now := q.clock.Now()
	out = Reservation{ReservationOptions: o, Job: j.ID, Target: p, Revision: 1, CreatedAt: now, ExpiresAt: now.Add(lifetime)}
	if q.s.Reservations == nil {
		q.s.Reservations = map[string]Reservation{}
	}
	q.s.Reservations[o.ID] = out
	return out, nil
}

func reservationTargetsOverlap(a, b ComponentExclusion) bool {
	if a.Host != b.Host {
		return false
	}
	if a.wholeHost() || b.wholeHost() || a.Kind == "gpu" || b.Kind == "gpu" {
		return true
	}
	if a.Platform != "" && a.Platform == b.Platform {
		return true
	}
	for _, d := range a.Devices {
		if b.Platform == d.Platform || slices.Contains(b.Devices, d) {
			return true
		}
	}
	for _, d := range b.Devices {
		if a.Platform == d.Platform {
			return true
		}
	}
	return false
}

func (q *Queue) CreateReservation(request ReservationRequest, contexts ...context.Context) (Reservation, error) {
	var out Reservation
	err := q.updateRequest(requestContext(contexts), func() error {
		var err error
		out, err = q.acquireReservation(request)
		if err != nil {
			return err
		}
		q.maintain()
		out = q.projectReservation(q.s.Reservations[out.ID])
		return nil
	}, nil)
	return out, err
}

func (q *Queue) releaseReservation(r Reservation, reason string) {
	now := q.clock.Now()
	r.ReleasedAt, r.ReleaseReason, r.Revision = &now, reason, r.Revision+1
	q.s.Reservations[r.ID] = r
}

// Conditional operator release changes only this admission record, never the
// bound job or its cleanup evidence, exclusions, drains, or another reservation.
func (q *Queue) ReleaseReservation(id string, revision uint64, reason string, contexts ...context.Context) (Reservation, error) {
	var out Reservation
	err := q.updateRequest(requestContext(contexts), func() error {
		r, ok := q.s.Reservations[id]
		if !ok || revision == 0 || r.Revision != revision {
			return ErrStale
		}
		if strings.TrimSpace(reason) == "" {
			return reservationRefusal("operator release reason required")
		}
		if r.ReleasedAt == nil {
			q.releaseReservation(r, "operator release: "+reason)
		}
		q.maintain()
		out = q.projectReservation(q.s.Reservations[id])
		return nil
	}, nil)
	return out, err
}

func (q *Queue) maintainReservations() {
	now := q.clock.Now()
	for _, r := range q.s.Reservations {
		if r.ReleasedAt != nil {
			continue
		}
		j := q.retainedJob(r.Job)
		if j == nil || terminal(j.State) {
			// Cached preparation offers cannot start after their binding ends.
			// Started preparations retain the existing exact completion protocol.
			for _, launch := range q.s.PreparationPlans {
				if launch.Demand == r.Job && !launch.Started {
					q.maintainHostObligation(HostObligation{Launch: &launch}, nil)
				}
			}
		}
		if r.ExpiredAt == nil && r.ReadyAt == nil {
			if !now.Before(r.ExpiresAt) && j != nil && j.State == "queued" && !q.reservationCleanupPending(r, j) {
				r.ExpiredAt = &now
				r.Revision++
				q.s.Reservations[r.ID] = r
			} else if len(q.reservationPending(r)) == 0 || j != nil && j.StartedAt != nil {
				r.ReadyAt = &now
				q.s.Reservations[r.ID] = r
			}
		}
		switch {
		case j != nil && terminal(j.State) && !q.reservationCleanupPending(r, j):
			q.releaseReservation(r, "bound job terminal and all attempts and preparations physically released")
		case j == nil && !now.Before(r.ExpiresAt) && !q.reservationCleanupPending(r, j):
			q.releaseReservation(r, "maximum lifetime elapsed (lost-job backstop); bound job absent and no retained execution custody")
		}
	}
	// Reuse maintenance's planned, no-retry-charge withdrawal of unstarted
	// offers. Started commands and started preparations are left alone.
	for _, j := range q.liveJobs() {
		if j.State == "running" && j.StartedAt == nil && !q.reservationAllows(j, allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)) {
			q.maintainHostObligation(HostObligation{Result: resultIdentity(j)}, nil)
		}
	}
}

func (q *Queue) reservationCleanupPending(r Reservation, j *Job) bool {
	for _, launch := range q.s.PreparationPlans {
		if launch.Demand == r.Job && launch.Started {
			return true
		}
	}
	if j != nil {
		return q.jobReleasePending(j)
	}
	// A removed job must not hide the durable exact-attempt release ledger.
	for _, release := range q.s.HostReleases {
		if release.Job == r.Job {
			return true
		}
	}
	return false
}

func resultIdentity(j *Job) Result {
	return Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
}

// Unknown component evidence conservatively belongs to the host. Custody never
// becomes idle just because a worker, inventory device, or report disappears.
func (q *Queue) reservationObligationMatches(r Reservation, p HostObligation) bool {
	if r.Target.wholeHost() {
		return true
	}
	machine, _ := obligationSession(p)
	m := q.s.Machines[machine]
	_, session := obligationSession(p)
	if m == nil || session != m.Session {
		return true // Old-session ordinals do not prove a current device identity.
	}
	device := ""
	switch {
	case p.Launch != nil:
		if p.Launch.Kind == "environment" && p.Launch.Device == "" && m.Devices != nil {
			// The machine protocol's empty environment device is the CPU
			// target, not unknown GPU custody. Its RAM/cores remain charged.
			return false
		}
		device = p.Launch.Device
	case p.Engine != nil:
		device = p.Engine.Device
	case p.Call != nil:
		device = p.Call.Device
	case p.Preparation:
		return true
	default:
		key := releaseKey(HostRelease{Job: p.Result.ID, Machine: p.Result.Machine, Session: p.Result.Session, Attempt: p.Result.Attempt})
		if release, ok := q.s.HostReleases[key]; ok {
			if release.CPUOnly {
				return false
			}
			device = release.Device
		} else if j := q.retainedJob(p.Result.ID); j != nil {
			for _, c := range j.Copies {
				if c.Machine == p.Result.Machine && c.Session == p.Result.Session && c.Attempt == p.Result.Attempt && len(c.AllocatedDevices) == 1 {
					device = c.AllocatedDevices[0]
				}
			}
		}
	}
	if m == nil || device == "" {
		return true
	}
	for _, target := range q.machineTargets(m) {
		if target.Device == device {
			return r.Target.matchesTarget(target)
		}
	}
	return true
}

func (q *Queue) reservationPending(r Reservation) []string {
	var pending []string
	if q.maintenanceBlocked(r.Target.Host) {
		pending = append(pending, "independent host maintenance")
	}
	bound := q.retainedJob(r.Job)
	for _, p := range q.hostObligations(r.Target.Host) {
		if p.Result.ID == r.Job || p.Launch != nil && p.Launch.Demand == r.Job {
			continue
		}
		if q.reservationObligationMatches(r, p) {
			pending = append(pending, obligationKey(p))
		}
	}
	present := false
	now := q.clock.Now()
	for _, m := range q.s.Machines {
		if hostID(m) != r.Target.Host || m.Replacement != nil {
			continue
		}
		for _, target := range q.machineTargets(m) {
			if r.Target.matchesTarget(target) {
				present = true
				if (bound == nil || !bound.Spec.Needs.cpuOnly()) && q.deviceAdmissionBlocked(target) {
					pending = append(pending, "independent device maintenance or exclusion")
				}
			}
		}
		if q.unreported[m.ID] || !q.workerFresh(m, now) {
			pending = append(pending, "worker evidence unavailable: "+m.ID)
		}
	}
	if !present {
		pending = append(pending, "reserved component absent from inventory")
	}
	q.observeReservationPending(r, pending)
	return pending
}

func (q *Queue) reservationAllows(j *Job, m *Machine) bool {
	if m == nil && j != nil {
		m = allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
	}
	if m == nil {
		// Missing inventory is not evidence of a reserved target. Other
		// admission and recovery gates still own worker availability.
		return true
	}
	if j != nil && j.Spec.Needs.cpuOnly() && m.Devices != nil && m.GPU != "cpu" && len(m.groupMembers) == 0 {
		// A CPU job runs on the host's CPU view. The host record keeps card 0's
		// scalar device ID, which must not match a card reservation (2026-11-02).
		cpu := *m
		cpu.GPU, cpu.Device = "cpu", ""
		m = &cpu
	}
	// Existing Started work retains normal recovery/custody and priority rules.
	if j != nil && j.StartedAt != nil && j.Machine == m.ID && (j.State == "running" || j.State == "stopping") {
		return true
	}
	if len(m.groupMembers) > 0 {
		// Every card must be free of others' reservations (or bound to this job's).
		return !slices.ContainsFunc(m.groupMembers, func(card *Machine) bool { return !q.reservationAllows(j, card) })
	}
	for _, r := range q.s.Reservations {
		if !r.active() {
			continue
		}
		bound := j != nil && j.ID == r.Job
		if r.ExpiredAt != nil {
			if bound {
				return false
			}
			continue
		}
		matches := r.Target.matchesTarget(m)
		// Acquiring a hold must not revoke its own existing assignment, even
		// between custody and Started. Readiness gates new assignments; other
		// reservations and independent launch/priority fences still apply.
		assigned := bound && j.Machine == m.ID && (j.State == "running" || j.State == "stopping")
		if bound && (!matches || !assigned && len(q.reservationPending(r)) > 0) || !bound && matches {
			return false
		}
	}
	return true
}

func (q *Queue) reservationReasonOnTarget(j *Job, m *Machine) string {
	for _, stored := range q.s.Reservations {
		if !stored.active() {
			continue
		}
		if stored.Job != j.ID && !stored.Target.matchesTarget(m) {
			continue
		}
		if stored.ExpiredAt != nil && stored.Job != j.ID {
			continue
		}
		r := q.projectReservation(stored)
		if r.ExpiredAt != nil {
			return r.Text
		}
		if r.Job == j.ID && len(r.Pending) > 0 {
			return r.Text + "; draining existing work or waiting for worker/cleanup evidence"
		}
		if r.Job != j.ID {
			return r.Text
		}
	}
	return ""
}

func (q *Queue) projectReservation(r Reservation) Reservation {
	r.Target.Devices = slices.Clone(r.Target.Devices)
	r.Text = "reserved for " + r.Owner + ": " + r.Reason
	j := q.retainedJob(r.Job)
	r.CleanupPending = q.reservationCleanupPending(r, j)
	if r.ReleasedAt != nil {
		r.State = "released"
		return r
	}
	if r.ExpiredAt != nil {
		r.State = "expired"
		r.Text = "reservation expired before ready; bound job remains queued; release explicitly before reserving again"
		return r
	}
	if !q.clock.Now().Before(r.ExpiresAt) {
		if j != nil && !terminal(j.State) {
			r.Text += "; reserved past max lifetime: bound job still " + j.State
		} else if r.CleanupPending {
			r.Text += "; reserved past max lifetime: physical cleanup unconfirmed"
		}
	}
	r.Pending = q.reservationPending(r)
	switch {
	case j == nil || terminal(j.State):
		r.State = "waiting_for_release"
	case j.StartedAt != nil && (j.State == "running" || j.State == "stopping"):
		r.State = "in_use"
	case len(r.Pending) > 0:
		r.State = "draining"
	default:
		r.State = "ready"
	}
	return r
}

func (q *Queue) reservations() []Reservation {
	out := []Reservation{}
	for _, r := range q.s.Reservations {
		out = append(out, q.projectReservation(r))
	}
	slices.SortFunc(out, func(a, b Reservation) int { return strings.Compare(a.ID, b.ID) })
	return out
}

func (q *Queue) Reservations(contexts ...context.Context) ([]Reservation, error) {
	v, _, err := q.captureObservation("", contexts...)
	if err != nil {
		return nil, err
	}
	return v.reservations(), nil
}

func (a API) registerReservations(mux *requestMux) {
	mux.HandleFunc("GET /api/reservations", func(w http.ResponseWriter, r *http.Request) {
		out, err := a.Queue.Reservations(r.Context())
		respondOperation(w, out, err, "reservation_invalid")
	})
	mux.HandleFunc("POST /api/reservations", func(w http.ResponseWriter, r *http.Request) {
		var request ReservationRequest
		if !decode(w, r, &request) {
			return
		}
		out, err := a.Queue.CreateReservation(request, r.Context())
		respondOperation(w, out, err, "reservation_invalid")
	})
	mux.HandleFunc("POST /api/reservations/{id}/release", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			IfRevision uint64 `json:"if_revision"`
			Reason     string `json:"reason"`
		}
		if !decode(w, r, &request) {
			return
		}
		out, err := a.Queue.ReleaseReservation(r.PathValue("id"), request.IfRevision, request.Reason, r.Context())
		respondOperation(w, out, err, "reservation_invalid")
	})
}
