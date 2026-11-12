package myce

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strconv"
	"time"
)

// Normal heartbeat period used by Worker.Run when no interval is configured.
const defaultWorkerInterval = time.Second

// HostLaunch is exact queue custody for a preparation, or an engine attempt.
// A cached reply is an offer, never permission to launch.
type HostLaunch struct {
	// Zero preserves legacy preparation sharing with an execution reservation.
	CPUCores     int                 `json:"cpu_cores,omitempty"`
	Device       string              `json:"device,omitempty"`
	StartedAt    *time.Time          `json:"started_at,omitempty"`
	Spec         *Spec               `json:"spec,omitempty"`
	Demand       string              `json:"demand,omitempty"`
	PriorityStop *PriorityPreemption `json:"priority_stop,omitempty"`
	StopAt       *time.Time          `json:"stop_at,omitempty"`
	Machine      string              `json:"machine"`
	Session      string              `json:"session"`
	Kind         string              `json:"kind"`
	Key          string              `json:"key"`
	Attempt      string              `json:"attempt"`
	Started      bool                `json:"started,omitempty"`
}

func launchAttempt(l *HostLaunch) string {
	if l == nil {
		return ""
	}
	return l.Attempt
}
func (q *Queue) issuePreparation(m *Machine, kind, key string) HostLaunch {
	return q.issuePreparationFor(m, kind, key, nil)
}

// demand names the job the build serves; nil uses this target's admission winner.
func (q *Queue) issuePreparationFor(m *Machine, kind, key string, demand *Job) HostLaunch {
	if q.s.PreparationPlans == nil {
		q.s.PreparationPlans = map[string]HostLaunch{}
	}
	for _, l := range q.s.PreparationPlans {
		if l.Machine == m.ID && l.Session == m.Session && l.Kind == kind && l.Key == key && (kind == "copy" || l.Device == targetDevice(m)) {
			return l
		}
	}
	l := HostLaunch{Device: targetDevice(m), Machine: m.ID, Session: m.Session, Kind: kind, Key: key, Attempt: ID()}
	if kind == "environment" {
		l.CPUCores = 1 // Background builds do not borrow unrelated command cores.
	}
	j := demand
	if j == nil {
		j = q.candidate(m, nil)
	}
	if j != nil {
		spec := Spec{Project: j.Spec.Project, Priority: j.Spec.Priority, Class: j.Spec.Class}
		l.Spec = &spec
		l.Demand = j.ID
	}
	q.s.PreparationPlans[l.Attempt] = l
	return l
}
func (q *Queue) hostLaunchAllowed(m *Machine, session string) bool {
	return q.hostSessionLaunchAllowed(m, session) && !q.workerSuspect(m, q.clock.Now())
}

// An already assigned batch retains its authorization/custody across suspicion.
// New preparation launches still require live arrival evidence.
func (q *Queue) hostSessionLaunchAllowed(m *Machine, session string) bool {
	return m != nil && q.hostHygieneReason(m) == "" && q.hostSessionOpen(m, session)
}

func (q *Queue) hostSessionOpen(m *Machine, session string) bool {
	return m != nil && !m.Recovering && m.Session == session && !m.Gone && !m.Leaving && !q.unreported[m.ID] && !q.maintenanceBlocked(hostID(m)) && !availabilityBlocked(m)
}

// #1299: placement refuses drained hosts, so a drain present when an assigned
// production attempt reaches its main gate was raised after its admission. A
// memory drain alone (pressure or swap growth) then does not refuse main; that
// would discard prepared work for a signal the attempt already passed. It still
// refuses when the condition is real: measured available memory is unknown or
// below the attempt pressure floor (hostPressureEvidence's AvailableFloorGB),
// or the drain is any other hygiene fault.
func (q *Queue) assignedLaunchAllowed(m *Machine, session string, j *Job) bool {
	if !q.hostSessionOpen(m, session) {
		return false
	}
	switch q.hostHygieneReason(m) {
	case "":
		return true
	case "host_memory_pressure", "host_swap_growth":
		return j != nil && j.Spec.Priority == "production" && assignedHeadroomSufficient(m, j)
	}
	return false
}

func assignedHeadroomSufficient(m *Machine, j *Job) bool {
	if !m.HostMemoryMeasured || !validMemoryValue(m.HostAvailableMemoryGB) {
		return false
	}
	reserve := 0.0
	if o := m.MemoryObservation; o != nil && validMemoryValue(o.ReserveGB) {
		reserve = o.ReserveGB
	}
	floor := max(reserve, .125, hostNeed(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)*.25)
	return m.HostAvailableMemoryGB >= floor
}
func (q *Queue) AuthorizeLaunch(l HostLaunch) error {
	return q.authorizeLaunchContext(context.Background(), l)
}

func (q *Queue) authorizeLaunchContext(ctx context.Context, l HostLaunch) error {
	timing := newQueueUpdateTiming()
	var rejected error
	var batch *Job
	reject := func(fence string) {
		log.Printf("event=launch_rejected fence=%s", fence)
		if fence == "host_launch" || fence == "preparation_stopping" {
			q.recordStartRefusal(batch, fence)
		}
		rejected = ErrStale
	}
	err := q.updateRequest(ctx, func() error {
		q.maintain()
		q.refreshHostMemory()
		if l.Kind == "batch" {
			attempt, err := strconv.Atoi(l.Attempt)
			if err != nil {
				reject("attempt_number")
				return nil
			}
			batch, err = q.current(Result{Machine: l.Machine, Session: l.Session, Execution: Execution{ID: l.Key, Attempt: attempt}})
			if err != nil {
				reject("attempt_custody")
				return nil
			}
		}
		m := q.s.Machines[l.Machine]
		allowed := q.hostLaunchAllowed(m, l.Session)
		p := q.s.PreparationPlans[l.Attempt]
		// Repeated gates within an already started preparation retain its exact
		// custody, just as an assigned batch does. Suspicion blocks new starts;
		// the current session, stop and preparation fences still apply below.
		continuing := p.Started && p.Machine == l.Machine && p.Session == l.Session && p.Kind == l.Kind && p.Key == l.Key
		if l.Kind == "batch" {
			allowed = q.assignedLaunchAllowed(m, l.Session, batch)
		} else if continuing {
			allowed = q.hostSessionLaunchAllowed(m, l.Session)
		}
		if !allowed {
			reject("host_launch")
			return nil
		}
		if q.s.HostMaintenance[hostID(q.s.Machines[l.Machine])].SharedHost != nil && l.Kind != "batch" {
			reject("shared_host_preparation")
			return nil
		}
		if (l.Kind == "batch" || l.Kind == "engine") && q.preparationStopping(q.s.Machines[l.Machine]) {
			reject("preparation_stopping")
			return nil
		}
		if l.Kind == "batch" {
			j := batch
			switch {
			case j.State != "running":
				reject("job_state")
			case j.ReleaseConfirmed:
				reject("execution_released")
			case cancelled(j):
				reject("cancelled")
			default:
				predicate := q.startPlacementRejection(j)
				if predicate == "" {
					q.clearStartRefusal(j)
					break
				}
				m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
				cpu := m != nil && cpuFits(j, m)
				log.Printf("event=launch_placement_rejected job=%s attempt=%d predicate=%s cpu_fits=%t", j.ID, j.Attempt, predicate, cpu)
				q.recordStartRefusal(j, predicate)
				reject("start_placement")
			}
			return nil
		}
		if l.Kind == "engine" {
			s := q.s.ModelSlots[slotKey(l.Machine, l.Key, l.Device)]
			if s == nil || s.Session != l.Session || s.Attempt != l.Attempt || s.State != "loading" || !q.reservationAllows(nil, allocationMachine(q.s.Machines[l.Machine], []string{l.Device})) || q.deviceAdmissionBlocked(allocationMachine(q.s.Machines[l.Machine], []string{l.Device})) {
				reject("engine_custody")
			} else {
				q.recordModelInterval(s, "load_started")
			}
			return nil
		}
		p, ok := q.s.PreparationPlans[l.Attempt]
		if !ok || p.PriorityStop != nil || p.Machine != l.Machine || p.Session != l.Session || p.Kind != l.Kind || p.Key != l.Key {
			reject("preparation_custody")
			return nil
		}
		if j := q.s.Jobs[p.Demand]; p.Kind == "environment" && j != nil && j.CommandDiscovery != nil {
			target := allocationMachine(m, []string{p.Device})
			var allowed bool
			if p.Started {
				allowed = q.placementDemandAllowed(j, target)
			} else {
				allowed = q.placementAllowed(j, target)
			}
			// A delayed offer must retain live demand for this exact frozen
			// target/recipe, even before another heartbeat withdraws it.
			if j.State != "queued" || cancelled(j) || j.Pause != nil || !allowed || !commandDiscoveryAllows(j, target) || !slices.ContainsFunc(commandEnvironmentRecipes(j, target), func(r EnvironmentRecipe) bool { return r.Key == p.Key }) {
				reject("discovery_demand")
				return nil
			}
		}
		if !p.Started {
			if j := q.s.Jobs[p.Demand]; j != nil && q.productionAdmissionHold(j, allocationMachine(m, []string{p.Device})) != nil {
				reject("production_admission")
				return nil
			}
		}
		if !p.Started && !q.reservationAllows(q.s.Jobs[p.Demand], allocationMachine(m, []string{p.Device})) {
			reject("reservation")
			return nil
		}
		// A model-cache copy writes shared host storage, not the selected GPU.
		if !p.Started && p.Kind != "copy" && p.Device != "" && q.deviceAdmissionBlocked(allocationMachine(m, []string{p.Device})) {
			reject("device_admission")
			return nil
		}
		if !p.Started && (p.CPUCores > 0 || !q.workerHasCPUReservation(m)) && !cpuAllocationFits(m, max(1, p.CPUCores)) {
			reject("preparation_cpu")
			return nil
		}
		p.Started = true
		if p.StartedAt == nil {
			now := q.clock.Now()
			p.StartedAt = &now
		}
		q.s.PreparationPlans[l.Attempt] = p
		return nil
	}, timing)
	// updateTimed has released q.mu; logging never extends the transaction.
	timing.logLaunch(err == nil && rejected == nil)
	if err != nil {
		return err
	}
	return rejected
}
func (q *Queue) reconcilePreparationPlans(c Checkin) {
	for id, l := range q.s.PreparationPlans {
		if l.Machine != c.Machine.ID {
			continue
		}
		if c.Fresh && c.PreviousSession == l.Session && c.Machine.Session != l.Session {
			delete(q.s.PreparationPlans, id)
			continue
		}
		if l.Session != c.Machine.Session {
			continue
		}
		done := false
		if l.Kind == "environment" {
			for _, r := range c.Machine.EnvironmentReports {
				if r.Key == l.Key && r.Attempt == l.Attempt && (r.State == "ready" || r.State == "failed" || r.State == "uploading" || r.State == "paused") {
					done = true
				}
			}
		} else if l.Kind == "copy" {
			for _, r := range c.Machine.CopyReports {
				if r.Model == l.Key && r.Attempt == l.Attempt && (r.State == "ready" || r.State == "failed" || r.State == "paused") {
					done = true
				}
			}
		} else if l.Kind == "benchmark" {
			done = true // The onboarding benchmark is retired (MY-49).
		}
		if done {
			delete(q.s.PreparationPlans, id)
		}
	}
}
func (w Worker) authorizeLaunch(l HostLaunch) error {
	return w.authorizeLaunchContext(w.launchContext(), l)
}

func (w Worker) authorizeLaunchContext(ctx context.Context, l HostLaunch) error {
	w.launchCtx = ctx
	if err := w.checkAvailability(); err != nil {
		return err
	}
	var reply struct {
		OK        bool   `json:"ok"`
		StartGate string `json:"start_gate"`
	}
	if _, err := w.controlHandshake("/api/workers/launch", l, &reply); err != nil {
		return fmt.Errorf("%w: %w", errWorkerStartAuthorization, err)
	}
	if !reply.OK || reply.StartGate != startGateProtocol {
		return errWorkerStartAuthorization
	}
	if l.Kind == "batch" && w.batchAdmission != nil {
		if err := w.batchAdmission(ctx, l); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.checkAvailability()
}

type launchGateKey struct{}

func withLaunchGate(ctx context.Context, gate func(context.Context) error) context.Context {
	return context.WithValue(ctx, launchGateKey{}, gate)
}

// Wrappers capture the prior gate before installing their own, but pass the
// invoking operation's context through it. A build/readiness child may stop
// while its enclosing preparation still owns the same offer.
func launchGateFrom(ctx context.Context) func(context.Context) error {
	if gate, ok := ctx.Value(launchGateKey{}).(func(context.Context) error); ok {
		return gate
	}
	return func(context.Context) error { return nil }
}

func checkLaunchGate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return launchGateFrom(ctx)(ctx)
}

func (q *Queue) cancelUnofferedPreparations(m *Machine, reply Reply) {
	offered := map[string]bool{}
	for _, r := range reply.EnvironmentPlan {
		offered[launchAttempt(r.Launch)] = true
	}
	for _, r := range reply.CopyPlan {
		offered[launchAttempt(r.Launch)] = true
	}
	for id, l := range q.s.PreparationPlans {
		if l.Machine == m.ID && l.Session == m.Session && !l.Started && !offered[id] {
			delete(q.s.PreparationPlans, id)
		}
	}
}

func (q *Queue) untrackedPreparations(m *Machine) int {
	tracked := func(kind, key, attempt string) bool {
		l, ok := q.s.PreparationPlans[attempt]
		return ok && l.Machine == m.ID && l.Session == m.Session && l.Kind == kind && l.Key == key
	}
	count := 0
	for _, r := range m.EnvironmentReports {
		if r.State == "building" && !tracked("environment", r.Key, r.Attempt) {
			count++
		}
	}
	for _, r := range m.CopyReports {
		if r.State == "copying" && !tracked("copy", r.Model, r.Attempt) {
			count++
		}
	}
	return count
}
