package myce

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

// Normal heartbeat period used by Worker.Run when no interval is configured.
const defaultWorkerInterval = time.Second

// HostLaunch is exact queue custody for a preparation, or an engine attempt.
// A cached reply is an offer, never permission to launch.
type HostLaunch struct {
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
	if q.s.PreparationPlans == nil {
		q.s.PreparationPlans = map[string]HostLaunch{}
	}
	for _, l := range q.s.PreparationPlans {
		if l.Machine == m.ID && l.Session == m.Session && l.Kind == kind && l.Key == key && (kind == "copy" || l.Device == targetDevice(m)) {
			return l
		}
	}
	l := HostLaunch{Device: targetDevice(m), Machine: m.ID, Session: m.Session, Kind: kind, Key: key, Attempt: ID()}
	if j := q.candidate(m, nil); j != nil {
		spec := Spec{Project: j.Spec.Project, Priority: j.Spec.Priority, Class: j.Spec.Class}
		l.Spec = &spec
		l.Demand = j.ID
	}
	q.s.PreparationPlans[l.Attempt] = l
	return l
}
func (q *Queue) hostLaunchAllowed(m *Machine, session string) bool {
	return m != nil && !m.Recovering && m.Session == session && !m.Gone && !m.Leaving && !q.unreported[m.ID] && !q.maintenanceBlocked(hostID(m)) && !availabilityBlocked(m) && !m.HostMemoryPolicyMismatch
}
func (q *Queue) AuthorizeLaunch(l HostLaunch) error {
	return q.authorizeLaunchContext(context.Background(), l)
}

func (q *Queue) authorizeLaunchContext(ctx context.Context, l HostLaunch) error {
	timing := newQueueUpdateTiming()
	var rejected error
	reject := func(fence string) {
		log.Printf("event=launch_rejected fence=%s", fence)
		rejected = ErrStale
	}
	err := q.updateRequest(ctx, func() error {
		q.maintain()
		q.refreshHostMemory()
		if !q.hostLaunchAllowed(q.s.Machines[l.Machine], l.Session) {
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
			attempt, err := strconv.Atoi(l.Attempt)
			j, errCurrent := q.current(Result{Machine: l.Machine, Session: l.Session, Execution: Execution{ID: l.Key, Attempt: attempt}})
			switch {
			case err != nil:
				reject("attempt_number")
			case errCurrent != nil:
				reject("attempt_custody")
			case j.State != "running":
				reject("job_state")
			case cancelled(j):
				reject("cancelled")
			default:
				predicate := q.startPlacementRejection(j)
				if predicate == "" {
					break
				}
				m := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
				cpu, kind, transition := false, "", "other"
				if m != nil {
					cpu, kind = cpuFits(j, m), q.dockerHoldDetail(hostID(m)).Kind
					if dockerStartingOrRunning(m.DockerCleanup) {
						transition = m.DockerCleanup
					}
				}
				log.Printf("event=launch_placement_rejected job=%s attempt=%d predicate=%s cpu_fits=%t docker_hold_kind=%q docker_transition=%q", j.ID, j.Attempt, predicate, cpu, kind, transition)
				reject("start_placement")
			}
			return nil
		}
		if l.Kind == "engine" {
			s := q.s.ModelSlots[slotKey(l.Machine, l.Key, l.Device)]
			if s == nil || s.Session != l.Session || s.Attempt != l.Attempt || s.State != "loading" || !q.reservationAllows(nil, allocationMachine(q.s.Machines[l.Machine], []string{l.Device})) || q.deviceAdmissionBlocked(allocationMachine(q.s.Machines[l.Machine], []string{l.Device})) {
				reject("engine_custody")
			}
			return nil
		}
		p, ok := q.s.PreparationPlans[l.Attempt]
		if !ok || p.PriorityStop != nil || p.Machine != l.Machine || p.Session != l.Session || p.Kind != l.Kind || p.Key != l.Key {
			reject("preparation_custody")
			return nil
		}
		m := q.s.Machines[l.Machine]
		if !p.Started && p.Kind == "benchmark" && q.benchmarkDeviceAdmissionBlocked(m) {
			reject("device_admission")
			return nil
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
		if l.Kind == "benchmark" && !q.benchmarkIdle(m) {
			reject("benchmark_busy")
			return nil
		}
		if !q.workerHasCPUReservation(m) && !cpuAllocationFits(m, 1) {
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
				if r.Key == l.Key && r.Attempt == l.Attempt && (r.State == "ready" || r.State == "failed" || r.State == "paused") {
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
			done = c.BenchmarkDone == l.Attempt
		}
		if done {
			delete(q.s.PreparationPlans, id)
		}
	}
}
func (w Worker) authorizeLaunch(l HostLaunch) error {
	if err := w.checkAvailability(); err != nil {
		return err
	}
	// Queue contention must fit the normal control-request budget, independently
	// of heartbeat cadence. A timeout still cannot authorize a payload (#225).
	ctx, cancel := context.WithTimeout(context.Background(), workerStartRequestTimeout)
	defer cancel()
	var reply struct {
		OK        bool   `json:"ok"`
		StartGate string `json:"start_gate"`
	}
	if err := w.Client.Call(ctx, "POST", "/api/workers/launch", l, &reply); err != nil {
		return fmt.Errorf("%w: %w", errWorkerStartAuthorization, err)
	}
	if !reply.OK || reply.StartGate != startGateProtocol {
		return errWorkerStartAuthorization
	}
	return w.checkAvailability()
}

type launchGateKey struct{}

func withLaunchGate(ctx context.Context, gate func() error) context.Context {
	return context.WithValue(ctx, launchGateKey{}, gate)
}
func checkLaunchGate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gate, ok := ctx.Value(launchGateKey{}).(func() error); ok {
		return gate()
	}
	return nil
}

func (q *Queue) cancelUnofferedPreparations(m *Machine, reply Reply) {
	offered := map[string]bool{}
	if reply.BenchmarkPlan != nil {
		offered[reply.BenchmarkPlan.Attempt] = true
	}
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
