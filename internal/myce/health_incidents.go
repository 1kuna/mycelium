package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// One refusal can be an ordinary dispatch/start race (#601); three independent
// attempts in ten minutes need attention. Three stale edges in an hour cover
// the short recoveries in #661 without calling one long absence a flap.
const launchRefusalPeriod = 10 * time.Minute
const staleFlapPeriod = time.Hour
const healthRepeatLimit = 3

type healthSample struct {
	ID string    `json:"id"`
	At time.Time `json:"at"`
}

func (q *Queue) healthSamples(key string, period time.Duration, id string) int {
	if q.s.Health.Conditions == nil {
		q.s.Health.Conditions = map[string]healthCondition{}
	}
	c := q.s.Health.Conditions[key]
	now := q.clock.Now()
	// Copy before editing: read snapshots may still own the previous slice.
	c.Samples = slices.DeleteFunc(slices.Clone(c.Samples), func(s healthSample) bool { return now.Sub(s.At) >= period })
	if id != "" && !slices.ContainsFunc(c.Samples, func(s healthSample) bool { return s.ID == id }) {
		c.Samples = append(c.Samples, healthSample{ID: id, At: now})
		if len(c.Samples) > healthRepeatLimit {
			c.Samples = c.Samples[len(c.Samples)-healthRepeatLimit:]
		}
	}
	q.s.Health.Conditions[key] = c
	return len(c.Samples)
}

func launchRefusalCause(c *RuntimeCause) bool {
	return c != nil && (c.Operation == "launch" && c.Class == launchRefused && c.Predicate == "launch_authorization" ||
		(c.Operation == "start_authorization" || c.Operation == "start_handoff") && c.Class == "admission_changed")
}

func launchRefusalOutcome(o AttemptOutcome) bool {
	// Older workers called a 409 io_error. The queue's recorded placement
	// refusal supplies the positive fence evidence; transport errors don't.
	return launchRefusalCause(o.ExecutionCause) || o.ExecutionCause != nil &&
		(o.ExecutionCause.Operation == "start_authorization" || o.ExecutionCause.Operation == "start_handoff") &&
		strings.HasPrefix(o.Reason, "requeued: start placement refused (")
}

func (q *Queue) recordLaunchRefusalHealth(j *Job, outcome AttemptOutcome) {
	if j.StartedAt != nil || cancelled(j) || outcome.PoliteStop || outcome.OperatorStop || outcome.PriorityStop != nil || !launchRefusalOutcome(outcome) {
		return
	}
	// Requeue and Complete may both write the same outcome. Exact attempts,
	// never endpoint retries or heartbeat count, are the repetition evidence.
	for _, old := range j.AttemptHistory {
		if old.Attempt == outcome.Attempt && old.Machine == outcome.Machine && old.Session == outcome.Session && launchRefusalOutcome(old) {
			return
		}
	}
	host := outcome.Machine
	if m := q.s.Machines[outcome.Machine]; m != nil {
		host = hostID(m)
	}
	key := "launch-refusals/" + host
	q.healthSamples(key, launchRefusalPeriod, fmt.Sprintf("%s/%d/%s", j.ID, outcome.Attempt, outcome.Session))
}

func (q *Queue) detectIncidentHealth() {
	now := q.clock.Now()
	seen := map[string]bool{}
	for key := range q.s.Health.Conditions {
		if host, ok := strings.CutPrefix(key, "launch-refusals/"); ok {
			q.observeHealth(key, q.healthSamples(key, launchRefusalPeriod, "") >= healthRepeatLimit, 0,
				HealthAlert{Kind: "repeated_launch_refusals", Host: host})
		}
	}
	for _, m := range q.s.Machines {
		if m.Replacement != nil || q.migrationHistoricalMachine(m) {
			continue
		}
		host := hostID(m)
		mode := q.s.HostMaintenance[host].Mode
		intentional := m.Leaving || mode == "disabled" || mode == "drained" || mode == "draining"
		key := "stale-flaps/" + m.ID
		seen[key] = true
		freshness := min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2]))
		stale := m.Gone || now.Sub(q.lastContact(m, now)) >= freshness
		cold := q.unreported[m.ID] && now.Before(q.restartUntil)
		c := q.s.Health.Conditions[key]
		id := ""
		if intentional || cold {
			c.Samples, c.SeenHealthy = nil, false
		} else if !stale {
			c.SeenHealthy = true
		} else if c.SeenHealthy && !c.WasBad {
			id = now.Format(time.RFC3339Nano)
		}
		c.WasBad = stale
		q.s.Health.Conditions[key] = c
		q.observeHealth(key, q.healthSamples(key, staleFlapPeriod, id) >= healthRepeatLimit, 0,
			HealthAlert{Kind: "worker_stale_flapping", Host: host})
		for _, d := range m.Devices {
			key := "teardown-faults/" + m.ID + "/" + deviceFaultKey(d)
			seen[key] = true
			q.observeHealth(key, !intentional && d.Platform == "xpu" && d.RepeatedTeardownFault && deviceFaultReason(d.Reason), 0,
				HealthAlert{Kind: "xpu_repeated_teardown_faults", Host: host, Device: d.ID})
		}
	}
	for id, r := range q.s.Reservations {
		key := "reservation-never-ready/" + id
		seen[key] = true
		q.observeHealth(key, r.active() && r.ReadyAt == nil && !now.Before(r.ExpiresAt), 0,
			HealthAlert{Kind: "reservation_never_ready", Host: r.Target.Host, Reservation: id, JobID: r.Job})
	}
	// The envelope pass changes health conditions only. Share retained
	// evidence across its permanent-capacity views, then discard it before
	// another queue mutation can change history or inventory.
	previous := q.memoryPlanning
	q.memoryPlanning = newMemoryPlanningCache()
	defer func() { q.memoryPlanning = previous }()
	for _, j := range q.liveJobs() {
		if j.State == "queued" && !cancelled(j) && (j.Spec.Needs.Memory == "auto" || j.Spec.extensionJob()) {
			key := envelopeHealthKey(j)
			if seen[key] {
				continue
			}
			seen[key] = true
			q.observeEnvelopeHealth(j, false)
		}
	}
	for key, c := range q.s.Health.Conditions {
		switch {
		case strings.HasPrefix(key, "learned-envelope/"):
			if !seen[key] && c.Envelope != nil {
				q.observeEnvelopeHealth(c.Envelope, true)
			} else if !seen[key] {
				q.observeHealth(key, false, 0, HealthAlert{})
				if c.LastAlert.IsZero() || now.Sub(c.LastAlert) >= healthAlertCooldown {
					delete(q.s.Health.Conditions, key)
				}
			}
		case strings.HasPrefix(key, "stale-flaps/"), strings.HasPrefix(key, "teardown-faults/"), strings.HasPrefix(key, "reservation-never-ready/"):
			if !seen[key] {
				q.observeHealth(key, false, 0, HealthAlert{})
			}
		}
	}
}

func envelopeHealthKey(j *Job) string {
	s := j.Spec
	s.SubmissionKey, s.RequireTarget = "", false
	return "learned-envelope/" + identityHash(s)
}

// Reuse permanent target qualification. A memory-free probe must pass every
// other predicate: wrong platforms, missing tools/recipes and cores are not
// learned-envelope incidents. A fitting peer excludes temporary occupancy.
func (q *Queue) learnedEnvelopeOversized(j *Job) bool {
	eligible, allOversized := false, true
	q.evaluateTargets(j, func(_ targetFit, candidate *Job, m *Machine) bool {
		if !q.jobAllowsMachine(j, m.ID) {
			return true
		}
		probe := *candidate
		probe.MemoryEnvelope = nil
		probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB = 0, minimumHostMemoryGB
		if len(m.groupMembers) > 0 {
			probe.Spec.Needs.MemoryGB = m.groupFloorGB + minimumHostMemoryGB
		}
		if !fitsResources(&probe, m, m.TotalMemoryGB) || !hostFits(m, 0, minimumHostMemoryGB) {
			return true
		}
		eligible = true
		envelope := candidate.MemoryEnvelope
		oversized := false
		if candidate.Spec.Needs.Memory == "auto" && envelope != nil && envelope.Basis == "learned" {
			// Assignment's existing comparison excludes declared member caps,
			// hints and retry floors. Only accepted learning is this incident.
			learnedProbe := *candidate
			capacity := Queue{clock: q.clock, s: q.s, memoryOrigin: q}
			capacity.s.Jobs, capacity.s.ModelSlots, capacity.s.HostReleases = nil, nil, nil
			capacity.s.Machines = map[string]*Machine{m.ID: m}
			capacity.recordLearnedMemory(&learnedProbe, m)
			if learned := learnedProbe.MemoryEnvelope.Learned; learned != nil {
				probe.Spec.Needs.HostMemoryGB, probe.Spec.Needs.MemoryGB = learned.HostGB, 0
				if learned.DeviceGB != nil {
					probe.Spec.Needs.MemoryGB = *learned.DeviceGB
				}
				oversized = !hostFits(m, probe.Spec.Needs.MemoryGB, probe.Spec.Needs.HostMemoryGB) || probe.Spec.Needs.MemoryGB > m.TotalMemoryGB || !capabilityFits(&probe, m)
			}
		}
		allOversized = allOversized && oversized
		return allOversized
	})
	return eligible && allOversized
}

func (q *Queue) observeEnvelopeHealth(j *Job, refused bool) {
	key := envelopeHealthKey(j)
	bad := q.learnedEnvelopeOversized(j)
	old, exists := q.s.Health.Conditions[key]
	if !bad && (!exists || old.LastAlert.IsZero() || q.clock.Now().Sub(old.LastAlert) >= healthAlertCooldown) {
		delete(q.s.Health.Conditions, key)
		return
	}
	e := HealthAlert{Kind: "learned_envelope_exceeds_allowances", Project: j.Spec.Project}
	if !refused {
		e.JobID = j.ID
	}
	q.observeHealth(key, bad, 0, e)
	c := q.s.Health.Conditions[key]
	if bad && refused && c.Envelope == nil {
		copy := cloneJob(j)
		c.Envelope = &copy
	} else if !bad || !refused {
		c.Envelope = nil
	}
	q.s.Health.Conditions[key] = c
}
