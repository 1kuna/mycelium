package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Blocker is an observation, never an admission decision or permission to run.
// Evidence comes from the existing fit, preparation and waiting-reason machinery.
type Blocker struct {
	Code       string `json:"code"`
	Dependency string `json:"dependency"`
	Owner      string `json:"owner"`
	Action     string `json:"action"`
	Evidence   string `json:"evidence,omitempty"`
}

func (b *Blocker) Text() string {
	if b == nil {
		return ""
	}
	text := fmt.Sprintf("%s: %s; next owner: %s; action: %s", b.Code, b.Dependency, b.Owner, b.Action)
	if b.Evidence != "" {
		text += "; " + strings.Join(strings.Fields(b.Evidence), " ")
	}
	return text
}

// Resolve findings already carry the field, reading side and remedy. Keep that
// evidence intact instead of replacing validation errors with a generic hold.
func ResolveFindingBlocker(findings []ResolveFinding) *Blocker {
	var primary *ResolveFinding
	for i := range findings {
		f := &findings[i]
		if f.Field == "source" || strings.HasPrefix(f.Field, "run.command[") || strings.HasPrefix(f.Field, "args[") || f.Field == "allowed_machines" && strings.Contains(f.Problem, "host pin limits") {
			continue
		}
		primary = f
		break
	}
	if primary == nil {
		return nil
	}
	f := *primary
	owner := "consumer"
	if f.PathDiagnostic != nil && f.Side != "submitter" {
		owner = f.Side + " owner"
	}
	return &Blocker{"unprepared", f.Field, owner, f.Fix, f.Problem}
}

func (q *Queue) inventoryFresh(m *Machine) bool {
	return q.workerFresh(m, q.clock.Now())
}

// Called only on detached observation copies. Sort by the shared closest-fit
// ordering so targeted GET, aggregate status and resolve choose the same cause.
func (q *Queue) actionableBlocker(j *Job) (blocker *Blocker) {
	if j.State != "queued" {
		return nil
	}
	if q.startCapacityPaused(j) {
		// Every blocker owns its diagnosis, including environment and stale
		// inventory returns below. Keep the suspended budget visible with it.
		defer func() {
			if blocker != nil {
				blocker.Evidence += "; " + pausedStartBudget(j)
			}
		}()
	}
	if !dependencyReady(j) {
		copy := *j
		copy.Reason = q.waitReason(j)
		return &Blocker{"unprepared", "accepted upstream result", "producer", "complete and publish the required upstream result", q.statusWaitingReasonDetail(&copy)}
	}
	type observation struct {
		fit targetFit
		job *Job
		m   *Machine
	}
	var targets []observation
	q.evaluateTargets(j, func(f targetFit, candidate *Job, m *Machine) bool {
		if j.Spec.Needs.cpuOnly() && m.Device != "" {
			return true
		}
		targets = append(targets, observation{f, candidate, m})
		return true
	})
	slices.SortFunc(targets, func(a, b observation) int {
		if a.fit.better(b.fit) {
			return -1
		}
		if b.fit.better(a.fit) {
			return 1
		}
		return 0
	})
	var environment *Blocker
	for _, t := range targets {
		if t.fit.excluded || t.fit.ineligible > 0 && t.fit.first != "environment" {
			continue
		}
		live := q.s.Machines[t.m.ID]
		if !q.inventoryFresh(live) {
			if environment == nil {
				environment = &Blocker{"unknown_observation", "worker inventory", "queue owner", "restore fresh worker observations", "inventory is missing or stale"}
			}
			continue
		}
		if t.fit.failures != 0 && t.fit.first != "environment" {
			continue
		}
		if environmentReady(t.job, t.m) {
			// A ready alternative means environment preparation is not the
			// blocking dependency. The ordinary reason owns resource/queue holds.
			environment = nil
			break
		}
		b := q.environmentBlocker(t.job, t.m)
		if b == nil {
			continue
		}
		if environment == nil || blockerRank(b.Code) < blockerRank(environment.Code) {
			environment = b
		}
	}
	if environment != nil {
		return environment
	}
	copy := *j
	copy.Reason = q.waitReason(j)
	reason := q.statusWaitingReasonDetail(&copy)
	if key := j.Spec.Environment.Key(); key != "" {
		reason = strings.ReplaceAll(reason, key, "required software")
	}
	b := &Blocker{"resource_pressure", "eligible execution capacity", "queue", "wait for the named capacity or reservation to be released; keep other work moving", reason}
	if strings.HasPrefix(reason, productionParityHostHold+":") {
		b.Code, b.Dependency, b.Owner, b.Action = productionParityHostHold, "production parity host approval", "queue owner", "compare candidate outputs with the reference and explicitly approve the physical host in the exact work's production_parity_hosts list"
	} else if deviceFaultRetryReason(reason) {
		b.Code, b.Dependency, b.Owner, b.Action = deviceFaultRetryHold, "healthy accelerator capacity after a driver fault", "queue and worker", "wait for another healthy device or host, or a health-proven quarantine recovery"
	} else if !dependencyReady(j) {
		b.Code, b.Dependency, b.Owner, b.Action = "unprepared", "accepted upstream result", "producer", "complete and publish the required upstream result"
	} else if strings.Contains(reason, "waiting for software preparation for job ") {
		b.Code, b.Dependency, b.Owner, b.Action = "waiting_for_builder", "software preparation for the named queued job", "queue", "wait for the named preparation; ask the queue owner if its offer or exact release makes no progress"
	} else if len(targets) == 0 || strings.Contains(reason, "readings") || strings.Contains(reason, "telemetry unavailable") || strings.Contains(reason, "unknown") && !strings.Contains(reason, "utilization unknown") {
		b.Code, b.Owner, b.Action = "unknown_observation", "queue owner", "restore the missing observation named in the reason"
	} else if strings.Contains(reason, "hypha to take") {
		b.Code, b.Dependency, b.Action = "waiting_for_worker", "worker assignment", "wait for an eligible worker to take this job"
	}
	return b
}

func blockerRank(code string) int {
	switch code {
	case "building":
		return 0
	case "unprepared":
		return 1
	case "waiting_for_builder":
		return 2
	case "deterministically_failed":
		return 3
	default:
		return 4
	}
}

func (q *Queue) environmentBlocker(j *Job, m *Machine) *Blocker {
	live := q.s.Machines[m.ID]
	if !q.inventoryFresh(live) {
		return &Blocker{"unknown_observation", "worker inventory", "queue owner", "restore fresh worker observations", "inventory is missing or stale"}
	}
	if environmentReady(j, m) {
		return nil
	}
	key := environmentKey(j, m)
	if j.CommandDiscovery != nil {
		keys := environmentFailureKeys(j, m)
		for _, r := range m.EnvironmentReports {
			if slices.Contains(keys, r.Key) && q.environmentReportBlocked(r) {
				key = r.Key
				break
			}
		}
	}
	if e := qualifiedExtension(j, m); e != nil {
		key = extensionPreparationKey(*e, m)
	}
	if key == "" {
		return nil
	}
	label := "environment"
	if extensionName.MatchString(key) {
		label += " " + key
	}
	b := &Blocker{"unprepared", label, "queue", "submit normally; the queue prepares the immutable environment before launch", "no builder has started"}
	builder := m
	for _, view := range q.placementTargets(live) {
		if sameTarget(view, m) {
			builder = view
			break
		}
	}
	if !q.environmentPreparationAvailable(builder) {
		b.Code, b.Owner, b.Action = "waiting_for_builder", "queue owner", "wait for an eligible builder with a free preparation core, or restore builder availability"
	}
	for _, p := range q.s.PreparationPlans {
		if p.Machine == live.ID && p.Session == live.Session && p.Key == key {
			b.Code, b.Owner, b.Action = "waiting_for_builder", "queue", "wait for the offered builder to start; ask the queue owner if the offer makes no progress"
			if p.Started {
				b.Code, b.Owner, b.Action, b.Evidence = "unknown_observation", "queue owner", "inspect the exact preparation attempt and restore its report", "launch authorized; no matching builder report observed"
			}
		}
	}
	for _, r := range live.EnvironmentReports {
		if r.Key != key {
			continue
		}
		switch r.State {
		case "building":
			b.Code, b.Owner, b.Action, b.Evidence = "building", "builder", "wait for preparation to finish; inspect environment logs if progress stalls", "builder reports "+r.State
		case "failed", "uploading":
			b.Code, b.Owner, b.Action, b.Evidence = "deterministically_failed", "consumer", "fix the recipe and resubmit with changed immutable inputs", environmentFailureReason(r, j.Spec.Logs != "none")
			if environmentFailureKind(r) == "transient" {
				b.Code, b.Owner, b.Action = "waiting_for_builder", "queue", "wait for the recorded retry; ask the queue owner if infrastructure remains unavailable"
			}
		}
	}
	return b
}
