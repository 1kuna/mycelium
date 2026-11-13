package myce

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// withAction gives a hold a site-specific owner and next action, keeping the
// kind's reason and clear condition.
func withAction(h *Hold, owner, next string) *Hold {
	h.Owner, h.Next = owner, next
	return h
}

// retag moves a job hold to another kind, keeping its evidence.
func (h *Hold) retag(kind, owner, next string) {
	*h = *withAction(newHold(kind, h.Detail), owner, next)
}

// Resolve findings already carry the field, reading side and remedy. Keep that
// evidence intact instead of replacing validation errors with a generic hold.
func ResolveFindingHold(findings []ResolveFinding) *Hold {
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
	return withAction(newHold("unprepared", f.Problem), owner, f.Fix)
}

func (q *Queue) inventoryFresh(m *Machine) bool {
	return q.workerFresh(m, q.clock.Now())
}

// Called only on detached observation copies. Sort by the shared closest-fit
// ordering so targeted GET, aggregate status and resolve choose the same cause.
func (q *Queue) jobHold(j *Job) (blocker *Hold) {
	if j.State != "queued" {
		return nil
	}
	if q.startCapacityPaused(j) {
		// Every blocker owns its diagnosis, including environment and stale
		// inventory returns below. Keep the suspended budget visible with it.
		defer func() {
			if blocker != nil {
				blocker.Detail += "; " + pausedStartBudget(j)
			}
		}()
	}
	// The scheduler's wait, as its projection computed it when one did.
	wait := j.wait
	if wait == nil {
		wait = q.waitHold(j)
	}
	if !dependencyReady(j) {
		copy := *j
		copy.Reason = wait.Detail
		return newHold("upstream", q.statusWaitingReasonDetail(&copy))
	}
	type observation struct {
		fit targetFit
		job *Job
		m   *Machine
	}
	var targets []observation
	compatibleHosts := map[string]bool{}
	freshTargets := false
	q.evaluateTargets(j, func(f targetFit, candidate *Job, m *Machine) bool {
		if j.Spec.Needs.cpuOnly() && m.Device != "" {
			return true
		}
		targets = append(targets, observation{f, candidate, m})
		fresh := !f.excluded && q.inventoryFresh(q.s.Machines[m.ID]) && m.Replacement == nil
		freshTargets = freshTargets || fresh
		if fresh && f.failures == 0 {
			compatibleHosts[hostID(m)] = true
		}
		return true
	})
	defer func() {
		if blocker == nil {
			return
		}
		if (j.Spec.Priority == "production" || jobClass(j.Spec) == "production") && j.Spec.ExtensionCheck == nil {
			if approvals, configured := q.parityApprovals(j.Spec.Project, j.Spec.workType()); configured {
				labels := []string{}
				for _, a := range approvals {
					labels = append(labels, a.label())
				}
				blocker.Detail += fmt.Sprintf("; the project approves these machine kinds for this work's production: %s; Mycelium places it on any host of those kinds without a job host pin; the project approves another kind with myce parity add after a passing output comparison", cmp.Or(strings.Join(labels, ", "), "none"))
			}
		}
		blocker.RunsNowOn = q.portableRoomNow(j, compatibleHosts, func(visit func(targetFit, *Job, *Machine)) {
			for _, t := range targets {
				visit(t.fit, t.job, t.m)
			}
		})
		if blocker.RunsNowOn != "" {
			// Part of the one action: default status prints only reason and next.
			blocker.Next += "; this job would run now on " + blocker.RunsNowOn + " if its targets and environment allowed it: add that platform (myce portability), never name a host"
		}
		if len(compatibleHosts) == 1 && len(j.Spec.AllowedMachines) == 0 && len(j.Spec.ExcludedMachines) == 0 && len(q.s.Policies[j.Spec.Project].AllowedMachines) == 0 {
			blocker.Detail += "; only one freshly observed physical host meets the declared software, resource, qualification and parity requirements; this is observed coverage, not a required host pin; keep the job unpinned unless an explicit restriction is intended"
		}
	}()
	slices.SortFunc(targets, func(a, b observation) int {
		if a.fit.better(b.fit) {
			return -1
		}
		if b.fit.better(a.fit) {
			return 1
		}
		return 0
	})
	if wait.Kind == "no_buildable_worker" {
		return wait
	}
	var environment *Hold
	for _, t := range targets {
		if t.fit.excluded || t.fit.ineligible > 0 && t.fit.first != "environment" {
			continue
		}
		live := q.s.Machines[t.m.ID]
		if !q.inventoryFresh(live) {
			if environment == nil {
				environment = withAction(newHold("unknown_observation", "inventory is missing or stale"), "queue owner", "restore fresh worker observations")
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
		b := q.environmentHold(t.job, t.m)
		if b == nil {
			continue
		}
		if environment == nil || blockerRank(b.Kind) < blockerRank(environment.Kind) {
			environment = b
		}
	}
	if environment != nil {
		return environment
	}
	// The scheduler's reason names its mechanism; target coverage below refines
	// only the generic capacity kinds, never a specific hold.
	copy := *j
	copy.Reason = wait.Detail
	reason := q.statusWaitingReasonDetail(&copy)
	if key := j.Spec.Environment.Key(); key != "" {
		reason = strings.ReplaceAll(reason, key, "required software")
	}
	b := newHold(wait.Kind, reason)
	if specificWaitKind(wait.Kind) {
		return b
	} else if j.Spec.extensionJob() && len(j.Extensions) == 0 {
		b.retag("missing_extension", "consumer", "register an implementation for this work with its supported targets, environment and native check; keep the queued job identity")
	} else if len(targets) > 0 && !targets[0].fit.excluded && targets[0].fit.first == "extension" {
		if !freshTargets {
			b.retag("unknown_observation", "queue owner", "restore fresh worker observations before deciding implementation coverage or qualification")
			return b
		}
		b.retag("execution_coverage", "consumer", "run myce portability for the registered extension; add an implementation for a supported target with its own environment and native check, or retain a genuine backend requirement; naming a host cannot create a missing variant")
		var declared []string
		for _, e := range j.Extensions {
			for _, v := range e.Variants {
				declared = append(declared, targetLabel(portabilityTarget(v.Target)))
			}
		}
		if len(declared) > 0 {
			b.Detail += "; declared implementation targets: " + strings.Join(placementStrings(declared), ", ")
		}
		staleCompatible := false
		for _, t := range targets {
			if t.fit.excluded || t.m.Replacement != nil {
				continue
			}
			for _, candidate := range extensionJobs(j, t.m, false) {
				if j.Spec.Resumable && !candidate.Extensions[0].Resumable {
					continue
				}
				if !q.inventoryFresh(q.s.Machines[t.m.ID]) {
					staleCompatible = true
					continue
				}
				b.retag("native_qualification", "queue", "wait; nothing to do")
				b.Reason = q.nativeCheckProgress(candidate.Extensions[0], candidate.Spec.workType(), t.m)
				return b
			}
		}
		if staleCompatible {
			b.retag("unknown_observation", "queue owner", "restore fresh observations from compatible hardware before deciding native qualification")
		}
	} else if len(targets) > 0 && !targets[0].fit.excluded && targets[0].fit.first == "production parity" {
		b.retag("production_parity", "consumer and queue owner", variantParityAction)
	} else if len(targets) > 0 && !targets[0].fit.excluded && (targets[0].fit.first == "target" || targets[0].fit.first == "platform") {
		b.retag("execution_coverage", "consumer and queue owner", "declare supported targets and environment coverage; if the backend is a genuine requirement, keep this job queued for compatible hardware or add and verify another implementation; a host pin cannot supply a missing backend")
		if !freshTargets {
			b.retag("unknown_observation", "queue owner", "restore fresh worker observations before deciding target coverage")
		}
	} else if len(targets) == 0 {
		b.retag("unknown_observation", ownerPolicy, "restore the missing observation named in the detail")
	}
	return b
}

// Platforms with room now for a waiting job that only its authored targets
// refuse. Uses the targets jobHold already evaluated against permanent
// capacity; only those failing solely on "target" are re-checked against the
// live target's current free memory, cores and host memory. A platform where
// the job can already be placed is not advice. Names platforms, never hosts.
func (q *Queue) portableRoomNow(j *Job, compatibleHosts map[string]bool, targets func(func(targetFit, *Job, *Machine))) string {
	if len(j.Spec.Targets) == 0 {
		return ""
	}
	covered := map[string]bool{}
	room := map[string]bool{}
	targets(func(fit targetFit, candidate *Job, m *Machine) {
		label := targetLabel(portabilityTarget(machineTarget(m)))
		if compatibleHosts[hostID(m)] {
			covered[label] = true
		}
		if fit.excluded || m.Replacement != nil || len(fit.failed) != 1 || fit.failed[0] != "target" || room[label] {
			return
		}
		live := q.s.Machines[m.ID]
		if live == nil || !q.inventoryFresh(live) {
			return
		}
		for _, view := range q.placementTargets(live) {
			if !sameTarget(view, m) {
				continue
			}
			now := targetFit{}
			fitsResourcesChecked(candidate, view, view.FreeMemoryGB, &now)
			if len(now.failed) == 1 && now.failed[0] == "target" && hostFits(view, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) {
				room[label] = true
			}
		}
	})
	var out []string
	for label := range room {
		if !covered[label] {
			out = append(out, label)
		}
	}
	slices.Sort(out)
	return strings.Join(out, ", ")
}

// What the queue's own native check is doing now, in one clause. The hold once
// promised the queue ran the check while no check existed for days (gap 3).
func (q *Queue) nativeCheckProgress(e Extension, work string, m *Machine) string {
	host := hostID(m)
	for _, j := range q.liveJobs() {
		r := automaticExtensionCheck(j)
		if r == nil || r.Digest != e.Digest || r.Work != work || terminal(j.State) || len(j.Spec.AllowedMachines) != 1 || j.Spec.AllowedMachines[0] != m.ID {
			continue
		}
		switch {
		case j.State == "running" || j.State == "stopping" || j.State == "assigned":
			return "its native check " + ShortID(j.ID) + " is running on " + host
		case j.Reason != "":
			return "its native check " + ShortID(j.ID) + " on " + host + " is " + j.State + ": " + compactLine(j.Reason)
		default:
			return "its native check " + ShortID(j.ID) + " on " + host + " is " + j.State
		}
	}
	return "the queue submits its native check on " + host + " at that host's next check-in"
}

// Kinds whose mechanism already says exactly why and who acts. Generic
// capacity kinds may still be refined by target coverage.
func specificWaitKind(kind string) bool {
	switch kind {
	case "capacity", "memory", "host_backfill", "no_eligible_host", "waiting_for_worker", "host_unavailable", "capability", "extension", "unknown_observation":
		return false
	}
	return true
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

func (q *Queue) environmentHold(j *Job, m *Machine) *Hold {
	live := q.s.Machines[m.ID]
	if !q.inventoryFresh(live) {
		return withAction(newHold("unknown_observation", "inventory is missing or stale"), "queue owner", "restore fresh worker observations")
	}
	if environmentReady(j, m) {
		return nil
	}
	key := environmentKey(j, m)
	if j.CommandDiscovery != nil {
		keys := environmentFailureKeys(j, m)
		for _, r := range m.EnvironmentReports {
			if environmentReportMatches(r, m) && slices.Contains(keys, r.Key) && q.environmentReportBlocked(r) {
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
	b := withAction(newHold("unprepared", "no builder has started"), "queue", "wait; the queue prepares the immutable environment before launch")
	builder := m
	for _, view := range q.placementTargets(live) {
		if sameTarget(view, m) {
			builder = view
			break
		}
	}
	if !q.environmentPreparationAvailable(builder) {
		b.retag("waiting_for_builder", "queue owner", "wait for an eligible builder with a free preparation core, or restore builder availability")
	}
	for _, p := range q.s.PreparationPlans {
		if p.Machine == live.ID && p.Session == live.Session && p.Key == key {
			b.retag("waiting_for_builder", "queue", "wait for the offered builder to start; ask the queue owner if the offer makes no progress")
			if p.Started {
				b.retag("unknown_observation", "queue owner", "inspect the exact preparation attempt and restore its report")
				b.Detail = "launch authorized; no matching builder report observed"
			}
		}
	}
	for _, r := range live.EnvironmentReports {
		if r.Key != key || !environmentReportMatches(r, m) {
			continue
		}
		switch r.State {
		case "building":
			b.retag("building", "builder", "wait for preparation to finish; inspect environment logs if progress stalls")
			b.Detail = "builder reports " + r.State
		case "failed", "uploading":
			b.retag("deterministically_failed", "consumer", "repair this worker and retry its build, or submit changed immutable inputs")
			b.Detail = environmentFailureReason(r, j.Spec.Logs != "none")
			if environmentFailureKind(r) == "transient" {
				b.retag("waiting_for_builder", "queue", "wait for the recorded retry; ask the queue owner if infrastructure remains unavailable")
			}
		}
	}
	return b
}
