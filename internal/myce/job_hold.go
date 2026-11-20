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
func (q *Queue) jobHold(j *Job, resolving bool) (blocker *Hold) {
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
		hold := *wait
		return holdSource(&hold, "matching environment preparation reports")
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
	}
	if wait.Kind == "waiting_for_worker" && len(compatibleHosts) != 0 {
		return b
	}
	if j.Spec.extensionJob() && len(j.Extensions) == 0 {
		b.retag("missing_extension", ownerConsumer, "register an implementation for this work with its targets, environment and native check")
		holdSource(b, "extension registry")
	} else if len(targets) > 0 && !targets[0].fit.excluded && targets[0].fit.first == "extension" {
		if !freshTargets {
			b.retag("unknown_observation", "queue owner", "restore fresh worker observations before deciding implementation coverage or qualification")
			return holdSource(b, "worker inventory")
		}
		b.retag("execution_coverage", ownerConsumer, "specify a candidate variant for an observed target in the adapter")
		holdSource(b, "registered variant targets and fresh worker inventory")
		var declared []string
		for _, e := range j.Extensions {
			for _, v := range e.Variants {
				declared = append(declared, targetLabel(portabilityTarget(v.Target)))
			}
		}
		if len(declared) > 0 {
			defer func() {
				if blocker != nil {
					blocker.Detail = strings.TrimPrefix(blocker.Detail+"; declared implementation targets: "+strings.Join(placementStrings(declared), ", "), "; ")
				}
			}()
		}
		staleCompatible := false
		compatible := false
		var pending, verdict *Hold
		for _, t := range targets {
			if t.fit.excluded || t.m.Replacement != nil {
				continue
			}
			for _, candidate := range extensionJobs(j, t.m, false) {
				compatible = true
				if !memorySizingTargetFits(candidate, t.m) || !q.structuralDemandPolicy(candidate, t.m) || !deviceAllowed(candidate, t.m) || q.diskHeadroomReason(candidate, t.m) != "" {
					continue
				}
				if j.Spec.Resumable && !candidate.Extensions[0].Resumable {
					continue
				}
				if !q.inventoryFresh(q.s.Machines[t.m.ID]) {
					staleCompatible = true
					continue
				}
				progress, terminal, active := q.nativeCheckProgress(candidate.Extensions[0], candidate.Spec.workType(), t.m, resolving)
				switch {
				case active:
					return progress
				case terminal && verdict == nil:
					verdict = progress
				case !terminal && pending == nil:
					pending = progress
				}
			}
		}
		switch {
		case pending != nil:
			return pending
		case staleCompatible:
			b.retag("unknown_observation", ownerPolicy, "restore fresh observations from compatible hardware before deciding native qualification")
			holdSource(b, "worker inventory")
		case verdict != nil:
			return verdict
		case compatible:
			b.retag("no_eligible_host", ownerPolicy, "restore eligible native check admission")
			b.Reason = "no eligible host can currently receive a native check for this work"
			holdSource(b, "native check admission policy and worker inventory")
		case darwinDeveloperToolsRequired(j):
			return darwinDeveloperToolsHold()
		}
	} else if len(targets) > 0 && !targets[0].fit.excluded && targets[0].fit.first == "production parity" {
		b.retag("production_parity", "consumer and queue owner", variantParityAction)
	} else if len(targets) == 0 {
		b.retag("unknown_observation", ownerPolicy, "restore the missing observation named in the detail")
		holdSource(b, "worker inventory")
	} else if !j.Spec.extensionJob() && len(j.Spec.Targets) != 0 {
		var alternatives []ExecutionTarget
		for _, t := range targets {
			if t.fit.excluded || !q.inventoryFresh(q.s.Machines[t.m.ID]) || t.m.Replacement != nil {
				continue
			}
			target := portabilityTarget(machineTarget(t.m))
			if target.OS == "" || target.Arch == "" || portabilityRuntime(target) != "supported" {
				continue
			}
			if slices.Contains(t.fit.failed, "target") {
				if darwinDeveloperToolsRequired(j) && len(compatibleHosts) == 0 {
					return darwinDeveloperToolsHold()
				}
				if target.Platform == "cpu" && j.Spec.Needs.cpuOnly() &&
					(len(t.fit.failed) == 1 || len(t.fit.failed) == 2 && slices.Contains(t.fit.failed, "environment")) {
					alternatives = append(alternatives, target)
				}
			}
		}
		if len(alternatives) > 0 {
			return commandCompatibilityHold(j, b, alternatives, len(compatibleHosts) != 0)
		}
		if !targets[0].fit.excluded && (targets[0].fit.first == "target" || targets[0].fit.first == "platform") {
			if !freshTargets {
				b.retag("unknown_observation", ownerPolicy, "restore fresh worker observations before deciding target coverage")
				holdSource(b, "worker inventory")
			} else {
				b.retag("execution_coverage", ownerConsumer, "verify a target-native implementation for the workload")
				holdSource(b, "authored targets and fresh worker inventory")
			}
		}
	}
	return b
}

// Classify the check on this exact native target. Active work and dispatchable
// offers outrank pending work and retained terminal reports on other targets.
func (q *Queue) nativeCheckProgress(e Extension, work string, m *Machine, resolving bool) (hold *Hold, terminalReport, active bool) {
	host := hostID(m)
	view, _ := e.forMachine(m)
	_, baseline := view.baseline(work, m.GPU)
	if len(view.Check) == 0 || !baseline {
		h := withAction(newHold("execution_coverage", ""), ownerConsumer, "complete the adapter's native qualification command and baseline")
		h.Reason = "the registered variant has incomplete native qualification metadata"
		return holdSource(h, "registered variant check and baseline"), true, false
	}
	progress := func(reason, source, next string) *Hold {
		h := withAction(newHold("native_qualification", ""), ownerQueue, next)
		h.Reason = reason
		return holdSource(h, source)
	}
	for _, j := range q.liveJobs() {
		r := automaticExtensionCheck(j)
		if r == nil || r.Digest != e.Digest || r.Work != work || !matchingExtensionReport(e, m, *r) || terminal(j.State) || len(j.Spec.AllowedMachines) != 1 || j.Spec.AllowedMachines[0] != m.ID {
			continue
		}
		var reason string
		switch j.State {
		case "running", "stopping", "assigned":
			reason = "its native check " + ShortID(j.ID) + " is running on " + host
		default:
			reason = "its native check " + ShortID(j.ID) + " on " + host + " is " + j.State
			copy := *j
			if h := q.waitHold(&copy); h != nil {
				reason += ": " + compactLine(h.Detail)
			}
		}
		return progress(reason, "queued native check job", "wait; nothing to do"), false, true
	}

	releasedRefusal := false
	for _, r := range m.ExtensionReports {
		if r.Work != work || !matchingExtensionReport(e, m, r) || r.Qualified || r.CheckID == "" || !terminal(r.CheckState) {
			continue
		}
		if q.automaticCheckRetryable(m.ID, r) {
			releasedRefusal = releasedRefusal || r.CheckState == "failed"
			continue
		}
		h := withAction(newHold("no_eligible_host", ""), ownerConsumer, nativeFailureAction)
		h.Reason = "no eligible host can currently receive a native check; " + nativeFailureReason
		return holdSource(h, nativeFailureSource), true, false
	}
	reason, source := "native qualification on "+host+" is waiting for preparation or dispatch eligibility; no check has been submitted", "worker preparation and check admission"
	if releasedRefusal {
		reason, source = "the native check was refused before launch and released; qualification remains pending", "accepted release and exact-attempt execution evidence"
	}
	for _, plan := range q.extensionQualificationPlan(m) {
		if plan.Report.Digest == e.Digest && plan.Report.Work == work {
			offer := "the queue submits its native check on " + host + " at that host's next check-in"
			if releasedRefusal {
				return progress(reason+"; "+offer, source, "wait for the automatic retry"), false, true
			}
			return progress(offer, "dispatchable native check offer", "wait; nothing to do"), false, true
		}
	}
	if resolving && !q.extensionQualificationDemand(e, m) {
		if !releasedRefusal {
			reason, source = "automatic native qualification requires submitted demand; resolve creates no check", "registered variant and queue qualification demand"
		}
		h := progress(reason, source, "submit the job without require_target to request native qualification")
		h.Owner = ownerConsumer
		return h, false, false
	}
	next := "wait; nothing to do"
	if releasedRefusal {
		next = "wait for the automatic retry"
	}
	return progress(reason, source, next), false, false
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
			b.retag("deterministically_failed", ownerConsumer, "inspect the recorded failure for the frozen environment build")
			b.Detail = environmentFailureReason(r, j.Spec.Logs != "none")
			if environmentFailureKind(r) == "transient" {
				b.retag("waiting_for_builder", ownerQueue, "wait for the recorded retry")
			}
			holdSource(b, "matching environment preparation report")
		}
	}
	return b
}
