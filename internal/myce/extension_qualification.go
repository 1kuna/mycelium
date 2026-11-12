package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// This is a demand offer, not permission to execute. The worker submits an
// ordinary frozen check job, which still needs admission and launch authority.
type ExtensionQualificationPlan struct {
	Machine string          `json:"machine"`
	Report  ExtensionReport `json:"report"`
}

func automaticExtensionCheck(j *Job) *ExtensionReport {
	if j.Spec.ExtensionCheck == nil {
		return nil
	}
	return j.Spec.ExtensionCheck.Automatic
}

func extensionReportIdentity(r ExtensionReport) string {
	r.Setup, r.Qualified, r.CheckID, r.CheckState, r.Reason = false, false, "", "", ""
	return identityHash(r)
}

func matchingExtensionReport(e Extension, m *Machine, r ExtensionReport) bool {
	return m.ExtensionFingerprint != "" && extensionReportIdentity(r) == extensionReportIdentity(newExtensionReport(e, m, r.Work))
}

func automaticExtensionCheckID(machine string, r ExtensionReport) string {
	return "auto-" + identityHash([2]string{machine, extensionReportIdentity(r)})
}

// A cancelled automatic check has no verdict, so it is not negative evidence.
// Its successor gets a new immutable key derived from the cancelled one; the
// queue reruns the check itself instead of waiting for an explicit retry.
func automaticExtensionRetryCheckID(machine string, r ExtensionReport, previous string) string {
	return "auto-" + identityHash([3]string{machine, extensionReportIdentity(r), previous})
}

func automaticCheckCancelled(r ExtensionReport) bool {
	return strings.HasPrefix(r.CheckID, "auto-") && r.CheckState == "cancelled" && !r.Qualified
}

// The base key, or a successor whose every predecessor the queue recorded as
// cancelled. Each step is one submission lookup; the chain cannot fork.
func (q *Queue) automaticCheckKeyValid(machine string, r ExtensionReport) bool {
	id := automaticExtensionCheckID(machine, r)
	for range len(q.s.Submissions) + 1 {
		if r.CheckID == id {
			return true
		}
		record := q.s.Submissions[submissionDigest("extension-check", id)]
		if record == nil {
			return false
		}
		j := q.s.Jobs[record.JobID]
		if j == nil {
			j = record.Job
		}
		if j == nil || !terminal(j.State) || !cancelled(j) {
			return false
		}
		id = automaticExtensionRetryCheckID(machine, r, id)
	}
	return false
}

// Demand includes all work types of the selected extension. Never let an
// internal check itself manufacture demand, or a cancelled useful job retain it.
// Device admission is separate: disabling cannot cancel an existing Started
// check while its useful demand and prepared identity still exist.
func (q *Queue) extensionQualificationDemand(e Extension, m *Machine) bool {
	return q.extensionQualificationDemands(m, &e)[e.Digest]
}

// One planning pass collects demand by frozen digest, rather than running disk
// and policy checks for every registered extension times every queued job.
// In particular, plain commands cannot demand extension qualification. The
// optional selection keeps authoritative single-check validation equally cheap.
// This map belongs only to this locked pass; it never survives a queue mutation.
func (q *Queue) extensionQualificationDemands(m *Machine, only *Extension) map[string]bool {
	demand := map[string]bool{}
	// A new admission reservation cannot revoke useful demand and cancel a
	// check that already Started. New check offers/submissions gate separately.
	for _, j := range q.liveJobs() {
		if j.State != "queued" || j.Spec.ExtensionCheck != nil || len(j.Extensions) == 0 {
			continue
		}
		for _, candidate := range extensionJobs(j, m, false) {
			eligible := func(e Extension) bool {
				return (!j.Spec.Resumable || e.Resumable) && (only == nil || e.Digest == only.Digest)
			}
			if !slices.ContainsFunc(candidate.Extensions, eligible) {
				continue
			}
			if q.diskHeadroomReason(candidate, m) != "" || !q.qualificationDemandPolicy(candidate, m) || !deviceAllowed(candidate, m) {
				continue
			}
			for _, selected := range candidate.Extensions {
				if eligible(selected) {
					demand[selected.Digest] = true
				}
			}
			if only != nil && demand[only.Digest] {
				return demand
			}
		}
	}
	return demand
}

// Whether the useful job could use this host once it has room: the host-level
// and job-level requirements of placementDemandPolicy without its momentary
// capacity and production holds. Those holds kept the queue's own check from
// ever being offered on a busy host, so the job waited for a check that never
// ran. The check, once submitted, still passes every admission hold itself.
func (q *Queue) qualificationDemandPolicy(j *Job, m *Machine) bool {
	if q.variantParityHold(j, m) != nil || !dependencyReady(j) || m.Recovering || cancelled(j) || !dockerRuntimeFits(j, m) || q.maintenanceBlocked(hostID(m)) || !logPolicyFits(j.Spec.Logs, m) || availabilityBlocked(m) || !q.jobAllowsMachine(j, m.ID) {
		return false
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok && (!logPolicyFits(model.Logs, m) || !modelFenceAllows(model, m) || !q.projectAllows(model.Project, m.ID)) {
		return false
	}
	return q.checkpointTargetAdmission(j, m) == nil
}

func (q *Queue) extensionQualificationPlan(m *Machine) []ExtensionQualificationPlan {
	defer q.bodyPhaseFinish("qualification_plan", q.bodyPhaseStart())
	if !q.reservationAllows(nil, m) || q.deviceAdmissionBlocked(m) || !q.hostLaunchAllowed(m, m.Session) {
		return nil
	}
	var plans []ExtensionQualificationPlan
	demand := q.extensionQualificationDemands(m, nil)
	for _, e := range q.s.Extensions {
		if !demand[e.Digest] {
			continue
		}
		view, ok := e.forMachine(m)
		if !ok || len(view.Check) == 0 {
			continue
		}
		for _, work := range extensionWorks(e) {
			if _, ok := view.baseline(work, m.GPU); !ok {
				continue // Smoke commands cannot manufacture native qualification.
			}
			for _, r := range m.ExtensionReports {
				if r.Work != work || !r.Setup || !matchingExtensionReport(e, m, r) {
					continue
				}
				retry := automaticCheckCancelled(r)
				if r.Qualified || r.CheckID != "" && (!strings.HasPrefix(r.CheckID, "auto-") || terminal(r.CheckState)) && !retry {
					break // Warm success or a failed verdict; manual check can retry.
				}
				probe := &Job{Spec: Spec{ExtensionCheck: &ExtensionCheckRequest{Work: work}}, Extensions: []Extension{e}}
				if !environmentReady(probe, m) {
					break
				}
				previous := r.CheckID
				r = newExtensionReport(e, m, work)
				r.Setup, r.Reason = true, ""
				r.CheckID = automaticExtensionCheckID(m.ID, r)
				if retry {
					r.CheckID = automaticExtensionRetryCheckID(m.ID, r, previous)
				} else if current := r; strings.HasPrefix(previous, "auto-") && previous != r.CheckID {
					// The worker holds a successor in this key's chain. Offer that
					// key so its replay reports the outcome; the base key is one the
					// worker ignores, which left a cancelled successor unretried.
					current.CheckID = previous
					if q.automaticCheckKeyValid(m.ID, current) {
						r.CheckID = previous
					}
				}
				plans = append(plans, ExtensionQualificationPlan{Machine: m.ID, Report: r})
				break
			}
		}
	}
	slices.SortFunc(plans, func(a, b ExtensionQualificationPlan) int { return strings.Compare(a.Report.CheckID, b.Report.CheckID) })
	return plans
}

// Called in the submit transaction. Key replay happens first, so a reconnect
// adopts the original immutable invocation, including a terminal failure.
func (q *Queue) validateAutomaticExtensionCheck(s Spec, extensions []Extension) error {
	if s.ExtensionCheck == nil || s.ExtensionCheck.Automatic == nil {
		return nil
	}
	r := *s.ExtensionCheck.Automatic
	if len(s.AllowedMachines) != 1 || len(extensions) != 1 || s.Project != "extension-check" || s.SubmissionKey != r.CheckID || !q.automaticCheckKeyValid(s.AllowedMachines[0], r) || s.ExtensionCheck.Work != r.Work || !r.Setup || r.Qualified || r.CheckState != "" || r.Reason != "" {
		return ErrStale
	}
	e := extensions[0]
	owner := q.s.Machines[s.AllowedMachines[0]]
	if owner == nil {
		return ErrStale
	}
	for _, m := range q.machineTargets(owner) {
		if !q.reservationAllows(nil, m) || q.deviceAdmissionBlocked(m) || !matchingExtensionReport(e, m, r) || !q.hostLaunchAllowed(m, m.Session) || !q.extensionQualificationDemand(e, m) {
			continue
		}
		view, ok := e.forMachine(m)
		_, baseline := view.baseline(r.Work, m.GPU)
		probe := &Job{Spec: s, Extensions: extensions}
		if ok && baseline && len(view.Check) > 0 && environmentReady(probe, m) && deviceAllowed(probe, m) {
			return nil
		}
	}
	return ErrStale
}

// The queue accepts qualification only for the exact invocation it accepted as
// successful. The worker's comparison runs in the ordinary check runner before
// Complete; stale, cancelled and unsuccessful results cannot reach this state.
func (q *Queue) acceptExtensionQualifications(owner *Machine) {
	for _, m := range q.machineTargets(owner) {
		for i := range m.ExtensionReports {
			r := &m.ExtensionReports[i]
			if !r.Qualified || !strings.HasPrefix(r.CheckID, "auto-") || r.Platform != m.GPU || extensionReportDevice(r.Platform, r.Device) != extensionReportDevice(m.GPU, m.Device) {
				continue
			}
			accepted := false
			record := q.s.Submissions[submissionDigest("extension-check", r.CheckID)]
			if record != nil {
				j := q.s.Jobs[record.JobID]
				if j != nil && j.State == "succeeded" && !cancelled(j) && j.ReleaseConfirmed && j.Machine == m.ID {
					check := automaticExtensionCheck(j)
					for _, e := range j.Extensions {
						if check != nil && extensionReportIdentity(*check) == extensionReportIdentity(*r) && matchingExtensionReport(e, m, *r) {
							accepted = true
						}
					}
				}
			}
			if !accepted {
				r.Qualified = false
				r.Reason = "accepted native check required"
			}
		}
	}
}

// Recompute shared demand from queued work. A reversible pause parks unstarted
// checks without poisoning their immutable key. Assigned checks drain; actual
// cancellation or a changed prepared identity keeps the existing failure fence.
func (q *Queue) maintainExtensionQualifications() {
	for _, j := range q.liveJobs() {
		r := automaticExtensionCheck(j)
		if r == nil || terminal(j.State) || cancelled(j) {
			continue
		}
		wanted, identityValid, identityUnknown := false, false, true
		if len(j.Spec.AllowedMachines) == 1 {
			if owner := q.s.Machines[j.Spec.AllowedMachines[0]]; owner != nil {
				identityUnknown = q.automaticCheckIdentityUnknown(owner, *r)
				for _, m := range q.machineTargets(owner) {
					for _, e := range j.Extensions {
						if matchingExtensionReport(e, m, *r) {
							identityValid = true
							wanted = wanted || q.extensionQualificationDemand(e, m)
						}
					}
				}
			}
		}
		// Any live owner retains the invocation while its demand is temporarily
		// absent (paused, host drained, disk or policy hold), or while its host
		// has not yet reported the identity again after a worker restart.
		// Cancelling it then left no verdict and no rerun. Only an observed
		// identity change fences it.
		retained := false
		if !wanted && (identityValid || identityUnknown) {
			for _, owner := range q.liveJobs() {
				retained = retained || !terminal(owner.State) && !cancelled(owner) && owner.Spec.ExtensionCheck == nil && slices.ContainsFunc(owner.Extensions, func(e Extension) bool { return e.Digest == r.Digest })
			}
		}
		if !wanted && !retained {
			q.cancelJob(j)
		} else if !wanted && j.State == "queued" {
			q.touchJob(j.ID)
			j.State, j.Reason = "waiting", "waiting for runnable extension demand"
		} else if wanted && j.State == "waiting" {
			q.touchJob(j.ID)
			j.State, j.Reason = "queued", ""
		}
	}
}

// A restarted worker rejoins before it reports its native identity and device
// inventory again. That gap is not evidence that the identity changed: the live
// fleet lost a pending check this way and the job waited for days (gap 3).
func (q *Queue) automaticCheckIdentityUnknown(owner *Machine, r ExtensionReport) bool {
	if owner.Gone || owner.Replacement != nil {
		return false
	}
	if q.unreported[owner.ID] || !q.inventoryFresh(owner) {
		return true
	}
	for _, m := range q.machineTargets(owner) {
		if extensionReportDevice(m.GPU, m.Device) == extensionReportDevice(r.Platform, r.Device) {
			return m.ExtensionFingerprint == ""
		}
	}
	return true // the check's device is not reported right now
}

// One short control-plane reconciliation per offer, never a second executor.
// Persist before submitting: a lost response or worker restart replays the same
// check key and scratch directory. Terminal reports are the negative cache.
func (w Worker) reconcileExtensionQualification(ctx context.Context, plan ExtensionQualificationPlan) error {
	r := plan.Report
	if plan.Machine != w.Machine.ID {
		return ErrStale
	}
	var err error
	if w.Machine.Devices != nil || r.Platform == "cpu" {
		j := Job{AllocatedDevices: []string{r.Device}}
		if r.Platform == "cpu" {
			j.Spec.Needs.Platform.Required = []string{"cpu"}
		}
		w, err = w.forAttempt(j)
		if err != nil {
			return err
		}
	}
	e, err := ReadExtension(extensionFolder(w.ExtensionRoot, Extension{Name: r.Name, Version: r.Version}))
	if err != nil {
		return err
	}
	if !matchingExtensionReport(e, &w.Machine, r) || !strings.HasPrefix(r.CheckID, "auto-") {
		return ErrStale
	}
	keyValid := r.CheckID == automaticExtensionCheckID(w.Machine.ID, r)
	unlock, err := extensionLock(w.ExtensionRoot, e, w.Machine.GPU+"-check", false)
	if err != nil {
		return err
	}
	defer unlock()
	reports := readExtensionReports(w.ExtensionRoot, e, r.Platform)
	index := -1
	for i, old := range reports {
		if old.Work == r.Work && old.Device == r.Device {
			if !old.Setup || !matchingExtensionReport(e, &w.Machine, old) {
				return ErrStale
			}
			index = i
			successor := automaticCheckCancelled(old) && r.CheckID == automaticExtensionRetryCheckID(w.Machine.ID, r, old.CheckID)
			keyValid = keyValid || successor || old.CheckID == r.CheckID
			if old.CheckID != "" && !successor {
				if old.CheckID != r.CheckID || terminal(old.CheckState) || old.Qualified {
					return nil // A manual retry or terminal evidence supersedes this offer.
				}
			}
			break
		}
	}
	if index < 0 {
		return ErrStale // Preparation has not published setup on this worker.
	}
	if !keyValid {
		return ErrStale
	}
	scratch := filepath.Join(extensionCache(w.ExtensionRoot, e, r.Platform), r.CheckID)
	if err := os.MkdirAll(scratch, 0700); err != nil {
		return err
	}
	scratch, err = filepath.Abs(scratch)
	if err != nil {
		return err
	}
	reports[index] = r
	reports[index].CheckState, reports[index].Reason = "queued", "native check pending"
	if err := writeExtensionReports(w.ExtensionRoot, e, r.Platform, reports); err != nil {
		return err
	}
	selection := ResourceNeeds{}
	if v := e.variant(machineTarget(&w.Machine)); v != nil {
		selection.Memory = v.Needs.Memory
	}
	if r.Platform != "cpu" {
		selection.GPU = &GPUSelection{Devices: []string{r.Device}}
	}
	var j Job
	err = w.Client.Call(ctx, "POST", "/api/jobs", Spec{Name: "extension-check", Project: "extension-check", Class: "experiment", SubmissionKey: r.CheckID,
		Extension: extensionKey(e), ExtensionDigest: e.Digest, AllowedMachines: []string{plan.Machine}, Needs: selection,
		ExtensionCheck: &ExtensionCheckRequest{Work: r.Work, Directory: scratch, Automatic: &r}}, &j)
	if err != nil {
		return err // An uncertain reply must not destroy scratch or create a new key.
	}
	report := &reports[index]
	report.CheckState = j.State
	if terminal(j.State) && (j.ReleaseConfirmed || j.Attempt == 0) {
		report.Qualified = j.State == "succeeded" && !cancelled(&j) && j.Machine == plan.Machine && automaticExtensionCheck(&j) != nil && extensionReportIdentity(*automaticExtensionCheck(&j)) == extensionReportIdentity(r)
		report.Reason = "native check " + j.State + "; explicit check required to retry"
		if report.Qualified {
			report.Reason = extensionQualificationReason(e, *report, &w.Machine)
			report.Qualified = report.Reason == ""
		}
		// Only accepted terminal release proves the directory has no active user.
		err = os.RemoveAll(scratch)
	} else if terminal(j.State) {
		report.CheckState = "releasing"
	}
	return errors.Join(err, writeExtensionReports(w.ExtensionRoot, e, r.Platform, reports))
}

// extensionDeadEnd names a qualification or coverage condition only the
// consumer can change: no compatible variant on any known host, or every
// compatible variant lacks a native check/baseline or has a failed verdict.
// Pending preparation, pending or cancelled checks and unknown inventory are
// the queue's own work and stay waits.
func (q *Queue) extensionDeadEnd(j *Job) string {
	if !j.Spec.extensionJob() || len(j.Extensions) == 0 {
		return ""
	}
	for _, name := range j.Spec.AllowedMachines {
		if q.s.Machines[name] == nil {
			return "" // a named worker may still join
		}
	}
	var dead []string
	compatible := false
	fleet := map[string]bool{}
	for _, owner := range q.s.Machines {
		if owner.Replacement != nil || !q.jobAllowsMachine(j, owner.ID) {
			continue
		}
		if owner.CapabilityProfile == nil {
			return "" // unknown OS/arch cannot rule out a variant
		}
		for _, t := range possibleTargets(owner) {
			fleet[targetLabel(t)] = true
		}
		for _, m := range q.machineTargets(owner) {
			for _, candidate := range extensionJobs(j, m, false) {
				e := candidate.Extensions[0]
				if j.Spec.Resumable && !e.Resumable {
					continue
				}
				compatible = true
				why := q.qualificationDeadEnd(candidate, e, m, owner)
				if why == "" {
					return ""
				}
				dead = append(dead, why)
			}
		}
	}
	if compatible {
		slices.Sort(dead)
		return dead[0]
	}
	if len(fleet) == 0 {
		return ""
	}
	var declared []string
	for _, e := range j.Extensions {
		if j.Spec.Resumable && !e.Resumable {
			continue
		}
		if len(e.Variants) == 0 {
			for _, p := range e.platforms() {
				for label := range fleet {
					if strings.HasSuffix(label, "/"+p) {
						return "" // a legacy implementation can serve a known host whose device is absent now
					}
				}
				declared = append(declared, "*/*/"+p)
			}
		}
		for _, v := range e.Variants {
			label := targetLabel(v.Target)
			if fleet[label] {
				return "" // the host is known; its device may report again
			}
			declared = append(declared, label)
		}
	}
	known := make([]string, 0, len(fleet))
	for label := range fleet {
		known = append(known, label)
	}
	slices.Sort(known)
	return fmt.Sprintf("dead end: no registered implementation matches a known host (declared %s; hosts offer %s); action: register a variant for one of the offered targets and resubmit", strings.Join(placementStrings(declared), ", "), strings.Join(known, ", "))
}

// Targets a host can offer even while a device is briefly unreported.
func possibleTargets(owner *Machine) []ExecutionTarget {
	base := machineTarget(owner)
	platforms := []string{"cpu", owner.GPU}
	for _, d := range owner.Devices {
		platforms = append(platforms, d.Platform)
	}
	for _, a := range owner.CapabilityProfile.Accelerators {
		platforms = append(platforms, a.Type)
	}
	var out []ExecutionTarget
	for _, p := range platforms {
		if p != "" {
			out = append(out, ExecutionTarget{OS: base.OS, Arch: base.Arch, Platform: p})
		}
	}
	return out
}

func (q *Queue) qualificationDeadEnd(j *Job, e Extension, m, owner *Machine) string {
	if qualifiedExtension(j, m) != nil {
		return ""
	}
	view, ok := e.forMachine(m)
	if !ok {
		return ""
	}
	work, label := j.Spec.workType(), e.Name+"@"+e.Version
	target := targetLabel(machineTarget(m))
	if len(view.Check) == 0 {
		return fmt.Sprintf("dead end: %s declares no native check for %s on %s, so it cannot be qualified; action: add a check command and baselines[%s], register a new version and resubmit", label, work, target, work)
	}
	if _, ok := view.baseline(work, m.GPU); !ok {
		return fmt.Sprintf("dead end: %s declares no native baseline for %s on %s, so it cannot be qualified; action: add baselines[%s], register a new version and resubmit", label, work, target, work)
	}
	if !q.inventoryFresh(owner) {
		return ""
	}
	for _, r := range m.ExtensionReports {
		if r.Work != work || r.Digest != e.Digest || !matchingExtensionReport(e, m, r) {
			continue
		}
		auto := strings.HasPrefix(r.CheckID, "auto-")
		if auto && r.CheckState == "failed" || !auto && r.CheckID != "" && strings.HasPrefix(r.Reason, "check failed") {
			return fmt.Sprintf("dead end: the native check for %s failed on %s (%s); action: fix the implementation or its check, register a new version and resubmit", label, hostID(m), r.Reason)
		}
		return ""
	}
	return ""
}
