package myce

import (
	"context"
	"errors"
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

// Demand includes all work types of the selected extension. Never let an
// internal check itself manufacture demand, or a cancelled useful job retain it.
// Device admission is separate: disabling cannot cancel an existing Started
// check while its useful demand and prepared identity still exist.
func (q *Queue) extensionQualificationDemand(e Extension, m *Machine) bool {
	for _, j := range q.liveJobs() {
		if j.State != "queued" || j.Spec.ExtensionCheck != nil || j.Spec.Resumable && !e.Resumable {
			continue
		}
		for _, candidate := range extensionJobs(j, m, false) {
			if q.diskHeadroomReason(candidate, m) != "" || !q.placementPolicyAllowed(candidate, m, false) || !deviceAllowed(candidate, m) {
				continue
			}
			for _, selected := range candidate.Extensions {
				if selected.Digest == e.Digest {
					return true
				}
			}
		}
	}
	return false
}

func (q *Queue) extensionQualificationPlan(m *Machine) []ExtensionQualificationPlan {
	if q.deviceAdmissionBlocked(m) || !q.hostLaunchAllowed(m, m.Session) {
		return nil
	}
	var plans []ExtensionQualificationPlan
	for _, e := range q.s.Extensions {
		if !q.extensionQualificationDemand(e, m) {
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
				if r.Qualified || r.CheckID != "" && (!strings.HasPrefix(r.CheckID, "auto-") || terminal(r.CheckState)) {
					break // Warm success or visible negative cache; manual check can retry.
				}
				probe := &Job{Spec: Spec{ExtensionCheck: &ExtensionCheckRequest{Work: work}}, Extensions: []Extension{e}}
				if !environmentReady(probe, m) {
					break
				}
				r = newExtensionReport(e, m, work)
				r.Setup, r.Reason = true, ""
				r.CheckID = automaticExtensionCheckID(m.ID, r)
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
	if len(s.AllowedMachines) != 1 || len(extensions) != 1 || s.Project != "extension-check" || s.SubmissionKey != r.CheckID || r.CheckID != automaticExtensionCheckID(s.AllowedMachines[0], r) || s.ExtensionCheck.Work != r.Work || !r.Setup || r.Qualified || r.CheckState != "" || r.Reason != "" {
		return ErrStale
	}
	e := extensions[0]
	owner := q.s.Machines[s.AllowedMachines[0]]
	if owner == nil {
		return ErrStale
	}
	for _, m := range q.machineTargets(owner) {
		if q.deviceAdmissionBlocked(m) || !matchingExtensionReport(e, m, r) || !q.hostLaunchAllowed(m, m.Session) || !q.extensionQualificationDemand(e, m) {
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

// No automatic retries of an interrupted check. Losing useful demand or the
// prepared identity fences in-flight checks through the existing cancellation.
func (q *Queue) maintainExtensionQualifications() {
	for _, j := range q.liveJobs() {
		r := automaticExtensionCheck(j)
		if r == nil || terminal(j.State) || cancelled(j) {
			continue
		}
		wanted := false
		if len(j.Spec.AllowedMachines) == 1 {
			if owner := q.s.Machines[j.Spec.AllowedMachines[0]]; owner != nil {
				for _, m := range q.machineTargets(owner) {
					for _, e := range j.Extensions {
						wanted = wanted || matchingExtensionReport(e, m, *r) && q.extensionQualificationDemand(e, m)
					}
				}
			}
		}
		if !wanted {
			q.cancelJob(j)
		}
	}
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
	if !matchingExtensionReport(e, &w.Machine, r) || r.CheckID != automaticExtensionCheckID(w.Machine.ID, r) {
		return ErrStale
	}
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
			if matchingExtensionReport(e, &w.Machine, old) && old.CheckID != "" {
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
