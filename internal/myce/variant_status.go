package myce

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

const variantParityAction = "MY-19 production parity approval required: compare this variant's outputs with the reference within the project's accuracy criteria in a check or experiment on a machine of this kind, then approve the kind with myce parity add --project PROJECT --work WORK --evidence JOB_ID; native qualification alone is not cross-variant parity"

// The hold kind for production outside the project's approved machine kinds.
// Previous releases called it production_parity_hosts.
const productionParityHostHold = "production_parity_kind"

// Existing portability projection supplies registration warnings; registering
// an implementation neither executes it nor proves hard requirements/parity.
type ExtensionRegistration struct {
	OK       bool     `json:"ok"`
	Warnings []string `json:"warnings,omitempty"`
}

func portabilityWarnings(report PortabilityReport) []string {
	warnings := []string{}
	for _, row := range report.Targets {
		if !row.Observed || row.Runtime != "supported" {
			continue
		}
		fresh := slices.ContainsFunc(row.Hosts, func(h PortabilityHost) bool { return h.Freshness == "fresh" })
		if !fresh {
			continue
		}
		if !row.Declared {
			warnings = append(warnings, "Missing variant for observed "+targetLabel(row.Target)+". Investigate an explicit implementation with its own resources, lock/environment and native check; run myce portability for per-target actions. Preserve genuine requirement_reasons and obtain owner-accepted MY-19 parity before production.")
		} else if slices.ContainsFunc(row.Hosts, func(h PortabilityHost) bool { return h.Freshness == "fresh" && h.Qualification != "qualified" }) {
			warnings = append(warnings, "Declared "+targetLabel(row.Target)+" coverage still needs exact native qualification on unqualified hosts; declaration or environment preparation alone does not qualify it.")
		}
	}
	if report.Inventory != "fresh" {
		warnings = append(warnings, "Inventory coverage is "+report.Inventory+"; unavailable or stale inventory cannot establish complete portability. Run myce portability again after fresh inventory arrives.")
	}
	return warnings
}

func (q *Queue) extensionRegistrationWarnings(e Extension, contexts ...context.Context) []string {
	report, err := q.Portability(PortabilityRequest{Extension: extensionKey(e)}, contexts...)
	if err != nil {
		return []string{"Live portability coverage unknown; run myce portability --extension NAME@VERSION after repairing inventory or frozen inputs. Registration remains accepted."}
	}
	return portabilityWarnings(report)
}

// Registration metadata is server-owned and frozen with accepted jobs. Older
// registrations retain their admission contract subject to live owner policy.
func (q *Queue) variantParityReason(j *Job, m *Machine) string {
	if q.commandDiscoveryProduction(j) {
		return commandDiscoveryParityAction
	}
	if j.Spec.ExtensionCheck != nil {
		return ""
	}
	production := j.Spec.Priority == "production" || jobClass(j.Spec) == "production"
	if q.parityHostListHolds(j, m) {
		return productionParityHostHold + ": held: " + targetLabel(jobTargetOn(j, m)) + " is not a machine kind the project approved for production; " + variantParityAction
	}
	approved, _ := q.parityApproved(j, m)
	reason := ""
	for _, e := range j.Extensions {
		if !e.matchesTarget(m) || (j.State == "running" || j.State == "stopping") && j.Extension != nil && j.Extension.Digest != e.Digest {
			continue
		}
		copy := *j
		copy.Extensions = []Extension{e}
		if qualifiedExtension(&copy, m) == nil {
			continue
		}
		measured, failure := variantQuality(e, j.Spec, m)
		if failure != "" {
			reason = failure
			continue
		}
		if production && !measured && !approved && (e.ProductionParityRequired || len(j.Spec.QualityTolerance) > 0 || e.hasQuality(j.Spec.workType())) {
			reason = "quality measurements unknown; " + variantParityAction
			continue
		}
		return ""
	}
	return reason
}

func jobHasVariants(j *Job) bool {
	for _, e := range j.Extensions {
		if len(e.Variants) > 0 {
			return true
		}
	}
	return false
}

// The owner's production parity host list excludes this host. Placement and
// the wait both read this one decision.
func (q *Queue) parityHostListHolds(j *Job, m *Machine) bool {
	production := j.Spec.Priority == "production" || jobClass(j.Spec) == "production"
	return !q.commandDiscoveryProduction(j) && j.Spec.ExtensionCheck == nil && production && !q.productionParityAllowed(j, m)
}

// Variant parity that holds this job on every qualified host, from the first
// such host's reason.
func (q *Queue) variantParityWait(j *Job) *Hold {
	if q.commandDiscoveryProduction(j) {
		return newHold("production_parity", commandDiscoveryParityAction)
	}
	_, hasPolicy := q.parityApprovals(j.Spec.Project, j.Spec.workType())
	if !jobHasVariants(j) && !hasPolicy {
		return nil
	}
	var first *Hold
	for _, m := range q.plannerMachines() {
		if !q.jobAllowsMachine(j, m.ID) || qualifiedExtension(j, m) == nil {
			continue
		}
		h := q.variantParityHold(j, m)
		if h == nil {
			return nil
		}
		if first == nil {
			first = h
		}
	}
	return first
}

// variantParityHold is the per-target parity check: the owner's host list
// holds, or the variant needs reference-output parity.
func (q *Queue) variantParityHold(j *Job, m *Machine) *Hold {
	why := q.variantParityReason(j, m)
	if why == "" {
		return nil
	}
	if q.parityHostListHolds(j, m) {
		return newHold(productionParityHostHold, why)
	}
	return newHold("production_parity", why)
}

// Diagnostic rows describe frozen variants against live host/device targets.
// Eligible means currently admissible, not selected, reserved or ranked first.
type VariantEligibility struct {
	Extension string          `json:"extension"`
	Digest    string          `json:"digest"`
	Variant   ExecutionTarget `json:"variant"`
	Host      string          `json:"host"`
	Device    string          `json:"device,omitempty"`
	Target    ExecutionTarget `json:"target"`
	Freshness string          `json:"freshness"`
	Qualified bool            `json:"qualified"`
	Eligible  bool            `json:"eligible"`
	Reasons   []string        `json:"reasons"`
}

func (r VariantEligibility) Summary() string {
	reason := "eligible now; selection still follows queue priority and least-valuable fit"
	if len(r.Reasons) > 0 {
		reason = strings.Join(r.Reasons, "; ")
	}
	return fmt.Sprintf("%s %s on %s device %s (%s): %s", r.Extension, targetLabel(r.Variant), r.Host, r.Device, targetLabel(r.Target), reason)
}

func (q *Queue) variantFreshness(m *Machine) string {
	if m.LastSeen.IsZero() {
		return "unknown"
	}
	now := q.clock.Now()
	if !q.workerFresh(m, now) {
		return "stale"
	}
	return "fresh"
}

// Only detached observations call this; it never hydrates bundles or does I/O.
// Started attempts expose their fixed assignment instead of hypothetical moves.
func (q *Queue) projectVariantEligibility(j *Job) {
	j.VariantEligibility, j.VariantAdvice = nil, nil
	if terminal(j.State) || j.Spec.ExtensionCheck != nil || !j.Spec.extensionJob() {
		return
	}
	for _, e := range j.Extensions {
		if len(e.Variants) == 0 {
			continue
		}
		copy := *j
		copy.Extensions = []Extension{e}
		targets := q.plannerMachines()
		for _, v := range e.Variants {
			for _, m := range targets {
				r := VariantEligibility{Extension: safePortabilityID(e.Name) + "@" + safePortabilityID(e.Version), Digest: e.Digest, Variant: portabilityTarget(v.Target), Host: hostID(m), Device: m.Device, Target: portabilityTarget(machineTarget(m)), Freshness: q.variantFreshness(m), Reasons: []string{}}
				if v.Target != machineTarget(m) {
					r.Reasons = append(r.Reasons, "variant target does not match host target")
				} else {
					r.Qualified = qualifiedExtension(&copy, m) != nil
					if !r.Qualified {
						reason := "native qualification required for this exact digest, work, target, device, environment and fingerprint; prepare and check this variant"
						for _, report := range m.ExtensionReports {
							if report.Work == j.Spec.workType() && report.Digest == e.Digest && report.Platform == m.GPU {
								if why := extensionQualificationReason(e, report, m); why != "" {
									reason = why
								}
							}
						}
						r.Reasons = append(r.Reasons, reason)
					}
					if reason := q.variantParityReason(&copy, m); reason != "" {
						r.Reasons = append(r.Reasons, reason)
					}
					if r.Qualified {
						if j.State != "queued" {
							r.Reasons = append(r.Reasons, "attempt already assigned; placement alternatives are evaluated after requeue")
						} else {
							r.Reasons = append(r.Reasons, q.variantAdmissionReasons(&copy, m)...)
						}
					}
				}
				if r.Freshness != "fresh" {
					r.Reasons = append(r.Reasons, "host inventory "+r.Freshness+"; current eligibility unknown")
				}
				r.Reasons = slices.Compact(r.Reasons)
				r.Eligible = len(r.Reasons) == 0
				j.VariantEligibility = append(j.VariantEligibility, r)
			}
		}
	}
	slices.SortFunc(j.VariantEligibility, func(a, b VariantEligibility) int {
		return strings.Compare(a.Extension+targetLabel(a.Variant)+a.Host+"/"+a.Device, b.Extension+targetLabel(b.Variant)+b.Host+"/"+b.Device)
	})
	j.VariantAdvice = q.missingVariantAdvice(j)
}

func (q *Queue) variantAdmissionReasons(j *Job, m *Machine) []string {
	var best []string
	for _, candidate := range q.memoryCandidates(j, m) {
		var fit targetFit
		fitsResourcesChecked(candidate, m, m.FreeMemoryGB, &fit)
		fit.check(hostFits(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB), "host memory")
		reasons := slices.Clone(fit.failed)
		// These are fixed dimension names, not raw worker errors or recipe paths.
		if !q.jobAllowsMachine(candidate, m.ID) {
			reasons = append(reasons, "job or project host restriction")
		}
		withoutParity := *candidate
		withoutParity.Spec.QualityTolerance = nil
		withoutParity.Extensions = slices.Clone(candidate.Extensions)
		for i := range withoutParity.Extensions {
			withoutParity.Extensions[i].ProductionParityRequired = false
			withoutParity.Extensions[i].Variants = slices.Clone(withoutParity.Extensions[i].Variants)
			for k := range withoutParity.Extensions[i].Variants {
				withoutParity.Extensions[i].Variants[k].Quality = nil
			}
		}
		if !q.placementAllowed(&withoutParity, m) {
			reasons = append(reasons, "maintenance, policy, reservation or release hold")
		}
		if !environmentReady(candidate, m) {
			reasons = append(reasons, "environment preparation required")
		}
		if !q.workerIdle(m, candidate) {
			reasons = append(reasons, "current attempt or exact cleanup occupies capacity")
		}
		if !q.checkpointReady(candidate, m) {
			reasons = append(reasons, "durable checkpoint unavailable")
		}
		if q.preparationActive(m) || q.preparationStopping(m) {
			reasons = append(reasons, "software preparation occupies capacity")
		}
		if q.preemptionReleasesPending(candidate) {
			reasons = append(reasons, "selected preemption release pending")
		}
		if !q.preemptionPlacementAllows(candidate, m) {
			reasons = append(reasons, "committed preemption target must assign or lose its session first")
		}
		if q.capReasonFor(candidate, m) != "" {
			reasons = append(reasons, "project concurrency or resource cap")
		}
		if candidate.Pause != nil {
			reasons = append(reasons, "job paused by owner")
		}
		if best == nil || len(reasons) < len(best) {
			best = reasons
		}
		if len(reasons) == 0 {
			return nil
		}
	}
	if best == nil {
		return []string{"no qualified variant resource envelope for this target"}
	}
	return best
}

func (q *Queue) missingVariantAdvice(j *Job) []string {
	if j.State != "queued" || !dependencyReady(j) || cancelled(j) || j.Pause != nil || len(j.Spec.AllowedMachines) > 0 || len(q.s.Policies[j.Spec.Project].AllowedMachines) > 0 {
		return nil
	}
	for _, r := range j.VariantEligibility {
		if r.Eligible {
			return nil
		}
	}
	values := q.hostValues()
	targets := q.plannerMachines()
	var advice []string
	for _, e := range j.Extensions {
		if len(e.Variants) == 0 || len(e.RequirementReasons) > 0 {
			continue
		}
		for _, scarce := range targets {
			v := e.variant(machineTarget(scarce))
			h := values[hostID(scarce)]
			if v == nil || h == nil || !h.known || q.variantFreshness(scarce) != "fresh" {
				continue
			}
			// Only a qualified, fitting implementation constrained by live capacity
			// establishes scarce-host contention. A parity hold is its own remedy.
			copy := *j
			copy.Extensions = []Extension{e}
			if qualifiedExtension(&copy, scarce) == nil || q.variantParityReason(&copy, scarce) != "" {
				continue
			}
			// Do not attribute policy, preparation, caps or durable-storage holds
			// to missing implementations; those have their own direct remedy.
			if !q.placementAllowed(&copy, scarce) || !q.checkpointReady(&copy, scarce) || q.capReasonFor(&copy, scarce) != "" || !environmentReady(&copy, scarce) {
				continue
			}
			capacity := q.qualificationCapacity(scarce)
			fitsEmpty := false
			for _, c := range extensionJobs(&copy, &capacity, true) {
				fitsEmpty = fitsEmpty || fits(c, &capacity, capacity.TotalMemoryGB) && hostFits(&capacity, c.Spec.Needs.MemoryGB, c.Spec.Needs.HostMemoryGB)
			}
			if !fitsEmpty {
				continue
			}
			for _, other := range targets {
				if hostID(other) == hostID(scarce) || e.variant(machineTarget(other)) != nil || q.variantFreshness(other) != "fresh" || !q.jobAllowsMachine(j, other.ID) || !q.preemptionPlacementAllows(j, other) {
					continue
				}
				ov := values[hostID(other)]
				if ov == nil || !ov.known {
					continue
				}
				comparison, known := compareHostValue(values, other, scarce)
				if !known || comparison >= 0 || other.Recovering || other.Leaving || availabilityBlocked(other) || q.maintenanceBlocked(hostID(other)) || !q.workerBatchIdle(other) || q.preparationActive(other) || q.preparationStopping(other) {
					continue
				}
				// Existing resource floors are a screening bound, never inferred
				// resource/qualification evidence for the unimplemented backend.
				hostFloor := max(v.Needs.HostMemoryGB, j.Spec.SizeHint.HostMemoryGB)
				if v.Needs.cpuOnly() {
					hostFloor = max(hostFloor, v.Needs.MemoryGB, j.Spec.SizeHint.MemoryGB)
				}
				if j.OOMRetries > 0 {
					hostFloor = max(hostFloor, j.Spec.Needs.HostMemoryGB)
					if j.Spec.Needs.cpuOnly() {
						hostFloor = max(hostFloor, j.Spec.Needs.MemoryGB)
					}
				}
				if other.FreeCPUCores < v.Needs.cores() || !hostFits(other, 0, hostFloor) {
					continue
				}
				target := machineTarget(other)
				device := target.Platform
				if name := backendFor(device).variantDevice; name != "" {
					device = name
				}
				advice = append(advice, fmt.Sprintf("Missing %s variant limits placement while a qualified variant waits on a scarcer host. Investigate an explicit implementation for %s with its own resources, frozen environment/lock and native check; select %s through MYCE_DEVICE/assigned visibility. Run myce portability for per-target preparation steps. Verify actual RAM/CPU/device needs and native qualification, then obtain owner-accepted MY-19 reference-output parity before production. Missing coverage is not a hard software requirement or permission to move a running attempt.", targetLabel(portabilityTarget(target)), target.OS+"/"+target.Arch, device))
			}
		}
	}
	slices.Sort(advice)
	return slices.Compact(advice)
}
