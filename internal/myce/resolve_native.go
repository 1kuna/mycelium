package myce

import (
	"context"
	"strings"
)

// Reuse the bounded metadata-only portability reader. An authored recipe has
// no frozen registration, so never infer launch eligibility from local bytes.
func (q *Queue) resolveNative(req ResolveRequest, ctx context.Context) (ResolveReport, error) {
	p, err := q.Portability(PortabilityRequest{SourceUnverified: req.SourceUnverified, Recipe: req.Recipe, Project: req.Spec.Project, RequirementReasons: req.RequirementReasons}, ctx)
	if err != nil {
		return ResolveReport{}, err
	}
	r := ResolveReport{ObservedAt: p.ObservedAt, Resolved: []string{"native adapter: authored metadata; immutable registration and qualification remain unverified"}, Targets: []ResolveTarget{}, Findings: []ResolveFinding{}, Notes: []string{"Read-only: no registration, upload, build, qualification or payload execution. Register and qualify the adapter through myce run before expecting eligible frozen evidence."}, Portability: &p}
	for _, target := range p.Targets {
		for _, env := range target.Environments {
			if len(env.MissingInputs) > 0 {
				r.Findings = append(r.Findings, ResolveFinding{Field: "environment." + env.Name, Side: "submitter", Problem: "native recipe inputs missing: " + strings.Join(env.MissingInputs, ", "), Fix: "Include the locked recipe and its build inputs in the selected source directory."})
			}
		}
		for _, host := range target.Hosts {
			r.Targets = append(r.Targets, ResolveTarget{Machine: host.Host, Target: target.Target, Freshness: host.Freshness, Eligible: false, Reason: "unknown: frozen native registration and qualification required; " + host.Admission})
		}
	}
	r.Hold = ResolveFindingHold(r.Findings)
	if r.Hold == nil {
		r.Hold = withAction(newHold("unprepared", "authored metadata is not qualified execution evidence"), ownerConsumer, "register and qualify the selected adapter through myce run; verify output parity before production use")
	}
	return r, nil
}
