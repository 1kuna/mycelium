package myce

import (
	"slices"
	"strings"
)

const (
	nativeFailureReason = "the recorded native check failed"
	nativeFailureSource = "matching native qualification report"
	nativeFailureAction = "inspect the recorded native check failure before changing the implementation"
	darwinToolsReason   = "the job requires Darwin developer tools; SDK availability requires native qualification"
	darwinToolsSource   = "authored Darwin targets and xcodebuild requirement"
	darwinToolsAction   = "qualify the required SDK on a Darwin target"
)

// The existing hold reason reaches resolve, job receipts and compact status,
// including their JSON views. Keep the source with the one next action there.
func holdSource(h *Hold, source string) *Hold {
	h.Reason = "source: " + source + "; " + h.Reason
	return h
}

func darwinDeveloperToolsRequired(j *Job) bool {
	if len(j.Spec.Targets) > 0 && slices.Contains(j.Spec.Needs.Tools, "xcodebuild") &&
		!slices.ContainsFunc(j.Spec.Targets, func(t ExecutionTarget) bool { return t.OS != "darwin" }) {
		return true
	}
	if len(j.Extensions) == 0 {
		return false
	}
	for _, e := range j.Extensions {
		if len(e.Variants) == 0 {
			return false
		}
		for _, v := range e.Variants {
			if v.Target.OS != "darwin" || !slices.Contains(v.Needs.Tools, "xcodebuild") && !slices.Contains(j.Spec.Needs.Tools, "xcodebuild") {
				return false
			}
		}
	}
	return true
}

func darwinDeveloperToolsHold() *Hold {
	h := withAction(newHold("execution_coverage", ""), ownerConsumer, darwinToolsAction)
	h.Reason = darwinToolsReason
	return holdSource(h, darwinToolsSource)
}

const commandLockTargets = "selected_member_lock_targets"

// Like myce portability, dependency coverage is the intersection of the
// selected immutable member locks. Derive it before submission moves lock
// bytes to the file store. Status consumes only the compact placement intent.
func frozenCommandLockCoverage(s Spec, main map[string]EnvironmentRecipe, sides map[string]map[string]EnvironmentRecipe) ([]ExecutionTarget, bool) {
	if s.extensionJob() || !s.Needs.cpuOnly() || len(s.Targets) == 0 || s.Environment.Key() == "" {
		return nil, false
	}
	member := func(recipes map[string]EnvironmentRecipe) ([]ExecutionTarget, bool) {
		r, ok := recipes["cpu"]
		if !ok {
			return nil, len(recipes) != 0
		}
		c := environmentLockCoverage(r, nil, "cpu")
		if !strings.HasPrefix(c.State, "declared:") {
			return nil, false
		}
		if r.Target != nil {
			c.Targets = slices.DeleteFunc(c.Targets, func(t ExecutionTarget) bool { return t != *r.Target })
		}
		return c.Targets, true
	}
	targets, known := member(main)
	if !known {
		return nil, false
	}
	for _, recipes := range sides {
		memberTargets, known := member(recipes)
		if !known {
			return nil, false
		}
		targets = slices.DeleteFunc(targets, func(t ExecutionTarget) bool { return !slices.Contains(memberTargets, t) })
	}
	return targets, true
}

// An authored exclusion can be widened only after checking the payload.
// Even complete locks prove only a dependency candidate.
func commandCompatibilityHold(j *Job, h *Hold, alternatives []ExecutionTarget, coveredCurrentTarget bool) *Hold {
	alternatives = placementTargets(alternatives)
	target := alternatives[0]
	known := j.PlacementIntent != nil && j.PlacementIntent.Environment.State == commandLockTargets
	covered := false
	if known {
		for _, other := range alternatives {
			if slices.Contains(j.PlacementIntent.Environment.Targets, other) {
				target, covered = other, true
				break
			}
		}
	}
	if !coveredCurrentTarget {
		h.retag("execution_coverage", ownerConsumer, "")
		h.Reason = "the authored targets exclude an observed CPU alternative"
	}
	h.Owner = ownerConsumer
	label := targetLabel(target)
	switch {
	case covered:
		h.Reason += "; " + label + " is a dependency candidate"
		h.Next = "verify the complete workload on " + label + " before widening targets"
		return holdSource(h, "fresh worker inventory and selected immutable member locks")
	case !known:
		h.Reason += "; environment coverage for " + label + " is unknown"
		h.Next = "verify target-native environment coverage for " + label
		return holdSource(h, "fresh worker inventory and available member environment metadata")
	default:
		h.Reason += "; a member recipe or lock excludes " + label
		h.Next = "cover " + label + " in the required member environments"
		return holdSource(h, "fresh worker inventory and selected member recipes or locks")
	}
}
