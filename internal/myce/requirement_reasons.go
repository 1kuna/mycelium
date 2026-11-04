package myce

import (
	"fmt"
	"slices"
	"strings"
)

// Raw prose and arbitrary map keys remain in frozen intent. Observation only
// exposes finite constraint names, presence and a location in that intent.
type RequirementReasonStatus struct {
	Constraint   string `json:"constraint"`
	ReasonSource string `json:"reason_source"` // authored or missing; never proof
	Source       string `json:"source"`
}

func (r RequirementReasonStatus) Summary() string {
	return fmt.Sprintf("%s: reason %s; consult %s (raw text omitted)", r.Constraint, r.ReasonSource, r.Source)
}

func requirementConstraint(key string) string {
	switch key {
	case "host", "device", "backend:cpu", "backend:cuda", "backend:xpu", "backend:metal", "backend:fake":
		return key
	default:
		return "other"
	}
}

func requirementReasonStatus(j *Job) []RequirementReasonStatus {
	var rows []RequirementReasonStatus
	add := func(source string, reasons map[string]string, required []string) {
		states := map[string]bool{}
		for _, key := range required {
			states[requirementConstraint(key)] = false
		}
		for key, text := range reasons {
			key = requirementConstraint(key)
			states[key] = states[key] || strings.TrimSpace(text) != ""
		}
		for key, authored := range states {
			state := "missing"
			if authored {
				state = "authored"
			}
			location := source + "[" + key + "]"
			if key == "other" {
				location = source + "[redacted key]"
			}
			rows = append(rows, RequirementReasonStatus{key, state, location})
		}
	}
	var required []string
	if len(j.Spec.AllowedMachines) > 0 || len(j.Spec.ExcludedMachines) > 0 {
		required = append(required, "host")
	}
	if j.PlacementIntent != nil {
		if j.PlacementIntent.DeviceConstraint == "selector" {
			required = append(required, "device")
		}
	} else if j.Spec.Needs.GPU != nil && len(j.Spec.Needs.GPU.Devices) > 0 {
		required = append(required, "device")
	}
	if len(j.Extensions) == 0 {
		add("spec.requirement_reasons", j.Spec.RequirementReasons, required)
	} else {
		add("spec.requirement_reasons", j.Spec.RequirementReasons, nil)
		for _, e := range j.Extensions {
			constraints := slices.Clone(required)
			for _, v := range e.Variants {
				if v.Needs.GPU != nil && len(v.Needs.GPU.Devices) > 0 {
					constraints = append(constraints, "device")
				}
			}
			// Frozen bundle digests identify the source without publishing an
			// authored path, arbitrary key or extension recipe content.
			add("extensions[digest="+e.Digest+"].requirement_reasons", e.RequirementReasons, constraints)
		}
	}
	slices.SortFunc(rows, func(a, b RequirementReasonStatus) int {
		return strings.Compare(a.Source, b.Source)
	})
	return rows
}
