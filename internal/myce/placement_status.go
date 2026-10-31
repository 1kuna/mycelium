package myce

import (
	"slices"
	"sort"
	"time"
)

// PlacementIntent freezes safe submission facts before resolution/assignment
// changes the executable spec. It never copies recipes or authored reason prose.
type PlacementIntent struct {
	Origin              string            `json:"origin"`
	Scope               string            `json:"scope"`
	HostConstraint      string            `json:"host_constraint"`
	DeviceConstraint    string            `json:"device_constraint"`
	Targets             []ExecutionTarget `json:"targets,omitempty"`
	RecipeDeviceTargets []ExecutionTarget `json:"recipe_device_targets,omitempty"`
	RequiredBackends    []string          `json:"required_backends,omitempty"`
	PreferredBackends   []string          `json:"preferred_backends,omitempty"`
	Environment         PlacementCoverage `json:"environment"`
	Qualification       string            `json:"qualification"`
	Parity              string            `json:"parity"`
}

type PlacementCoverage struct {
	Origin    string            `json:"origin"`
	State     string            `json:"state"`
	Platforms []string          `json:"platforms,omitempty"`
	Targets   []ExecutionTarget `json:"targets,omitempty"`
}

// PlacementDecision is historical assignment evidence, not current eligibility.
// A constrained choice still records which authority supplied the restriction.
type PlacementDecision struct {
	Origin       string    `json:"origin"`
	Attempt      int       `json:"attempt"`
	AssignedAt   time.Time `json:"assigned_at"`
	Host         string    `json:"host"`
	Devices      []string  `json:"devices,omitempty"`
	HostChoice   string    `json:"host_choice"`
	DeviceChoice string    `json:"device_choice"`
	Reason       string    `json:"reason"`
	LimitedBy    []string  `json:"limited_by"`
}

type PlacementBinding struct {
	Origin string `json:"origin"`
	Host   string `json:"host"`
	Reason string `json:"reason"`
}

func placementStrings(values []string) []string {
	values = slices.Clone(values)
	sort.Strings(values)
	return slices.Compact(values)
}

func placementTargets(values []ExecutionTarget) []ExecutionTarget {
	values = slices.Clone(values)
	sort.Slice(values, func(i, j int) bool {
		a, b := values[i], values[j]
		if a.OS != b.OS {
			return a.OS < b.OS
		}
		if a.Arch != b.Arch {
			return a.Arch < b.Arch
		}
		return a.Platform < b.Platform
	})
	return slices.Compact(values)
}

func submissionPlacementIntent(s Spec, j *Job) *PlacementIntent {
	p := &PlacementIntent{Origin: "authored", Scope: "work_resources", HostConstraint: "none", DeviceConstraint: "none", Targets: placementTargets(s.Targets), RequiredBackends: placementStrings(s.Needs.Platform.Required), PreferredBackends: placementStrings(s.Needs.Platform.Preferred), Qualification: "not_requested", Parity: "unknown"}
	if len(p.Targets) > 0 || len(p.RequiredBackends) > 0 {
		p.Scope = "platform_constraints"
	}
	if len(s.AllowedMachines) > 0 {
		p.HostConstraint = "allowlist"
		if len(placementStrings(s.AllowedMachines)) == 1 {
			p.HostConstraint = "pin"
		}
		p.Scope = "host_device_restriction"
	}
	if s.Needs.GPU != nil && len(s.Needs.GPU.Devices) > 0 {
		p.DeviceConstraint = "selector"
		p.Scope = "host_device_restriction"
	}
	if s.extensionJob() || s.RequireTarget {
		p.Qualification = "required"
	}
	p.Environment = PlacementCoverage{Origin: "submission", State: "not_required"}
	sets := []map[string]EnvironmentRecipe{}
	if s.Environment.Key() != "" {
		sets = append(sets, j.EnvironmentRecipes)
	}
	for _, recipes := range j.SidecarEnvironmentRecipes {
		sets = append(sets, recipes)
	}
	if len(sets) > 0 {
		p.Environment = PlacementCoverage{Origin: "catalog_snapshot", State: "platforms_only"}
		if s.Environment.Name == "" {
			p.Environment.Origin = "submission_snapshot"
		}
		platforms := map[string]bool{}
		for name := range sets[0] {
			platforms[name] = true
		}
		for _, recipes := range sets {
			if len(recipes) == 0 {
				p.Environment.State = "unknown"
			}
			for name := range platforms {
				if _, ok := recipes[name]; !ok {
					delete(platforms, name)
				}
			}
		}
		for name := range platforms {
			p.Environment.Platforms = append(p.Environment.Platforms, name)
		}
		p.Environment.Platforms = placementStrings(p.Environment.Platforms)
	}
	if s.extensionJob() {
		p.Environment = PlacementCoverage{Origin: "extension_snapshot", State: "unknown"}
		for _, e := range j.Extensions {
			for _, v := range e.Variants {
				p.Environment.Targets = append(p.Environment.Targets, v.Target)
				if v.Needs.GPU != nil && len(v.Needs.GPU.Devices) > 0 {
					p.RecipeDeviceTargets = append(p.RecipeDeviceTargets, v.Target)
				}
			}
		}
		p.Environment.Targets = placementTargets(p.Environment.Targets)
		p.RecipeDeviceTargets = placementTargets(p.RecipeDeviceTargets)
		if len(p.Environment.Targets) > 0 {
			p.Environment.State = "targets"
		}
	}
	return p
}

func (q *Queue) recordPlacementDecision(j *Job, m *Machine) {
	intent := j.PlacementIntent
	hostChoice, deviceChoice := "unknown", "unknown"
	limited := []string{}
	if intent != nil {
		hostChoice, deviceChoice = "scheduler", "scheduler"
		if intent.HostConstraint != "none" {
			hostChoice = "scheduler_with_authored_constraint"
			if intent.HostConstraint == "pin" {
				hostChoice = "authored_pin"
			}
			limited = append(limited, "authored_host")
		}
		if intent.DeviceConstraint != "none" {
			deviceChoice = "authored_selector"
			limited = append(limited, "authored_device")
		}
		if j.Extension != nil {
			for _, e := range j.Extensions {
				if e.Digest != j.Extension.Digest {
					continue
				}
				if v := e.variant(machineTarget(m)); v != nil && v.Needs.GPU != nil && len(v.Needs.GPU.Devices) > 0 {
					if deviceChoice == "scheduler" {
						deviceChoice = "authored_recipe_selector"
					}
					limited = append(limited, "authored_recipe_device")
				}
			}
		}
		if len(intent.Targets) > 0 || len(intent.RequiredBackends) > 0 {
			limited = append(limited, "authored_platform")
		}
		if intent.Environment.State != "not_required" {
			limited = append(limited, "environment_coverage")
		}
		if intent.Qualification == "required" {
			limited = append(limited, "qualification")
		}
	}
	if len(q.s.Policies[j.Spec.Project].AllowedMachines) > 0 {
		limited = append(limited, "project_policy")
	}
	if j.ProductionHost != "" {
		limited = append(limited, "scheduler_production_binding")
	}
	reason := "normal admission"
	if j.Timing != nil && j.Timing.PlacementReason != "" {
		reason = j.Timing.PlacementReason
	}
	devices := slices.Clone(j.AllocatedDevices)
	if len(devices) == 0 && m.GPU != "cpu" && m.Device != "" {
		devices = []string{m.Device}
	}
	j.Placement = &PlacementDecision{Origin: "scheduler", Attempt: j.Attempt, AssignedAt: q.clock.Now(), Host: hostID(m), Devices: devices, HostChoice: hostChoice, DeviceChoice: deviceChoice, Reason: reason, LimitedBy: limited}
}

func placementStatus(j *Job) {
	if j.PlacementIntent == nil {
		j.PlacementIntent = &PlacementIntent{Origin: "unknown", Scope: "unknown", HostConstraint: "unknown", DeviceConstraint: "unknown", Environment: PlacementCoverage{Origin: "unknown", State: "unknown"}, Qualification: "unknown", Parity: "unknown"}
	}
	j.ProductionBinding = nil
	if j.ProductionHost != "" {
		j.ProductionBinding = &PlacementBinding{Origin: "scheduler", Host: j.ProductionHost, Reason: "production_hold"}
	}
}
