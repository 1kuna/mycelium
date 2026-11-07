package myce

import (
	"path"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// EnvironmentLockCoverage describes only frozen dependency metadata. It grants
// neither native build support nor application portability or production parity.
type EnvironmentLockCoverage struct {
	State             string            `json:"state"`
	Environment       string            `json:"environment,omitempty"`
	Targets           []ExecutionTarget `json:"targets"`
	UnmappedPlatforms bool              `json:"unmapped_platforms,omitempty"`
}

func environmentInput(r EnvironmentRecipe, files map[string][]byte, name string) []byte {
	if b := r.Files[name]; len(b) > 0 {
		return b
	}
	if b := r.Files["extension/"+path.Join(path.Dir(r.LockFile), name)]; len(b) > 0 {
		return b
	}
	return files[path.Join(path.Dir(r.LockFile), name)]
}

func environmentLockCoverage(r EnvironmentRecipe, files map[string][]byte, platform string) EnvironmentLockCoverage {
	out := EnvironmentLockCoverage{State: "unknown: native frozen install required", Targets: []ExecutionTarget{}}
	if r.Backend != "pixi" {
		if r.Binary != "" || len(r.Build) > 0 {
			out.State = "unknown: native build toolchain required"
		}
		return out
	}
	name := r.Environment
	if name == "" {
		name = "default"
	}
	out.Environment = safePortabilityID(name)
	data := environmentInput(r, files, "pixi.lock")
	if len(data) == 0 || len(environmentInput(r, files, "pixi.toml")) == 0 {
		out.State = "unknown: missing inputs"
		return out
	}
	var lock struct {
		Environments map[string]struct {
			Packages map[string][]any `yaml:"packages"`
		} `yaml:"environments"`
	}
	if yaml.Unmarshal(data, &lock) != nil {
		out.State = "unknown: invalid lock"
		return out
	}
	env, ok := lock.Environments[name]
	if !ok || env.Packages == nil {
		out.State = "unknown: selected lock environment missing"
		return out
	}
	out.State = "declared: native frozen install required"
	// Package-manager vocabulary, not a list of this installation's machines.
	for key := range env.Packages {
		target, ok := map[string]ExecutionTarget{
			"linux-64":      {"linux", "amd64", platform},
			"linux-aarch64": {"linux", "arm64", platform},
			"osx-64":        {"darwin", "amd64", platform},
			"osx-arm64":     {"darwin", "arm64", platform},
			"win-64":        {"windows", "amd64", platform},
		}[key]
		if !ok {
			out.UnmappedPlatforms = true
			continue
		}
		out.Targets = append(out.Targets, target)
	}
	slices.SortFunc(out.Targets, func(a, b ExecutionTarget) int { return strings.Compare(targetLabel(a), targetLabel(b)) })
	return out
}
