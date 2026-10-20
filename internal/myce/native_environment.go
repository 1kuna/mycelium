package myce

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func (q *Queue) resolveSidecarEnvironments(s Spec) (map[string]map[string]EnvironmentRecipe, error) {
	out := map[string]map[string]EnvironmentRecipe{}
	for _, side := range s.Run.Sidecars {
		if side.Environment != "" {
			recipes, err := q.resolveEnvironment(Environment{Name: side.Environment})
			if err != nil {
				return nil, err
			}
			// Unlike legacy advertised environments, helpers need a cache root to bind.
			if len(recipes) == 0 {
				return nil, rejectFieldRule("job_environment_invalid", fmt.Errorf("sidecar %s requires a registered environment recipe", side.Name))
			}
			out[side.Name] = recipes
		}
	}
	return out, nil
}

func (g *nativeGroup) sidecarEnvironment(j Job, side NativeSidecar, inherited string) (string, func(), error) {
	if side.Environment == "" {
		return inherited, nil, nil
	}
	recipes := j.SidecarEnvironmentRecipes[side.Name]
	recipe, ok := recipes[g.worker.Machine.GPU]
	if j.Extension != nil {
		for _, e := range j.Extensions {
			if e.Digest == j.Extension.Digest {
				if v := e.variant(machineTarget(&g.worker.Machine)); v != nil {
					recipe, ok = v.Environments[side.Environment]
				}
			}
		}
	}
	if !ok || g.worker.environmentCache == nil {
		return "", nil, errors.New("sidecar environment unavailable for this target")
	}
	if recipe.BinaryRelative != "" && g.worker.EnvironmentSourceRoot != "" {
		recipe.Binary = filepath.Join(g.worker.EnvironmentSourceRoot, recipe.BinaryRelative)
	}
	recipe.readyOnly = j.Extension != nil
	return g.worker.environmentCache.Acquire(withLaunchGate(g.ctx, func() error {
		return g.worker.authorizeLaunch(HostLaunch{Machine: g.worker.Machine.ID, Session: g.session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)})
	}), recipe)
}

func expandNativeExtension(s *Spec, env map[string]string) {
	r := strings.NewReplacer("{extension}", env["MYCE_EXTENSION_DIR"], "{cache}", env["MYCE_EXTENSION_CACHE"])
	for i := range s.Run.Sidecars {
		side := &s.Run.Sidecars[i]
		side.Command = expandArgs(side.Command, r)
		for k, v := range side.Env {
			side.Env[k] = r.Replace(v)
		}
	}
}
