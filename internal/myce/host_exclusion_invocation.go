package myce

import (
	"context"
	"os/exec"
	"time"
)

// InspectExclusion performs fresh bounded reads. It never mutates, initializes,
// adopts, signals, reconciles queues or opens worker files in the root process.
func (v HostInvocation) InspectExclusion(ctx context.Context, launch HostChildLauncher, handle HostMutationHandler, k HostKernelReaders) (HostExclusionReport, error) {
	r, err := v.Mutate(ctx, HostMutation{Operation: "inspect-exclusion"}, launch, handle)
	if err != nil || r.Input == nil {
		if err == nil {
			err = ErrHostChild
		}
		return HostExclusionReport{}, err
	}
	command := func(ctx context.Context, endpoint string, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "/usr/bin/docker", append([]string{"--host", endpoint}, args...)...)
		// The privileged read-only CLI must not load worker-owned Docker config.
		cmd.Env = []string{"HOME=/", "DOCKER_CONFIG=/etc/mycelium/docker-observer", "PATH=/usr/bin:/bin", "LANG=C.UTF-8"}
		cmd.Dir = "/"
		cmd.WaitDelay = 100 * time.Millisecond
		b, err := cmd.Output()
		if err != nil {
			return b, ErrHostChild
		}
		return b, nil
	}
	return ObserveHostExclusion(ctx, v.ObservationContext(), *r.Input, RealClock{}, k, command), nil
}
