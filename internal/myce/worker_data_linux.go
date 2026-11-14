package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Recheck at inventory and launch; never change the host's global routing.
func ephemeralCoreRoute(stateDirs ...string) error {
	pattern, err := os.ReadFile("/proc/sys/kernel/core_pattern")
	state := ""
	if len(stateDirs) > 0 {
		state = stateDirs[0]
	}
	return checkEphemeralCoreRoute(pattern, err, state, qualifyCoreRouter)
}

var ephemeralCoreProbe = sync.OnceValue(func() error {
	if nativeLinuxDetection().mode != "namespace" {
		return errors.New("ephemeral worker data requires native PID/mount namespaces; fallback confinement is unsupported")
	}
	if err := ephemeralCoreRoute(); err != nil {
		return err
	}
	args, err := nativePolicyArgs(nativeLinuxPolicy{Read: []string{"/usr", "/bin", "/lib", "/lib64"}, Ephemeral: true}, []string{"/bin/sh", "-c", `test "$(ulimit -c)" = 0 && test "$(ulimit -H -c)" = 0`})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = exec.CommandContext(ctx, args[0], args[1:]...).Run(); err != nil {
		return errors.New("ephemeral core-limit helper probe failed")
	}
	return nil
})

func ephemeralSupport(stateDirs ...string) error {
	if err := ephemeralCoreRoute(stateDirs...); err != nil {
		return err
	}
	return ephemeralCoreProbe()
}

func (w Worker) ephemeralWorkerSupport() error {
	pattern, err := os.ReadFile("/proc/sys/kernel/core_pattern")
	if err := checkEphemeralCoreRoute(pattern, err, w.StateDir, qualifyCoreRouter); err != nil {
		return err
	}
	if strings.TrimSpace(string(pattern)) == myceCorePattern && w.memoryCeilings == nil {
		return errors.New("ephemeral core routing requires delegated attempt cgroups")
	}
	return ephemeralCoreProbe()
}

func suppressEphemeralCores() error {
	// Root ownership and the handler protocol were checked by the worker at
	// launch, outside the rootless user namespace. Host root may be unmapped
	// here; repeating host-UID checks would reject the already-qualified route.
	pattern, err := os.ReadFile("/proc/sys/kernel/core_pattern")
	if err := ephemeralCorePattern(pattern, err); err != nil {
		return err
	}
	return unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{Cur: 0, Max: 0})
}
