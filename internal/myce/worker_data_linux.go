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

// Piped Linux core handlers ignore RLIMIT_CORE. They are not qualified for
// this policy, and the worker never changes the host's global routing.
func ephemeralCoreRoute() error {
	pattern, err := os.ReadFile("/proc/sys/kernel/core_pattern")
	return ephemeralCorePattern(pattern, err)
}

func ephemeralCorePattern(pattern []byte, err error) error {
	if err != nil || strings.TrimSpace(string(pattern)) == "" || strings.HasPrefix(strings.TrimSpace(string(pattern)), "|") {
		return errors.New("ephemeral worker data requires a readable non-piped core_pattern; piped core handlers ignore RLIMIT_CORE")
	}
	return nil
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

func ephemeralSupport() error {
	if err := ephemeralCoreRoute(); err != nil {
		return err
	}
	return ephemeralCoreProbe()
}
func suppressEphemeralCores() error {
	if err := ephemeralCoreRoute(); err != nil {
		return err
	}
	return unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{Cur: 0, Max: 0})
}
