//go:build !darwin && !linux

package myce

import (
	"errors"
	"os/exec"
)

// Private paths and an environment allowlist cannot replace kernel isolation.
func (w Worker) isolateNativeCommand(cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network bool) error {
	return errors.New("native_isolation_unavailable: this platform has no qualified confinement mechanism")
}

func (w Worker) nativeIsolationCapabilities() []string {
	return []string{"native_isolation_unavailable"}
}
