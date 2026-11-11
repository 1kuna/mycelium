//go:build !darwin && !linux

package myce

import "os/exec"

// Native jobs cannot launch here at all (isolateNativeCommand refuses), so
// there is nothing further to withhold.
func (w Worker) withholdNativeCredentials(*exec.Cmd, string, string, map[string]string) error {
	return nil
}

func protectServiceCredentials() error { return nil }
