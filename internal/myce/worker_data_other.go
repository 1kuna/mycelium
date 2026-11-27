//go:build !linux

package myce

import (
	"errors"
	"runtime"
)

func ephemeralSupport(_ ...string) error {
	_, err := (Worker{}).ephemeralWorkerSupport()
	return err
}

func (Worker) ephemeralWorkerSupport() (ephemeralRefusal, error) {
	if runtime.GOOS == "darwin" {
		// Seatbelt does not enclose process sessions. The worker's birth-fenced
		// snapshots might never observe a descendant that leaves the session
		// and is then orphaned. No host setting qualifies that boundary.
		return ephemeralDarwinCustodyUnqualified, errors.New(ephemeralDarwinCustodyReason)
	}
	return ephemeralLinuxRequired, errors.New("ephemeral worker data is currently supported only on Linux with qualified PID/mount namespaces and core routing")
}
