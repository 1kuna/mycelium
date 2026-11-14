//go:build !linux

package myce

import "errors"

func ephemeralSupport(_ ...string) error {
	return errors.New("ephemeral worker data is currently supported only on Linux with qualified PID/mount namespaces and core routing")
}

func (w Worker) ephemeralWorkerSupport() error { return ephemeralSupport() }
