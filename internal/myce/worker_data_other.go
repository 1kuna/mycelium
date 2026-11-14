//go:build !linux

package myce

import "errors"

func ephemeralSupport() error {
	return errors.New("ephemeral worker data is currently supported only on Linux with qualified PID/mount namespaces and non-piped core routing")
}
