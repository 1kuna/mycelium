//go:build !linux

package myce

import "os"

func GuardRootStartup() error {
	if os.Geteuid() == 0 {
		return ErrHostPrivilegeUnsupported
	}
	return nil
}
func hostChildExecutable(force bool) (string, error) {
	// A laptop can exercise the real pipe/entry/environment path as itself. Root
	// is unsupported here; production Linux always uses /proc/self/exe.
	if !force || os.Geteuid() == 0 {
		return "", ErrHostPrivilegeUnsupported
	}
	return os.Executable()
}
