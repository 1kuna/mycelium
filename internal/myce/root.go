package myce

import "os"

func GuardRootStartup() error {
	return refuseRoot(os.Geteuid())
}

func refuseRoot(euid int) error {
	if euid == 0 {
		return ErrHostIdentity
	}
	return nil
}
