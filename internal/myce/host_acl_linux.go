package myce

import "golang.org/x/sys/unix"

// Any access ACL is rejected, even if its current mask happens to be safe.
func hostAccessACL(path string) error {
	_, err := unix.Lgetxattr(path, "system.posix_acl_access", nil)
	return hostACLResult(err)
}
func hostFileAccessACL(fd int) error {
	_, err := unix.Fgetxattr(fd, "system.posix_acl_access", nil)
	return hostACLResult(err)
}
func hostACLResult(err error) error {
	if err == unix.ENODATA || err == unix.ENOTSUP {
		return nil
	}
	return ErrHostTarget
}
