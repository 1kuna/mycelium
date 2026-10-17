//go:build !linux

package myce

// Linux POSIX ACLs use a platform-specific xattr. Native root control is Linux-only.
func hostAccessACL(string) error  { return nil }
func hostFileAccessACL(int) error { return nil }
