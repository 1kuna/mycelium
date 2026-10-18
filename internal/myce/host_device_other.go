//go:build !linux

package myce

func hostDeviceNumber(string) (string, error) { return "", ErrHostPrivilegeUnsupported }
