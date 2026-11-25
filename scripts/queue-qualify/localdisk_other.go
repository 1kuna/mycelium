//go:build !darwin && !linux

package main

import "errors"

func isLocalDisk(string) (bool, error) {
	return false, errors.New("cannot tell whether the directory is on a local disk on this platform")
}
