//go:build !darwin || !cgo

package myce

import "errors"

func verifyDarwinNativeListener(string, map[int]bool) error {
	return errors.New("native listener ownership requires Darwin cgo")
}
