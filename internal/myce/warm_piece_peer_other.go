//go:build !darwin && !linux

package myce

import "errors"

func warmPeerPID(int) (int, error) {
	return 0, errors.New("Unix peer PID unavailable")
}
