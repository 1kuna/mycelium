package myce

import (
	"errors"
	"syscall"
	"testing"
)

// Successful group delivery is evidence before session cleanup finishes. A
// disappeared or refused group is not evidence that a live payload was stopped.
func TestSignalWorkerGroupDeliveryEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"delivered", nil, true},
		{"gone", syscall.ESRCH, false},
		{"permission", syscall.EPERM, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := false
			err := signalWorkerGroupUsing(42, syscall.SIGTERM, func(int, syscall.Signal) error {
				return tc.err
			}, func(int) ([]int, error) {
				return []int{42}, nil
			}, func() { delivered = true })
			if delivered != tc.want || tc.err == syscall.EPERM && !errors.Is(err, syscall.EPERM) {
				t.Fatalf("delivery=%v error=%v", delivered, err)
			}
		})
	}
}
