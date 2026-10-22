package myce

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func TestLocalNetworkFailureClassification(t *testing.T) {
	for _, cause := range []error{syscall.EHOSTUNREACH, syscall.ENETUNREACH} {
		if !localNetworkUnavailable(fmt.Errorf("private route: %w", cause)) {
			t.Fatal("wrapped route denial not classified")
		}
	}
	for _, err := range []error{nil, errJoiningIdentity, &HTTPError{Status: 401}, errors.New("connection failed")} {
		if localNetworkUnavailable(err) {
			t.Fatal("non-route error entered consent retry", err)
		}
	}
}
