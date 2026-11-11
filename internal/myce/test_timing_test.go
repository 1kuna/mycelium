package myce

import (
	"testing"
	"time"
)

// returnsWhileBlocked runs call while its blocker stays held for the rest of
// the test, so returning at all proves the product's bound. testWaitTimeout
// only turns a regression into a prompt failure; it is not a latency budget,
// so shared-host load cannot fail it. call must not use t.Fatal.
func returnsWhileBlocked(t testing.TB, what string, call func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		call()
	}()
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatalf("%s still blocked after %s", what, testWaitTimeout)
	}
}
