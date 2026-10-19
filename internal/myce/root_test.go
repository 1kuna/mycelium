package myce

import (
	"errors"
	"testing"
)

func TestRootStartupRefusal(t *testing.T) {
	if !errors.Is(refuseRoot(0), ErrHostIdentity) {
		t.Fatal("root was not refused")
	}
	if err := refuseRoot(501); err != nil {
		t.Fatal("ordinary account refused", err)
	}
}
