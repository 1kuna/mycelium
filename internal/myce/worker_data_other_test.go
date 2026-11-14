//go:build !linux

package myce

import "testing"

// The laptop exercises real process/file custody with a capability test seam;
// it does not claim Linux confinement or core-route qualification.
func requireEphemeralTestBoundary(t *testing.T) { t.Helper() }

func TestEphemeralUnsupportedHostRefusesCapability(t *testing.T) {
	if err := ephemeralSupport(); err == nil {
		t.Fatal("unsupported host advertised ephemeral data custody")
	}
}
