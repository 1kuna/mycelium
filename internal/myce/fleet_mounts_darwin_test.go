package myce

import (
	"context"
	"testing"
)

// #260: the fleet helper must dispatch the installer's internal mount probe
// in process. A laptop with no NAS still has a valid cached mount inventory.
func TestFleetCommandDarwinMountInventory(t *testing.T) {
	if _, err := MountedNASRoots(context.Background(), "darwin", fleetCommand); err != nil {
		t.Fatalf("fleet storage preflight could not read cached Darwin mounts: %v", err)
	}
}
