package myce

import (
	"os"
	"runtime"
	"testing"
)

// Run over SSH in the native tier, never from the fleet attempt's kill tree.
// The two binaries come from the fleet test build; no installed service changes.
func TestWorkerUnitMigrationNative(t *testing.T) {
	if runtime.GOOS != "linux" || os.Getenv("MYCE_NATIVE_CUSTODIAN_TEST") != "1" {
		t.Skip("opt-in Linux systemd migration tier")
	}
	next := os.Getenv("MYCE_NATIVE_REPLACEMENT_BINARY")
	if next == "" {
		t.Fatal("native tier requires its fleet-built replacement binary")
	}
	for _, mode := range []string{"migration", "migration-model"} {
		t.Run(mode, func(t *testing.T) { custodianNativeReplacement(t, next, mode) })
	}
}
