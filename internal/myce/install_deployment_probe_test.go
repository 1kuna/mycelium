package myce

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

// The unmounted install path probes each NAS namespace the same way.
func TestInstallNASProbeReusesFixedEntries(t *testing.T) {
	c := ServiceConfig{NAS: t.TempDir(), Role: "queue"}
	for i := 0; i < 3; i++ {
		if err := prepareInstallNASMode(context.Background(), c, nil, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"checkpoints", "models", "environments"} {
		got := probeEntries(t, filepath.Join(c.NAS, "mycelium", name))
		base := filepath.Base(storageProbePath("", ".myce-install-probe-", "install"))
		if want := []string{base, base + ".rename"}; !slices.Equal(got, want) {
			t.Fatalf("%s entries after repeated install probes = %v, want %v", name, got, want)
		}
	}
}
