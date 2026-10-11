package main

import (
	"strings"
	"testing"
)

func TestResolveDockerCLIRequiresExplicitConfirmation(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	for _, args := range [][]string{{"resolve-docker"}, {"resolve-docker", "--state-dir", t.TempDir(), "--attempt-id", "owned"}} {
		if err := run(args); err == nil || !strings.Contains(err.Error(), "--confirm-gone") {
			t.Fatalf("missing confirmation accepted or wrong error: %v", err)
		}
	}
}
