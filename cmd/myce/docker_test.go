package main

import (
	"strings"
	"testing"
)

func TestResolveDockerCLIRejectsAssertionAndRequiresCustody(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	if err := run([]string{"resolve-docker", "--confirm-gone"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatal("assertion escape remains", err)
	}
	for _, args := range [][]string{{"resolve-docker"}, {"resolve-docker", "--state-dir", t.TempDir(), "--attempt-id", "owned"}} {
		if err := run(args); err == nil {
			t.Fatal("missing custody accepted")
		}
	}
	for _, mode := range []string{"queue", "join"} {
		if err := run([]string{mode, "--simulated"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatal("production exposes simulation flag", mode, err)
		}
	}
}
