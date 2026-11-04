package main

import (
	"os/exec"
	"testing"
)

func TestReleaseBuildProfile(t *testing.T) {
	profile, _, _, err := readProfile("../../.mycelium/profiles/release-build.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if profile.Source.Git != "HEAD" {
		t.Fatal("release source must be committed HEAD")
	}
	// Fake build/signing tools only: no real keychain, key or fleet submission.
	cmd := exec.Command("python3", "../../scripts/release-build-test.py")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("release profile regressions: %v\n%s", err, out)
	}
}
