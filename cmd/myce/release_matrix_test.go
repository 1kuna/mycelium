package main

import (
	"strings"
	"testing"
)

func TestReleaseBuildTargetsList(t *testing.T) {
	output := captureJoiningOutput(t, func() error { return releaseCommand([]string{"build-targets"}) })
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		t.Fatal("empty release build matrix")
	}
	seen := map[string]bool{}
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "0" && fields[2] != "1" {
			t.Fatal("invalid build target row")
		}
		key := fields[0] + "/" + fields[1]
		if seen[key] {
			t.Fatal("duplicate build target")
		}
		seen[key] = true
	}
	// Required adapters remain mandatory; the test has no fixed target count.
	for _, key := range []string{"linux/amd64", "linux/arm64", "darwin/arm64"} {
		if !seen[key] {
			t.Fatal("missing required adapter", key)
		}
	}
	if !strings.Contains(output, "darwin arm64 1") {
		t.Fatal("Mac adapter lost native sensing build")
	}
}
