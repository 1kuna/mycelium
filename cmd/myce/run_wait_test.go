package main

import (
	"strings"
	"testing"
)

// Redaction rebuilds the error text; it must keep the exit code a caller
// branches on (the command's own, typed refusal or runInfraExit) and never
// repeat the token.
func TestRunRedactionKeepsExitCode(t *testing.T) {
	for _, code := range []int{7, 78, runInfraExit} {
		got := redactRunError(waitError(code, "job x: secret-token"), "secret-token")
		if exitCode(got) != code || strings.Contains(got.Error(), "secret-token") {
			t.Fatalf("code %d: %d %v", code, exitCode(got), got)
		}
	}
}
