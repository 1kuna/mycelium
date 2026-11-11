package main

import (
	"errors"
	"strings"
	"testing"
)

// Redaction rebuilds the error text; it must keep the command's own exit code
// from being reported as a Mycelium failure, and never repeat the token.
func TestRunWaitContractKeepsOwnExitThroughRedaction(t *testing.T) {
	own := redactRunError(ownExit(7, "job x failed: secret-token"), "secret-token")
	if got := runWaitContract(own); exitCode(got) != 7 || strings.Contains(got.Error(), "secret-token") {
		t.Fatalf("own exit: %d %v", exitCode(got), got)
	}
	infra := redactRunError(waitError(69, "queue unavailable: secret-token"), "secret-token")
	if got := runWaitContract(infra); exitCode(got) != runInfraExit || strings.Contains(got.Error(), "secret-token") {
		t.Fatalf("infrastructure exit: %d %v", exitCode(got), got)
	}
	if exitCode(runWaitContract(errors.New("usage"))) != runInfraExit || runWaitContract(nil) != nil {
		t.Fatal("contract mapping")
	}
}
